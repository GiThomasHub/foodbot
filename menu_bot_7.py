import os
import re
import json
import random
import pandas as pd
import gspread
import warnings
import requests
import logging
import httpx
import math
import asyncio
from html import escape, unescape
from datetime import datetime
from pathlib import Path
from collections import Counter
from fpdf import FPDF
from fpdf.enums import XPos, YPos
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI
from typing import Set
from decimal import Decimal, ROUND_HALF_UP
from telegram.error import BadRequest
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
import telegram
from telegram.constants import ChatAction, ParseMode
from telegram.helpers import escape_markdown
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ConversationHandler,
    ContextTypes,
    CallbackQueryHandler,
)
from telegram.warnings import PTBUserWarning

warnings.filterwarnings("ignore", category=PTBUserWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s – %(message)s",
)


logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore.http11").setLevel(logging.WARNING)
logging.getLogger("fpdf").setLevel(logging.ERROR)
logging.getLogger("fpdf.output").setLevel(logging.ERROR)
logging.getLogger("fontTools").setLevel(logging.ERROR)
logging.getLogger("fontTools.subset").setLevel(logging.ERROR)




MENU_INPUT, ASK_BEILAGEN, SELECT_MENUES, BEILAGEN_SELECT, ASK_FINAL_LIST, ASK_SHOW_LIST, FERTIG_PERSONEN, REZEPT_INDEX, REZEPT_PERSONEN, TAUSCHE_SELECT, TAUSCHE_CONFIRM, ASK_CONFIRM, EXPORT_OPTIONS, FAV_OVERVIEW, FAV_DELETE_SELECT, PDF_EXPORT_CHOICE, FAV_ADD_SELECT, RESTART_CONFIRM, PROFILE_CHOICE, PROFILE_NEW_A, PROFILE_NEW_B, PROFILE_NEW_C, PROFILE_OVERVIEW, QUICKONE_START, QUICKONE_CONFIRM, PERSONS_SELECTION, PERSONS_MANUAL, MENU_COUNT, MENU_AUFWAND = range(29)





# === ENV & Sheets Setup ===
load_dotenv()
TOKEN = os.getenv("TELEGRAM_API_KEY")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # darf None sein
SHEET_ID = os.getenv("SHEET_ID", "1XzhGPWz7EFJAyZzaJQhoLyl-cTFNEa0yKvst0D0yVUs")
SHEET_GERICHTE = os.getenv("SHEET_GERICHTE", "Gerichte")
SHEET_ZUTATEN = os.getenv("SHEET_ZUTATEN", "Zutaten")


# Instantiate OpenAI client (new SDK)
openai_client = OpenAI(api_key=OPENAI_KEY)

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    os.getenv("GOOGLE_CRED_JSON", "credentials.json"), scope
)
client = gspread.authorize(creds)

# === Persistence Files ===
SESSIONS_FILE = "sessions.json"
FAVORITES_FILE = "favorites.json"
CACHE_FILE = "recipe_cache.json"
PROFILES_FILE = "profiles.json"
FAV_FILE = "favorites.json"
HISTORY_FILE = "history.json"


# === Profil-Optionen ===
RESTRICTION_CHOICES = {
    "res_vegi": "Vegi",   # akzeptiert Gerichte "Vegi" ODER "beides"
    "res_open": "offen",  # keine Einschränkung
}

STYLE_CHOICES = {
    "style_klassisch":   "Klassisch",
    "style_international": "International",
    "style_mediterran":  "Mediterran",
    "style_asiatisch":   "Asiatisch",
    "style_orient":      "Orientalisch",
}

ALL_STYLE_KEYS = set(STYLE_CHOICES.keys())

# Mapping für die Art-Spalte (leichter Fallback über ΔArt)
ART_ORDER = {"1": 1, "2": 2, "3": 3}

# Emoji-Zuordnung für Kategorien
CAT_EMOJI = {
    "Fleisch & Fisch":       "🥩🐟",
    "Obst & Gemüse":        "🍎🥕",
    "Getränke":      "🧃🍷",
    "Trockenware & Vorrat":"🍝🥫",
    "Milchwaren":    "🧀🥛",
    "Backwaren":     "🥖🥐",
    "Kühlregal": "🥶🧊",
    "Haushalt & Sonstiges": "🧽🧻",

}


WEIGHT_CHOICES = {f"weight_{i}": i for i in range(1, 8)}
WEIGHT_CHOICES["weight_any"] = None          #  «Egal» = keine Einschränkung





#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# === Utilites: Load/Save Helpers ===
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Nur für Admins: Aufwand-Verteilung anzeigen
def show_debug_for(update: Update) -> bool:
    ADMIN_IDS = {7650843881}  # in telegram @userinfobot ersichtlich
    return update.effective_user.id in ADMIN_IDS


def load_json(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def save_favorites():
    with open(FAV_FILE, "w", encoding="utf-8") as f:
        json.dump(favorites, f, indent=2, ensure_ascii=False)

def load_favorites() -> dict:
    """Favoriten aus Datei laden (oder leeres Dict, wenn Datei fehlt)"""
    try:
        with open(FAV_FILE, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        print("⚠️ Fehler beim Laden der Favoriten – ungültiges JSON.")
        return {}


def format_amount(q):
    """
    Gibt q zurück:
     - als Ganzzahl, wenn es ganzzahlig ist,
     - sonst bis zu 2 Dezimalstellen (z.B. 2.25, 2.2),
       gerundet nach ROUND_HALF_UP (0.255 → 0.26).
    """
    # Decimal für korrektes Half-Up-Runden verwenden
    qd  = Decimal(str(q))
    qd2 = qd.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    # Ganzzahl?
    if qd2 == qd2.to_integral_value():
        return str(int(qd2))
    # Sonst normalize, um überflüssige Nullen zu entfernen
    return format(qd2.normalize(), 'f')


def load_json(filename):
    try:
        with open(filename, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        print(f"⚠️ Fehler beim Laden von {filename} – Datei ist beschädigt.")
        return {}

def save_json(filename, data):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


profiles  = load_json(PROFILES_FILE)
favorites = load_json(FAV_FILE)
sessions  = load_json(SESSIONS_FILE)
history   = load_json(HISTORY_FILE)
recipe_cache = {}



def build_swap_keyboard(menus: list[str], selected: set[int]) -> InlineKeyboardMarkup:
    """Buttons 1…N mit Toggle-Häkchen + ‘Fertig’."""
    btns = []
    for idx, _g in enumerate(menus, 1):
        label = f"{'✅ ' if idx in selected else ''}{idx}"
        btns.append(InlineKeyboardButton(label, callback_data=f"swap_sel:{idx}"))
    rows = distribute_buttons_equally(btns, max_per_row=7)
    rows.append([InlineKeyboardButton("Fertig", callback_data="swap_done")])
    return InlineKeyboardMarkup(rows)



def format_dish_with_sides(dish: str, sides: list[str]) -> str:
    """
    Gibt den Gerichtenamen zurück, gefolgt von den Beilagen:
      - bei einer Beilage:      "Gericht mit Beilage"
      - bei mehreren Beilagen: "Gericht mit erste_beilage und zweite_beilage und dritte_beilage …"
    """
    if not sides:
        return dish
    text = f"{dish} mit {sides[0]}"
    for side in sides[1:]:
        text += f" und {side}"
    return text


async def mark_yes_no(q, yes_selected: bool, yes_cb: str, no_cb: str):
    """
    Zeigt in der *ursprünglichen* Nachricht einen grünen Haken
    neben 'Ja' oder 'Nein' und lässt die Callback-Daten unverändert.
    """
    yes_label = ("✅ " if yes_selected else "") + "Ja"
    no_label  = ("✅ " if not yes_selected else "") + "Nein"
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(yes_label, callback_data=yes_cb),
        InlineKeyboardButton(no_label,  callback_data=no_cb),
    ]])
    # alte Inline-Buttons ersetzen
    try:
        await q.edit_message_reply_markup(kb)
    except Exception:
        # falls Nachricht inzwischen weitergeleitet/gelöscht wurde – ignorieren
        pass

def load_profiles() -> dict:
    """Lädt alle gespeicherten Nutzerprofile aus der JSON-Datei."""
    global profiles
    profiles = load_json(PROFILES_FILE)
    return profiles

def save_profiles() -> None:
    """Speichert das globale profiles-Dict in die JSON-Datei."""
    save_json(PROFILES_FILE, profiles)


async def cleanup_prof_loop(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Löscht alle während des Profil-Wizards entstandenen Nachrichten."""
    bot = context.bot
    msg_ids: list[int] = context.user_data.get("prof_msgs", [])
    for mid in msg_ids:
        try:
            await bot.delete_message(chat_id, mid)
        except Exception:
            pass  # z. B. zu alt oder bereits gelöscht
    context.user_data["prof_msgs"] = []


def pad_message(text: str, min_width: int = 35) -> str:
    """
    Füllt **nur die erste Zeile** von `text` mit Non-Breaking Spaces (U+00A0)
    auf, bis sie mindestens min_width Zeichen lang ist.
    """
    parts = text.split("\n", 1)
    first = parts[0]
    rest  = parts[1] if len(parts) > 1 else ""
    if len(first) < min_width:
        first += "\u00A0" * (min_width - len(first))
    return first + ("\n" + rest if rest else "")



async def ask_for_persons(update: Update, context: ContextTypes.DEFAULT_TYPE, page: str = "low") -> int:
    """
    Paginiertes Auswahl-Keyboard für 1–6 / 7–12 Personen.
    Nur bei den Navigation-Callback-Daten wird die Inline-ReplyMarkup editiert,
    ansonsten immer eine neue Nachricht gesendet.
    """
    query = update.callback_query
    chat_id = update.effective_chat.id
    data = query.data if query else None

    # 1) Nur bei echten Page-Wechseln editen
    if query and data in ("persons_page_low", "persons_page_high"):
        # Seite umschalten
        page = "high" if data == "persons_page_high" else "low"

        # Buttons je nach Seite
        if page == "low":
            nums = list(range(1, 7))
            nav_label, nav_data = "Mehr ➡️", "persons_page_high"
        else:
            nums = list(range(7, 13))
            nav_label, nav_data = "⬅️ Weniger", "persons_page_low"

        # Grid-Layout: 6 Zahlen in einer Zeile + Navigations-Button darunter
        num_buttons = [
            [InlineKeyboardButton(str(n), callback_data=f"persons_{n}") for n in nums]
        ]
        num_buttons.append([InlineKeyboardButton(nav_label, callback_data=nav_data)])

        kb = InlineKeyboardMarkup(num_buttons)

        # Nur das Keyboard editieren
        await query.edit_message_reply_markup(reply_markup=kb)
        return PERSONS_SELECTION

    # 2) Andernfalls (Erstversand oder Aufruf von menu_confirm_cb etc.) neu senden
    # Immer mit der "low"-Seite starten
    nums = list(range(1, 7))
    nav_label, nav_data = "Mehr ➡️", "persons_page_high"
    num_buttons = [
        [InlineKeyboardButton(str(n), callback_data=f"persons_{n}") for n in nums]
    ]
    num_buttons.append([InlineKeyboardButton(nav_label, callback_data=nav_data)])
    
    kb = InlineKeyboardMarkup(num_buttons)

    msg = await update.effective_message.reply_text(
        "Für wieviel Personen soll die Einkaufs- und Kochliste erstellt werden?",
        reply_markup=kb
    )
    context.user_data.setdefault("flow_msgs", []).append(msg.message_id)
    return PERSONS_SELECTION





# ============================================================================================
# ===================================== FAVORITEN–HELPER =====================================
# ============================================================================================

import math

def build_fav_numbers_keyboard(total: int, selected: set[int]) -> InlineKeyboardMarkup:
    """Zahlen-Buttons (max. 8 pro Zeile), gleichmäßig auf Zeilen verteilt + 'Fertig'."""
    btns = [
        InlineKeyboardButton(
            f"{'✅ ' if i in selected else ''}{i}",
            callback_data=f"fav_del_{i}"
        )
        for i in range(1, total + 1)
    ]
    rows = distribute_buttons_equally(btns, max_per_row=7)

    rows.append([InlineKeyboardButton("Fertig", callback_data="fav_del_done")])
    return InlineKeyboardMarkup(rows)


def build_fav_add_numbers_keyboard(total: int, selected: set[int]) -> InlineKeyboardMarkup:
    """Zahlen-Buttons (max. 7 pro Zeile), gleichmäßig auf Zeilen verteilt + 'Fertig'."""
    btns = [
        InlineKeyboardButton(
            f"{'✅ ' if i in selected else ''}{i}",
            callback_data=f"fav_add_{i}"
        )
        for i in range(1, total + 1)
    ]
    rows = distribute_buttons_equally(btns, max_per_row=7)

    rows.append([InlineKeyboardButton("Fertig", callback_data="fav_add_done")])
    return InlineKeyboardMarkup(rows)

#gerichte zuteilen, falls aus favoriten gerichte selektiert
def get_random_gerichte(profile, filters, aufwandsliste, block=None, limit=3, mode="session"):
    """
    Liefert bis zu `limit` passende Gerichte nach Profil, Aufwand und Filter.
    Vermeidet Duplikate mit `block`.
    """
    if block is None:
        block = []

    basis = apply_profile_filters(df_gerichte, profile)

    # Filter: exclude blockierte Gerichte
    basis = basis[~basis["Gericht"].isin(block)]

    result = []
    used = set()

    for stufe in aufwandsliste:
        kandidaten = basis[basis["Aufwand"] == stufe]
        kandidaten = kandidaten[~kandidaten["Gericht"].isin(used)]
        if kandidaten.empty:
            continue
        w = pd.to_numeric(kandidaten["Gewicht"], errors="coerce").fillna(1.0)
        choice = kandidaten.sample(n=1, weights=w)["Gericht"].iloc[0]
        result.append(choice)
        used.add(choice)
        if len(result) >= limit:
            break

    return result[:limit]


def distribute_buttons_equally(buttons, max_per_row=7):
    total = len(buttons)
    rows_needed = math.ceil(total / max_per_row)

    per_row = total // rows_needed
    extra = total % rows_needed

    rows = []
    index = 0
    for r in range(rows_needed):
        count = per_row + (1 if r < extra else 0)
        rows.append(buttons[index:index + count])
        index += count
    return rows

# ============================================================================================

async def send_main_buttons(msg):
    """Hauptmenü-Buttons erneut anzeigen (z. B. bei leerer Favoritenliste)."""
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🍲 Menü",      callback_data="start_menu"),
        InlineKeyboardButton("⚡ QuickOne",     callback_data="start_quickone")],
        [InlineKeyboardButton("🔖 Favoriten", callback_data="start_favs"),
        InlineKeyboardButton("🛠️ Übersicht",     callback_data="start_setup"),
    ]])
    await msg.reply_text(pad_message("➡️ Wähle eine Option:"), reply_markup=kb)

# ============================================================================================

async def send_action_menu(msg):
    """Zeigt die drei Haupt-Export/Restart-Buttons mit Frage an."""
    kb = InlineKeyboardMarkup([
        [ InlineKeyboardButton("🔖 Gerichte zu Favoriten hinzufügen",                callback_data="favoriten") ],
        [ InlineKeyboardButton("🛒 Einkaufsliste in Bring! exportieren", callback_data="export_bring") ],
        [ InlineKeyboardButton("📄 Als PDF exportieren",   callback_data="export_pdf")   ],
        [ InlineKeyboardButton("🔄 Das passt so. Neustart!",             callback_data="restart")      ],
    ])
    await msg.reply_text(pad_message("Was möchtest Du weiter tun?"), reply_markup=kb)




# Load persisted data
sessions = load_json(SESSIONS_FILE)
favorites = load_favorites()
recipe_cache = load_json(CACHE_FILE)
profiles = load_profiles()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# === Google Sheets Data ===
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def lade_gerichtebasis():
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_GERICHTE)
    rows  = sheet.get_all_values()  # Header = rows[0]
    # A–J: Nummer | Code | Aktiv | Gericht | Aufwand | Typ | Ernährungsstil | Küche | Beilagen | Link
    data  = [row[:10] for row in rows[1:]]
    df    = pd.DataFrame(
        data,
        columns=["Nummer", "Code", "Aktiv", "Gericht", "Aufwand", "Typ", "Ernährungsstil", "Küche", "Beilagen", "Link"],
    )
    df["Aufwand"] = pd.to_numeric(df["Aufwand"], errors="coerce").fillna(0).astype(int)
    # Aktiv: 0=aus, 1=selten, 2=normal, 3=oft (Default 2)
    df["Aktiv"]   = pd.to_numeric(df["Aktiv"], errors="coerce").fillna(2).astype(int)

    # Aktiv -> Gewicht
    weight_map = {0: 0.0, 1: 0.6, 2: 1.0, 3: 1.6}
    df["Gewicht"] = df["Aktiv"].map(weight_map).fillna(1.0)

    # nur aktive Gerichte
    df = df[df["Gewicht"] > 0]

    return df.drop_duplicates()



def lade_beilagen():
    sheet = client.open_by_key(SHEET_ID).worksheet("Beilagen")
    raw = sheet.get_all_values()[1:]       # überspringe Header
    data = [row[:5] for row in raw]        # nur erste 5 Spalten
    df = pd.DataFrame(data, columns=["Nummer","Beilagen","Kategorie","Relevanz","Aufwand"])
    # nicht-numerische Zeilen rauswerfen
    df["Nummer"] = pd.to_numeric(df["Nummer"], errors="coerce")
    df = df[df["Nummer"].notna()]
    df["Nummer"] = df["Nummer"].astype(int)
    return df

def parse_codes(s: str):
    return [int(x.strip()) for x in s.split(",") if x.strip().isdigit()]



def lade_zutaten():
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_ZUTATEN)
    raw = sheet.get_all_values()[1:]  # Header überspringen
    # Nur die ersten 6 Spalten („Gericht“, „Zutat“, „Kategorie“, „Typ“, „Menge“, „Einheit“)
    data = [row[:6] for row in raw]
    # Extrahiere vorab den Roh-String aus Spalte 5
    raw_mengen = [row[4] if len(row) > 4 else "" for row in raw]
    df = pd.DataFrame(data, columns=["Gericht", "Zutat", "Kategorie", "Typ", "Menge", "Einheit"])
    df["Menge_raw"] = raw_mengen  # <— neue Spalte
    # Filtern und Typkonversion
    df = df[df["Gericht"].notna() & df["Zutat"].notna()]
    # Komma‐Dezimalstellen wie "0,5" erst auf Punkt umbiegen
    df["Menge"] = df["Menge"].astype(str).str.replace(",", ".", regex=False)
    df["Menge"] = pd.to_numeric(df["Menge"], errors="coerce").fillna(0)
    return df

df_gerichte = lade_gerichtebasis()
df_beilagen = lade_beilagen()
df_zutaten  = lade_zutaten()


# -------------------------------------------------
# Gerichte-Filter basierend auf Profil
# -------------------------------------------------
def apply_profile_filters(df: pd.DataFrame, profile: dict | None) -> pd.DataFrame:
    """Filtert das Gerichte-DataFrame gemäss Profil-Einstellungen."""
    if not profile or profile.get("restriction") == "offen":
        filtered = df.copy()
    else:
        # (a) Vegi ⇒ Spalte F ⇢ ['Vegi', 'beides']
        if profile["restriction"] == "Vegi":
            filtered = df[df["Ernährungsstil"].isin(["Vegi", "beides"])].copy()
        else:
            filtered = df.copy()

    # (b) Stil
    styles = profile.get("styles", []) if profile else []
    if styles:
        filtered = filtered[filtered["Küche"].isin(styles)]

    return filtered.reset_index(drop=True)

def sample_by_weight(df: pd.DataFrame, weight: int, k: int) -> pd.DataFrame:
    """
    Liefert bis zu k Gerichte gemäss Gewichtungstabellen. Fehlende Mengen
    werden nach fester Ersatz-Hierarchie aufgefüllt:

        fehlt leicht   → mittel, dann schwer
        fehlt mittel   → leicht, dann schwer
        fehlt schwer   → mittel, dann leicht
    """

    # Aufteilen
    df_light  = df[df["Typ"] == "1"].copy()
    df_medium = df[df["Typ"] == "2"].copy()
    df_heavy  = df[df["Typ"] == "3"].copy()

    mapping = {
        1: (1, 0, 0),
        2: (2, 1, 0),
        3: (2, 3, 1),
        4: (1, 1, 1),   # ausgeglichen
        5: (1, 3, 2),
        6: (0, 1, 2),
        7: (0, 0, 1),
    }
    l_part, m_part, h_part = mapping[weight]
    total_parts = l_part + m_part + h_part
    target = {
        "light":  int(k * l_part / total_parts),
        "medium": int(k * m_part / total_parts),
        "heavy":  k - int(k * l_part / total_parts) - int(k * m_part / total_parts),
    }

    chosen = {
        "light":  df_light.sample(
            n=min(len(df_light),  target["light"]),
            replace=False,
            weights=pd.to_numeric(df_light["Gewicht"], errors="coerce").fillna(1.0)
        ),
        "medium": df_medium.sample(
            n=min(len(df_medium), target["medium"]),
            replace=False,
            weights=pd.to_numeric(df_medium["Gewicht"], errors="coerce").fillna(1.0)
        ),
        "heavy":  df_heavy.sample(
            n=min(len(df_heavy),  target["heavy"]),
            replace=False,
            weights=pd.to_numeric(df_heavy["Gewicht"], errors="coerce").fillna(1.0)
        ),
    }

    # --------------------- Auffüllen nach Hierarchie --------------------
    def take(df_src, need):
        if need <= 0 or df_src.empty:
            return pd.DataFrame(), 0
        pick = df_src.sample(
            n=min(len(df_src), need),
            replace=False,
            weights=pd.to_numeric(df_src["Gewicht"], errors="coerce").fillna(1.0)
        )
        return pick, len(pick)

    # Fehlender LEICHT → mittel, dann schwer
    deficit = target["light"] - len(chosen["light"])
    if deficit > 0:
        extra, n = take(df_medium.drop(chosen["medium"].index), deficit)
        chosen["medium"] = pd.concat([chosen["medium"], extra])
        deficit -= n
    if deficit > 0:
        extra, n = take(df_heavy.drop(chosen["heavy"].index), deficit)
        chosen["heavy"] = pd.concat([chosen["heavy"], extra])

    # Fehlender SCHWER → mittel, dann leicht
    deficit = target["heavy"] - len(chosen["heavy"])
    if deficit > 0:
        extra, n = take(df_medium.drop(chosen["medium"].index), deficit)
        chosen["medium"] = pd.concat([chosen["medium"], extra])
        deficit -= n
    if deficit > 0:
        extra, n = take(df_light.drop(chosen["light"].index), deficit)
        chosen["light"] = pd.concat([chosen["light"], extra])

    # Fehlender MITTEL → leicht, dann schwer
    deficit = target["medium"] - len(chosen["medium"])
    if deficit > 0:
        extra, n = take(df_light.drop(chosen["light"].index), deficit)
        chosen["light"] = pd.concat([chosen["light"], extra])
        deficit -= n
    if deficit > 0:
        extra, n = take(df_heavy.drop(chosen["heavy"].index), deficit)
        chosen["heavy"] = pd.concat([chosen["heavy"], extra])

    # Zusammenführen – evtl. < k wenn nicht mehr genug Daten übrig
    result = pd.concat(list(chosen.values()))
    return result.sample(frac=1).reset_index(drop=True)

def choose_random_dish() -> str:
    """Zufällig ein Gericht auswählen, ohne Filter."""
    return df_gerichte.sample(1)["Gericht"].iloc[0]

def choose_sides(codes: list[int]) -> list[int]:
    """Beilagen basierend auf Codes zufällig auswählen, ohne Fehler bei leeren Kategorien."""
    # Listen der Beilagen-Nummern
    kh = df_beilagen[df_beilagen["Kategorie"] == "Kohlenhydrate"]["Nummer"].astype(int).tolist()
    gv = df_beilagen[df_beilagen["Kategorie"] == "Gemüse"]["Nummer"].astype(int).tolist()

    sides = []

    # 99: 1× KH + 1× Gemüse (sofern verfügbar)
    if 99 in codes:
        if kh:
            sides.append(random.choice(kh))
        if gv:
            sides.append(random.choice(gv))
        return sides

    # 88: 1× KH
    if 88 in codes:
        if kh:
            sides.append(random.choice(kh))
        return sides

    # 77: 1× Gemüse
    if 77 in codes:
        if gv:
            sides.append(random.choice(gv))
        return sides

    # spezifische Nummern: nur aus gültigem Bereich wählen
    valid = [c for c in codes if c in df_beilagen["Nummer"].astype(int).tolist()]
    if valid:
        sides.append(random.choice(valid))
    return sides




##############################################
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# === Commands ===
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
##############################################

##############################################
#>>>>>>>>>>>>START / SETUP
##############################################

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Willkommen!\n"
        "Übersicht der Befehle:\n\n"
        "🍲 Menü – Lass Dir leckere Gerichte vorschlagen\n\n"
        "⚡ QuickOne – Ein Gericht ohne Einschränkungen\n\n"
        "🔖 Favoriten – Deine Favoriten\n\n"
        "🛠️ Übersicht – Alle Funktionen\n\n"
    )

    # 2. Buttons in einer neuen Nachricht
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("🍲 Menü",      callback_data="start_menu"),
        InlineKeyboardButton("⚡QuickOne",     callback_data="start_quickone")],
        [InlineKeyboardButton("🔖 Favoriten", callback_data="start_favs"),
        InlineKeyboardButton("🛠️ Übersicht",     callback_data="start_setup"),
    ]])
    await update.message.reply_text(
        pad_message("➡️ Wähle eine Option:"),
        reply_markup=keyboard
    )

async def setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🛠 Kommandos im Menu Bot:\n"
        "/start – Hilfe & Einführung\n"
        "/menu – generiere Gerichtevorschläge\n"
        "/meinefavoriten – Übersicht deiner Favoriten\n"
        #"/meinProfil – Übersicht Deiner Favoriten\n"
        "/status – zeigt aktuelle Gerichtewahl\n"
        "/reset – setzt Session zurück (Favoriten bleiben)\n"
        "/setup – zeigt alle Kommandos\n"
        "/neustart – Startet neuen Prozess (Favoriten bleiben)\n"
        f"\nDeine User-ID: {update.effective_user.id}"
    )

async def menu_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/menu per Text – startet Profil-Loop"""
    # frische Liste für alle Wizard-Nachrichten
    context.user_data["prof_msgs"] = []

    sent = await update.message.reply_text(
        pad_message("Wie möchtest Du fortfahren?"),
        reply_markup=build_profile_choice_keyboard(),
    )
    # erste Message fürs spätere Cleanup merken
    context.user_data["prof_msgs"].append(sent.message_id)
    return PROFILE_CHOICE

async def menu_start_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/menu über den 🍲-Button im Hauptmenü"""
    q = update.callback_query
    await q.answer()

    # neue Liste für alle Wizard-Nachrichten
    context.user_data["prof_msgs"] = []

    sent = await q.message.reply_text(
        pad_message("Wie möchtest Du fortfahren?"),
        reply_markup=build_profile_choice_keyboard(),
    )
    context.user_data["prof_msgs"].append(sent.message_id)
    return PROFILE_CHOICE


# 3) Übersicht
async def start_setup_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    text = (
        "🛠 Kommandos im Menu Bot:\n"
        "/start – Hilfe & Einführung\n"
        "/menu – generiere Gerichtevorschläge\n"
        "/meinefavoriten – Übersicht Deiner Favoriten\n"
        #"/meinProfil – Übersicht Deiner Favoriten\n"
        "/status – zeigt aktuelle Auswahl\n"
        "/reset – setzt Session zurück (Favoriten bleiben)\n"
        "/setup – zeigt alle Kommandos\n"
        "/neustart – neuer Prozess\n"
        f"\nDeine User-ID: {update.effective_user.id}"
    )
    await q.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("Alles klar", callback_data="setup_ack")
        ]])
    )

async def setup_ack_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Löscht die Setup-Übersicht, wenn auf ‚Alles klar‘ geklickt wird."""
    q = update.callback_query
    await q.answer()
    try:
        await context.bot.delete_message(
            chat_id=q.message.chat.id,
            message_id=q.message.message_id
        )
    except:
        pass





##############################################
#>>>>>>>>>>>>MENU
##############################################


async def profile_choice_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Verarbeitet die vier Buttons: Bestehendes Profil, Ohne Einschränkung,
    Neues Profil, Mein Profil.
    Löscht am Ende die Wizard-Nachrichten (cleanup_prof_loop), bevor es in den
    alten Menü-Flow wechselt oder den Wizard startet.
    """
    q = update.callback_query
    await q.answer()
    uid    = str(q.from_user.id)
    choice = q.data                                   # prof_exist / prof_nolim / prof_new / prof_show

    # -----------------------------------------------------------
    # Hilfs-Funktion: Nachricht senden + (optional) ID speichern
    # -----------------------------------------------------------
    async def send_and_log(text: str, *, store_id: bool = True, **kwargs):
        msg = await q.message.reply_text(text, **kwargs)
        if store_id:
            context.user_data.setdefault("prof_msgs", []).append(msg.message_id)
        return msg

    # ===== 1)  Bestehendes Profil =========================================
    if choice == "prof_exist":
        if uid in profiles:
            # Profil vorhanden → Wizard-Messages weg, sofort zum alten Flow
            await cleanup_prof_loop(context, q.message.chat_id)
            return await start_menu_count_flow(update, context)


        # Kein Profil ⇒ Hinweis + Wizard starten
        await send_and_log("Es besteht noch kein Profil. Erstelle eines!")
        context.user_data["new_profile"] = {"styles": set()}
        await send_and_log(
            "Ernährungsstil:",
            reply_markup=build_restriction_keyboard()
        )
        return PROFILE_NEW_A

    # ===== 2)  Ohne Einschränkung =========================================
    if choice == "prof_nolim":
        # Wizard-Nachrichten löschen (falls vorhanden) und direkt weiter
        await cleanup_prof_loop(context, q.message.chat_id)
        return await start_menu_count_flow(update, context)


    # ===== 3)  Neues Profil ===============================================
    if choice == "prof_new":
        context.user_data["new_profile"] = {"styles": set()}
        await send_and_log(
            "Ernährungsstil:",
            reply_markup=build_restriction_keyboard()
        )
        return PROFILE_NEW_A

    # ===== 4)  Mein Profil =================================================
    if choice == "prof_show":
        if uid in profiles:
            await send_and_log(
                profile_overview_text(profiles[uid]),
                reply_markup=build_profile_overview_keyboard(),
                parse_mode="Markdown"
            )
            return PROFILE_OVERVIEW

        # Kein Profil gespeichert → Wizard
        await send_and_log("Es besteht noch kein Profil. Erstelle eines!")
        context.user_data["new_profile"] = {"styles": set()}
        await send_and_log(
            "Ernährungsstil:",
            reply_markup=build_restriction_keyboard()
        )
        return PROFILE_NEW_A




async def profile_new_a_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    # Auswahl speichern
    restrict_key = q.data          # res_vegi / res_open
    context.user_data["new_profile"]["restriction"] = RESTRICTION_CHOICES[restrict_key]

    # weiter zu (b)
    await q.message.edit_text(
        pad_message("Küche auswählen (Mehrfachauswahl möglich):"),
        reply_markup=build_style_keyboard(set())
    )
    return PROFILE_NEW_B


async def profile_new_b_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    data = q.data
    selected: set[str] = context.user_data["new_profile"]["styles"]

    # Fertig gedrückt → nächster Schritt (c) noch nicht implementiert
    if data == "style_done":
        # Wenn ALLE Stile gewählt wurden ⇒ keine Einschränkung = leere Liste
        if selected == ALL_STYLE_KEYS:
            context.user_data["new_profile"]["styles"] = []
        else:
            context.user_data["new_profile"]["styles"] = [
                STYLE_CHOICES[k] for k in selected
            ]

        # weiter zu (c) – Schwere
        await q.message.edit_text(
            pad_message("Schweregrad auswählen (1 = leicht … 7 = deftig):"),
            reply_markup=build_weight_keyboard(),
        )
        return PROFILE_NEW_C

    # Toggle Auswahl
    if data == "style_all":
        # Wenn schon alle gewählt → alles abwählen, sonst alles wählen
        if selected == ALL_STYLE_KEYS:
            selected.clear()
        else:
            selected.update(ALL_STYLE_KEYS)

    elif data in STYLE_CHOICES:
        # Einzelnes Stil-Toggle
        if data in selected:
            selected.remove(data)
        else:
            selected.add(data)

    # Keyboard aktualisieren
    await q.message.edit_reply_markup(reply_markup=build_style_keyboard(selected))
    return PROFILE_NEW_B

async def profile_new_c_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    # Gewählte Zahl abspeichern
    weight_key = q.data               # weight_1 … weight_7  oder weight_any
    context.user_data["new_profile"]["weight"] = WEIGHT_CHOICES.get(weight_key)


    # --- Profil in globalem Dict speichern ----------------------
    uid = str(q.from_user.id)
    profiles[uid] = {
        "restriction": context.user_data["new_profile"]["restriction"],
        "styles":      list(context.user_data["new_profile"]["styles"]),
        "weight":      context.user_data["new_profile"]["weight"],
    }
    save_profiles()

    # Übersicht + Buttons
    await q.message.edit_text(
        profile_overview_text(profiles[uid]),
        reply_markup=build_profile_overview_keyboard()
    )
    return PROFILE_OVERVIEW


async def profile_overview_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    choice = q.data  # prof_overwrite / prof_next

    if choice == "prof_overwrite":
        # Wizard neu starten
        context.user_data["new_profile"] = {"styles": set()}
        sent = await q.message.edit_text(
            pad_message("Ernährungsstil:"),
            reply_markup=build_restriction_keyboard(),
        )
        context.user_data["prof_msgs"].append(sent.message_id)
        return PROFILE_NEW_A

    # ------- Weiter → alten Menü-Flow starten -------------------------
    await cleanup_prof_loop(context, q.message.chat_id)
    return await start_menu_count_flow(update, context)


async def ask_menu_count(update: Update, context: ContextTypes.DEFAULT_TYPE, page: str = "low"):
    """Zeigt dem Nutzer Menümengen zur Auswahl (1–12) mit Umschaltung."""

    if page == "low":
        count_buttons = [InlineKeyboardButton(str(i), callback_data=f"menu_count_{i}") for i in range(1, 7)]
        nav_button = InlineKeyboardButton("Mehr ➡️", callback_data="menu_count_page_high")
    else:
        count_buttons = [InlineKeyboardButton(str(i), callback_data=f"menu_count_{i}") for i in range(7, 13)]
        nav_button = InlineKeyboardButton("⬅️ Weniger", callback_data="menu_count_page_low")

    rows = [count_buttons]
    rows.append([nav_button])
    kb = InlineKeyboardMarkup(rows)

    text = "Wie viele Menüs möchtest du?"

    if update.callback_query:
        data = update.callback_query.data
        if data in ["menu_count_page_high", "menu_count_page_low"]:
            # Navigation: Editiere vorhandene Nachricht
            await update.callback_query.message.edit_text(text, reply_markup=kb)
        else:
            # Kein Navigationsevent: sende neu (z. B. Ersteintritt)
            msg = await update.callback_query.message.reply_text(text, reply_markup=kb)
            context.user_data["flow_msgs"] = [msg.message_id]

    elif update.message:
        msg = await update.message.reply_text(text, reply_markup=kb)
        context.user_data["flow_msgs"] = [msg.message_id]

    else:
        chat_id = update.effective_chat.id
        msg = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)
        context.user_data["flow_msgs"] = [msg.message_id]

    return MENU_COUNT






async def menu_count_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "menu_count_page_high":
        return await ask_menu_count(update, context, page="high")
    elif data == "menu_count_page_low":
        return await ask_menu_count(update, context, page="low")
    elif data.startswith("menu_count_"):
        try:
            count = int(data.split("_")[-1])
        except ValueError:
            return MENU_COUNT

        context.user_data["menu_count"] = count
        context.user_data["aufwand_verteilung"] = {"light": 0, "medium": 0, "heavy": 0}
        await query.message.edit_text(
            f"Du suchst *{count}* Gerichte ✅\nDefiniere deren Aufwand:",
            reply_markup=build_aufwand_keyboard(context.user_data["aufwand_verteilung"], count)
        )
        return MENU_AUFWAND



async def start_menu_count_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Startet den neuen Menü-Auswahl-Flow mit Buttons."""
    await cleanup_prof_loop(context, update.effective_chat.id)

    return await ask_menu_count(update, context)


async def menu_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Erwartet Eingabe:  <gesamt> (<einfach>,<mittel>,<aufwändig>)
    Beispiel: 4 (2,1,1)
    """
    try:
        text = update.message.text.strip()
        user_id = str(update.message.from_user.id)

        m = re.match(r"(\d+)\s+\((\d+),(\d+),(\d+)\)", text)
        if not m:
            await update.message.reply_text("⚠️ Ungültiges Format. Beispiel: 4 (2,1,1)")
            return MENU_INPUT
        total, a1, a2, a3 = map(int, m.groups())

        if a1 + a2 + a3 != total:
            await update.message.reply_text("⚠️ Achtung: Die Summe muss der angegebenen Anzahl Menüs entsprechen.")
            return MENU_INPUT

        # === Schritt 3: Favoriten-Selektion verwenden ===
        if "fav_selection" in context.user_data:
            selected = context.user_data.pop("fav_selection")
            profile = profiles.get(user_id)
            if not profile:
                await update.message.reply_text("⚠️ Du hast noch kein Profil angelegt.")
                return MENU_PROFILE

            filters = context.user_data.get("filters", {})
            aufwand_wunsch = [1]*a1 + [2]*a2 + [3]*a3

            if len(selected) > total:
                selected = random.sample(selected, total)

            # Aufwand für die ausgewählten Favoriten aus df_gerichte extrahieren
            selected_aufwand = []
            for g in selected:
                match = df_gerichte[df_gerichte["Gericht"] == g]
                if not match.empty:
                    selected_aufwand.append(int(match.iloc[0]["Aufwand"]))
                else:
                    selected_aufwand.append(2)  # Default, falls Gericht nicht gefunden

            # Initial: Nur Favoriten verwenden
            final_gerichte = selected.copy()
            final_aufwand = selected_aufwand.copy()

            # Prüfen, ob wir noch auffüllen müssen
            if len(final_gerichte) < total:
                fehlend = total - len(final_gerichte)
                block = final_gerichte.copy()

                # Welche Aufwandwerte fehlen uns noch?
                # Schritt 1: Verfügbare und gewünschte Aufwandverteilung
                fav_counter = Counter(final_aufwand)
                wunsch_counter = Counter(aufwand_wunsch)

                # Schritt 2: Ziehe favoriten von Wunsch ab
                for stufe in (1, 2, 3):
                    abziehen = min(fav_counter[stufe], wunsch_counter[stufe])
                    fav_counter[stufe] -= abziehen
                    wunsch_counter[stufe] -= abziehen

                # Schritt 3: Überschüsse zuordnen
                # → Zuerst nach oben (höhere Stufe), dann nach unten (wenn nötig)
                for stufe in (2, 1):  # von mittel nach oben, dann leicht nach oben
                    while fav_counter[stufe] > 0:
                        if wunsch_counter[stufe + 1] > 0:
                            wunsch_counter[stufe + 1] -= 1
                        elif wunsch_counter[stufe - 1] > 0:
                            wunsch_counter[stufe - 1] -= 1
                        fav_counter[stufe] -= 1

                # Ergebnis: wunsch_counter enthält jetzt nur noch die fehlenden Gerichte pro Stufe
                rest_aufwand = []
                for stufe, anz in wunsch_counter.items():
                    rest_aufwand.extend([stufe] * anz)


                # Hole Restgerichte basierend auf Profil & restlichem Aufwand
                extra = get_random_gerichte(
                    profile, filters, rest_aufwand, block=block,
                    limit=fehlend, mode="session"
                )

                # Aufwand der extra Gerichte aus df_gerichte
                extra_aufwand = []
                for g in extra:
                    match = df_gerichte[df_gerichte["Gericht"] == g]
                    if not match.empty:
                        extra_aufwand.append(int(match.iloc[0]["Aufwand"]))
                    else:
                        extra_aufwand.append(2)

                final_gerichte.extend(extra)
                final_aufwand.extend(extra_aufwand)

            # Session speichern
            sessions[user_id] = {
                "menues": final_gerichte,
                "aufwand": final_aufwand,
            }
            save_json(SESSIONS_FILE, sessions)


            if show_debug_for(update):                                  #nur für ADMIN ersichtlich, as specified in def show_debug_for
                # Extrahiere Zusatzinfos zu den gewählten Gerichten
                gewaehlte_gerichte = df_gerichte[df_gerichte["Gericht"].isin(final_gerichte)]

                # Aufwand-Verteilung
                aufwand_counter = Counter(gewaehlte_gerichte["Aufwand"])
                aufwand_text = ", ".join(f"{v} x {k}" for k, v in aufwand_counter.items())

                # Küche-Verteilung
                kitchen_counter = Counter(gewaehlte_gerichte["Küche"])
                kitchen_text = ", ".join(f"{v} x {k}" for k, v in kitchen_counter.items())

                # Art-Verteilung
                typ_counter = Counter(gewaehlte_gerichte["Typ"])
                typ_text = ", ".join(f"{v} x {k}" for k, v in typ_counter.items())

                # Einschränkung-Verteilung
                einschr_counter = Counter(gewaehlte_gerichte["Ernährungsstil"])
                einschr_text = ", ".join(f"{v} x {k}" for k, v in einschr_counter.items())

                # Erweiterte Debug-Nachricht zusammenbauen
                debug_msg = (
                    f"\n📊 Aufwand-Verteilung: {aufwand_text}"
                    f"\n🎨 Küche-Verteilung: {kitchen_text}"
                    f"\n⚙️ Typ-Verteilung: {typ_text}"
                    f"\n🥗 Ernährungsstil-Verteilung: {einschr_text}"
                )
                msg_debug = await update.message.reply_text(debug_msg)
                context.user_data["flow_msgs"].append(msg_debug.message_id)


            reply = "🥣 Deine Menü-Auswahl:\n" + "\n".join(f"{i+1}. {g}" for i, g in enumerate(final_gerichte))
            msg1 = await update.message.reply_text(reply)
            context.user_data["flow_msgs"].append(msg1.message_id)

            confirm_kb = InlineKeyboardMarkup([[  
                InlineKeyboardButton("Ja",   callback_data="confirm_yes"),
                InlineKeyboardButton("Nein", callback_data="confirm_no"),
            ]])
            msg2 = await update.message.reply_text(
                pad_message("Passen diese Gerichte?"), reply_markup=confirm_kb
            )
            context.user_data["flow_msgs"].append(msg2.message_id)
            return ASK_CONFIRM




        # ---------- Basis-DataFrame gemäss Profil ----------------------
        uid     = str(update.message.from_user.id)
        profile = profiles.get(uid)                          # None = ohne Profil
        basis   = apply_profile_filters(df_gerichte, profile)


        weight_pref = profile.get("weight") if profile else None
        if weight_pref:
            subset = sample_by_weight(basis, weight_pref, total + round(total * 0.2))
            if not subset.empty:
                basis = subset


        # Falls nichts übrig bleibt → Fallback ohne Stil-Filter
        if basis.empty:
            await update.message.reply_text(
                "⚠️ Keine Gerichte passen exakt zu deinem Profil – ich suche ohne Stil-Einschränkung weiter."
            )
            tmp_profile = dict(profile) if profile else None
            if tmp_profile:
                tmp_profile["styles"] = []
            basis = apply_profile_filters(df_gerichte, tmp_profile)

        favs = favorites.get(user_id, [])

        # ---------------------------------------------------------
        #  Aufwand-Auswahl mit Ersatz-Hierarchie
        # ---------------------------------------------------------
        # Mapping Aufwand-Stufe → Spalte Art
        aufwand2art = {1: "leicht", 2: "mittel", 3: "schwer"}

        # Hilfsfunktion: n Gerichte aus einer Teilmenge ziehen
        def pick(df_src, n, exclude_ids):
            if n <= 0:
                return []
            pool = df_src[~df_src["Gericht"].isin(exclude_ids)].copy()
            if pool.empty:
                return []
            w = pd.to_numeric(pool["Gewicht"], errors="coerce").fillna(1.0)
            return pool.sample(n=min(n, len(pool)), replace=False, weights=w)["Gericht"].tolist()


        bereits, ausgewaehlt, aufwand_liste = set(), [], []

        # Primär­auswahl je Stufe
        bedarf = {1: a1, 2: a2, 3: a3}           # Soll­mengen
        reste  = {1: [],  2: [],  3: []}         # Fehlbestände je Stufe

        for stufe in (1, 2, 3):
            art   = aufwand2art[stufe]
            grund = basis[basis["Aufwand"] == stufe]
            picks = pick(grund, bedarf[stufe], bereits)
            ausgewaehlt += picks
            aufwand_liste += [stufe] * len(picks)
            bereits.update(picks)
            rest = bedarf[stufe] - len(picks)
            reste[stufe] = rest if rest > 0 else 0

        # ---------------------------------------------------------
        #  Auffüllen nach fester Hierarchie
        # ---------------------------------------------------------
        def ersatz(stufe_fehl):
            """liefert tuple von Ersatz-Stufen in der gewünschten Reihenfolge"""
            if stufe_fehl == 1:   # leicht fehlt
                return (2, 3)
            if stufe_fehl == 3:   # schwer fehlt
                return (2, 1)
            return (1, 3)         # mittel fehlt

        for stufe in (1, 2, 3):
            fehl = reste[stufe]
            if fehl == 0:
                continue
            for ers in ersatz(stufe):
                if fehl == 0:
                    break
                art = aufwand2art[ers]
                df_pool = basis[basis["Aufwand"] == ers]
                picks   = pick(df_pool, fehl, bereits)
                ausgewaehlt += picks
                aufwand_liste += [ers] * len(picks)
                bereits.update(picks)
                fehl -= len(picks)

        # Falls immer noch zu wenig Gerichte vorhanden, nimm beliebige übrige
        gesamt = a1 + a2 + a3
        if len(ausgewaehlt) < gesamt:
            rest_df = basis[~basis["Gericht"].isin(bereits)].copy()
            if not rest_df.empty:
                w = pd.to_numeric(rest_df["Gewicht"], errors="coerce").fillna(1.0)
                k = min(gesamt - len(ausgewaehlt), len(rest_df))
                extra = rest_df.sample(n=k, replace=False, weights=w)["Gericht"].tolist()
                for g in extra:
                    stufe = int(basis[basis["Gericht"] == g]["Aufwand"].iloc[0])
                    ausgewaehlt.append(g)
                    aufwand_liste.append(stufe)



        # ---------- Speichern & Ausgabe -------------------------------
        sessions[user_id] = {"menues": ausgewaehlt, "aufwand": aufwand_liste}
        save_json(SESSIONS_FILE, sessions)

        if show_debug_for(update):                                  #nur für ADMIN ersichtlich, as specified in def show_debug_for
            # Extrahiere Zusatzinfos zu den gewählten Gerichten
            gewaehlte_gerichte = df_gerichte[df_gerichte["Gericht"].isin(ausgewaehlt)]

            # Aufwand-Verteilung
            aufwand_counter = Counter(gewaehlte_gerichte["Aufwand"])
            aufwand_text = ", ".join(f"{v} x {k}" for k, v in aufwand_counter.items())

            # Küche-Verteilung
            kitchen_counter = Counter(gewaehlte_gerichte["Küche"])
            kitchen_text = ", ".join(f"{v} x {k}" for k, v in kitchen_counter.items())

            # Typ-Verteilung
            typ_counter = Counter(gewaehlte_gerichte["Typ"])
            typ_text = ", ".join(f"{v} x {k}" for k, v in typ_counter.items())

            # Ernährungsstil-Verteilung
            einschr_counter = Counter(gewaehlte_gerichte["Ernährungsstil"])
            einschr_text = ", ".join(f"{v} x {k}" for k, v in einschr_counter.items())

            # Erweiterte Debug-Nachricht zusammenbauen
            debug_msg = (
                f"\n📊 Aufwand-Verteilung: {aufwand_text}"
                f"\n🎨 Küche-Verteilung: {kitchen_text}"
                f"\n⚙️ Typ-Verteilung: {typ_text}"
                f"\n🥗 Ernährungsstil-Verteilung: {einschr_text}"
            )
            msg_debug = await update.message.reply_text(debug_msg)
            context.user_data["flow_msgs"].append(msg_debug.message_id)


        reply = "🥣 Deine Gerichte:\n" + "\n".join(f"{i+1}. {g}" for i, g in enumerate(ausgewaehlt))
        # Nachricht 1 senden + tracken
        msg1 = await update.message.reply_text(pad_message(reply))
        context.user_data["flow_msgs"].append(msg1.message_id)

        confirm_kb = InlineKeyboardMarkup([[  
            InlineKeyboardButton("Ja",   callback_data="confirm_yes"),
            InlineKeyboardButton("Nein", callback_data="confirm_no"),
        ]])
        # Nachricht 2 senden + tracken
        msg2 = await update.message.reply_text(pad_message("Passen diese Gerichte?"), reply_markup=confirm_kb)
        context.user_data["flow_msgs"].append(msg2.message_id)

        return ASK_CONFIRM


    except Exception as e:
        await update.message.reply_text(f"❌ Fehler: {e}")
        return MENU_INPUT


async def menu_input_direct(user_input: str, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Hilfsfunktion für direkten Aufruf von menu_input mit Text über FakeMessage."""
    class FakeMessage:
        def __init__(self, text, chat, message_id, from_user, bot):
            self.text = text
            self.chat = chat
            self.message_id = message_id
            self.from_user = from_user
            self.bot = bot

        async def reply_text(self, text, **kwargs):
            return await self.bot.send_message(chat_id=self.chat.id, text=text, **kwargs)

        async def delete(self):
            return await self.bot.delete_message(chat_id=self.chat.id, message_id=self.message_id)

    fake_message = FakeMessage(
        text=user_input,
        chat=update.effective_chat,
        message_id=update.effective_message.message_id,
        from_user=update.effective_user,
        bot=context.bot,
    )

    fake_update = Update(update.update_id, message=fake_message)

    return await menu_input(fake_update, context)


# ─────────── menu_confirm_cb ───────────
async def menu_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat.id
    uid     = str(query.from_user.id)

    if query.data == "confirm_yes":
        # 1) Feedback setzen
        await mark_yes_no(query, True, "confirm_yes", "confirm_no")

        # 2) Nur die Bestätigungs-Nachricht löschen
        flow = context.user_data.get("flow_msgs", [])
        if flow:
            last_id = flow.pop()
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=last_id)
            except:
                pass

        # 3) Menüs und Beilagen-fähige Menüs ermitteln
        menus = sessions[uid]["menues"]
        side_menus = []
        for idx, dish in enumerate(menus):
            raw = df_gerichte.loc[df_gerichte["Gericht"] == dish, "Beilagen"].iloc[0]
            codes = [c for c in parse_codes(raw) if c != 0]
            if codes:
                side_menus.append(idx)

        # 4a) 0 Beilagen-Menüs: überspringen → finale Übersicht + Personen
        if not side_menus:
            # • alle bisherigen Nachrichten löschen
            for mid in context.user_data.get("flow_msgs", []):
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=mid)
                except:
                    pass
            context.user_data["flow_msgs"].clear()

            # • finale Übersicht
            text = "🥣 Deine finale Liste:\n"
            for dish in menus:
                nums       = sessions[uid].get("beilagen", {}).get(dish, [])
                side_names = df_beilagen.loc[df_beilagen["Nummer"].isin(nums), "Beilagen"].tolist()
                text      += f"- {format_dish_with_sides(dish, side_names)}\n"
            msg = await query.message.reply_text(pad_message(text))
            context.user_data["flow_msgs"].append(msg.message_id)

            # • direkt Personen-Frage
            return await ask_for_persons(update, context)

        # 4b) Mindestens ein Gericht mit Beilagen → immer erst die Ja/Nein-Frage
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("Ja",   callback_data="ask_yes"),
            InlineKeyboardButton("Nein", callback_data="ask_no"),
        ]])
        msg = await query.message.reply_text(pad_message("Möchtest du Beilagen hinzufügen?"), reply_markup=kb)
        context.user_data["flow_msgs"].append(msg.message_id)
        return ASK_BEILAGEN

    if query.data == "confirm_no":
        await mark_yes_no(query, False, "confirm_yes", "confirm_no")
        # nur die Bestätigungs-Nachricht löschen
        flow = context.user_data.get("flow_msgs", [])
        if flow:
            last_id = flow.pop()
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=last_id)
            except:
                pass

        # Tausche-Loop starten
        context.user_data["swap_candidates"] = set()
        kb = build_swap_keyboard(sessions[uid]["menues"], set())
        msg = await query.message.reply_text(pad_message("Welche Gerichte möchtest Du tauschen?"), reply_markup=kb)
        context.user_data["flow_msgs"].append(msg.message_id)
        return TAUSCHE_SELECT

    return ConversationHandler.END









async def persons_selection_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    data = query.data

    # 1) Seitenwechsel
    if data in ("persons_page_low", "persons_page_high"):
        return await ask_for_persons(update, context, page="high" if data=="persons_page_high" else "low")

    # 2) Echte Personenzahl gewählt
    if data.startswith("persons_"):
        # a) alte Frage-Nachricht löschen
        flow = context.user_data.get("flow_msgs", [])
        if flow:
            last_id = flow.pop()
            try:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=last_id
                )
            except:
                pass

        # b) Auswahl speichern
        count = int(data.split("_")[1])
        context.user_data["temp_persons"] = count

        # c) Weiter zum Abschluss-Input
        return await fertig_input(update, context)

    # Fallback
    return ConversationHandler.END




async def persons_manual_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    chat_id = update.message.chat.id
    try:
        personen = int(update.message.text.strip())
        if personen <= 0: raise ValueError
    except:
        await update.message.reply_text("⚠️ Ungültige Zahl.")
        return PERSONS_MANUAL

    # (Keine Löschung über person_msgs nötig, flow_msgs wird im fertig_input komplett gelöscht)
    context.user_data["temp_persons"] = personen
    return await fertig_input(update, context)




# ===== QuickOne – Flow =====

# ─────────── quickone_start ───────────
async def quickone_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    chat_id = update.effective_chat.id

    # 1) Flow-Nachrichten zurücksetzen
    context.user_data["flow_msgs"] = []

    # 2) Pools nur beim echten Einstieg resetten
    if (not update.callback_query) or update.callback_query.data == "start_quickone":
        context.user_data.pop("quickone_remaining", None)
        context.user_data.pop("quickone_side_pools", None)

    # 3) Gerichtspool initialisieren
    remaining = context.user_data.get("quickone_remaining")
    all_dishes = [d for d in df_gerichte["Gericht"].tolist() if isinstance(d, str) and d.strip()]
    if not remaining:
        remaining = all_dishes.copy()

    # Favoriten (3x) × Aktiv-Gewicht
    uid       = str(update.effective_user.id)
    user_favs = favorites.get(uid, [])
    wmap      = df_gerichte.set_index("Gericht")["Gewicht"].to_dict()
    weights   = [ (3 if d in user_favs else 1) * float(wmap.get(d, 1.0)) for d in remaining ]

    dish = random.choices(remaining, weights=weights, k=1)[0]

    remaining.remove(dish)
    context.user_data["quickone_remaining"] = remaining

    # 4) Beilagen-Codes filtern und nur wenn vorhanden Pools nutzen
    raw = df_gerichte.loc[df_gerichte["Gericht"] == dish, "Beilagen"].iloc[0]
    all_codes = parse_codes(raw)
    codes = [c for c in all_codes if c != 0]
    if not codes:
        # keine Beilagen möglich
        side_nums = []
    else:
        # Pools initialisieren
        side_pools = context.user_data.setdefault("quickone_side_pools", {})
        if dish not in side_pools:
            carbs_list = df_beilagen[df_beilagen["Kategorie"] == "Kohlenhydrate"]["Nummer"].tolist()
            veggies_list = df_beilagen[df_beilagen["Kategorie"] == "Gemüse"]["Nummer"].tolist()
            if 99 in codes:
                pool = {"carbs": carbs_list.copy(), "veggies": veggies_list.copy()}
            elif 88 in codes:
                pool = {"single": carbs_list.copy()}
            elif 77 in codes:
                pool = {"single": veggies_list.copy()}
            else:
                pool = {"single": codes.copy()}
            side_pools[dish] = pool

        # aus den Pools ziehen
        pool = side_pools[dish]
        if "carbs" in pool and "veggies" in pool:
            if not pool["carbs"]:
                pool["carbs"] = df_beilagen[df_beilagen["Kategorie"] == "Kohlenhydrate"]["Nummer"].tolist()
            if not pool["veggies"]:
                pool["veggies"] = df_beilagen[df_beilagen["Kategorie"] == "Gemüse"]["Nummer"].tolist()
            c = random.choice(pool["carbs"]); pool["carbs"].remove(c)
            v = random.choice(pool["veggies"]); pool["veggies"].remove(v)
            side_nums = [c, v]
        else:
            single_pool = pool.get("single", [])
            if not single_pool:
                # Pool neu füllen
                pool["single"] = pool.get("single", []).copy()
                single_pool = pool["single"]
            num = random.choice(single_pool); pool["single"].remove(num)
            side_nums = [num]

    # 5) Namen ermitteln
    sides = df_beilagen[df_beilagen["Nummer"].isin(side_nums)]["Beilagen"].tolist()

    # 6) Session speichern
    sessions[uid] = {
        "menues": [dish],
        "aufwand": [0],
        "beilagen": {dish: side_nums}
    }
    save_json(SESSIONS_FILE, sessions)

    # 7) Gericht anzeigen
    text1 = f"🥣 *Dein Gericht:*\n{format_dish_with_sides(dish, sides)}"
    msg1 = await context.bot.send_message(chat_id, text=pad_message(text1), parse_mode="Markdown")
    context.user_data["flow_msgs"].append(msg1.message_id)

    # 8) Frage „Passt das?“
    buttons = [
        InlineKeyboardButton("Passt", callback_data="quickone_passt"),
        InlineKeyboardButton("Neu!", callback_data="quickone_neu"),
    ]
    if sides:
        buttons.append(InlineKeyboardButton("Beilagen neu", callback_data="quickone_beilagen_neu"))
    markup = InlineKeyboardMarkup([buttons])
    msg2 = await context.bot.send_message(chat_id, text=pad_message("Passt das?"), reply_markup=markup)
    context.user_data["flow_msgs"].append(msg2.message_id)

    return QUICKONE_CONFIRM


# ─────────── quickone_confirm_cb ───────────
async def quickone_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = str(update.effective_user.id)
    chat_id = q.message.chat.id
    data = q.data

    # Passt → sofort Frage-Nachricht löschen, dann Personenfrage
    if data == "quickone_passt":
        flow = context.user_data.get("flow_msgs", [])
        if flow:
            last_id = flow.pop()
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=last_id)
            except:
                pass
        return await ask_for_persons(update, context)

    # Neu! → komplett neu starten
    if data == "quickone_neu":
        for mid in context.user_data.get("flow_msgs", []):
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            except:
                pass
        context.user_data["flow_msgs"].clear()
        return await quickone_start(update, context)

    # Beilagen neu → nur Beilagen tauschen, Gericht bleibt
    if data == "quickone_beilagen_neu":
        for mid in context.user_data.get("flow_msgs", []):
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            except:
                pass
        context.user_data["flow_msgs"].clear()

        # Dynamische Beilagen-Auswahl wie in quickone_start
        dish = sessions[uid]["menues"][0]
        side_pools = context.user_data.setdefault("quickone_side_pools", {})
        pool = side_pools.get(dish)
        if not pool:
            # init as above
            raw = df_gerichte.loc[df_gerichte["Gericht"] == dish, "Beilagen"].iloc[0]
            codes = parse_codes(raw)
            carbs_list = df_beilagen[df_beilagen["Kategorie"] == "Kohlenhydrate"]["Nummer"].tolist()
            veggies_list = df_beilagen[df_beilagen["Kategorie"] == "Gemüse"]["Nummer"].tolist()
            if 99 in codes:
                pool = {"carbs": carbs_list.copy(), "veggies": veggies_list.copy()}
            elif 88 in codes:
                pool = {"single": carbs_list.copy()}
            elif 77 in codes:
                pool = {"single": veggies_list.copy()}
            else:
                pool = {"single": [c for c in codes if c != 0]}
            side_pools[dish] = pool

        # Auswahl wie oben
        if "carbs" in pool and "veggies" in pool:
            if not pool["carbs"]:
                pool["carbs"] = df_beilagen[df_beilagen["Kategorie"] == "Kohlenhydrate"]["Nummer"].tolist()
            if not pool["veggies"]:
                pool["veggies"] = df_beilagen[df_beilagen["Kategorie"] == "Gemüse"]["Nummer"].tolist()
            c = random.choice(pool["carbs"]); pool["carbs"].remove(c)
            v = random.choice(pool["veggies"]); pool["veggies"].remove(v)
            side_nums = [c, v]
        else:
            single_pool = pool.get("single", [])
            if not single_pool:
                pool["single"] = pool.get("single", []).copy()
                single_pool = pool["single"]
            num = random.choice(single_pool); pool["single"].remove(num)
            side_nums = [num]

        # Session aktualisieren
        sessions[uid]["beilagen"][dish] = side_nums
        save_json(SESSIONS_FILE, sessions)
        sides = df_beilagen[df_beilagen["Nummer"].isin(side_nums)]["Beilagen"].tolist()

        # Gericht erneut anzeigen
        text1 = f"🥣 *Dein Gericht:*\n{format_dish_with_sides(dish, sides)}"
        msg1 = await context.bot.send_message(chat_id, text=text1, parse_mode="Markdown")
        context.user_data["flow_msgs"].append(msg1.message_id)

        # Wieder „Passt das?“
        buttons = [
            InlineKeyboardButton("Passt", callback_data="quickone_passt"),
            InlineKeyboardButton("Neu!", callback_data="quickone_neu"),
            InlineKeyboardButton("Beilagen neu", callback_data="quickone_beilagen_neu"),
        ]
        markup = InlineKeyboardMarkup([buttons])
        msg2 = await context.bot.send_message(chat_id, text=pad_message("Passt das?"), reply_markup=markup)
        context.user_data["flow_msgs"].append(msg2.message_id)

        return QUICKONE_CONFIRM

    return ConversationHandler.END




async def ask_beilagen_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = str(query.from_user.id)

    # User hat „Nein“ bei Beilagen-Frage gewählt → finale Übersicht
    if query.data == "ask_no":
        # 1) Visuelles Feedback
        await mark_yes_no(query, False, "ask_yes", "ask_no")

        # 2) Alle bisherigen Flow-Messages löschen
        chat_id = query.message.chat.id
        for mid in context.user_data.get("flow_msgs", []):
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            except:
                pass
        context.user_data["flow_msgs"].clear()

        # 3) Finale Übersicht senden
        text = "🥣 Deine finale Liste:\n"
        for dish in sessions[uid]["menues"]:
            sel_nums   = sessions[uid].get("beilagen", {}).get(dish, [])
            side_names = df_beilagen.loc[
                df_beilagen["Nummer"].isin(sel_nums), "Beilagen"
            ].tolist()
            text += f"- {format_dish_with_sides(dish, side_names)}\n"
        msg = await query.message.reply_text(pad_message(text))
        context.user_data["flow_msgs"].append(msg.message_id)

        # 4) Direkt nach Personen fragen
        return await ask_for_persons(update, context)

    # User hat „Ja“ bei Beilagen-Frage gewählt → Beilagen-Loop wie bisher
    if query.data == "ask_yes":
        await mark_yes_no(query, True, "ask_yes", "ask_no")

        # 1) Prüfen, welche Gerichte Beilagen haben
        menus = sessions[uid]["menues"]
        side_menus = [
             idx for idx, dish in enumerate(menus)
             if any(c != 0 for c in parse_codes(
                 df_gerichte.loc[df_gerichte["Gericht"] == dish, "Beilagen"].iloc[0]
             ))
         ]

        # 2a) Genau 1 Gericht → direkt in den Einzel-Loop springen
        if len(side_menus) == 1:
            context.user_data["menu_list"] = menus
            context.user_data["to_process"] = side_menus
            context.user_data["menu_idx"]    = 0
            return await ask_beilagen_for_menu(query, context)


        # 2b) Mehrere Gerichte → normale Mehrfach-Auswahl wie bisher
        menus = (context.user_data.get("menu_list")
                 or sessions[str(query.from_user.id)]["menues"])
        buttons = []
        for i, gericht in enumerate(menus, start=1):
            codes = parse_codes(
                df_gerichte.loc[df_gerichte["Gericht"] == gericht, "Beilagen"].iloc[0]
            )
            if not codes or codes == [0]:
                continue
            buttons.append(InlineKeyboardButton(str(i), callback_data=f"select_{i}"))
        buttons.append(InlineKeyboardButton("Fertig", callback_data="select_done"))
        kb = [buttons[j:j+4] for j in range(0, len(buttons), 4)]
        msg = await query.message.reply_text(
            pad_message("Für welche Menüs? (Mehrfachauswahl, dann Fertig)"),
            reply_markup=InlineKeyboardMarkup(kb)
        )
        context.user_data["flow_msgs"].append(msg.message_id)
        context.user_data["selected_menus"] = set()
        return SELECT_MENUES

    # Fallback
    return BEILAGEN_SELECT
   
async def ask_beilagen_for_menu(update_or_query, context: ContextTypes.DEFAULT_TYPE):
    """
    Zeigt für ein einzelnes ausgewähltes Menü die Inline-Buttons
    aller erlaubten Beilagen an (max.2 KH + 2 Gemüse).
    """
    # 1) Welches Menü ist dran?
    idx = context.user_data["to_process"][context.user_data["menu_idx"]]
    # menus wurde vorher in user_data gefüllt
    menus = context.user_data["menu_list"]
    gericht = menus[idx]

    # 2) Beilage-Codes aus df_gerichte lesen und parsen
    raw = df_gerichte.loc[df_gerichte["Gericht"] == gericht, "Beilagen"].iloc[0]
    codes = parse_codes(raw)

    # 3) Erlaubte Nummern ermitteln
    erlaubt = set()
    if 99 in codes:
        # sowohl Kategorien Kohlenhydrate & Gemüse, keine spezifischen
        erlaubt = set(
            df_beilagen[df_beilagen["Kategorie"].isin(["Kohlenhydrate", "Gemüse"])]["Nummer"]
        )
    else:
        if 88 in codes:
            erlaubt |= set(df_beilagen[df_beilagen["Kategorie"] == "Kohlenhydrate"]["Nummer"])
            codes = [c for c in codes if c != 88]
        if 77 in codes:
            erlaubt |= set(df_beilagen[df_beilagen["Kategorie"] == "Gemüse"]["Nummer"])
            codes = [c for c in codes if c != 77]
        erlaubt |= set(codes)

    # Erlaube den späteren Zugriff in beilage_select_cb
    context.user_data["allowed_beilage_codes"] = erlaubt


    # 4) Inline-Buttons bauen (max. 3 pro Zeile) + 'Fertig' in eigener Zeile
    side_buttons = []
    for num, name, cat in zip(df_beilagen["Nummer"], df_beilagen["Beilagen"], df_beilagen["Kategorie"]):
        if num in erlaubt:
            side_buttons.append(
                InlineKeyboardButton(name, callback_data=f"beilage_{num}")
            )

    rows = distribute_buttons_equally(side_buttons, max_per_row=3)
    rows.append([InlineKeyboardButton("Fertig", callback_data="beilage_done")])

    # 5) Nachricht senden + tracken
    msg = await update_or_query.message.reply_text(
        pad_message(f"Wähle Beilagen für: *{gericht}*"),
        reply_markup=InlineKeyboardMarkup(rows),
        parse_mode="Markdown"
    )
    context.user_data["flow_msgs"].append(msg.message_id)



    # 6) Auswahl initialisieren
    uid = str(update_or_query.from_user.id)
    sessions.setdefault(uid, {}).setdefault("beilagen", {})[gericht] = []

    return BEILAGEN_SELECT


async def select_menus_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    # Fallback: falls menu_list nicht im user_data ist, aus sessions holen
    menus = context.user_data.get("menu_list")
    if menus is None:
        uid = str(query.from_user.id)
        menus = sessions.get(uid, {}).get("menues", [])
        context.user_data["menu_list"] = menus
    sel = context.user_data["selected_menus"]

    if data == "select_done":
        if not sel:
            await query.message.reply_text("⚠️ Keine Menüs ausgewählt. Abbruch.")
            return ConversationHandler.END
        context.user_data["to_process"] = sorted(i-1 for i in sel)  # in 0-basiert
        context.user_data["to_process"] = sorted(sel)
        context.user_data["menu_idx"] = 0
        return await ask_beilagen_for_menu(query, context)

    idx = int(data.split("_")[1]) - 1
    if idx in sel:
        sel.remove(idx)
    else:
        sel.add(idx)

    buttons = []
    for i, gericht in enumerate(menus, start=1):
        # Codes parsen und nur weiter, wenn mindestens ein Code ≠ 0 existiert
        codes = parse_codes(df_gerichte.loc[df_gerichte["Gericht"] == gericht, "Beilagen"].iloc[0])
        if not codes or codes == [0]:
            continue
        mark = " ✅" if (i-1) in sel else ""
        buttons.append(InlineKeyboardButton(f"{i}{mark}", callback_data=f"select_{i}"))
    buttons.append(InlineKeyboardButton("Fertig", callback_data="select_done"))
    kb = distribute_buttons_equally(buttons, max_per_row=4)
    await query.message.edit_reply_markup(InlineKeyboardMarkup(kb))
    return SELECT_MENUES


async def ask_showlist_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "show_no":
        await mark_yes_no(query, False, "show_yes", "show_no")
        # User will nicht – beendet das Gespräch
        return ConversationHandler.END

    # User möchte die Liste sehen: frage nach Personen
    if query.data == "show_yes":
        await mark_yes_no(query, True, "show_yes", "show_no")     # Haken bei Ja
        return await ask_for_persons(update, context)



async def beilage_select_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    uid = str(query.from_user.id)
    # Welches Gericht gerade dran ist?
    idx_list = context.user_data["to_process"]
    idx = idx_list[context.user_data["menu_idx"]]
    menus = context.user_data["menu_list"]
    gericht = menus[idx]

    # Aktuelle Auswahl-Liste aus sessions
    sel = sessions.setdefault(uid, {}).setdefault("beilagen", {}).setdefault(gericht, [])

    # Wenn „Fertig“ gedrückt wurde: nächstes Gericht oder Ende
    if data == "beilage_done":
        context.user_data["menu_idx"] += 1
        if context.user_data["menu_idx"] < len(idx_list):
            return await ask_beilagen_for_menu(query, context)

        # --- Nach Abschluss des Beilagen-Loops: finale Übersicht ---
        # 1) Alle bisherigen Flow-Messages löschen
        chat_id = query.message.chat.id
        for mid in context.user_data.get("flow_msgs", []):
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            except:
                pass
        context.user_data["flow_msgs"].clear()

        # 2) Finale Gerichte-Übersicht senden
        text = "🥣 Deine finale Liste:\n"
        for dish in sessions[uid]["menues"]:
            nums = sessions[uid].get("beilagen", {}).get(dish, [])
            names = df_beilagen.loc[df_beilagen["Nummer"].isin(nums), "Beilagen"].tolist()
            text += f"- {format_dish_with_sides(dish, names)}\n"
        msg = await query.message.reply_text(pad_message(text))
        context.user_data["flow_msgs"].append(msg.message_id)

        # 3) Direkt nach Personen fragen
        return await ask_for_persons(update, context)

    # Sonst: Toggle einer Beilage
    num = int(data.split("_")[1])
    if num in sel:
        sel.remove(num)
    else:
        sel.append(num)

    # Buttons neu aufbauen mit Markierungen (max. 3 pro Zeile) + 'Fertig' in eigener Zeile
    side_buttons = []
    for code in context.user_data.get("allowed_beilage_codes", []):
        name = df_beilagen.loc[df_beilagen["Nummer"] == code, "Beilagen"].iloc[0]
        mark = " ✅" if code in sel else ""
        side_buttons.append(
            InlineKeyboardButton(f"{mark}{name}", callback_data=f"beilage_{code}")
        )

    rows = distribute_buttons_equally(side_buttons, max_per_row=3)
    rows.append([InlineKeyboardButton("Fertig", callback_data="beilage_done")])
    await query.message.edit_reply_markup(InlineKeyboardMarkup(rows))


    return BEILAGEN_SELECT


async def ask_final_list_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        if query.data == "final_no":
                return ConversationHandler.END
        # final_yes → direkt in Einkaufsliste-Flow springen
        # Wir rufen _fertig_input_ mit context.user_data aus dem /menu-Flow auf.
        # Nutze denselben Context: er erwartet den Text mit Personenanzahl, also frag danach:
        return await ask_for_persons(update, context)




async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    basis = df_gerichte
    reply = f"✅ Google Sheet OK, {len(basis)} Menüs verfügbar.\n"
    if user_id in sessions:
        reply += "🥣 Aktualisierte Auswahl:\n"
        for dish in sessions[user_id]["menues"]:
            # Nummern der Beilagen aus der Session
            sel_nums = sessions[user_id].get("beilagen", {}).get(dish, [])
            # Map Nummer → Beilagen-Name
            beiname = df_beilagen.loc[
                df_beilagen["Nummer"].isin(sel_nums), "Beilagen"
            ].tolist()
            # Grammatik-korrekte Verkettung
            formatted = format_dish_with_sides(dish, beiname)
            reply += f"- {formatted}\n"
    else:
        reply += "ℹ️ Keine aktive Session."
    await update.message.reply_text(reply)



def build_profile_choice_keyboard() -> InlineKeyboardMarkup:
    """Inline-Buttons für die Frage ›Wie möchtest Du fortfahren?‹"""
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Bestehendes Profil",  callback_data="prof_exist"),
            InlineKeyboardButton("Ohne Einschränkung",  callback_data="prof_nolim"),
        ],
        [
            InlineKeyboardButton("Neues Profil",        callback_data="prof_new"),
            InlineKeyboardButton("Mein Profil",         callback_data="prof_show"),
        ]
    ])
    return kb


def build_restriction_keyboard() -> InlineKeyboardMarkup:
    """Vegi / offen – Single-Choice"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🥕 Vegi",  callback_data="res_vegi"),
            InlineKeyboardButton("🍽️ offen", callback_data="res_open"),
        ]
    ])


def build_style_keyboard(selected: set[str]) -> InlineKeyboardMarkup:
    """Mehrfachauswahl Stile + »Alles« + »Fertig«"""

    # Label-Helfer
    def label(key, text):
        return f"✅ {text}" if key in selected else text

    # Label für »Alles«: Haken nur, wenn wirklich alle Stile gewählt
    label_all = "✅ Alles" if selected == ALL_STYLE_KEYS else "Alles"

    rows = [
        [
            InlineKeyboardButton(label("style_klassisch",     "Klassisch"),     callback_data="style_klassisch"),
            InlineKeyboardButton(label("style_international", "International"), callback_data="style_international"),
        ],
        [
            InlineKeyboardButton(label("style_mediterran",    "Mediterran"),    callback_data="style_mediterran"),
            InlineKeyboardButton(label("style_asiatisch",     "Asiatisch"),     callback_data="style_asiatisch"),
        ],
        [
            InlineKeyboardButton(label("style_orient",        "Orientalisch"),  callback_data="style_orient"),
            InlineKeyboardButton(label_all,                   callback_data="style_all"),
        ],
        [
            InlineKeyboardButton("✔️ Fertig", callback_data="style_done"),
        ],
    ]
    return InlineKeyboardMarkup(rows)


def build_weight_keyboard() -> InlineKeyboardMarkup:
    """Single-Choice 1 … 7  +  Egal"""
    rows = [
        [InlineKeyboardButton(str(i), callback_data=f"weight_{i}") for i in range(1, 8)],
        [InlineKeyboardButton("Egal", callback_data="weight_any")],
    ]
    return InlineKeyboardMarkup(rows)


def build_aufwand_keyboard(verteilung: dict, total: int) -> InlineKeyboardMarkup:
    def zeile(label, key):
        anz = verteilung[key]
        return [
            InlineKeyboardButton("➖", callback_data=f"aufwand_{key}_minus"),
            InlineKeyboardButton(f"{label}: {anz} ", callback_data="noop"),
            InlineKeyboardButton("➕", callback_data=f"aufwand_{key}_plus"),
        ]

    rows = [
        zeile("Leicht", "light"),
        zeile("Mittel", "medium"),
        zeile("Aufwändig", "heavy")
    ]

    summe = sum(verteilung.values())
    if summe == total:
        rows.append([InlineKeyboardButton("✔️ Weiter", callback_data="aufwand_done")])
    else:
        rows.append([InlineKeyboardButton(f"{summe}/{total} gewählt", callback_data="noop")])

    return InlineKeyboardMarkup(rows)



def profile_overview_text(p: dict) -> str:
    """Formatiert die Profil-Übersicht"""
    styles_str = "Alle Stile" if not p["styles"] else ", ".join(p["styles"])
    return (
        "🗂 **Dein Profil**\n"
        f"• Ernährungsstil: {p['restriction']}\n"
        f"• Küche: {styles_str}\n"
        f"• Typ: {p['weight'] if p['weight'] else 'Egal'}"
    )

def build_profile_overview_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔄 Neu anlegen", callback_data="prof_overwrite"),
            InlineKeyboardButton("➡️ Weiter",       callback_data="prof_next"),
        ]
    ])



##############################################
#>>>>>>>>>>>>TAUSCHE
##############################################

async def tausche(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    if user_id not in sessions:
        return await update.message.reply_text("⚠️ Nutze erst /menu.")
    args = context.args
    if not args or not all(a.isdigit() for a in args):
        return await update.message.reply_text("❌ Nutzung: /tausche 1 3")

    df       = df_gerichte
    sess     = sessions[user_id]
    menues   = sess["menues"]
    aufw     = sess["aufwand"]

    # 1) Profil-harte Filter: Stil & Einschränkung
    profile  = profiles.get(user_id)
    basis_df = apply_profile_filters(df_gerichte, profile)

    # 2) Globaler Swap-History per Aufwand-Stufe initialisieren
    swap_history = sess.setdefault("swap_history", {1: [], 2: [], 3: []})
    # Beim ersten Mal: die initialen Menüs eintragen
    if all(len(v) == 0 for v in swap_history.values()):
        for dish, lvl in zip(menues, aufw):
            swap_history[lvl].append(dish)



    for arg in args:
        idx = int(arg) - 1
        if 0 <= idx < len(menues):
            # aktueller Slot und Level
            current_dish  = menues[idx]
            current_aufw  = aufw[idx]
            row_cur       = df_gerichte[df_gerichte["Gericht"] == current_dish].iloc[0]
            current_art   = ART_ORDER.get(row_cur["Typ"], 2)

            # a) Andere Slots ausschließen
            other_sel = set(menues) - {current_dish}

            # b) Kandidaten auf gleiche Aufwand-Stufe hart filtern
            cands = set(
                basis_df[basis_df["Aufwand"] == current_aufw]["Gericht"]
            ) - {current_dish} - other_sel

            # c) Aufwand-Fallback (nur wenn cands komplett leer)
            if not cands:
                for lvl in (current_aufw - 1, current_aufw + 1):
                    if 1 <= lvl <= 3:
                        fb = set(
                            basis_df[basis_df["Aufwand"] == lvl]["Gericht"]
                        ) - {current_dish} - other_sel
                        if fb:
                            cands = fb
                            break

            # d) No-Repeat global per Stufe
            used = set(swap_history[current_aufw])
            pool = list(cands - used)
            if not pool:
                # nur diese Stufe zurücksetzen auf die aktuellen Menüs dieser Stufe
                swap_history[current_aufw] = [
                    m for m, lvl in zip(menues, aufw) if lvl == current_aufw
                ]
                used = set(swap_history[current_aufw])
                pool = list(cands - used)
            if not pool:
                continue  # keine Kandidaten mehr


            # e) Scoring nach Aufwand & Art
            scored = []
            for cand in pool:
                row_c    = df_gerichte[df_gerichte["Gericht"] == cand].iloc[0]
                cand_aw  = int(row_c["Aufwand"])
                cand_art = ART_ORDER.get(row_c["Typ"], 2)
                d_aw     = abs(current_aufw - cand_aw)
                d_art    = abs(current_art   - cand_art)
                scored.append((cand, (d_aw, d_art)))

            min_score = min(score for _, score in scored)
            best      = [c for c, score in scored if score == min_score]

            # f) Neues Gericht wählen und History updaten
            # Aktiv-/Gewicht-Bias in Tie-Break
            def _aktiv_weight(name: str) -> float:
                row = df_gerichte[df_gerichte["Gericht"] == name].iloc[0]
                if "Gewicht" in row.index:
                    return float(row["Gewicht"]) or 1.0
                a = int(pd.to_numeric(row.get("Aktiv", 2), errors="coerce"))
                return {0: 0.0, 1: 0.5, 2: 1.0, 3: 2.0}.get(a, 1.0)

            weights = [_aktiv_weight(c) for c in best]
            neu = random.choices(best, weights=weights, k=1)[0]

            menues[idx] = neu
            # → Hier den Aufwand für das neue Gericht in der Session aktualisieren
            sess["aufwand"][idx] = int(
                df_gerichte.loc[df_gerichte["Gericht"] == neu, "Aufwand"].iloc[0]
            )
            swap_history[current_aufw].append(neu)


    save_json(SESSIONS_FILE, sessions)

    if show_debug_for(update):
        # gewählte Gerichte holen
        gewaehlte_gerichte = df_gerichte[df_gerichte["Gericht"].isin(menues)]
        # Aufwand-Verteilung
        aufwand_counter = Counter(gewaehlte_gerichte["Aufwand"])
        aufwand_text    = ", ".join(f"{v} x {k}" for k, v in aufwand_counter.items())
        # Küche-Verteilung
        kitchen_counter    = Counter(gewaehlte_gerichte["Küche"])
        kitchen_text       = ", ".join(f"{v} x {k}" for k, v in kitchen_counter.items())
        # Typ-Verteilung
        typ_counter     = Counter(gewaehlte_gerichte["Typ"])
        typ_text        = ", ".join(f"{v} x {k}" for k, v in typ_counter.items())
        # Ernährungsstil-Verteilung
        einschr_counter = Counter(gewaehlte_gerichte["Ernährungsstil"])
        einschr_text    = ", ".join(f"{v} x {k}" for k, v in einschr_counter.items())

        debug_msg = (
            f"\n📊 Aufwand-Verteilung: {aufwand_text}"
            f"\n🎨 Küche-Verteilung:    {kitchen_text}"
            f"\n⚙️ Typ-Verteilung:      {typ_text}"
            f"\n🥗 Ernährungsstil:       {einschr_text}"
        )
        await update.message.reply_text(debug_msg)

    await update.message.reply_text(
        "🔄 Neue Menüs:\n" +
        "\n".join(f"{i+1}. {g}" for i, g in enumerate(menues))
    )

async def tausche_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Entry-Point für '/tausche' ohne Argumente:
    Zeigt ein Inline-Keyboard mit den Menü-Indizes 1…N zum Mehrfach-Tausch.
    """
    # 1) Nur ohne args auf diesen Flow springen
    if context.args:
        return

    # 2) Session prüfen
    uid = str(update.effective_user.id)
    if uid not in sessions:
        await update.message.reply_text("⚠️ Bitte starte erst mit /menu.")
        return

    # 3) Swap-Kandidaten resetten und Keyboard senden
    # Swap-Flow starten: Tracking initialisieren
    context.user_data["swap_candidates"] = set()
    kb = build_swap_keyboard(sessions[uid]["menues"], set())
    msg = await update.message.reply_text(
        pad_message("Welche Gerichte möchtest Du tauschen?"),
        reply_markup=kb
    )
    # erste Prompt-Nachricht tracken
    context.user_data["flow_msgs"] = [msg.message_id]
    return TAUSCHE_SELECT


async def aufwand_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    verteilung = context.user_data["aufwand_verteilung"]
    total = context.user_data["menu_count"]

    if data.startswith("aufwand_") and data.count("_") == 2:
        _, key, op = data.split("_")
        if key not in verteilung:
            return MENU_AUFWAND

        changed = False

        if op == "plus":
            if sum(verteilung.values()) < total:
                verteilung[key] += 1
                changed = True
            else:
                await query.answer("Du hast schon alle Gerichte verteilt.", show_alert=False)
        elif op == "minus":
            if verteilung[key] > 0:
                verteilung[key] -= 1
                changed = True

        if changed:
            await query.message.edit_reply_markup(
                reply_markup=build_aufwand_keyboard(verteilung, total)
            )

        return MENU_AUFWAND

    elif data == "aufwand_done":
        if sum(verteilung.values()) != total:
            await query.answer("Noch nicht vollständig verteilt!", show_alert=True)
            return MENU_AUFWAND

        # Werte übernehmen und weiterreichen als Text wie bisher
        context.user_data["flow_msgs"].append(query.message.message_id)
        a1 = verteilung["light"]
        a2 = verteilung["medium"]
        a3 = verteilung["heavy"]
        total = a1 + a2 + a3  # sichere Gesamtmenge, basierend auf den realen Klicks

        return await menu_input_direct(f"{total} ({a1},{a2},{a3})", update, context)


    elif data == "noop":
        return MENU_AUFWAND




async def tausche_select_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback, um per Inline-Button mehrere Gerichte zu markieren."""
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)

    # current selection
    sel = context.user_data.setdefault("swap_candidates", set())
    data = q.data  # "swap_sel:2" oder "swap_done"

    # 1) Toggle-Logik für Auswahl
    if data.startswith("swap_sel:"):
        idx = int(data.split(":", 1)[1])
        if idx in sel:
            sel.remove(idx)
        else:
            sel.add(idx)

        # komplette Nachricht neu senden, damit Telegram das Label neu rendert
        text = "Welche Gerichte möchtest Du tauschen?"
        kb = build_swap_keyboard(sessions[uid]["menues"], sel)
        try:
            await q.edit_message_text(pad_message(text), reply_markup=kb)
        except BadRequest as e:
            # Ignoriere „Message is not modified“-Fehler, wir ändern ja nur bei echtem Wechsel
            if "Message is not modified" not in str(e):
                raise

        return TAUSCHE_SELECT

    # 2) 'Fertig' ohne Auswahl → Warnung
    if data == "swap_done" and not sel:
        await q.answer("Nichts ausgewählt.", show_alert=True)
        return TAUSCHE_SELECT

    # 3) 'Fertig' mit Auswahl → weiter in Schritt 4

    if data == "swap_done":
        # 1) Profil-hard filter + Basis-DataFrame
        profile  = profiles.get(uid)
        basis_df = apply_profile_filters(df_gerichte, profile)

        sessions[uid].setdefault("beilagen", {})
        menues   = sessions[uid]["menues"]
        aufw     = sessions[uid]["aufwand"]
        # Globaler Swap-History per Aufwand-Stufe initialisieren
        swap_history = sessions[uid].setdefault("swap_history", {1: [], 2: [], 3: []})
        # Beim ersten Mal: die initialen Menüs eintragen
        if all(len(v) == 0 for v in swap_history.values()):
            for dish, lvl in zip(menues, aufw):
                swap_history[lvl].append(dish)


        swapped_slots: list[int] = []
        for idx in sorted(sel):
            slot          = idx - 1
            current_dish  = menues[slot]
            current_aufw  = aufw[slot]
            row_cur       = df_gerichte[df_gerichte["Gericht"] == current_dish].iloc[0]
            current_art   = ART_ORDER.get(row_cur["Typ"], 2)

            # a) Andere Slots ausschließen
            other_sel = set(menues) - {current_dish}

            # b) Kandidaten auf diese Aufwand-Stufe einschränken
            cands = set(
                basis_df[basis_df["Aufwand"] == current_aufw]["Gericht"]
            ) - {current_dish} - other_sel

            # c) Aufwand-Fallback
            if not cands:
                for lvl in (current_aufw - 1, current_aufw + 1):
                    if 1 <= lvl <= 3:
                        fb = set(
                            basis_df[basis_df["Aufwand"] == lvl]["Gericht"]
                        ) - {current_dish} - other_sel
                        if fb:
                            cands = fb
                            break

            # d) No-Repeat global per Stufe
            used = set(swap_history[current_aufw])
            pool = list(cands - used)
            if not pool:
                swap_history[current_aufw] = [
                    m for m, lvl in zip(menues, aufw) if lvl == current_aufw
                ]
                used = set(swap_history[current_aufw])
                pool = list(cands - used)
            if not pool:
                continue


            # e) Scoring nach Aufwand & Art
            scored = []
            for cand in pool:
                row_c    = df_gerichte[df_gerichte["Gericht"] == cand].iloc[0]
                cand_aw  = int(row_c["Aufwand"])
                cand_art = ART_ORDER.get(row_c["Typ"], 2)
                d_aw     = abs(current_aufw - cand_aw)
                d_art    = abs(current_art   - cand_art)
                scored.append((cand, (d_aw, d_art)))

            min_score = min(score for _, score in scored)
            best      = [c for c, score in scored if score == min_score]

            # f) Tausche & History updaten
            # Aktiv-/Gewicht-Bias in Tie-Break
            def _aktiv_weight(name: str) -> float:
                row = df_gerichte[df_gerichte["Gericht"] == name].iloc[0]
                if "Gewicht" in row.index:
                    return float(row["Gewicht"]) or 1.0
                a = int(pd.to_numeric(row.get("Aktiv", 2), errors="coerce"))
                return {0: 0.0, 1: 0.5, 2: 1.0, 3: 2.0}.get(a, 1.0)

            weights = [_aktiv_weight(c) for c in best]
            neu = random.choices(best, weights=weights, k=1)[0]

            menues[slot] = neu
            sessions[uid]["aufwand"][slot] = int(
                df_gerichte.loc[df_gerichte["Gericht"] == neu, "Aufwand"].iloc[0]
            )
            swap_history[current_aufw].append(neu)
            sessions[uid]["beilagen"].pop(current_dish, None)
            swapped_slots.append(idx)




        # Änderungen speichern
        save_json(SESSIONS_FILE, sessions)
        context.user_data["swapped_indices"] = swapped_slots

        if show_debug_for(update):
            gewaehlte_gerichte = df_gerichte[df_gerichte["Gericht"].isin(sessions[uid]["menues"])]
            aufwand_counter = Counter(gewaehlte_gerichte["Aufwand"])
            aufwand_text    = ", ".join(f"{v} x {k}" for k, v in aufwand_counter.items())
            kitchen_counter    = Counter(gewaehlte_gerichte["Küche"])
            kitchen_text       = ", ".join(f"{v} x {k}" for k, v in kitchen_counter.items())
            typ_counter     = Counter(gewaehlte_gerichte["Typ"])
            typ_text        = ", ".join(f"{v} x {k}" for k, v in typ_counter.items())
            einschr_counter = Counter(gewaehlte_gerichte["Ernährungsstil"])
            einschr_text    = ", ".join(f"{v} x {k}" for k, v in einschr_counter.items())

            debug_msg = (
                f"\n📊 Aufwand-Verteilung: {aufwand_text}"
                f"\n🎨 Küche-Verteilung:    {kitchen_text}"
                f"\n⚙️ Typ-Verteilung:      {typ_text}"
                f"\n🥗 Ernährungsstil:       {einschr_text}"
            )
            msg_debug = await q.message.reply_text(debug_msg)
            context.user_data["flow_msgs"].append(msg_debug.message_id)



        # 3) Neue Liste als eigene Nachricht senden + tracken
        menutext = "\n".join(f"{i}. {g}" for i, g in enumerate(menues, 1))
        msg1 = await q.message.reply_text(pad_message(f"🥣 Deine Gerichte:\n{menutext}"))
        context.user_data["flow_msgs"].append(msg1.message_id)

        # 4) Frage separat senden + tracken
        confirm_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("Ja",   callback_data="swap_ok"),
            InlineKeyboardButton("Nein", callback_data="swap_again"),
        ]])
        msg2 = await q.message.reply_text(pad_message("Passen diese Gerichte?"), reply_markup=confirm_kb)
        context.user_data["flow_msgs"].append(msg2.message_id)

        return TAUSCHE_CONFIRM



# ─────────── tausche_confirm_cb ───────────
async def tausche_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    chat_id = q.message.chat.id
    uid     = str(q.from_user.id)

    if q.data == "swap_again":
        # 1) Visuelles Feedback zurücksetzen
        await mark_yes_no(q, False, "swap_ok", "swap_again")

        # 2) Swap-Selection-State komplett löschen
        context.user_data["swap_candidates"] = set()

        # 3) Nur die letzte Frage löschen (nicht die Auswahl-Liste)
        flow = context.user_data.get("flow_msgs", [])
        if flow:
            last_id = flow.pop()
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=last_id)
            except:
                pass

        # 4) Neuer Tausche-Prompt mit leerem Kandidaten-Set
        kb = build_swap_keyboard(sessions[uid]["menues"], context.user_data["swap_candidates"])
        msg = await q.message.reply_text(
            pad_message("Welche Gerichte möchtest Du tauschen?"),
            reply_markup=kb
        )
        context.user_data["flow_msgs"].append(msg.message_id)
        return TAUSCHE_SELECT


    if q.data == "swap_ok":
        await mark_yes_no(q, True, "swap_ok", "swap_again")
        # nur letzte Frage löschen
        flow = context.user_data.get("flow_msgs", [])
        if flow:
            last_id = flow.pop()
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=last_id)
            except:
                pass

        # jetzt gleiche Beilagen-Logik wie oben in menu_confirm_cb:
        menus = sessions[uid]["menues"]
        side_menus = []
        for idx, dish in enumerate(menus):
            raw = df_gerichte.loc[df_gerichte["Gericht"] == dish, "Beilagen"].iloc[0]
            codes = [c for c in parse_codes(raw) if c != 0]
            if codes:
                side_menus.append(idx)

        # 0 Beilagen-Menüs
        if not side_menus:
            for mid in context.user_data.get("flow_msgs", []):
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=mid)
                except:
                    pass
            context.user_data["flow_msgs"].clear()

            text = "🥣 Deine finale Liste:\n"
            for dish in menus:
                nums       = sessions[uid].get("beilagen", {}).get(dish, [])
                side_names = df_beilagen.loc[df_beilagen["Nummer"].isin(nums), "Beilagen"].tolist()
                text      += f"- {format_dish_with_sides(dish, side_names)}\n"
            msg = await q.message.reply_text(pad_message(text))
            context.user_data["flow_msgs"].append(msg.message_id)
            return await ask_for_persons(update, context)

        # 1 Beilagen-Menü
        if len(side_menus) == 1:
            context.user_data["menu_list"] = menus
            context.user_data["to_process"] = side_menus
            context.user_data["menu_idx"]    = 0
            return await ask_beilagen_for_menu(q, context)

        # >1 Beilagen-Menüs
        context.user_data["menu_list"] = menus
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("Ja",   callback_data="ask_yes"),
            InlineKeyboardButton("Nein", callback_data="ask_no"),
        ]])
        msg = await q.message.reply_text(pad_message("Möchtest du Beilagen hinzufügen?"), reply_markup=kb)
        context.user_data["flow_msgs"].append(msg.message_id)
        return ASK_BEILAGEN

    return ConversationHandler.END




##############################################
#>>>>>>>>>>>>FERTIG
##############################################

async def fertig_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.message.from_user.id) not in sessions:
        await update.message.reply_text("⚠️ Keine Menüs gewählt.")
        return ConversationHandler.END
    await update.message.reply_text(pad_message("Für wie viele Personen?"))
    return FERTIG_PERSONEN

async def fertig_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Personenzahl: zuerst aus context.user_data (Buttons), sonst aus Text
    user_id = str(update.effective_user.id)
    if "temp_persons" in context.user_data:
        personen = context.user_data.pop("temp_persons")
    else:
        try:
            personen = int(update.message.text.strip())
            if personen <= 0:
                       raise ValueError
        except:
            await update.message.reply_text("⚠️ Ungültige Zahl.")
            return PERSONS_MANUAL


    faktor = personen / 4
    # 1) Zutaten für Hauptgerichte
    df = df_zutaten
    ausgew = sessions[user_id]["menues"]
    context.user_data["final_list"] = ausgew

    zut_gericht = df[
        (df["Typ"] == "Gericht") &
        (df["Gericht"].isin(ausgew))
    ].copy()

    # === neu: Zutaten aller gewählten Beilagen sammeln ===
    all_nums = sum(sessions[user_id].get("beilagen", {}).values(), [])
    beilage_names = df_beilagen.loc[df_beilagen["Nummer"].isin(all_nums), "Beilagen"].tolist()
    zut_beilage = df[
        (df["Typ"] == "Beilagen") &
        (df["Gericht"].isin(beilage_names))
    ].copy()

    # 2) Beide DataFrames zusammenführen und skalieren
    zut = pd.concat([zut_gericht, zut_beilage], ignore_index=True)
    zut["Menge"] *= faktor

    #Bei Vegi-Profil: alle Fleisch-Zutaten entfernen
    profile = profiles.get(user_id)  # oder wie du dein Profil-Objekt ablegst
    if profile and profile.get("restriction") == "Vegi":
        zut = zut[zut["Kategorie"] != "Fleisch"]



    # --- Emoji-Gruppierte Einkaufsliste ---
    eink = (
        zut.groupby(["Zutat", "Kategorie", "Einheit"])
        .agg(
            Menge     = ("Menge",     "sum"),
            Menge_raw = ("Menge_raw", "first")
        )
        .reset_index()
        .sort_values(["Kategorie", "Zutat"])
    )

    eink_text = f"\n<b><u>🛒 Einkaufsliste für {personen} Personen:</u></b>\n"
    # nach Kategorie gruppieren
    for cat, group in eink.groupby("Kategorie"):
        emoji = CAT_EMOJI.get(cat, "")
        eink_text += f"\n{emoji} <u>{escape(str(cat))}</u>\n"
        for _, r in group.iterrows():
            raw = str(r["Menge_raw"]).strip()
            if not raw.replace(".", "").isdigit():  # kein reiner Zahl‐String
                txt = raw or "wenig"
                line = f"- {r.Zutat}: {txt}"
            else:
                amt  = format_amount(r.Menge)
                line = f"- {r.Zutat}: {amt} {r.Einheit}"
            eink_text += f"{line}\n"


   
    # --- Kochliste mit Hauptgericht- und Beilagen-Zutaten in der richtigen Reihenfolge ---
    koch_text = f"\n<b><u>🍽 Kochliste für {personen} Personen:</u></b>\n"

    for g in ausgew:
        # 1) Namen der gewählten Beilagen holen
        sel_nums       = sessions[user_id].get("beilagen", {}).get(g, [])
        beilagen_namen = df_beilagen.loc[
            df_beilagen["Nummer"].isin(sel_nums), "Beilagen"
        ].tolist()

        # 2) Zutaten für Hauptgericht
        part_haupt = zut[(zut["Typ"] == "Gericht") & (zut["Gericht"] == g)]

        # 3) Zutaten für jede Beilage nacheinander
        parts_list = [part_haupt]
        for b in beilagen_namen:
            part_b = zut[(zut["Typ"] == "Beilagen") & (zut["Gericht"] == b)]
            parts_list.append(part_b)

        # 4) Zusammenführen
        part = pd.concat(parts_list, ignore_index=True)

        # 5) Zutaten-Text bauen (HTML-escapen!)
        ze_parts = []
        for _, row in part.iterrows():
            raw = str(row["Menge_raw"]).strip()
            if not raw.replace(".", "").isdigit():
                txt = raw or "wenig"
                ze_parts.append(f"{row['Zutat']} {txt}")
            else:
                amt = format_amount(row["Menge"])
                ze_parts.append(f"{row['Zutat']} {amt} {row['Einheit']}")
        ze_html = escape(", ".join(ze_parts))                                                                                                                                                    # hier definieren, wie zutaten getrennt werden

        # 6) Titel bauen: Name fett & verlinkt (falls Link vorhanden), kein "- " mehr davor
        full_title = format_dish_with_sides(g, beilagen_namen)
        rest       = full_title[len(g):] if full_title.startswith(g) else ""

        # Link aus Gerichte-Tabelle laden (leerer String → kein Link)
        try:
            link_value = str(
                df_gerichte.loc[df_gerichte["Gericht"] == g, "Link"].iloc[0]
            ).strip()
        except Exception:
            link_value = ""

        name_html = f"<b>{escape(g)}</b>"
        if link_value:
            name_html = f'<b><a href="{escape(link_value, quote=True)}">{escape(g)}</a></b>'

        # Beilagen-Teil nur fett (nie verlinkt)
        rest_html = f"<b>{escape(rest)}</b>" if rest else ""

        # Aufwand (1/2/3) -> Text und HTML-escapen (wegen < und >)
        aufwand_label_html = ""
        try:
            aufwand_raw = df_gerichte.loc[df_gerichte["Gericht"] == g, "Aufwand"].iloc[0]
            mapping = {"1": "(<30min)", "2": "(30-60min)", "3": "(>60min)"}
            aufwand_txt = mapping.get(str(aufwand_raw).strip(), "")
            if aufwand_txt:
                aufwand_label_html = f"<i>{escape(aufwand_txt)}</i>"
        except Exception:
            aufwand_label_html = ""


        display_title_html = f"{name_html}{rest_html}{f' {aufwand_label_html}' if aufwand_label_html else ''}"
        koch_text += f"\n{display_title_html}\n{ze_html}\n"





    # — alle bisherigen Flow-Nachrichten löschen —
    chat_id = update.effective_chat.id
    for mid in context.user_data.get("flow_msgs", []):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except:
            pass
    context.user_data["flow_msgs"].clear()


    # — Einkaufs- & Kochliste senden —
    await context.bot.send_message(
        chat_id=chat_id,
        text= koch_text + eink_text,
        parse_mode="HTML",
        disable_web_page_preview=True
    )

    # — Für Exporte speichern —
    context.user_data["einkaufsliste_df"] = eink
    context.user_data["kochliste_text"]     = koch_text

    # — Export-Buttons senden —
    keyboard = InlineKeyboardMarkup([
        [ InlineKeyboardButton("🔖 Gerichte zu Favoriten hinzufügen", callback_data="favoriten") ],
        [ InlineKeyboardButton("🛒 Einkaufsliste in Bring! exportieren", callback_data="export_bring") ],
        [ InlineKeyboardButton("📄 Als PDF exportieren",   callback_data="export_pdf")   ],
        [ InlineKeyboardButton("🔄 Das passt so. Neustart!", callback_data="restart")      ],
    ])
    await context.bot.send_message(
        chat_id=chat_id,
        text="Deine Listen sind bereit – was möchtest du tun?",
        reply_markup=keyboard
    )

    return ConversationHandler.END


##############################################
#>>>>>>>>>>>>EXPORTE / FINALE
##############################################

###################---------------------- Export to Bring--------------------

async def export_to_bring(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Erstellt HTML-Rezept-Gist → Deeplink → Sendet Bring-Button"""
    query = update.callback_query
    await query.answer()

    eink = context.user_data.get("einkaufsliste_df")
    if eink is None:
        await query.edit_message_text("❌ Keine Einkaufsliste gefunden.")
        return ConversationHandler.END

    # --- 1) JSON-LD aufbereiten -------------------------------------------
    recipe_ingredients = [
        f"{format_amount(r.Menge)} {r.Einheit} {r.Zutat}"
        for _, r in eink.iterrows()
    ]
    recipe_jsonld = {
        "@context": "https://schema.org",
        "@type":    "Recipe",
        "name":     "Einkaufsliste",
        "author":   {"@type": "Organization", "name": "FoodApp"},
        "recipeIngredient": recipe_ingredients,
    }

    html_content = (
        "<!doctype html><html><head>"
        "<meta charset='utf-8'>"
        "<script type='application/ld+json'>"
        f"{json.dumps(recipe_jsonld, ensure_ascii=False)}</script>"
        "</head><body></body></html>"
    )

    logging.info("Bring-Import JSON: %s", json.dumps(recipe_jsonld, ensure_ascii=False))

    # --- 2) Öffentlichen Gist anlegen --------------------------------------
    if not GITHUB_TOKEN:
        await query.edit_message_text(
            "❌ Kein GitHub-Token gefunden (Umgebungsvariable GITHUB_TOKEN). "
            "Ohne öffentliches Rezept kann Bring! nichts importieren."
        )
        return ConversationHandler.END

    headers = {"Authorization": f"token {GITHUB_TOKEN}",
               "Accept": "application/vnd.github+json"}
    gist_payload = {
        "description": "FoodApp – temporärer Bring-Recipe-Import",
        "public": True,
        "files": {"recipe.html": {"content": html_content}},
    }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            gist_resp = await client.post(
                "https://api.github.com/gists",
                json=gist_payload,
                headers=headers,
            )
            gist_resp.raise_for_status()
            raw_url = gist_resp.json()["files"]["recipe.html"]["raw_url"]

            # --- 3) Deeplink von Bring holen --------------------------------
            dl_resp = await client.get(
                "https://api.getbring.com/rest/bringrecipes/deeplink",
                params={"url": raw_url, "source": "web"},
                follow_redirects=False,
            )

        if dl_resp.status_code in (301, 302, 303, 307, 308):
            deeplink = dl_resp.headers.get("location")
        else:
            dl_resp.raise_for_status()          # echte Fehler
            deeplink = dl_resp.json().get("deeplink")

        if not deeplink:
            raise RuntimeError("Kein Deeplink erhalten")

        logging.info("Erhaltener Bring-Deeplink: %s", deeplink)

    except (httpx.HTTPError, RuntimeError) as err:
        logging.error("Fehler bei Bring-Export: %s", err)
        await query.edit_message_text(
            "❌ Bring-Export fehlgeschlagen. Versuche es später erneut."
        )
        return ConversationHandler.END

    # --- 4) Button senden & Conversation beenden ---------------------------
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("In Bring! importieren", url=deeplink)]]
    )
    await query.edit_message_text(
        "🛒 Einkaufsliste an Bring! senden:",
        reply_markup=kb
    )
    await send_action_menu(query.message)
    return EXPORT_OPTIONS


###################---------------------- PDF Export--------------------

async def export_to_pdf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fragt, welche Listen exportiert werden sollen."""
    query = update.callback_query
    await query.answer()

    eink_df   = context.user_data.get("einkaufsliste_df")
    koch_text = context.user_data.get("kochliste_text")
    if eink_df is None or eink_df.empty or not koch_text:
        return await query.edit_message_text("❌ Keine Listen zum Export gefunden.")

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Einkaufsliste", callback_data="pdf_export_einkauf")],
        [InlineKeyboardButton("Kochliste",     callback_data="pdf_export_koch")],
        [InlineKeyboardButton("Beides",       callback_data="pdf_export_beides")],
    ])
    await query.edit_message_text("Was brauchst Du im PDF Export?", reply_markup=kb)
    return PDF_EXPORT_CHOICE


class PDF(FPDF):
    """FPDF mit Kopf-/Fußzeile und 2 cm Seitenrändern."""
    def __init__(self, date_str: str):
        super().__init__()
        self.date_str = date_str
        # 2 cm = 20 mm
        self.set_margins(20, 20, 20)              # links, oben, rechts
        self.set_auto_page_break(auto=True, margin=20)  # unten
        self.alias_nb_pages()  # ermöglicht {nb} (TotalSeiten)

    def header(self):
        # Kopfzeile: "Foodylenko - DD.MM.YYYY", zentriert
        self.set_y(10)
        # Core-Font verwenden, da add_font evtl. erst nach add_page kommt
        self.set_font("Helvetica", "B", 10)
        self.cell(0, 8, f"Foodylenko - {self.date_str}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")

    def footer(self):
        # Fußzeile: "Seite X/TotalSeiten", zentriert unten
        self.set_y(-15)
        self.set_font("Helvetica", "", 9)
        # {nb} wird beim finalen Rendern durch TotalSeiten ersetzt
        self.cell(0, 8, f"Seite {self.page_no()}/{{nb}}", new_x=XPos.RIGHT, new_y=YPos.TOP, align="C")




async def process_pdf_export_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    choice = q.data.split("_")[-1]  # "einkauf", "koch" oder "beides"

    eink_df   = context.user_data.get("einkaufsliste_df")
    koch_text = context.user_data.get("kochliste_text")

    # PDF initialisieren (mit Kopf-/Fußzeile und 2 cm Rändern)
    date_str = datetime.now().strftime("%d.%m.%Y")
    pdf = PDF(date_str)  # <<--- WICHTIG: unsere Unterklasse benutzen
    pdf.add_font("DejaVu",   "",  "fonts/DejaVuSans.ttf")
    pdf.add_font("DejaVu",   "B", "fonts/DejaVuSans-Bold.ttf")
    pdf.add_page()


    # ---------- Helper: KOCHLISTE (zweizeilig: Titel-Zeile, Zutaten-Zeile) ----------
    def write_kochliste():
        pdf.set_font("DejaVu", "B", 14)
        pdf.cell(0, 10, "Kochliste", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        # Header-Zeile(n) und Leerzeilen robust entfernen
        raw_lines = koch_text.splitlines()
        lines = []
        for l in raw_lines:
            if not l.strip():
                continue
            plain = unescape(re.sub(r"<[^>]+>", "", l))
            if "Kochliste" in plain:  # filtert z.B. "<b><u>🍽 Kochliste …</u></b>"
                continue
            lines.append(l)

        # Paarbildung: (Titel, Zutaten)
        last_title = None
        for l in lines:
            if last_title is None:
                last_title = l
                continue

            title_html = last_title
            ingredients_html = l
            last_title = None

            # HTML → Plaintext
            title_plain       = unescape(re.sub(r"<[^>]+>", "", title_html))
            ingredients_plain = unescape(re.sub(r"<[^>]+>", "", ingredients_html))

            # Ausgabe
            pdf.set_x(pdf.l_margin)
            pdf.set_font("DejaVu", "B", 12)
            pdf.multi_cell(pdf.epw, 8, title_plain, align="L")

            pdf.set_x(pdf.l_margin)
            pdf.set_font("DejaVu", "", 12)
            pdf.multi_cell(pdf.epw, 8, ingredients_plain, align="L")
            pdf.ln(2)

        # Falls am Ende eine Titelzeile ohne Zutaten übrig bleibt
        if last_title is not None:
            title_plain = unescape(re.sub(r"<[^>]+>", "", last_title))
            pdf.set_x(pdf.l_margin)
            pdf.set_font("DejaVu", "B", 12)
            pdf.multi_cell(pdf.epw, 8, title_plain, align="L")
            pdf.ln(2)

    # ---------- Helper: EINKAUFSLISTE (2 Spalten, Kategorien als Überschriften) ----------
    def write_einkaufsliste():
        # Überschrift
        pdf.set_font("DejaVu", "B", 14)
        pdf.cell(0, 10, "Einkaufsliste", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        # Spalten-Setup
        col_gap = 8
        col_w   = (pdf.epw - col_gap) / 2
        left_x  = pdf.l_margin
        right_x = pdf.l_margin + col_w + col_gap
        start_y = pdf.get_y()  # Start nach der Überschrift
        col     = 0            # 0 = links, 1 = rechts
        pdf.set_xy(left_x, start_y)  # <<— harte Positionierung auf Spaltenanfang


        def current_x():
            return left_x if col == 0 else right_x

        def page_bottom():
            return pdf.h - pdf.b_margin

        def switch_column():
            nonlocal col, start_y, left_x, right_x, col_w
            if col == 0:
                # auf rechte Spalte derselben Seite
                col = 1
                pdf.set_xy(right_x, start_y)
            else:
                # neue Seite + Überschrift + wieder linke Spalte
                pdf.add_page()
                pdf.set_font("DejaVu", "B", 14)
                pdf.cell(0, 10, "Einkaufsliste", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                # Neu berechnen (Seitenbreite kann sich u.U. geändert haben)
                col_w   = (pdf.epw - col_gap) / 2
                left_x  = pdf.l_margin
                right_x = pdf.l_margin + col_w + col_gap
                start_y = pdf.get_y()
                col = 0
                pdf.set_xy(left_x, start_y)

        def ensure_space(height_needed: float):
            # wenn das Element nicht mehr in die aktuelle Spalte passt → Spaltenwechsel/Seitenwechsel
            if pdf.get_y() + height_needed <= page_bottom():
                return
            switch_column()

        def calc_item_height(txt: str, line_h: float = 6.0) -> float:
            # grobe Zeilenanzahl anhand Textbreite
            width = pdf.get_string_width(txt)
            lines = max(1, math.ceil(width / col_w)) if col_w > 0 else 1
            return lines * line_h

        # nach Kategorie gruppiert (wie im Chat), Items darunter
        # Hinweis: CAT_EMOJI & format_amount sind global definert. :contentReference[oaicite:2]{index=2}
        pdf.set_font("DejaVu", "", 12)
        for cat, group in eink_df.sort_values(["Kategorie", "Zutat"]).groupby("Kategorie"):
            # Kategorie-Überschrift (exakt linksbündig in der Spalte)
            head = str(cat)
            ensure_space(8)
            pdf.set_font("DejaVu", "B", 12)
            pdf.set_x(current_x())  # nur X setzen, Y unverändert lassen
            pdf.multi_cell(col_w, 8, head, align="L")
            pdf.set_x(current_x())  # nach MultiCell X wieder exakt auf Spaltenanfang
            pdf.set_font("DejaVu", "", 12)



            # Items der Kategorie
            for _, row in group.iterrows():
                raw = str(row["Menge_raw"]).strip()
                if not raw.replace(".", "").isdigit():
                    txt  = raw or "wenig"
                    line = f"- {row['Zutat']}: {txt}"
                else:
                    amt  = format_amount(row["Menge"])
                    line = f"- {row['Zutat']}: {amt} {row['Einheit']}"

                h = calc_item_height(line, line_h=6)
                ensure_space(h)
                pdf.set_xy(current_x(), pdf.get_y())
                pdf.multi_cell(col_w, 6, line, align="L")

            # kleiner Abstand nach jeder Kategorie
            ensure_space(2)
            pdf.set_xy(current_x(), pdf.get_y() + 2)

    # ---------- Reihenfolge je nach Wahl ----------
    if choice == "koch":
        write_kochliste()
    elif choice == "einkauf":
        write_einkaufsliste()
    else:  # "beides" → Kochliste zuerst, dann Seitenumbruch, dann Einkaufsliste
        write_kochliste()
        pdf.add_page()
        write_einkaufsliste()

    # --- Speichern & Senden ---
    filename = f"liste_{q.from_user.id}.pdf"
    pdf.output(filename)
    await q.edit_message_text("📄 Hier ist dein PDF:")
    with open(filename, "rb") as f:
        await q.message.reply_document(document=f, filename="Liste.pdf")
    os.remove(filename)

    # sende allgemeines Aktions-Menu und kehre in den EXPORT_OPTIONS-State zurück
    await send_action_menu(q.message)
    return EXPORT_OPTIONS


###################---------------------- NEUSTART FLOW--------------------


async def restart_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry-Point für Neustart-Button: fragt nach Bestätigung."""
    q = update.callback_query
    await q.answer()
    text = "🔄 Bist Du sicher? Die Gerichtsauswahl wird zurückgesetzt (Favoriten bleiben bestehen)"
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Ja",   callback_data="restart_yes"),
        InlineKeyboardButton("Nein", callback_data="restart_no"),
    ]])
    await q.edit_message_text(text, reply_markup=kb)
    return RESTART_CONFIRM



async def restart_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    chat_id = q.message.chat.id

    # 1) Lösche die Bestätigungs-Nachricht
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=q.message.message_id)
    except:
        pass

    # 2) Bei "Ja": Abschied, kurze Pause, dann Start-Übersicht
    if q.data == "restart_yes":
        bye = await context.bot.send_message(chat_id, pad_message("Super, bis bald!👋"))
        await asyncio.sleep(1)

        # Abschieds-Nachricht entfernen
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=bye.message_id)
        except:
            pass

        overview = (
            "Übersicht der Befehle:\n\n"
            "🍲 Menü – Lass Dir leckere Gerichte vorschlagen\n\n"
            "⚡ QuickOne – Ein Gericht ohne Einschränkungen\n\n"
            "🔖 Favoriten – Deine Favoriten\n\n"
            "🛠️ Übersicht – Alle Funktionen"
        )
        await context.bot.send_message(chat_id, overview)

        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🍲 Menü",      callback_data="restart_menu"),
            InlineKeyboardButton("⚡ QuickOne",  callback_data="restart_quickone")],
            [InlineKeyboardButton("🔖 Favoriten", callback_data="restart_favs"),
            InlineKeyboardButton("🛠️ Übersicht", callback_data="restart_setup"),
        ]])
        await context.bot.send_message(chat_id, pad_message("Wähle eine Option:"), reply_markup=kb)

        return ConversationHandler.END

    # 3) Bei "Nein": wie nach Export/Bring/Favoriten-Flow ins Aktions-Menu
    #    ("Was möchtest Du weiter tun?" + Favoriten/Bring/PDF/Restart-Buttons)
    await send_action_menu(q.message)
    return EXPORT_OPTIONS




##############################################
#>>>>>>>>>>>>FAVORITEN
##############################################

async def favorit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    if user_id not in sessions:
        return await update.message.reply_text("⚠️ Bitte erst /menu.")
    if not context.args or not context.args[0].isdigit():
        return await update.message.reply_text("❌ Nutzung: /favorit 2")
    idx = int(context.args[0]) - 1
    menues = sessions[user_id]["menues"]
    if 0<=idx<len(menues):
        fav = menues[idx]
        favorites.setdefault(user_id, []).append(fav)
        save_json(FAVORITES_FILE, favorites)
        await update.message.reply_text(f"❤️ '{fav}' als Favorit gespeichert.")
    else:
        await update.message.reply_text("❌ Ungültiger Index.")


# ===================================== FAVORITEN–FLOW (anschauen & löschen)=============================
async def fav_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry-Point für /meinefavoriten oder Button „Favoriten“."""
    msg = update.message or update.callback_query.message
    user_id = str(update.effective_user.id)
    favs = favorites.get(user_id, [])
    # IDs aller Loop-Nachrichten sammeln
    context.user_data["fav_msgs"] = []

    if not favs:
        await msg.reply_text("Keine Favoriten vorhanden. Füge diese später hinzu!")
        await send_main_buttons(msg)
        return ConversationHandler.END

    # Übersicht senden und ID speichern
# Übersicht senden und ID speichern
    txt = "⭐ Deine Favoriten:\n" + "\n".join(f"- {d}" for d in favs)
    m1 = await msg.reply_text(pad_message(txt))
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Selektieren", callback_data="fav_action_select"),
        InlineKeyboardButton("Entfernen",   callback_data="fav_action_remove"),
        InlineKeyboardButton("Zurück",      callback_data="fav_action_back")
    ]])
    m2 = await msg.reply_text(
        "Was möchtest Du machen?\n\n"
        "🤩 Favoriten für Gerichteauswahl *selektieren*\n"
        "❌ Favoriten aus Liste *entfernen*\n"
        "⏪ *Zurück* zum Hauptmenü\n"
        , reply_markup=kb)
    context.user_data["fav_msgs"].extend([m1.message_id, m2.message_id])
    return FAV_OVERVIEW


async def fav_overview_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    msg = q.message

    # „Nein“: alle gesammelten Loop-Nachrichten löschen & zurück ins Hauptmenü
    if q.data == "fav_edit_no":
        for mid in context.user_data.get("fav_msgs", []):
            try:
                await context.bot.delete_message(chat_id=msg.chat.id, message_id=mid)
            except:
                pass
    #    await send_main_buttons(msg)                    #ggf. einfügen, wenn man nochmals Buttons angezeigt bekommen möchte
        return ConversationHandler.END

    # „Ja“: Auswahlmodus starten, IDs weiter sammeln
    if q.data == "fav_edit_yes":
        await mark_yes_no(q, True, "fav_edit_yes", "fav_edit_no")
        uid   = str(q.from_user.id)
        favs  = favorites.get(uid, [])
        context.user_data["fav_total"]   = len(favs)
        context.user_data["fav_del_sel"] = set()

        # Liste senden + ID speichern
        list_msg = await msg.reply_text(
            "Welche Favoriten löschen?\n" +
            "\n".join(f"{i}. {d}" for i, d in enumerate(favs, start=1))
        )
        context.user_data["fav_msgs"].append(list_msg.message_id)

        # Keyboard senden + ID speichern
        sel_msg = await msg.reply_text(
            "Wähle Nummern (Mehrfachauswahl) und klicke »Fertig«:",
            reply_markup=build_fav_numbers_keyboard(len(favs), set())
        )
        context.user_data["fav_msgs"].append(sel_msg.message_id)

        return FAV_DELETE_SELECT

async def fav_action_choice_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    msg = q.message

    if q.data == "fav_action_back":
        for mid in context.user_data.get("fav_msgs", []):
            try:
                await context.bot.delete_message(chat_id=msg.chat.id, message_id=mid)
            except:
                pass
        return ConversationHandler.END

    if q.data == "fav_action_remove":
        # Direkt Entfernen starten – simuliert wie früher "Ja"
        favs = favorites.get(uid, [])
        if not favs:
            await msg.reply_text("Du hast aktuell keine Favoriten gespeichert.")
            return ConversationHandler.END

        context.user_data["fav_total"] = len(favs)
        context.user_data["fav_del_sel"] = set()
        list_msg = await msg.reply_text(
            "Welche Favoriten möchtest Du entfernen?\n" +
            "\n".join(f"{i}. {d}" for i, d in enumerate(favs, start=1))
        )
        sel_msg = await msg.reply_text(
            "Wähle Nummern (Mehrfachauswahl) und klicke »Fertig«:",
            reply_markup=build_fav_numbers_keyboard(len(favs), set())
        )
        context.user_data["fav_msgs"].extend([list_msg.message_id, sel_msg.message_id])
        return FAV_ADD_SELECT

    if q.data == "fav_action_select":
        favs = favorites.get(uid, [])
        if not favs:
            await msg.reply_text("Keine Favoriten vorhanden.")
            return ConversationHandler.END

        context.user_data["fav_total"] = len(favs)
        context.user_data["fav_sel_sel"] = set()

        list_msg = await msg.reply_text(
            "Welche Favoriten möchtest Du Deiner Gerichteliste hinzufügen?\n" +
            "\n".join(f"{i}. {d}" for i, d in enumerate(favs, start=1))
        )
        sel_msg = await msg.reply_text(
            "Wähle Nummern (Mehrfachauswahl) und klicke »Fertig«:",
            reply_markup=build_fav_selection_keyboard(len(favs), set())
        )
        context.user_data["fav_msgs"].extend([list_msg.message_id, sel_msg.message_id])
        return FAV_ADD_SELECT



async def fav_selection_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    sel = sorted(context.user_data.get("fav_sel_sel", set()))
    favs = favorites.get(uid, [])

    selected = []
    for idx in sel:
        if 1 <= idx <= len(favs):
            selected.append(favs[idx - 1])

    if not selected:
        msg_warn = await q.message.reply_text("⚠️ Keine Favoriten ausgewählt.")
        await asyncio.sleep(1)

        for mid in context.user_data.get("fav_msgs", []):
            try:
                await context.bot.delete_message(chat_id=q.message.chat.id, message_id=mid)
            except:
                pass
        try:
            await msg_warn.delete()
        except:
            pass
        return ConversationHandler.END

    context.user_data["fav_selection"] = selected

    # Aufräumen: Auswahlnachrichten löschen
    msg_info = await q.message.reply_text("✅ Favoriten gespeichert. Wähle nun im Hauptmenü Deine Gerichteliste.")
    await asyncio.sleep(1)

    for mid in context.user_data.get("fav_msgs", []):
        try:
            await context.bot.delete_message(chat_id=q.message.chat.id, message_id=mid)
        except:
            pass
    try:
        await msg_info.delete()
    except:
        pass

    return ConversationHandler.END


async def fav_del_number_toggle_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    idx = int(q.data.split("_")[-1])
    sel = context.user_data.setdefault("fav_del_sel", set())
    sel.symmetric_difference_update({idx})
    total = context.user_data["fav_total"]
    await q.edit_message_reply_markup(build_fav_numbers_keyboard(total, sel))
    return FAV_ADD_SELECT

async def fav_del_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    sel = sorted(context.user_data.get("fav_del_sel", set()))
    favs = favorites.get(uid, [])

    removed = []
    for idx in sel[::-1]:  # rückwärts löschen, um Indexverschiebung zu vermeiden
        if 1 <= idx <= len(favs):
            removed.append(favs.pop(idx - 1))

    if removed:
        favorites[uid] = favs
        save_favorites()
        msg_del = await q.message.reply_text(f"🗑️ {len(removed)} Favoriten entfernt.")
        await asyncio.sleep(1)
        try:
            await msg_del.delete()
        except:
            pass
    else:
        msg_warn = await q.message.reply_text("⚠️ Keine Favoriten entfernt.")
        await asyncio.sleep(1)
        try:
            await msg_warn.delete()
        except:
            pass

    # Aufräumen: Auswahlnachrichten löschen
    for mid in context.user_data.get("fav_msgs", []):
        try:
            await context.bot.delete_message(chat_id=q.message.chat.id, message_id=mid)
        except:
            pass

    return ConversationHandler.END



async def fav_number_toggle_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    idx = int(q.data.split("_")[2])
    sel = context.user_data.setdefault("fav_del_sel", set())
    sel.symmetric_difference_update({idx})
    total = context.user_data["fav_total"]
    await q.edit_message_reply_markup(build_fav_numbers_keyboard(total, sel))
    return FAV_DELETE_SELECT

def build_fav_selection_keyboard(total: int, selected: set[int]) -> InlineKeyboardMarkup:
    """Zahlen-Tastatur für Selektieren-Modus"""
    btns = [
        InlineKeyboardButton(
            f"{'✅ ' if i in selected else ''}{i}",
            callback_data=f"fav_sel_{i}"
        )
        for i in range(1, total + 1)
    ]
    rows = distribute_buttons_equally(btns, max_per_row=7)
    rows.append([InlineKeyboardButton("Fertig", callback_data="fav_sel_done")])
    return InlineKeyboardMarkup(rows)



async def fav_selection_toggle_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    idx = int(q.data.split("_")[-1])
    sel = context.user_data.setdefault("fav_sel_sel", set())
    sel.symmetric_difference_update({idx})
    total = context.user_data.get("fav_total", 0)
    await q.edit_message_reply_markup(reply_markup=build_fav_selection_keyboard(total, sel))
    return FAV_ADD_SELECT





async def fav_delete_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    sel = sorted(context.user_data.get("fav_del_sel", set()), reverse=True)
    favs = favorites.get(uid, [])

    # Favoriten löschen
    for idx in sel:
        if 1 <= idx <= len(favs):
            favs.pop(idx - 1)
    favorites[uid] = favs
    save_json(FAVORITES_FILE, favorites)

    # alle bisherigen Loop-Nachrichten löschen
    msg = q.message
    for mid in context.user_data.get("fav_msgs", []):
        try:
            await context.bot.delete_message(chat_id=msg.chat.id, message_id=mid)
        except:
            pass

    # falls leer → Hauptmenü & Ende
    if not favs:
        await q.message.reply_text("Keine Favoriten vorhanden. Füge diese später hinzu!")
        await send_main_buttons(q.message)
        return ConversationHandler.END

    # sonst neue Übersicht + Ja/Nein, IDs neu setzen
    txt = "⭐ Deine Favoriten:\n" + "\n".join(f"- {d}" for d in favs)
    m1  = await q.message.reply_text(pad_message(txt))
    m2  = await q.message.reply_text(
        "Bearbeiten?",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("Ja",   callback_data="fav_edit_yes"),
            InlineKeyboardButton("Nein", callback_data="fav_edit_no")
        ]])
    )
    context.user_data["fav_msgs"] = [m1.message_id, m2.message_id]
    return FAV_OVERVIEW

# ===================================== FAVORITEN–FLOW (hinzufügen)=============================

async def fav_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    msg = q.message

    # Liste der Gerichte aus user_data holen
    dishes = context.user_data.get("final_list", [])
    if not dishes:
        return await msg.reply_text("ℹ️ Keine Gerichte verfügbar.")

    # Auswahl initialisieren
    context.user_data["fav_add_sel"]  = set()
    context.user_data["fav_add_msgs"] = []

    # 0) Überschrift mit Erklärung senden + ID speichern
    header_msg = await msg.reply_text(
        "🥣 Deine aktuellen Gerichte (* bereits bei den Favoriten)"
    )
    context.user_data["fav_add_msgs"].append(header_msg.message_id)

    # 1) Nummerierte Liste senden + ID speichern (Stern bei bestehenden Favoriten)
    user_id       = str(q.from_user.id)
    existing_favs = set(favorites.get(user_id, []))
    list_msg = await msg.reply_text(
        "\n".join(
            f"{i+1}. {d}{' *' if d in existing_favs else ''}"
            for i, d in enumerate(dishes)
        )
    )
    context.user_data["fav_add_msgs"].append(list_msg.message_id)

    # 2) Auswahl-Keyboard senden + ID speichern
    sel_msg = await msg.reply_text(
        "Welche Gerichte möchtest Du zu deinen Favoriten hinzufügen?",
        reply_markup=build_fav_add_numbers_keyboard(len(dishes), set())
    )
    context.user_data["fav_add_msgs"].append(sel_msg.message_id)

    return FAV_ADD_SELECT


async def fav_add_number_toggle_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    idx = int(q.data.split("_")[-1])
    sel = context.user_data.setdefault("fav_add_sel", set())
    # Toggle
    if idx in sel:
        sel.remove(idx)
    else:
        sel.add(idx)
    # Keyboard updaten
    dishes = context.user_data.get("final_list", [])
    await q.edit_message_reply_markup(
        build_fav_add_numbers_keyboard(len(dishes), sel)
    )
    return FAV_ADD_SELECT


async def fav_add_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    sel    = sorted(context.user_data.get("fav_add_sel", []))
    dishes = context.user_data.get("final_list", [])
    user_id = str(q.from_user.id)

    # In Favoriten speichern
    favs = favorites.get(user_id, [])
    for i in sel:
        if 1 <= i <= len(dishes):
            dish = dishes[i-1]
            if dish not in favs:
                favs.append(dish)
    favorites[user_id] = favs
    save_json(FAVORITES_FILE, favorites)

    # Alle Loop-Messages löschen
    msg = q.message
    for mid in context.user_data.get("fav_add_msgs", []):
        try:
            await context.bot.delete_message(chat_id=msg.chat.id, message_id=mid)
        except:
            pass

    # Favoriten-Übersicht senden
    txt = "⭐ Deine aktualisierten Favoriten:\n" + "\n".join(f"- {d}" for d in favs)
    await msg.reply_text(txt)

    # Zurück ins Aktions-Menu
    await send_action_menu(msg)
    return EXPORT_OPTIONS




# ============================================================================================


##############################################
#>>>>>>>>>>>>CANCEL / RESET / RESTART
##############################################

async def delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    favs = favorites.get(user_id, [])
    if not context.args or not context.args[0].isdigit():
        return await update.message.reply_text("❌ Nutzung: /delete 1")
    idx = int(context.args[0]) - 1
    if 0<=idx<len(favs):
        rem = favs.pop(idx)
        save_json(FAVORITES_FILE, favorites)
        await update.message.reply_text(f"🗑 Favorit '{rem}' gelöscht.")
    else:
        await update.message.reply_text("❌ Ungültiger Index.")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Fallback-Handler für /cancel: bricht den aktuellen Flow ab.
    """
    await update.message.reply_text("Abgebrochen.")
    return ConversationHandler.END

async def restart_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Option c): löscht alle Daten und erlaubt Neustart"""
    query = update.callback_query
    await query.answer()
    context.user_data.clear()
    await query.edit_message_text("🔄 Alles gelöscht! Nutze /start, um neu zu beginnen.")


# ------------------------------------------------------------------
# /reset – setzt alles für den Nutzer zurück (ausser Favoriten. Siehte unten  5) für favoriten zurücksetzen)
# ------------------------------------------------------------------
async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)

    # 1) Profil entfernen
    #if uid in profiles:
    #    del profiles[uid]
    #    save_profiles()

    # 2) Offene Menü-Session löschen
    if uid in sessions:
        del sessions[uid]
        save_json(SESSIONS_FILE, sessions)

    # 3) Wizard-Nachrichten aufräumen (falls gerade ein Loop offen war)
    await cleanup_prof_loop(context, update.effective_chat.id)

    # 4) Kontext-Speicher leeren
    context.user_data.clear()

    # 5) Favoriten zurücksetzen
    #if uid in favorites:
    #    del favorites[uid]
    #    save_json(FAVORITES_FILE, favorites)


    await update.message.reply_text("🔄 Alles wurde zurückgesetzt. Du kannst neu starten mit /start.")
    return ConversationHandler.END


##############################################
#>>>>>>>>>>>>REZEPT
##############################################

async def rezept_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.message.from_user.id) not in sessions:
        await update.message.reply_text("⚠️ Keine Menüs gewählt.")
        return ConversationHandler.END
    await update.message.reply_text("Welches Menü (z. B. 2)?")
    return REZEPT_INDEX

async def rezept_index(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        idx = int(update.message.text.strip()) - 1
        context.user_data["rezept_idx"] = idx
    except:
        await update.message.reply_text("⚠️ Ungültiger Index.")
        return REZEPT_INDEX
    await update.message.reply_text("Für wie viele Personen?")
    return REZEPT_PERSONEN


async def rezept_personen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = str(update.message.from_user.id)
        personen = int(update.message.text.strip())
        if personen <= 0:
            raise ValueError

        idx = context.user_data["rezept_idx"]
        menues = sessions[user_id]["menues"]
        if not (0 <= idx < len(menues)):
            await update.message.reply_text("❌ Ungültiger Index.")
            return ConversationHandler.END

        dish = menues[idx]
        df = df_zutaten
        zutaten = df[df["Gericht"] == dish].copy()
        zutaten["Menge"] *= personen / 4
        zut_text = "\n".join(
            f"- {row.Zutat}: {format_amount(row.Menge)} {row.Einheit}"
            for _, row in zutaten.iterrows()
        )

        basis = df_gerichte
        st = basis.loc[basis["Gericht"] == dish, "Aufwand"].iloc[0]
        time_str = {1: "30 Minuten", 2: "45 Minuten"}.get(st, "1 Stunde")
        cache_key = f"{dish}|{personen}"
        if cache_key in recipe_cache:
            steps = recipe_cache[cache_key]
        else:
            prompt = f"""Erstelle ein Rezept für '{dish}' für {personen} Personen:
Zutaten:
{zut_text}

Anleitung (kurz Schritt-für-Schritt):"""
            resp = openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}]
            )
            steps = resp.choices[0].message.content.strip()
            recipe_cache[cache_key] = steps
            save_json(CACHE_FILE, recipe_cache)

        msg = f"""📖 Rezept für *{dish}* für *{personen}* Personen:

*Zutaten:*
{zut_text}

*Zubereitungszeit:* ca. {time_str}

*Anleitung:*
{steps}"""
        await update.message.reply_markdown(msg)
        return ConversationHandler.END

    except Exception as e:
        await update.message.reply_text(f"❌ Fehler: {e}")
        return REZEPT_PERSONEN


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# === Bot Setup ===
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def main():
    app = ApplicationBuilder().token(TOKEN).build()
    cancel_handler = CommandHandler("cancel", cancel)
    reset_handler  = CommandHandler("reset", reset_command)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("setup", setup))
    app.add_handler(CommandHandler("tausche", tausche, filters=filters.Regex(r"^\s*/tausche\s+\d"), block=True))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("reset", reset_command))
    app.add_handler(CommandHandler("favorit", favorit))
    #app.add_handler(CommandHandler("meinefavoriten", meinefavoriten))
    app.add_handler(CommandHandler("delete", delete))


    #### ---- MENU-Conversation ----

    app.add_handler(ConversationHandler(
        entry_points=[
            CommandHandler("menu", menu_start),
            CallbackQueryHandler(menu_start_cb, pattern="^start_menu$"),
            CallbackQueryHandler(menu_start_cb, pattern="^restart_menu$")
        ],
        states={
            PROFILE_CHOICE: [CallbackQueryHandler(profile_choice_cb, pattern="^prof_")],
            PROFILE_NEW_A: [CallbackQueryHandler(profile_new_a_cb, pattern="^res_")],
            PROFILE_NEW_B: [CallbackQueryHandler(profile_new_b_cb, pattern="^style_")],
            PROFILE_NEW_C: [CallbackQueryHandler(profile_new_c_cb, pattern="^weight_")],
            PROFILE_OVERVIEW: [CallbackQueryHandler(profile_overview_cb, pattern="^prof_(over|next)")],
            MENU_INPUT:    [MessageHandler(filters.TEXT & ~filters.COMMAND, menu_input)],
            ASK_CONFIRM:   [CallbackQueryHandler(menu_confirm_cb, pattern="^confirm_")],
            ASK_BEILAGEN:  [CallbackQueryHandler(ask_beilagen_cb)],
            SELECT_MENUES:   [CallbackQueryHandler(select_menus_cb)],
            BEILAGEN_SELECT: [CallbackQueryHandler(beilage_select_cb)],
            ASK_FINAL_LIST:  [CallbackQueryHandler(ask_final_list_cb)],
            ASK_SHOW_LIST:   [CallbackQueryHandler(ask_showlist_cb)],
            PERSONS_SELECTION: [
                        CallbackQueryHandler(persons_selection_cb, pattern="^persons_page_(low|high)$"),
                        CallbackQueryHandler(persons_selection_cb, pattern="^persons_\\d+$"),
            ],
            PERSONS_MANUAL:    [MessageHandler(filters.TEXT & ~filters.COMMAND, persons_manual_cb)],
            TAUSCHE_SELECT:  [CallbackQueryHandler(tausche_select_cb,   pattern=r"^swap_(sel:\d+|done)$")],
            TAUSCHE_CONFIRM: [CallbackQueryHandler(tausche_confirm_cb, pattern=r"^swap_(ok|again)$")],
            PDF_EXPORT_CHOICE: [CallbackQueryHandler(process_pdf_export_choice, pattern="^pdf_export_")],
            MENU_COUNT: [CallbackQueryHandler(menu_count_cb, pattern=r"^menu_count_.*")],
            MENU_AUFWAND: [CallbackQueryHandler(aufwand_cb, pattern=r"^aufwand_.*|^noop$")],
            EXPORT_OPTIONS: [
                        CallbackQueryHandler(fav_add_start,   pattern="^favoriten$"),
                        CallbackQueryHandler(export_to_bring, pattern="^export_bring$"),
                        CallbackQueryHandler(export_to_pdf,   pattern="^export_pdf$"),
                        CallbackQueryHandler(restart_start,           pattern="^restart$"),
            ],
            RESTART_CONFIRM: [
                        CallbackQueryHandler(restart_confirm_cb, pattern="^restart_yes$"),
                        CallbackQueryHandler(restart_confirm_cb, pattern="^restart_no$")
            ],
            FAV_ADD_SELECT: [
                        CallbackQueryHandler(fav_add_number_toggle_cb, pattern=r"^fav_add_\d+$"),
                        CallbackQueryHandler(fav_add_done_cb,          pattern="^fav_add_done$"),
                        CallbackQueryHandler(fav_selection_toggle_cb,   pattern=r"^fav_sel_\d+$"),
                        CallbackQueryHandler(fav_selection_done_cb,     pattern="^fav_sel_done$")
            ],
            
        },
        fallbacks=[cancel_handler, reset_handler],
        allow_reentry=True
    ))

    #### ---- Globale Handler ----

    #app.add_handler(CallbackQueryHandler(start_favs_cb,   pattern="^start_favs$"))
    app.add_handler(CallbackQueryHandler(start_setup_cb,  pattern="^start_setup$"))
    app.add_handler(CallbackQueryHandler(setup_ack_cb,    pattern="^setup_ack$"))
    app.add_handler(CallbackQueryHandler(fav_add_start,    pattern="^favoriten$"))
    app.add_handler(CallbackQueryHandler(export_to_bring,  pattern="^export_bring$"))
    app.add_handler(CallbackQueryHandler(export_to_pdf,    pattern="^export_pdf$"))
    app.add_handler(CallbackQueryHandler(process_pdf_export_choice, pattern="^pdf_export_"))
    app.add_handler(CallbackQueryHandler(restart_start,    pattern="^restart$"))
    app.add_handler(CallbackQueryHandler(restart_confirm_cb, pattern="^restart_yes$|^restart_no$"))
    app.add_handler(CallbackQueryHandler(fav_add_number_toggle_cb, pattern=r"^fav_add_\d+$"))
    app.add_handler(CallbackQueryHandler(fav_add_done_cb,          pattern="^fav_add_done$"))
    app.add_handler(CallbackQueryHandler(start_setup_cb,  pattern="^restart_setup$"))



    #### ---- QuickOne-Conversation ----

    app.add_handler(ConversationHandler(
        entry_points=[
            CommandHandler("quickone", quickone_start),
            CallbackQueryHandler(quickone_start, pattern="^start_quickone$"),
            CallbackQueryHandler(quickone_start, pattern="^restart_quickone$")
        ],
        states={
            QUICKONE_START:    [CallbackQueryHandler(quickone_start,    pattern="^start_quickone$")],
            QUICKONE_CONFIRM:  [CallbackQueryHandler(quickone_confirm_cb,pattern="^quickone_")],
            PERSONS_SELECTION: [CallbackQueryHandler(persons_selection_cb,pattern="^persons_")],
            PERSONS_MANUAL:    [MessageHandler(filters.TEXT & ~filters.COMMAND, persons_manual_cb)],
        },

        fallbacks=[cancel_handler, reset_handler],
        allow_reentry=True
    ))



    #### ---- Favoriten-Conversation ----

    app.add_handler(ConversationHandler(
        entry_points=[
            CommandHandler("meinefavoriten", fav_start),
            CallbackQueryHandler(fav_start,    pattern="^start_favs$"),
            CallbackQueryHandler(fav_start,              pattern="^restart_favs$"),
            # neu: Einstieg über Aktions-Menu
            CallbackQueryHandler(fav_add_start, pattern="^favoriten$")
        ],
        states={
            FAV_OVERVIEW: [
                CallbackQueryHandler(fav_overview_cb, pattern="^fav_edit_yes$"),
                CallbackQueryHandler(fav_overview_cb, pattern="^fav_edit_no$"),
                CallbackQueryHandler(fav_action_choice_cb, pattern="^fav_action_")
            ],
            FAV_DELETE_SELECT: [
                CallbackQueryHandler(fav_number_toggle_cb, pattern=r"^fav_del_\d+$"),
                CallbackQueryHandler(fav_delete_done_cb,   pattern="^fav_del_done$"),
                CallbackQueryHandler(fav_overview_cb,      pattern="^fav_edit_yes$"),
                CallbackQueryHandler(fav_overview_cb,      pattern="^fav_edit_no$")
            ],
            # neu: Favoriten hinzufügen-Loop
            FAV_ADD_SELECT: [
                CallbackQueryHandler(fav_add_number_toggle_cb, pattern=r"^fav_add_\d+$"),
                CallbackQueryHandler(fav_add_done_cb,          pattern="^fav_add_done$"),
                CallbackQueryHandler(fav_selection_toggle_cb,  pattern=r"^fav_sel_\d+$"),
                CallbackQueryHandler(fav_selection_done_cb,    pattern="^fav_sel_done$"),
                CallbackQueryHandler(fav_del_number_toggle_cb, pattern=r"^fav_del_\d+$"), 
                CallbackQueryHandler(fav_del_done_cb,          pattern="^fav_del_done$")
            ],

        },
        fallbacks=[],
        allow_reentry=True
    ))




    #### ---- REZEPT-Conversation ----

    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("rezept", rezept_start)],
        states={
            REZEPT_INDEX: [MessageHandler(filters.TEXT & ~filters.COMMAND, rezept_index)],
            REZEPT_PERSONEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, rezept_personen)],
        },
        fallbacks=[cancel_handler, reset_handler],
        allow_reentry=True
    ))




    print("✅ Bot läuft...")
    app.run_polling()


if __name__ == "__main__":
    main()
