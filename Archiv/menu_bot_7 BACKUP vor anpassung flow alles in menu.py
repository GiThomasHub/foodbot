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
from fpdf import FPDF
from fpdf.enums import XPos, YPos
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI
from typing import Set
from telegram.error import BadRequest
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
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




MENU_INPUT, ASK_BEILAGEN, SELECT_MENUES, BEILAGEN_SELECT, ASK_FINAL_LIST, ASK_SHOW_LIST, FERTIG_PERSONEN, REZEPT_INDEX, REZEPT_PERSONEN, TAUSCHE_SELECT, TAUSCHE_CONFIRM, ASK_CONFIRM, EXPORT_OPTIONS, FAV_OVERVIEW, FAV_DELETE_SELECT, PDF_EXPORT_CHOICE, FAV_ADD_SELECT, RESTART_CONFIRM, PROFILE_CHOICE, PROFILE_NEW_A, PROFILE_NEW_B, PROFILE_NEW_C, PROFILE_OVERVIEW, QUICKONE_START, QUICKONE_CONFIRM, PERSONS_SELECTION, PERSONS_MANUAL = range(27)





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


# === In-Memory Data ===
sessions = {}
favorites = {}
recipe_cache = {}
profiles = {}


# === Profil-Optionen ===
RESTRICTION_CHOICES = {
    "res_vegi": "Vegi",   # akzeptiert Gerichte "Vegi" ODER "beides"
    "res_open": "offen",  # keine Einschränkung
}

STYLE_CHOICES = {
    "style_klassisch":  "Klassisch",
    "style_mediterran": "Mediterran",
    "style_asiatisch":  "Asiatisch",
    "style_orient":     "Orientalisch",
    "style_suess":      "Süss",        # <-- neu
}

ALL_STYLE_KEYS = set(STYLE_CHOICES.keys())

WEIGHT_CHOICES = {f"weight_{i}": i for i in range(1, 8)}
WEIGHT_CHOICES["weight_any"] = None          #  «Egal» = keine Einschränkung





#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# === Utilites: Load/Save Helpers ===
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def load_json(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def format_amount(q):
    """
    Gibt q zurück:
     - als Ganzzahl, wenn es ganzzahlig ist,
     - sonst bis zu 2 Dezimalstellen (z.B. 2.25, 2.2).
    """
    qf = float(q)
    if qf.is_integer():
        return str(int(qf))
    # runde auf 2 Stellen, baue String, entferne überflüssige Nullen/Punkt
    s = f"{qf:.2f}".rstrip('0').rstrip('.')
    return s

def build_swap_keyboard(menus: list[str], selected: set[int]) -> InlineKeyboardMarkup:
    """Buttons 1…N mit Toggle-Häkchen + ‘Fertig’."""
    btns = []
    for idx, _g in enumerate(menus, 1):
        label = f"{'✅ ' if idx in selected else ''}{idx}"
        btns.append(InlineKeyboardButton(label, callback_data=f"swap_sel:{idx}"))
    rows = [btns[i:i+3] for i in range(0, len(btns), 3)]
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

async def ask_for_persons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Sendet die Inline-Buttons 1–6 + 'Mehr' und speichert Message-ID in der Session.
    Liefert PERSONS_SELECTION zurück.
    """
    # User-ID als String
    uid = str(update.effective_user.id)
    # Inline-Buttons
    buttons = [InlineKeyboardButton(str(i), callback_data=f"persons_{i}") for i in range(1,7)]
    buttons.append(InlineKeyboardButton("Mehr", callback_data="persons_more"))
    markup = InlineKeyboardMarkup([buttons])

    # Sende Frage
    if update.callback_query:
        await update.callback_query.answer()
        msg = await update.callback_query.message.reply_text(
            "Gut, Deine Auswahl steht. Für wieviel Personen soll die Einkaufs- und Kochliste erstellt werden?",
            reply_markup=markup
        )
    else:
        msg = await update.message.reply_text(
            "Gut, Deine Auswahl steht. Für wieviel Personen soll die Einkaufs- und Kochliste erstellt werden?",
            reply_markup=markup
        )

    # Message-ID speichern
    sessions[uid].setdefault("person_msgs", []).append(msg.message_id)
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
    max_per_row = 8
    n = len(btns)
    if n <= max_per_row:
        rows = [btns]
    else:
        rows_count = math.ceil(n / max_per_row)
        base       = n // rows_count
        extras     = n % rows_count
        rows = []
        idx  = 0
        for i in range(rows_count):
            size = base + 1 if i < extras else base
            rows.append(btns[idx : idx + size])
            idx += size
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
    max_per_row = 7
    n = len(btns)
    if n <= max_per_row:
        rows = [btns]
    else:
        rows_count = math.ceil(n / max_per_row)
        base       = n // rows_count
        extras     = n % rows_count
        rows = []
        idx  = 0
        for i in range(rows_count):
            size = base + 1 if i < extras else base
            rows.append(btns[idx : idx + size])
            idx += size
    rows.append([InlineKeyboardButton("Fertig", callback_data="fav_add_done")])
    return InlineKeyboardMarkup(rows)


# ============================================================================================

async def send_main_buttons(msg):
    """Hauptmenü-Buttons erneut anzeigen (z. B. bei leerer Favoritenliste)."""
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🍲 Menü",      callback_data="start_menu"),
        InlineKeyboardButton("⚡ QuickOne",     callback_data="start_quickone"),
        InlineKeyboardButton("🔖 Favoriten", callback_data="start_favs"),
        InlineKeyboardButton("🛠️ Übersicht",     callback_data="start_setup"),
    ]])
    await msg.reply_text("➡️ Wähle eine Option:", reply_markup=kb)

# ============================================================================================

async def send_action_menu(msg):
    """Zeigt die drei Haupt-Export/Restart-Buttons mit Frage an."""
    kb = InlineKeyboardMarkup([
        [ InlineKeyboardButton("🔖 Gerichte zu Favoriten hinzufügen",                callback_data="favoriten") ],
        [ InlineKeyboardButton("🛒 Einkaufsliste in Bring! exportieren", callback_data="export_bring") ],
        [ InlineKeyboardButton("📄 Als PDF exportieren",   callback_data="export_pdf")   ],
        [ InlineKeyboardButton("🔄 Das passt so. Neustart!",             callback_data="restart")      ],
    ])
    await msg.reply_text("Was möchtest Du weiter tun?", reply_markup=kb)




# Load persisted data
sessions = load_json(SESSIONS_FILE)
favorites = load_json(FAVORITES_FILE)
recipe_cache = load_json(CACHE_FILE)
profiles = load_profiles()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# === Google Sheets Data ===
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def lade_gerichtebasis():
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_GERICHTE)
    rows  = sheet.get_all_values()                # Header = rows[0]
    # A–F:   Gericht | Aufwand | Art | Beilage | Stil | Einschränkung
    data  = [row[:6] for row in rows[1:]]
    df    = pd.DataFrame(
        data,
        columns=["Gericht", "Aufwand", "Art", "Beilage", "Stil", "Einschränkung"],
    )
    df["Aufwand"] = pd.to_numeric(df["Aufwand"], errors="coerce").fillna(0).astype(int)
    return df.drop_duplicates()


def lade_beilagen():
    sheet = client.open_by_key(SHEET_ID).worksheet("Beilagen")
    raw = sheet.get_all_values()[1:]       # überspringe Header
    data = [row[:5] for row in raw]        # nur erste 5 Spalten
    df = pd.DataFrame(data, columns=["Nummer","Beilage","Kategorie","Relevanz","Aufwand"])
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
    df = pd.DataFrame(data, columns=["Gericht", "Zutat", "Kategorie", "Typ", "Menge", "Einheit"])
    # Filtern und Typkonversion
    df = df[df["Gericht"].notna() & df["Zutat"].notna()]
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
            filtered = df[df["Einschränkung"].isin(["Vegi", "beides"])].copy()
        else:
            filtered = df.copy()

    # (b) Stil
    styles = profile.get("styles", []) if profile else []
    if styles:
        filtered = filtered[filtered["Stil"].isin(styles)]

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
    df_light  = df[df["Art"] == "leicht"].copy()
    df_medium = df[df["Art"] == "mittel"].copy()
    df_heavy  = df[df["Art"] == "schwer"].copy()

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
        "light":  df_light.sample(n=min(len(df_light),  target["light"]),  replace=False),
        "medium": df_medium.sample(n=min(len(df_medium), target["medium"]), replace=False),
        "heavy":  df_heavy.sample(n=min(len(df_heavy),  target["heavy"]),  replace=False),
    }

    # --------------------- Auffüllen nach Hierarchie --------------------
    def take(df_src, need):
        if need <= 0 or df_src.empty:
            return pd.DataFrame(), 0
        pick = df_src.sample(n=min(len(df_src), need), replace=False)
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
        "⚡ QuickOne – Ein Gericht - ohne Einschränkungen\n\n"
        "🔖 Favoriten – Übersicht deiner Favoriten\n\n"
        "🛠️ Übersicht – Übersicht aller Funktionen\n\n"
        "(Du kannst alle Befehle jederzeit auch im Textfeld eingeben!)\n"
        "Tippe: /menu | /meinefavoriten | /setup"                                 ####setup in uebersicht ändern!
    )

    # 2. Buttons in einer neuen Nachricht
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("🍲 Menü",      callback_data="start_menu"),
                     InlineKeyboardButton("⚡ QuickOne",     callback_data="start_quickone"),
        InlineKeyboardButton("🔖 Favoriten", callback_data="start_favs"),
        InlineKeyboardButton("🛠️ Übersicht",     callback_data="start_setup"),
    ]])
    await update.message.reply_text(
        "➡️ Wähle eine Option:",
        reply_markup=keyboard
    )

async def setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🛠 Kommandos im Menu Bot:\n"
        "/start – Hilfe & Einführung\n"
        "/menu – generiere Gerichtevorschläge\n"
        "/tausche – tauscht Gerichte aus\n"
        "/fertig – Einkaufs- & Kochliste erstellen\n\n"
        "/favorit x – speichert Gericht x in deinen Favoriten\n"
        "/meinefavoriten – Übersicht deiner Favoriten\n"
        "/delete x – löscht Favorit x\n\n"
        #"/rezept 2 [x] – Erstellt ein Rezept für Menü 2 via KI (optional x Personen)\n"
        "/status – zeigt aktuelle Gerichtewahl\n"
        "/reset – setzt Session zurück (Favoriten bleiben)\n"
        "/setup – zeigt alle Kommandos"
        "/neustart – Startet neuen Prozess (Favoriten bleiben)\n"
    )

async def menu_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/menu per Text – startet Profil-Loop"""
    # frische Liste für alle Wizard-Nachrichten
    context.user_data["prof_msgs"] = []

    sent = await update.message.reply_text(
        "Wie möchtest Du fortfahren?",
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
        "Wie möchtest Du fortfahren?",
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
        "/tausche – tauscht Gerichte aus\n"
        "/fertig – Einkaufs- & Kochliste erstellen\n\n"
        "/favorit x – speichert Gericht x in Deinen Favoriten\n"
        "/meinefavoriten – Übersicht Deiner Favoriten\n"
        "/delete x – löscht Favorit x\n\n"
        "/status – zeigt aktuelle Auswahl\n"
        "/reset – setzt Session zurück (Favoriten bleiben)\n"
        "/setup – zeigt alle Kommandos\n"
        "/neustart – neuer Prozess\n"
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
            await q.message.reply_text(
                "Bitte gib Anzahl Gerichte an inkl. Aufwand "
                "(#einfach, #mittel, #aufwändig).\nBeispiel: 4 (2,1,1)"
            )
            return MENU_INPUT

        # Kein Profil ⇒ Hinweis + Wizard starten
        await send_and_log("Es besteht noch kein Profil. Erstelle eines!")
        context.user_data["new_profile"] = {"styles": set()}
        await send_and_log(
            "Einschränkungen:",
            reply_markup=build_restriction_keyboard()
        )
        return PROFILE_NEW_A

    # ===== 2)  Ohne Einschränkung =========================================
    if choice == "prof_nolim":
        # Wizard-Nachrichten löschen (falls vorhanden) und direkt weiter
        await cleanup_prof_loop(context, q.message.chat_id)
        await q.message.reply_text(
            "Bitte gib Anzahl Gerichte an inkl. Aufwand "
            "(#einfach, #mittel, #aufwändig).\nBeispiel: 4 (2,1,1)"
        )
        return MENU_INPUT

    # ===== 3)  Neues Profil ===============================================
    if choice == "prof_new":
        context.user_data["new_profile"] = {"styles": set()}
        await send_and_log(
            "Einschränkungen:",
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
            "Einschränkungen:",
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
        "Stil auswählen (Mehrfachauswahl möglich):",
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
            "Schweregrad auswählen (1 = leicht … 7 = deftig):",
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
            "Einschränkungen:",
            reply_markup=build_restriction_keyboard(),
        )
        context.user_data["prof_msgs"].append(sent.message_id)
        return PROFILE_NEW_A

    # ------- Weiter → alten Menü-Flow starten -------------------------
    await cleanup_prof_loop(context, q.message.chat_id)        # 1.) alles weg
    await q.message.reply_text(                                # 2.) neuer Prompt
        "Bitte gib Anzahl Gerichte an inkl. Aufwand "
        "(#einfach, #mittel, #aufwändig).\nBeispiel: 4 (2,1,1)"
    )
    return MENU_INPUT



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
                "⚠️ Keine Gerichte passen exakt zu deinem Profil – "
                "ich suche ohne Stil-Einschränkung weiter."
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
            pool = list(set(df_src["Gericht"]) - exclude_ids)
            return random.sample(pool, min(n, len(pool)))

        bereits, ausgewaehlt, aufwand_liste = set(), [], []

        # Primär­auswahl je Stufe
        bedarf = {1: a1, 2: a2, 3: a3}           # Soll­mengen
        reste  = {1: [],  2: [],  3: []}         # Fehlbestände je Stufe

        for stufe in (1, 2, 3):
            art   = aufwand2art[stufe]
            grund = basis[(basis["Aufwand"] == stufe) & (basis["Art"] == art)]
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
                df_pool = basis[(basis["Aufwand"] == ers) & (basis["Art"] == art)]
                picks   = pick(df_pool, fehl, bereits)
                ausgewaehlt += picks
                aufwand_liste += [ers] * len(picks)
                bereits.update(picks)
                fehl -= len(picks)

        # Falls immer noch zu wenig Gerichte vorhanden, nimm beliebige übrige
        gesamt = a1 + a2 + a3
        if len(ausgewaehlt) < gesamt:
            rest_pool = list(set(basis["Gericht"]) - bereits)
            extra = random.sample(rest_pool, min(gesamt - len(ausgewaehlt), len(rest_pool)))
            # Aufwand der Extra-Gerichte anhand Spalte Aufwand setzen
            for g in extra:
                stufe = int(basis[basis["Gericht"] == g]["Aufwand"].iloc[0])
                ausgewaehlt.append(g)
                aufwand_liste.append(stufe)


        # ---------- Speichern & Ausgabe -------------------------------
        sessions[user_id] = {"menues": ausgewaehlt, "aufwand": aufwand_liste}
        save_json(SESSIONS_FILE, sessions)

        reply = "🎲 Deine Gerichte:\n" + "\n".join(f"{i+1}. {g}" for i, g in enumerate(ausgewaehlt))
        await update.message.reply_text(reply)

        confirm_kb = InlineKeyboardMarkup([[  # passt?
            InlineKeyboardButton("Ja",   callback_data="confirm_yes"),
            InlineKeyboardButton("Nein", callback_data="confirm_no"),
        ]])
        await update.message.reply_text("Passen diese Gerichte?", reply_markup=confirm_kb)
        return ASK_CONFIRM

    except Exception as e:
        await update.message.reply_text(f"❌ Fehler: {e}")
        return MENU_INPUT


async def menu_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "confirm_yes":
        await mark_yes_no(query, True,  "confirm_yes", "confirm_no")
        # Wie vorher: Frage nach Beilagen
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("Ja",   callback_data="ask_yes"),
            InlineKeyboardButton("Nein", callback_data="ask_no"),
        ]])
        await query.message.reply_text(
            "Möchtest du Beilagen hinzufügen?",
            reply_markup=kb
        )
        return ASK_BEILAGEN

    if query.data == "confirm_no":
        await mark_yes_no(query, False, "confirm_yes", "confirm_no")
        # confirm_no → Swap-Flow manuell starten (kein update.message vorhanden)
        uid = str(query.from_user.id)
        # Reset der Swap-Auswahl
        context.user_data["swap_candidates"] = set()
        # Inline-Keyboard für Tausch-Indizes
        kb = build_swap_keyboard(sessions[uid]["menues"], set())
        await query.message.reply_text(
            "Welche Gerichte möchtest Du tauschen?",
            reply_markup=kb
        )
        return TAUSCHE_SELECT


async def persons_selection_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    chat_id = q.message.chat.id

    # Alte Frage löschen
    for mid in sessions[uid].get("person_msgs", []):
        try: await context.bot.delete_message(chat_id, mid)
        except: pass

    if q.data == "persons_more":
        msg = await q.message.reply_text("Gib die Anzahl bitte an:")
        sessions[uid]["person_msgs"] = [msg.message_id]
        return PERSONS_MANUAL

    personen = int(q.data.split("_")[1])
    # Temporär speichern, wird in fertig_input ausgelesen
    context.user_data["temp_persons"] = personen
    return await fertig_input(update, context)


async def persons_manual_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    chat_id = update.message.chat.id
    try:
        personen = int(update.message.text.strip())
        if personen <= 0: raise ValueError
    except:
        await update.message.reply_text("⚠️ Ungültige Zahl.")
        return PERSONS_MANUAL

    # Frage löschen
    for mid in sessions[user_id].get("person_msgs", []):
        try: await context.bot.delete_message(chat_id, mid)
        except: pass

    context.user_data["temp_persons"] = personen
    return await fertig_input(update, context)



# ===== QuickOne – Flow =====

async def quickone_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Startet QuickOne: sofort ein Gericht+Beilagen vorschlagen."""
    # USER-ID immer als String behandeln!
    uid = str(update.effective_user.id)
    sessions.pop(uid, None)

    # Gericht & Beilagen auswählen
    dish = choose_random_dish()
    raw_codes = df_gerichte.loc[df_gerichte["Gericht"] == dish, "Beilage"].iloc[0]
    codes = parse_codes(raw_codes)
    side_nums = choose_sides(codes)
    sides = df_beilagen[df_beilagen["Nummer"].isin(side_nums)]["Beilage"].tolist()

    # Session speichern (unter String-Key)
    sessions[uid] = {
        "menues": [dish],
        "aufwand": [0],
        "beilagen": {dish: side_nums}
    }

    # Nachricht & Buttons
    text = f"Dein Gericht:\n*{format_dish_with_sides(dish, sides)}*\n\nPasst das?"
    buttons = [
        InlineKeyboardButton("Passt",     callback_data="quickone_passt"),
        InlineKeyboardButton("Neu!", callback_data="quickone_neu"),
    ]
    if sides:
        buttons.append(InlineKeyboardButton("Beilagen ändern", callback_data="quickone_beilagen_neu"))
    markup = InlineKeyboardMarkup([buttons])

    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.message.delete()
        except BadRequest:
            pass
        await update.callback_query.message.reply_text(text, reply_markup=markup, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, reply_markup=markup, parse_mode="Markdown")

    return QUICKONE_CONFIRM


async def quickone_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Verarbeitet Passt/Neu/Beilagen neu."""
    q = update.callback_query
    await q.answer()
    data = q.data
    # USER-ID als String abrufen
    uid = str(update.effective_user.id)

    if data == "quickone_passt":
        # Frage nach Personen per Helper
        return await ask_for_persons(update, context)


    if data == "quickone_neu":
        await q.message.delete()
        return await quickone_start(update, context)

    if data == "quickone_beilagen_neu":
        # nur Beilagen neu mischen
        dish = sessions[uid]["menues"][0]
        raw_codes = df_gerichte.loc[df_gerichte["Gericht"] == dish, "Beilage"].iloc[0]
        codes = parse_codes(raw_codes)
        side_nums = choose_sides(codes)
        sessions[uid]["beilagen"][dish] = side_nums
        sides = df_beilagen[df_beilagen["Nummer"].isin(side_nums)]["Beilage"].tolist()

        text = f"Dein Gericht:\n*{format_dish_with_sides(dish, sides)}*\n\nPasst das?"
        buttons = [
            InlineKeyboardButton("Passt",     callback_data="quickone_passt"),
            InlineKeyboardButton("Neu!", callback_data="quickone_neu"),
            InlineKeyboardButton("Beilagen ändern", callback_data="quickone_beilagen_neu")
        ]
        markup = InlineKeyboardMarkup([buttons])
        await q.message.delete()
        await q.message.reply_text(text, reply_markup=markup, parse_mode="Markdown")
        return QUICKONE_CONFIRM

    return QUICKONE_CONFIRM






async def ask_beilagen_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "ask_no":
        await mark_yes_no(query, False,  "ask_yes", "ask_no")
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("Ja",  callback_data="show_yes"),
            InlineKeyboardButton("Nein", callback_data="show_no"),
        ]])
        await query.message.reply_text(
            "Willst Du die Einkaufsliste und Kochliste anzeigen?",
            reply_markup=keyboard
        )
        return ASK_SHOW_LIST

    if query.data == "ask_yes":
        await mark_yes_no(query, True, "ask_yes", "ask_no")
        # Menü-Buttons zum Auswählen der Gerichte bauen
        menus = (context.user_data.get("menu_list") or sessions[str(query.from_user.id)]["menues"])
        buttons = []
        for i, gericht in enumerate(menus, start=1):
            codes = parse_codes(
                df_gerichte.loc[df_gerichte["Gericht"] == gericht, "Beilage"].iloc[0]
            )
            if not codes or codes == [0]:
                continue
            buttons.append(InlineKeyboardButton(str(i), callback_data=f"select_{i}"))
        buttons.append(InlineKeyboardButton("Fertig", callback_data="select_done"))
        kb = [buttons[j:j+4] for j in range(0, len(buttons), 4)]
        await query.message.reply_text(
            "Für welche Menüs? (Mehrfachauswahl, dann Fertig)",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        context.user_data["selected_menus"] = set()
        return SELECT_MENUES
   
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
    raw = df_gerichte.loc[df_gerichte["Gericht"] == gericht, "Beilage"].iloc[0]
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


    # 4) Inline-Buttons bauen
    buttons = []
    for num, name, cat in zip(df_beilagen["Nummer"], df_beilagen["Beilage"], df_beilagen["Kategorie"]):
        if num in erlaubt:
            buttons.append(
                InlineKeyboardButton(name, callback_data=f"beilage_{num}")
            )
    buttons.append(InlineKeyboardButton("Fertig", callback_data="beilage_done"))

    kb = [buttons[i:i+3] for i in range(0, len(buttons), 3)]

    # 5) Nachricht senden (nutze update_or_query.message, da CallbackQuery oder Update möglich)
    await update_or_query.message.reply_text(
        f"Wähle Beilagen für: *{gericht}*",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown"
    )

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
        codes = parse_codes(df_gerichte.loc[df_gerichte["Gericht"] == gericht, "Beilage"].iloc[0])
        if not codes or codes == [0]:
            continue
        mark = " ✅" if (i-1) in sel else ""
        buttons.append(InlineKeyboardButton(f"{i}{mark}", callback_data=f"select_{i}"))
    buttons.append(InlineKeyboardButton("Fertig", callback_data="select_done"))
    kb = [buttons[j:j+4] for j in range(0, len(buttons), 4)]
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
    sel = sessions[uid]["beilagen"].setdefault(gericht, [])

    # Wenn „Fertig“ gedrückt wurde: nächstes Gericht oder Ende
    if data == "beilage_done":
        context.user_data["menu_idx"] += 1
        if context.user_data["menu_idx"] < len(idx_list):
            return await ask_beilagen_for_menu(query, context)

        # --- Neu ab hier ---
        uid = str(query.from_user.id)

        # 1) Kopfzeile
        text = "✅ Beilagen-Auswahl abgeschlossen!\n Hier deine gewählten Gerichte:\n\n"

        # 2) Wie in /status: Gerichte plus formatierte Beilagen
        for dish in sessions[uid]["menues"]:
            sel_nums      = sessions[uid].get("beilagen", {}).get(dish, [])
            side_names    = df_beilagen.loc[
                df_beilagen["Nummer"].isin(sel_nums), "Beilage"
            ].tolist()
            text += f"- {format_dish_with_sides(dish, side_names)}\n"


        # 3) Sende ohne Inline-Keyboard
        return await ask_for_persons(update, context)

        # --- Ende der Änderung ---


    # Sonst: Toggle einer Beilage
    num = int(data.split("_")[1])
    # Kategorie und Limit prüfen
    dfb = df_beilagen.set_index("Nummer")
    cat = dfb.loc[num, "Kategorie"]
    current_count = sum(1 for b in sel if dfb.loc[b, "Kategorie"] == cat)

    if cat == "Kohlenhydrate" and current_count >= 2:
        await query.answer("Maximal 2 Kohlenhydrat-Beilagen erlaubt", show_alert=True)
        return BEILAGEN_SELECT
    if cat == "Gemüse" and current_count >= 2:
        await query.answer("Maximal 2 Gemüse-Beilagen erlaubt", show_alert=True)
        return BEILAGEN_SELECT

    # Hinzufügen oder Entfernen
    if num in sel:
        sel.remove(num)
    else:
        sel.append(num)

    # speichere Auswahl
    save_json(SESSIONS_FILE, sessions)

    # Keyboard neu bauen mit Häkchen
    allowed = context.user_data.get("allowed_beilage_codes", set())
    buttons = []
    for n, name, cat in zip(df_beilagen["Nummer"], df_beilagen["Beilage"], df_beilagen["Kategorie"]):
        if n not in allowed:
            continue
        mark = " ✅" if n in sel else ""
        buttons.append(InlineKeyboardButton(f"{mark}{name}", callback_data=f"beilage_{n}"))
    buttons.append(InlineKeyboardButton("Fertig", callback_data="beilage_done"))
    kb = [buttons[i:i+3] for i in range(0, len(buttons), 3)]
    await query.message.edit_reply_markup(InlineKeyboardMarkup(kb))
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
        reply += "📝 Aktuelle Auswahl:\n"
        for dish in sessions[user_id]["menues"]:
            # Nummern der Beilagen aus der Session
            sel_nums = sessions[user_id].get("beilagen", {}).get(dish, [])
            # Map Nummer → Beilagen-Name
            beiname = df_beilagen.loc[
                df_beilagen["Nummer"].isin(sel_nums), "Beilage"
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
            InlineKeyboardButton(label("style_klassisch",  "Klassisch"),  callback_data="style_klassisch"),
            InlineKeyboardButton(label("style_mediterran", "Mediterran"), callback_data="style_mediterran"),
        ],
        [
            InlineKeyboardButton(label("style_asiatisch",  "Asiatisch"),  callback_data="style_asiatisch"),
            InlineKeyboardButton(label("style_orient",     "Orientalisch"), callback_data="style_orient"),
        ],
        [
            InlineKeyboardButton(label("style_suess",      "Süss"),       callback_data="style_suess"),
            InlineKeyboardButton(label_all,                callback_data="style_all"),
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


def profile_overview_text(p: dict) -> str:
    """Formatiert die Profil-Übersicht"""
    styles_str = "Alle Stile" if not p["styles"] else ", ".join(p["styles"])
    return (
        "🗂 **Dein Profil**\n"
        f"• Einschränkung: {p['restriction']}\n"
        f"• Stil: {styles_str}\n"
        f"• Schwere: {p['weight'] if p['weight'] else 'Egal'}"
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

    df = df_gerichte
    sess = sessions[user_id]
    menues, aufw = sess["menues"], sess["aufwand"]

    # Initialisiere Replacement-Historie pro Slot, falls noch nicht vorhanden
    history = sess.setdefault(
        "history",
        {i: [menues[i]] for i in range(len(menues))}
    )

    for arg in args:
        idx = int(arg) - 1
        if 0 <= idx < len(menues):
            st = aufw[idx]
            all_cands = set(df[df["Aufwand"] == st]["Gericht"])

            # andere aktuell gewählte Menüs (außer dem zu tauschenden)
            other_sel = set(menues) - {menues[idx]}

            # bereits in diesem Slot getauschte Gerichte
            used = set(history[idx])

            # mögliche neue Kandidaten
            allowed = list(all_cands - other_sel - used)

            if not allowed:
                # alle Alternativen aufgebraucht → Historie zurücksetzen
                history[idx] = []
                allowed = list(all_cands - other_sel)

            if allowed:
                neu = random.choice(allowed)
                menues[idx] = neu
                history[idx].append(neu)

    save_json(SESSIONS_FILE, sessions)
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
    context.user_data["swap_candidates"] = set()
    kb = build_swap_keyboard(sessions[uid]["menues"], set())
    await update.message.reply_text(
        "Welche Gerichte möchtest Du tauschen?",
        reply_markup=kb
    )
    return TAUSCHE_SELECT


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
        await q.edit_message_text(text, reply_markup=kb)
        return TAUSCHE_SELECT

    # 2) 'Fertig' ohne Auswahl → Warnung
    if data == "swap_done" and not sel:
        await q.answer("Nichts ausgewählt.", show_alert=True)
        return TAUSCHE_SELECT

    # 3) 'Fertig' mit Auswahl → weiter in Schritt 4
    #if data == "swap_done":
        # Hier kommt später die Logik, die die Gerichte austauscht
        # Wir lassen es jetzt stub-mäßig durchfallen, um die Toggle-Logik zu testen
        #await q.edit_message_text("Tausch wird verarbeitet…")
        #return ConversationHandler.END
    # 3) 'Fertig' mit Auswahl → echte Swap-Logik + Frage
    if data == "swap_done":
        menues = sessions[uid]["menues"]
        aufw   = sessions[uid]["aufwand"]
        # Stelle sicher, dass es ein 'beilagen'-Dict gibt
        sessions[uid].setdefault("beilagen", {})

        # Initialisiere History, falls noch nicht vorhanden
        history = sessions[uid].setdefault(
            "history",
            {i: [menues[i]] for i in range(len(menues))}
        )
        swapped_slots: list[int] = []

        for idx in sorted(sel):              # idx ist 1-basiert
            slot = idx - 1
            stufe = aufw[slot]
            # Alle Kandidaten derselben Aufwand-Stufe
            all_cands = set(df_gerichte[df_gerichte["Aufwand"] == stufe]["Gericht"])
            other_sel = set(menues) - {menues[slot]}
            used      = set(history.get(slot, []))
            allowed   = list(all_cands - other_sel - used)

            # Wenn alle durchsucht, Historie flushen
            if not allowed:
                history[slot] = []
                allowed = list(all_cands - other_sel)

            if allowed:
                neu = random.choice(allowed)
                menues[slot] = neu
                history[slot].append(neu)
                # Alte Beilagen-Liste dieses Slots löschen
                sessions[uid]["beilagen"].pop(idx, None)
                swapped_slots.append(idx)

        # Speichere geänderte Sessions
        save_json(SESSIONS_FILE, sessions)
        context.user_data["swapped_indices"] = swapped_slots

        # 1) Liste als eigenständige Nachricht senden, bleibt im Chat
        menutext = "\n".join(f"{i}. {g}" for i, g in enumerate(menues, 1))
        await q.message.reply_text(
            f"🔄 Neue Auswahl:\n{menutext}"
        )
        # 2) Frage zum Bestätigen als neue Nachricht
        confirm_kb = InlineKeyboardMarkup([[ 
            InlineKeyboardButton("Ja",   callback_data="swap_ok"),
            InlineKeyboardButton("Nein", callback_data="swap_again")
        ]])
        await q.message.reply_text(
            "Passen diese Gerichte?",
            reply_markup=confirm_kb
        )
        return TAUSCHE_CONFIRM


async def tausche_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    # 1) nochmal Tauschen?
    if q.data == "swap_again":   # entspricht Nein
        await mark_yes_no(q, False, "swap_ok", "swap_again")
        uid = str(q.from_user.id)
        context.user_data["swap_candidates"] = set()
        kb = build_swap_keyboard(sessions[uid]["menues"], set())
        # rein reply, so that the previously sent „Neue Auswahl“ bleibt stehen
        await q.message.reply_text(
            "Welche Gerichte möchtest Du tauschen?",
            reply_markup=kb
        )
        return TAUSCHE_SELECT


    # 2) Bestätigung 'Ja' → Swap-Flow beenden und zurück in den Beilagen-Flow
    
    if q.data == "swap_ok":
        await mark_yes_no(q, True, "swap_ok", "swap_again")
        await q.message.reply_text("Gut, dann kümmern wir uns um die Beilagen.")

        keyboard = [[
            InlineKeyboardButton("Ja",   callback_data="ask_yes"),
            InlineKeyboardButton("Nein", callback_data="ask_no"),
        ]]
        await q.message.reply_text(
            "Möchtest du Beilagen hinzufügen?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ASK_BEILAGEN


##############################################
#>>>>>>>>>>>>FERTIG
##############################################

async def fertig_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.message.from_user.id) not in sessions:
        await update.message.reply_text("⚠️ Keine Menüs gewählt.")
        return ConversationHandler.END
    await update.message.reply_text("Für wie viele Personen?")
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


    faktor = personen / 2
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
    beilage_names = df_beilagen.loc[df_beilagen["Nummer"].isin(all_nums), "Beilage"].tolist()
    zut_beilage = df[
        (df["Typ"] == "Beilage") &
        (df["Gericht"].isin(beilage_names))
    ].copy()

    # 2) Beide DataFrames zusammenführen und skalieren
    zut = pd.concat([zut_gericht, zut_beilage], ignore_index=True)
    zut["Menge"] *= faktor

    eink = (
        zut.groupby(["Zutat", "Kategorie", "Einheit"])["Menge"]
        .sum().reset_index().sort_values("Kategorie")
    )
    eink_text = f"🛒 Einkaufsliste für {personen} Personen:\n"
    for _, r in eink.iterrows():
        if float(r.Menge) == 99:
             # Sonderfall: wenig
             eink_text += f"- {r.Zutat}: wenig | {r.Kategorie}\n"
        else:
            m = format_amount(r.Menge)
            eink_text += f"- {r.Zutat}: {m} {r.Einheit} | {r.Kategorie}\n"

   
    koch_text = "\n🍽 Kochliste:\n"
    
    for g in ausgew:
        # Namen der ausgewählten Beilagen für genau dieses Gericht
        sel_nums = sessions[user_id].get("beilagen", {}).get(g, [])
        beilagen_namen = df_beilagen.loc[
            df_beilagen["Nummer"].isin(sel_nums), "Beilage"
        ].tolist()

        # Zutaten-String wie gehabt
        part_gericht = zut_gericht[zut_gericht["Gericht"] == g]
        part_beilage = pd.DataFrame(columns=df_zutaten.columns)
        if beilagen_namen:
            part_beilage = df_zutaten[
                (df_zutaten["Typ"] == "Beilage") &
                (df_zutaten["Gericht"].isin(beilagen_namen))
            ].copy()
        part = pd.concat([part_gericht, part_beilage], ignore_index=True) if not part_beilage.empty else part_gericht.reset_index(drop=True)
        
        parts = []
        for _, row in part.iterrows():
            if float(row["Menge"]) == 99:
                parts.append(f"{row['Zutat']} wenig")
            else:
                amt = format_amount(row["Menge"])
                parts.append(f"{row['Zutat']} {amt} {row['Einheit']}")
        ze = " | ".join(parts)


        # *** hier die Änderung: Gericht plus Beilagen-Formatierung ***
        title = format_dish_with_sides(g, beilagen_namen)
        # Unterstreichen mit __…__
        koch_text += f"- *{title}*: {ze}\n"




    # — vorherige Personen-Fragen löschen —
    chat_id = update.effective_chat.id
    for mid in sessions[user_id].get("person_msgs", []):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except:
            pass
    sessions[user_id].pop("person_msgs", None)

    # — Einkaufs- & Kochliste senden —
    await context.bot.send_message(
        chat_id=chat_id,
        text=eink_text + koch_text,
        parse_mode="Markdown"
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

    return EXPORT_OPTIONS


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


async def process_pdf_export_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    choice = q.data.split("_")[-1]  # "einkauf", "koch" oder "beides"
    eink_df   = context.user_data.get("einkaufsliste_df")
    koch_text = context.user_data.get("kochliste_text")

    # PDF initialisieren
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.add_font("DejaVu",   "",  "fonts/DejaVuSans.ttf")
    pdf.add_font("DejaVu",   "B", "fonts/DejaVuSans-Bold.ttf")

    # --- Einkaufsliste ---
    if choice in ["einkauf", "beides"]:
        pdf.set_font("DejaVu", "B", 14)
        pdf.cell(0, 10, "Einkaufsliste", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.set_font("DejaVu", "", 12)
        half_epw = pdf.epw / 2
        spacing  = " " * 5
        for _, row in eink_df.iterrows():
            pdf.set_x(pdf.l_margin)
            menge     = format_amount(row["Menge"])
            left_text = f"- {row['Zutat']}: {menge} {row['Einheit']}"
            pdf.cell(pdf.get_string_width(left_text), 8, left_text, new_x=XPos.RIGHT, new_y=YPos.TOP)
            min_indent = pdf.l_margin + pdf.get_string_width(left_text + spacing)
            center_x   = pdf.l_margin + half_epw
            indent_x   = max(min_indent, center_x)
            pdf.set_x(indent_x)
            pdf.cell(0, 8, row["Kategorie"], new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(6)

    # --- Kochliste ---
    if choice in ["koch", "beides"]:
        pdf.set_font("DejaVu", "B", 14)
        pdf.cell(0, 10, "Kochliste", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        lines = [l for l in koch_text.splitlines() if l.startswith("- *")]
        for l in lines:
            m = re.match(r"- \*(.+?)\*: (.+)", l)
            if not m:
                continue
            title, ingredients = m.groups()
            pdf.set_x(pdf.l_margin)
            pdf.set_font("DejaVu", "B", 12)
            pdf.cell(0, 8, f"• {title}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            pdf.set_x(pdf.l_margin + 6)
            pdf.set_font("DejaVu", "", 12)
            pdf.multi_cell(pdf.epw - 6, 8, ingredients)
            pdf.ln(2)

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

async def restart_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Verarbeitet Neustart-Bestätigung (Ja/Nein)."""
    q = update.callback_query
    await q.answer()
    msg = q.message

    if q.data == "restart_no":
        # Frage löschen und zurück ins Aktions-Menu
        try:
            await context.bot.delete_message(chat_id=msg.chat.id, message_id=msg.message_id)
        except:
            pass
        await send_action_menu(msg)
        return EXPORT_OPTIONS

    # == q.data == "restart_yes" ==
    # Session zurücksetzen (Gerichte, Beilagen)
    user_id = str(q.from_user.id)
    sessions.pop(user_id, None)
    save_json(SESSIONS_FILE, sessions)

    # Abschieds-Nachricht + Menüs-Buttons: Menü, Favoriten, Übersicht
    text = "Super, bis bald!👋\n\n Über die Buttons kannst Du jederzeit neu starten."
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🍲 Menü",      callback_data="start_menu"),
        InlineKeyboardButton("⚡ QuickOne",     callback_data="start_quickone"),
        InlineKeyboardButton("🔖 Favoriten", callback_data="start_favs"),
        InlineKeyboardButton("🛠️ Übersicht",     callback_data="start_setup"),
    ]])
    await q.edit_message_text(text, reply_markup=kb)
    return ConversationHandler.END



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
    txt = "⭐ Deine Favoriten:\n" + "\n".join(f"- {d}" for d in favs)
    m1 = await msg.reply_text(txt)
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Ja",   callback_data="fav_edit_yes"),
        InlineKeyboardButton("Nein", callback_data="fav_edit_no")
    ]])
    m2 = await msg.reply_text("Bearbeiten?", reply_markup=kb)
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


async def fav_number_toggle_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    idx = int(q.data.split("_")[2])
    sel = context.user_data.setdefault("fav_del_sel", set())
    sel.symmetric_difference_update({idx})
    total = context.user_data["fav_total"]
    await q.edit_message_reply_markup(build_fav_numbers_keyboard(total, sel))
    return FAV_DELETE_SELECT


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
    m1  = await q.message.reply_text(txt)
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
        "Deine aktuellen Gerichte (* bereits bei den Favoriten)"
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
    txt = "⭐ Deine Favoriten:\n" + "\n".join(f"- {d}" for d in favs)
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
    if uid in profiles:
        del profiles[uid]
        save_profiles()

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
        zutaten["Menge"] *= personen / 2
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
    #app.add_handler(CommandHandler("reset", reset))
    app.add_handler(CommandHandler("favorit", favorit))
    #app.add_handler(CommandHandler("meinefavoriten", meinefavoriten))
    app.add_handler(CommandHandler("delete", delete))


    #### ---- MENU-Conversation ----

    app.add_handler(ConversationHandler(
        entry_points=[
            CommandHandler("menu", menu_start),
            CallbackQueryHandler(menu_start_cb, pattern="^start_menu$")
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
            PERSONS_SELECTION: [CallbackQueryHandler(persons_selection_cb, pattern="^persons_")],
            PERSONS_MANUAL:    [MessageHandler(filters.TEXT & ~filters.COMMAND, persons_manual_cb)],
            TAUSCHE_SELECT:  [CallbackQueryHandler(tausche_select_cb,   pattern=r"^swap_(sel:\d+|done)$")],
            TAUSCHE_CONFIRM: [CallbackQueryHandler(tausche_confirm_cb, pattern=r"^swap_(ok|again)$")],
            PDF_EXPORT_CHOICE: [CallbackQueryHandler(process_pdf_export_choice, pattern="^pdf_export_")],
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
                        CallbackQueryHandler(fav_add_done_cb,          pattern="^fav_add_done$")
            ],
            
        },
        fallbacks=[cancel_handler, reset_handler],
        allow_reentry=True
    ))

    #app.add_handler(CallbackQueryHandler(start_favs_cb,   pattern="^start_favs$"))
    app.add_handler(CallbackQueryHandler(start_setup_cb,  pattern="^start_setup$"))
    app.add_handler(CallbackQueryHandler(setup_ack_cb,    pattern="^setup_ack$"))



    #### ---- QuickOne-Conversation ----
    app.add_handler(ConversationHandler(
        entry_points=[
            CommandHandler("quickone", quickone_start),
            CallbackQueryHandler(quickone_start, pattern="^start_quickone$")
        ],
        states={
            QUICKONE_START:    [CallbackQueryHandler(quickone_start,    pattern="^start_quickone$")],
            QUICKONE_CONFIRM:  [CallbackQueryHandler(quickone_confirm_cb,pattern="^quickone_")],
            PERSONS_SELECTION: [CallbackQueryHandler(persons_selection_cb,pattern="^persons_")],
            PERSONS_MANUAL:    [MessageHandler(filters.TEXT & ~filters.COMMAND, persons_manual_cb)],
            PDF_EXPORT_CHOICE: [CallbackQueryHandler(process_pdf_export_choice, pattern="^pdf_export_")],
            EXPORT_OPTIONS: [
                CallbackQueryHandler(fav_add_start,    pattern="^favoriten$"),
                CallbackQueryHandler(export_to_bring,  pattern="^export_bring$"),
                CallbackQueryHandler(export_to_pdf,    pattern="^export_pdf$"),
                CallbackQueryHandler(restart_start,    pattern="^restart$"),
            ],
            RESTART_CONFIRM: [
                CallbackQueryHandler(restart_confirm_cb, pattern="^restart_yes$"),
                CallbackQueryHandler(restart_confirm_cb, pattern="^restart_no$"),
            ],
        },

        fallbacks=[cancel_handler, reset_handler],
        allow_reentry=True
    ))



    #### ---- Favoriten-Conversation ----
    app.add_handler(ConversationHandler(
        entry_points=[
            CommandHandler("meinefavoriten", fav_start),
            CallbackQueryHandler(fav_start,    pattern="^start_favs$"),
            # neu: Einstieg über Aktions-Menu
            CallbackQueryHandler(fav_add_start, pattern="^favoriten$")
        ],
        states={
            FAV_OVERVIEW: [
                CallbackQueryHandler(fav_overview_cb, pattern="^fav_edit_yes$"),
                CallbackQueryHandler(fav_overview_cb, pattern="^fav_edit_no$")
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
                CallbackQueryHandler(fav_add_done_cb,          pattern="^fav_add_done$")
            ],

        },
        fallbacks=[],
        allow_reentry=True
    ))

    app.add_handler(CallbackQueryHandler(fav_add_number_toggle_cb, pattern=r"^fav_add_\d+$"))
    app.add_handler(CallbackQueryHandler(fav_add_done_cb,          pattern="^fav_add_done$"))


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
