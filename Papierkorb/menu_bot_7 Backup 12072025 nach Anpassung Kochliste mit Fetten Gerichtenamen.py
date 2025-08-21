import os
import re
import json
import random
import pandas as pd
import gspread
import warnings
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI
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
    CallbackQueryHandler,  # <-- hier
)
from telegram.warnings import PTBUserWarning

warnings.filterwarnings("ignore", category=PTBUserWarning)

MENU_INPUT, ASK_BEILAGEN, SELECT_MENUES, BEILAGEN_SELECT, ASK_FINAL_LIST, ASK_SHOW_LIST, FERTIG_PERSONEN, REZEPT_INDEX, REZEPT_PERSONEN, TAUSCHE_SELECT, TAUSCHE_CONFIRM = range(11)


# === ENV & Sheets Setup ===
load_dotenv()
TOKEN = os.getenv("TELEGRAM_API_KEY")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
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

# === In-Memory Data ===
sessions = {}
favorites = {}
recipe_cache = {}

######################################################################## === Load/Save Helpers ===
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

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Fallback-Handler für /cancel: bricht den aktuellen Flow ab.
    """
    await update.message.reply_text("Abgebrochen.")
    return ConversationHandler.END

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


# Load persisted data
sessions = load_json(SESSIONS_FILE)
favorites = load_json(FAVORITES_FILE)
recipe_cache = load_json(CACHE_FILE)

######################################################################## === Google Sheets Data ===

def lade_gerichtebasis():
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_GERICHTE)
    rows = sheet.get_all_values()
    # Header in rows[0], Daten in rows[1:]
    data = [row[:4] for row in rows[1:]]  # NUR die ersten 4 Spalten
    df = pd.DataFrame(data, columns=["Gericht", "Aufwand", "Art", "Beilage"])
    # Aufwand in Integer konvertieren
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

######################################################################## === Commands ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Willkommen!\n"
        "Übersicht der Befehle:\n"
        "/menu – generiere Gerichtevorschläge\n"
        "/tausche – tauscht Gerichte aus\n"
        "/fertig – Einkaufs- & Kochliste erstellen\n\n"
        "/favorit [x] – speichert Gericht x in deinen Favoriten\n"
        "/meinefavoriten – Übersicht deiner Favoriten\n"
        "/delete [x] – löscht Favorit x\n\n"
        #"/rezept – GPT-Rezept erstellen\n"
        "/status – zeigt aktuelle Gerichtewahl\n"
        "/reset – setzt Session zurück (Favoriten bleiben)\n"               ##### braucht es Reset wirklich? oder ist /neustart neues /reset?
        "/setup – zeigt alle Kommandos\n"
        "/neustart – Startet neuen Prozess (Favoriten bleiben)\n"
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
    )

async def menu_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Bitte gib Anzahl Gerichte an inkl. Aufwand (#einfach, #mittel, #aufwändig).\n"
        "Formatbeispiel: 4 (2,1,1)"
    )
    return MENU_INPUT

async def menu_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        #GANZER TEIL, 5 Zeilen, rausnehmen, falls menüerstellung genug schnell läuft, dass es keine ZWISCHENMELDUNG braucht
        #await update.message.reply_text("Ich suche nun leckere Gerichte…")
        #await context.bot.send_chat_action(
        #chat_id=update.effective_chat.id,
        #action=ChatAction.TYPING
        #)

        text = update.message.text.strip()
        user_id = str(update.message.from_user.id)
        match = re.match(r"(\d+)\s+\((\d+),(\d+),(\d+)\)", text)
        if not match:
            await update.message.reply_text("⚠️ Ungültiges Format. Beispiel: 4 (2,1,1)")
            return MENU_INPUT
        total, a1, a2, a3 = map(int, match.groups())
        if a1 + a2 + a3 != total:
            await update.message.reply_text("⚠️ Achtung: Die Summe muss der angegebenen Anzahl Menüs entsprechen.")
            return MENU_INPUT

        basis = df_gerichte
        bereits = set()
        ausgewaehlt = []
        aufwand_liste = []
        favs = favorites.get(user_id, [])

        for stufe, menge in zip([1, 2, 3], [a1, a2, a3]):
            verf = list(set(basis[basis["Aufwand"] == stufe]["Gericht"]) - bereits)
            picks = []
            for _ in range(min(len(verf), menge)):
                weights = [3 if dish in favs else 1 for dish in verf]
                choice = random.choices(verf, weights=weights, k=1)[0]
                picks.append(choice)
                verf.remove(choice)
            ausgewaehlt += picks
            aufwand_liste += [stufe] * len(picks)
            bereits.update(picks)

        sessions[user_id] = {"menues": ausgewaehlt, "aufwand": aufwand_liste}
        save_json(SESSIONS_FILE, sessions)
        reply = "🎲 Deine Gerichte:\n" + "\n".join(f"{i+1}. {g}" for i, g in enumerate(ausgewaehlt))
        await update.message.reply_text(reply)
        # → Inline-Buttons Ja/Nein
        keyboard = [
            [InlineKeyboardButton("Ja", callback_data="ask_yes")],
            [InlineKeyboardButton("Nein", callback_data="ask_no")],
        ]
        await update.message.reply_text(
            "Möchtest du Beilagen hinzufügen?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ASK_BEILAGEN
    except Exception as e:
        await update.message.reply_text(f"❌ Fehler: {e}")
        return MENU_INPUT

async def ask_beilagen_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "ask_no":
        keyboard = [
            [InlineKeyboardButton("Ja",  callback_data="show_yes")],
            [InlineKeyboardButton("Nein", callback_data="show_no")],
        ]
        await query.message.reply_text(
            "Willst Du die Einkaufsliste und Kochliste anzeigen?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ASK_SHOW_LIST
    # (ask_yes behandeln wir in Schritt 2)
    if query.data == "ask_yes":
        menus = context.user_data.get("menu_list") or sessions[str(query.from_user.id)]["menues"]
        buttons = []
        for i, gericht in enumerate(menus, start=1):
            # Codes parsen und nur weiter, wenn mindestens ein Code ≠ 0 existiert
            codes = parse_codes(df_gerichte.loc[df_gerichte["Gericht"] == gericht, "Beilage"].iloc[0])
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
        # User will nicht – beendet das Gespräch
        return ConversationHandler.END

    # User möchte die Liste sehen: frage nach Personen
    await query.message.reply_text("Für wie viele Personen?")
    return FERTIG_PERSONEN


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
        await query.message.reply_text(
            "✅ Beilagen-Auswahl abgeschlossen!\n"
            "Möchtest Du jetzt die Einkaufsliste & Kochliste sehen?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Ja",  callback_data="final_yes")],
                [InlineKeyboardButton("Nein",callback_data="final_no")],
            ])
           )
        return ASK_FINAL_LIST

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
        buttons.append(InlineKeyboardButton(f"{name}{mark}", callback_data=f"beilage_{n}"))
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
        await query.message.reply_text("Für wie viele Personen?")
        return FERTIG_PERSONEN



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

from telegram import Update
from telegram.ext import ContextTypes

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
    if q.data == "swap_again":
        uid = str(q.from_user.id)
        context.user_data["swap_candidates"] = set()
        kb = build_swap_keyboard(sessions[uid]["menues"], set())
        # rein reply, so that the previously sent „Neue Auswahl“ bleibt stehen
        await q.message.reply_text(
            "Welche Gerichte möchtest Du tauschen?",
            reply_markup=kb
        )
        return TAUSCHE_SELECT


    # 2) Bestätigung 'Ja' → Beilagen-Flow
    #    wir erwarten hier bereits in context.user_data["swapped_indices"]
    # === q.data == "swap_ok" ===
    # Bestätigung als neue Nachricht (Liste bleibt sichtbar)
    await q.message.reply_text("Gut, dann kümmern wir uns um die Beilagen.")

    keyboard = [
        [InlineKeyboardButton("Ja",   callback_data="ask_yes")],
        [InlineKeyboardButton("Nein", callback_data="ask_no")],
    ]
    await q.message.reply_text(
        "Möchtest du Beilagen hinzufügen?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return ASK_BEILAGEN






async def fertig_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.message.from_user.id) not in sessions:
        await update.message.reply_text("⚠️ Keine Menüs gewählt.")
        return ConversationHandler.END
    await update.message.reply_text("Für wie viele Personen?")
    return FERTIG_PERSONEN

async def fertig_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    try:
        personen = int(update.message.text.strip())
        if personen <= 0:
            raise ValueError
    except:
        await update.message.reply_text("⚠️ Ungültige Zahl.")
        return FERTIG_PERSONEN

    faktor = personen / 2
    # 1) Zutaten für Hauptgerichte
    df = df_zutaten
    ausgew = sessions[user_id]["menues"]
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
        m = format_amount(r.Menge)
        eink_text += f"- {r.Zutat} ({r.Kategorie}): {m}{r.Einheit}\n"
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
        ze = " | ".join(
            f"{row['Zutat']} {format_amount(row['Menge'])}{row['Einheit']}"
            for _, row in part.iterrows()
        )

        # *** hier die Änderung: Gericht plus Beilagen-Formatierung ***
        title = format_dish_with_sides(g, beilagen_namen)
        # Unterstreichen mit __…__
        koch_text += f"- *{title}*: {ze}\n"




    await update.message.reply_markdown(eink_text + koch_text)
    return ConversationHandler.END


async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    if user_id in sessions:
        del sessions[user_id]
        save_json(SESSIONS_FILE, sessions)
        await update.message.reply_text("🔁 Session zurückgesetzt.")
    else:
        await update.message.reply_text("ℹ️ Keine aktive Session.")

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

async def meinefavoriten(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    favs = favorites.get(user_id, [])
    if not favs:
        return await update.message.reply_text("ℹ️ Keine Favoriten vorhanden.")
    text = "⭐ Deine Favoriten:\n" + "\n".join(f"{i+1}. {d}" for i,d in enumerate(favs))
    await update.message.reply_text(text)

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

async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)

    # 1) Session-Daten löschen
    sessions.pop(user_id, None)
    save_json(SESSIONS_FILE, sessions)

    # 2) Alle aktiven Conversations des Nutzers beenden
    for conv in context.application.conversations:
        # conv key = (chat_id, user_id) oder ähnlich
        if conv[1] == update.effective_user.id:
            context.application.conversations[conv] = ConversationHandler.END

    await update.message.reply_text("🔁 Session & Dialoge zurückgesetzt.")


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
            f"- {row.Zutat}: {round(row.Menge,1) if row.Menge % 1 else int(row.Menge)} {row.Einheit}"
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

# === Bot Setup ===

def main():
    app = ApplicationBuilder().token(TOKEN).build()
    cancel_handler = CommandHandler("cancel", cancel)
    reset_handler  = CommandHandler("reset", cancel)   # nutzt dieselbe cancel-Funktio
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("setup", setup))
    app.add_handler(CommandHandler("tausche", tausche, filters=filters.Regex(r"^\s*/tausche\s+\d"), block=True))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("tausche", tausche_start)],
        states={
            TAUSCHE_SELECT: [
                CallbackQueryHandler(tausche_select_cb, pattern=r"^swap_(sel:\d+|done)$")
            ],
            TAUSCHE_CONFIRM: [
                CallbackQueryHandler(tausche_confirm_cb, pattern=r"^swap_(ok|again)$")
            ],
        },
        fallbacks=[cancel_handler, reset_handler],
        allow_reentry=True
    ))

    app.add_handler(CallbackQueryHandler(tausche_select_cb, pattern=r"^(swap_sel:\d+|swap_done)$"))
    app.add_handler(CommandHandler("status", status))
    #app.add_handler(CommandHandler("reset", reset))
    app.add_handler(CommandHandler("favorit", favorit))
    app.add_handler(CommandHandler("meinefavoriten", meinefavoriten))
    app.add_handler(CommandHandler("delete", delete))

    # Neue Conversations

    cancel_handler = CommandHandler("cancel", cancel)
    reset_handler  = CommandHandler("reset", cancel)   # nutzt dieselbe cancel-Funktion

    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("menu", menu_start)],
        states={
            MENU_INPUT:    [MessageHandler(filters.TEXT & ~filters.COMMAND, menu_input)],
            ASK_BEILAGEN:  [CallbackQueryHandler(ask_beilagen_cb)],
            SELECT_MENUES:   [CallbackQueryHandler(select_menus_cb)],
            BEILAGEN_SELECT: [CallbackQueryHandler(beilage_select_cb)],
            ASK_FINAL_LIST:  [CallbackQueryHandler(ask_final_list_cb)],
            ASK_SHOW_LIST: [CallbackQueryHandler(ask_showlist_cb)],
            FERTIG_PERSONEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, fertig_input)],
        },
        fallbacks=[cancel_handler, reset_handler],
        allow_reentry=True
    ))

    

    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("fertig", fertig_start)],
        states={FERTIG_PERSONEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, fertig_input)]},
        fallbacks=[cancel_handler, reset_handler],
        allow_reentry=True
    ))

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
