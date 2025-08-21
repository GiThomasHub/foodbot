```python
import os
import re
import json
import random
import pandas as pd
import gspread
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    filters, ConversationHandler, ContextTypes, CallbackQueryHandler
)

# Conversation states
MENU_INPUT, ASK_BEILAGEN, SELECT_MENUES, BEILAGEN_SELECT, FERTIG_PERSONEN, REZEPT_INDEX, REZEPT_PERSONEN = range(7)

# === ENV & Sheets Setup ===
load_dotenv()
TOKEN = os.getenv("TELEGRAM_API_KEY")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
SHEET_ID = os.getenv("SHEET_ID")
SHEET_GERICHTE = os.getenv("SHEET_GERICHTE")
SHEET_ZUTATEN = os.getenv("SHEET_ZUTATEN")

# Instantiate OpenAI & Sheets clients
openai_client = OpenAI(api_key=OPENAI_KEY)
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    os.getenv("GOOGLE_CRED_JSON"), scope
)
client = gspread.authorize(creds)

# Persistence
SESSIONS_FILE = "sessions.json"
FAVORITES_FILE = "favorites.json"
CACHE_FILE = "recipe_cache.json"

# In-memory data
sessions = {}
favorites = {}
recipe_cache = {}

# Helpers to load/save JSON
def load_json(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# Initialize persisted data
sessions = load_json(SESSIONS_FILE)
favorites = load_json(FAVORITES_FILE)
recipe_cache = load_json(CACHE_FILE)

# === Google Sheets Data ===
def lade_gerichtebasis():
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_GERICHTE)
    rows = sheet.get_all_values()[1:]
    data = [
        (row[0], int(row[1]), row[2], row[3])
        for row in rows
        if len(row) >= 4 and row[0] and row[1].isdigit()
    ]
    return pd.DataFrame(data, columns=["Gericht", "Aufwand", "Art", "Beilage"])

def lade_zutaten():
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_ZUTATEN)
    rows = sheet.get_all_values()[1:]
    df = pd.DataFrame(rows, columns=["Gericht", "Zutat", "Kategorie", "Typ", "Menge", "Einheit"])
    df = df[df["Gericht"].notna() & df["Zutat"].notna()]
    df["Menge"] = pd.to_numeric(df["Menge"], errors="coerce").fillna(0)
    return df

def lade_beilagen():
    sheet = client.open_by_key(SHEET_ID).worksheet("Beilagen")
    rows = sheet.get_all_values()[1:]
    df = pd.DataFrame(rows, columns=["Nummer", "Beilage", "Kategorie", "Relevanz", "Aufwand"])
    df["Nummer"] = df["Nummer"].astype(int)
    return df

# parse comma-separated codes to ints
def parse_codes(s: str):
    return [int(x.strip()) for x in s.split(',') if x.strip().isdigit()]

# determine allowed side dishes by rules
def erlaube_beilagen(codes, df_be):
    erlaubt = set()
    if 99 in codes:
        return set(df_be["Nummer"])
    if 88 in codes:
        erlaubt |= set(df_be[df_be["Kategorie"] == "Kohlenhydrate"]["Nummer"])
        codes = [c for c in codes if c != 88]
    if 77 in codes:
        erlaubt |= set(df_be[df_be["Kategorie"] == "Gemüse"]["Nummer"])
        codes = [c for c in codes if c != 77]
    erlaubt |= set(codes)
    return erlaubt

# === Bot Handlers ===

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Willkommen!\n"
        "/menu – Menüs auswählen und Beilagen hinzufügen\n"
        "/fertig – Einkaufsliste erstellen\n"
        "/rezept – Rezept generieren"
    )

async def menu_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Bitte gib Anzahl Menüs an inkl. Verteilung Aufwand (#einfach,#mittel,#schwer).\n"
        "Beispiel: 4 (2,1,1)"
    )
    return MENU_INPUT

async def menu_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        text = update.message.text.strip()
        m = re.match(r"(\d+)\s+\((\d+),(\d+),(\d+)\)", text)
        if not m:
            await update.message.reply_text("⚠️ Format: 4 (2,1,1)")
            return MENU_INPUT
        total, a1, a2, a3 = map(int, m.groups())
        if a1 + a2 + a3 != total:
            await update.message.reply_text("⚠️ Summe stimmt nicht.")
            return MENU_INPUT

        df = lade_gerichtebasis()
        chosen = []
        for lvl, count in zip([1,2,3], [a1,a2,a3]):
            opts = df[df['Aufwand'] == lvl]['Gericht'].tolist()
            chosen += random.sample(opts, min(count, len(opts)))

        uid = str(update.message.from_user.id)
        sessions[uid] = {'menues': chosen, 'beilagen': {}}
        save_json(SESSIONS_FILE, sessions)

        context.user_data['menu_list'] = chosen
        await update.message.reply_text(
            "🎲 Deine Menüs:\n" + "\n".join(f"{i+1}. {g}" for i, g in enumerate(chosen))
        )

        # ask to add side dishes
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
        await query.message.reply_text("Okay, keine Beilagen.")
        return ConversationHandler.END

    # Ja: Menüs zum Belegen auswählen
    menus = context.user_data["menu_list"]
    context.user_data["selected_menus"] = set()
    buttons = [
        InlineKeyboardButton(str(i+1), callback_data=f"select_{i}")
        for i in range(len(menus))
    ] + [InlineKeyboardButton("Fertig", callback_data="select_done")]
    kb = [buttons[i:i+4] for i in range(0, len(buttons), 4)]
    await query.message.reply_text(
        "Für welche Menüs? (Nummern wählen)",
        reply_markup=InlineKeyboardMarkup(kb)
    )
    return SELECT_MENUES

async def select_menus_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    sel = context.user_data["selected_menus"]

    if data == "select_done":
        if not sel:
            await query.message.reply_text("Keine Menüs ausgewählt. Abbruch.")
            return ConversationHandler.END
        context.user_data["to_process"] = sorted(sel)
        context.user_data["menu_idx"] = 0
        return await ask_beilagen_for_menu(query, context)

    idx = int(data.split("_")[1])
    if idx in sel:
        sel.remove(idx)
    else:
        sel.add(idx)

    # Buttons mit Häkchen aktualisieren
    menus = context.user_data["menu_list"]
    buttons = []
    for i, name in enumerate(menus):
        mark = "✅" if i in sel else ""
        buttons.append(InlineKeyboardButton(f"{i+1}{mark}", callback_data=f"select_{i}"))
    buttons.append(InlineKeyboardButton("Fertig", callback_data="select_done"))
    kb = [buttons[i:i+4] for i in range(0, len(buttons), 4)]
    await query.message.edit_reply_markup(InlineKeyboardMarkup(kb))
    return SELECT_MENUES

async def ask_beilagen_for_menu(update_or_query, context: ContextTypes.DEFAULT_TYPE):
    menus = context.user_data["menu_list"]
    idx = context.user_data["to_process"][context.user_data["menu_idx"]]
    gericht = menus[idx]
    dfg = lade_gerichtebasis()
    codes = parse_codes(dfg.loc[dfg["Gericht"]==gericht, "Beilage"].iloc[0])
    dfb = lade_beilagen()
    erlaubt = erlaube_beilagen(codes, dfb)

    buttons = [
        InlineKeyboardButton(name, callback_data=f"beilage_{num}")
        for num, name, cat in zip(dfb["Nummer"], dfb["Beilage"], dfb["Kategorie"])
        if num in erlaubt
    ]
    buttons.append(InlineKeyboardButton("Fertig", callback_data="beilage_done"))
    kb = [buttons[i:i+3] for i in range(0, len(buttons), 3)]

    await update_or_query.message.reply_text(
        f"Wähle Beilagen für: {gericht}\n(max.1 KH +2 Gemüse)",
        reply_markup=InlineKeyboardMarkup(kb)
    )
    uid = str(update_or_query.from_user.id)
    sessions[uid]["beilagen"][gericht] = []
    return BEILAGEN_SELECT

async def beilage_select_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    uid = str(query.from_user.id)
    menus = context.user_data["menu_list"]
    cur = menus[context.user_data["to_process"][context.user_data["menu_idx"]]]
    sel = sessions[uid]["beilagen"][cur]

    if data == "beilage_done":
        context.user_data["menu_idx"] += 1
        if context.user_data["menu_idx"] < len(context.user_data["to_process"]):
            return await ask_beilagen_for_menu(query, context)
        await query.message.reply_text("✅ Beilagen-Auswahl abgeschlossen.")
        return ConversationHandler.END

    num = int(data.split("_")[1])
    dfb = lade_beilagen().set_index("Nummer")
    cat = dfb.loc[num, "Kategorie"]
    count_cat = sum(1 for b in sel if dfb.loc[b, "Kategorie"] == cat)

    if cat == "Kohlenhydrate" and count_cat >= 1:
        await query.answer("Max. 1 Kohlenhydrat-Beilage", show_alert=True)
        return BEILAGEN_SELECT
    if cat == "Gemüse" and count_cat >= 2:
        await query.answer("Max. 2 Gemüse-Beilagen", show_alert=True)
        return BEILAGEN_SELECT

    if num in sel:
        sel.remove(num)
    else:
        sel.append(num)
    save_json(SESSIONS_FILE, sessions)
    return BEILAGEN_SELECT

# ========== Fertig-Handler ==========
async def fertig_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.message.from_user.id)
    if uid not in sessions:
        await update.message.reply_text("⚠️ Keine Menüs gewählt.")
        return ConversationHandler.END
    await update.message.reply_text("Für wie viele Personen?")
    return FERTIG_PERSONEN

async def fertig_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.message.from_user.id)
    try:
        personen = int(update.message.text.strip())
        if personen <= 0:
            raise ValueError
    except:
        await update.message.reply_text("⚠️ Ungültige Zahl.")
        return FERTIG_PERSONEN

    faktor = personen / 2
    dfz = lade_zutaten()
    chosen = sessions[uid]["menu_list"]

    # Hauptgerichte
    zut = dfz[(dfz["Typ"]=="Gericht") & dfz["Gericht"].isin(chosen)].copy()
    # Beilagen
    for g in chosen:
        for b in sessions[uid]["beilagen"].get(g, []):
            name = lade_beilagen().set_index("Nummer").loc[b, "Beilage"]
            zut = zut.append(dfz[(dfz["Typ"]=="Beilage") & (dfz["Gericht"]==name)])

    zut["Menge"] *= faktor
    eink = (
        zut.groupby(["Zutat","Kategorie","Einheit"])["Menge"]
        .sum().reset_index().sort_values("Kategorie")
    )
    text = "🛒 Einkaufsliste:\n"
    for _, r in eink.iterrows():
        m = round(r.Menge,1) if r.Menge % 1 else int(r.Menge)
        text += f"- {r.Zutat} ({r.Kategorie}): {m}{r.Einheit}\n"
    await update.message.reply_markdown(text)
    return ConversationHandler.END

# ========== Rezept-Handler ==========
async def rezept_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.message.from_user.id)
    if uid not in sessions:
        await update.message.reply_text("⚠️ Keine Menüs gewählt.")
        return ConversationHandler.END
    await update.message.reply_text("Welches Menü (z. B. 2)?")
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
    uid = str(update.message.from_user.id)
    try:
        personen = int(update.message.text.strip())
        if personen <= 0:
            raise ValueError
    except:
        await update.message.reply_text("⚠️ Ungültige Anzahl.")
        return REZEPT_PERSONEN

    idx = context.user_data["rezept_idx"]
    menus = sessions[uid]["menu_list"]
    if not (0 <= idx < len(menus)):
        await update.message.reply_text("❌ Ungültiger Index.")
        return ConversationHandler.END

    dish = menus[idx]
    dfz = lade_zutaten()
    zut = dfz[(dfz["Typ"]=="Gericht") & (dfz["Gericht"]==dish)].copy()
    for b in sessions[uid]["beilagen"].get(dish, []):
        name = lade_beilagen().set_index("Nummer").loc[b, "Beilage"]
        zut = zut.append(dfz[(dfz["Typ"]=="Beilage") & (dfz["Gericht"]==name)])

    zut["Menge"] *= personen / 2
    zut_text = "\n".join(
        f"- {row.Zutat}: {int(row.Menge) if row.Menge.is_integer() else round(row.Menge,1)} {row.Einheit}"
        for _, row in zut.iterrows()
    )

    st = lade_gerichtebasis().set_index("Gericht").loc[dish, "Aufwand"]
    time_str = {1: "30 Minuten", 2: "45 Minuten"}.get(st, "1 Stunde")

    cache_key = f"{dish}|{personen}"
    if cache_key in recipe_cache:
        steps = recipe_cache[cache_key]
    else:
        prompt = f"Erstelle ein Rezept für '{dish}' für {personen} Personen:\nZutaten:\n{zut_text}\n\nAnleitung:"
        resp = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        steps = resp.choices[0].message.content.strip()
        recipe_cache[cache_key] = steps
        save_json(CACHE_FILE, recipe_cache)

    await update.message.reply_markdown(
        f"📖 Rezept für *{dish}*:\n\n*Zutaten:*\n{zut_text}\n\n*Zeit:* ca. {time_str}\n\n*Anleitung:*\n{steps}"
    )
    return ConversationHandler.END

# ========== Bot Setup ==========
def main():
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    # Menu + Beilagen Conversation
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("menu", menu_start)],
        states={
            MENU_INPUT:      [MessageHandler(filters.TEXT & ~filters.COMMAND, menu_input)],
            ASK_BEILAGEN:    [CallbackQueryHandler(ask_beilagen_cb)],
            SELECT_MENUES:   [CallbackQueryHandler(select_menus_cb)],
            BEILAGEN_SELECT: [CallbackQueryHandler(beilage_select_cb)],
        },
        fallbacks=[]
    ))

    # Fertig-Flow
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("fertig", fertig_start)],
        states={FERTIG_PERSONEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, fertig_input)]},
        fallbacks=[]
    ))

    # Rezept-Flow
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("rezept", rezept_start)],
        states={
            REZEPT_INDEX:    [MessageHandler(filters.TEXT & ~filters.COMMAND, rezept_index)],
            REZEPT_PERSONEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, rezept_personen)],
        },
        fallbacks=[]
    ))

    print("✅ Bot läuft…")
    app.run_polling()

if __name__ == "__main__":
    main()
```
