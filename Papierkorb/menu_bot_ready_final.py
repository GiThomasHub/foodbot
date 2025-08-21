
import os
import re
import json
import random
import hashlib
import pandas as pd
import gspread
from openai import OpenAI
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    filters,
    ContextTypes,
)

# === ENV & Setup ===
load_dotenv()
TOKEN = os.getenv("TELEGRAM_API_KEY")
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SHEET_ID = "1XzhGPWz7EFJAyZzaJQhoLyl-cTFNEa0yKvst0D0yVUs"
SHEET_NAME = "Basisdaten"

DATA_DIR = "data"
SESSIONS_DIR = os.path.join(DATA_DIR, "sessions")
CACHE_DIR = os.path.join(DATA_DIR, "rezepte_cache")
os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)

# === Session Management ===
def get_session_path(user_id):
    return os.path.join(SESSIONS_DIR, f"{user_id}.json")

def load_session(user_id):
    path = get_session_path(user_id)
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {"menues": [], "personen": 2, "favoriten": []}

def save_session(user_id, session):
    path = get_session_path(user_id)
    with open(path, "w") as f:
        json.dump(session, f, indent=2)

def reset_session(user_id):
    session = load_session(user_id)
    session["menues"] = []
    session["personen"] = 2
    save_session(user_id, session)

# === GPT Rezeptgenerator ===
def get_gpt_rezept(menuname, zutaten, personen):
    key = hashlib.md5((menuname + ",".join(zutaten)).encode()).hexdigest()
    path = os.path.join(CACHE_DIR, f"{key}.txt")
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read()

    prompt = f"""
Erstelle ein detailliertes Rezept für das Gericht "{menuname}" mit diesen Zutaten:
{', '.join(zutaten)}.

Das Rezept soll für genau {personen} Personen gedacht sein.

Bitte gib eine Schritt-für-Schritt Kochanleitung aus, inklusive:
- Zubereitungszeit
- Garmethode
- Kochreihenfolge
- Tipps zur Präsentation
"""

    response = openai_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7
    )
    result = response.choices[0].message.content.strip()
    with open(path, "w") as f:
        f.write(result)
    return result

# === Daten aus Google Sheet ===
def lade_gerichtebasis():
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    daten = sheet.get_all_values()[1:]
    return pd.DataFrame([(row[1], int(row[2])) for row in daten if len(row) >= 3 and row[1] and row[2].isdigit()], columns=["Gericht", "Aufwand"]).drop_duplicates()

def lade_zutaten():
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    daten = sheet.get_all_values()
    extrahiert = [row[4:10] for row in daten if len(row) >= 10]
    df = pd.DataFrame(extrahiert[1:], columns=["Gericht", "Zutat", "Kategorie", "Aufwand", "Menge", "Einheit"])
    df = df[df["Gericht"].notna() & df["Zutat"].notna()]
    df["Aufwand"] = df["Aufwand"].astype(int)
    df["Menge"] = pd.to_numeric(df["Menge"], errors="coerce").fillna(0)
    return df

# === Conversation States ===
FERTIG_PERSONEN, REZEPT_PERSONEN, MENU_VERTEILUNG = range(3)

# Die weiteren Handler folgen darunter...

# === /start ===

async def setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(KOMMANDO_UEBERSICHT)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    load_session(user_id)
    await update.message.reply_text(
        """👋 Willkommen beim Menu Bot!
Nutze /setup für eine Übersicht aller Kommandos."""
    )

# === /setup ===

KOMMANDO_UEBERSICHT = """🛠 Kommandos:
/start – Hilfe & Einstieg
/menu X – Wähle X Menüs, dann Verteilung angeben
/fertig – Erzeugt Einkaufsliste & Kochliste
/rezept X – Rezept zu Menü X erstellen
/reset – Auswahl zurücksetzen
/status – Aktueller Stand
/favorit X – Menü X als Favorit speichern
/meinefavoriten – Favoriten anzeigen
/delete X – Favorit X löschen
/setup – Diese Hilfe anzeigen"""


# === /status ===
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    session = load_session(user_id)
    basis = lade_gerichtebasis()
    antwort = f"✅ Verbindung zum Google Sheet steht.\n📄 Menüs verfügbar: {len(basis)}"
    if session.get("menues"):
        antwort += "\n📝 Aktuelle Auswahl:\n" + "\n".join(f"- {m}" for m in session["menues"])
    else:
        antwort += "\nℹ️ Noch keine Menüauswahl aktiv."
    await update.message.reply_text(antwort)




# === /fertig Conversation ===
async def fertig_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    session = load_session(user_id)
    if not session.get("menues"):
        await update.message.reply_text("⚠️ Du hast noch keine Menüs gewählt.")
        return ConversationHandler.END
    await update.message.reply_text("Für wie viele Personen soll die Liste erstellt werden?")
    return FERTIG_PERSONEN

async def fertig_personen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    try:
        personen = int(update.message.text.strip())
        if personen <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Bitte gib eine gültige Zahl ein.")
        return FERTIG_PERSONEN

    session = load_session(user_id)
    session["personen"] = personen
    save_session(user_id, session)

    faktor = personen / 2
    daten = lade_zutaten()
    ausgewaehlt = session["menues"]
    zutatenliste = daten[daten["Gericht"].isin(ausgewaehlt)].copy()
    zutatenliste["Menge"] *= faktor

    einkaufsliste = (
        zutatenliste.groupby(["Zutat", "Kategorie", "Einheit"])["Menge"]
        .sum().reset_index().sort_values(by="Kategorie")
    )
    einkauf_text = f"🛒 *Einkaufsliste* für {personen} Personen:\n"
    for _, row in einkaufsliste.iterrows():
        menge = round(row['Menge'], 1) if row['Menge'] % 1 else int(row['Menge'])
        einkauf_text += f"- {row['Zutat']} ({row['Kategorie']}): {menge}{row['Einheit']}\n"

    koch_text = "\n\n🍽 *Kochliste*:\n"
    for gericht in ausgewaehlt:
        zutaten = zutatenliste[zutatenliste["Gericht"] == gericht]
        zt = " | ".join(
            f"{z['Zutat']} {round(z['Menge'], 1) if z['Menge'] % 1 else int(z['Menge'])}{z['Einheit']}"
            for _, z in zutaten.iterrows()
        )
        koch_text += f"- *{gericht}*: {zt}\n"

    await update.message.reply_markdown(einkauf_text + koch_text)
    return ConversationHandler.END


# === /rezept Conversation ===
async def rezept_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    text = update.message.text.strip().split()
    if len(text) != 2 or not text[1].isdigit():
        await update.message.reply_text("❌ Format: /rezept 2")
        return ConversationHandler.END

    nummer = int(text[1]) - 1
    session = load_session(user_id)
    if nummer < 0 or nummer >= len(session.get("menues", [])):
        await update.message.reply_text("❌ Ungültige Menü-Nummer.")
        return ConversationHandler.END

    context.user_data["rezept_menunummer"] = nummer
    await update.message.reply_text("Für wie viele Personen soll das Rezept sein?")
    return REZEPT_PERSONEN

async def rezept_personen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    try:
        personen = int(update.message.text.strip())
        if personen <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Bitte gib eine gültige Zahl ein.")
        return REZEPT_PERSONEN

    session = load_session(user_id)
    nummer = context.user_data["rezept_menunummer"]
    menue = session["menues"][nummer]
    zutaten = lade_zutaten()
    zutatenliste = zutaten[zutaten["Gericht"] == menue]["Zutat"].tolist()
    rezept_text = get_gpt_rezept(menue, zutatenliste, personen)
    await update.message.reply_text(f"📖 Rezept für {menue} ({personen} Personen):\n"

{rezept_text}")
    return ConversationHandler.END
def main():
    print("✅ Bot läuft ...")
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("setup", setup))
    app.add_handler(CommandHandler("status", status))

    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("fertig", fertig_start)],
        states={FERTIG_PERSONEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, fertig_personen)]},
        fallbacks=[]
    ))

    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("rezept", rezept_start)],
        states={REZEPT_PERSONEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, rezept_personen)]},
        fallbacks=[]
    ))

    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("menu", menu_start)],
        states={MENU_VERTEILUNG: [MessageHandler(filters.TEXT & ~filters.COMMAND, menu_verteilung)]},
        fallbacks=[]
    ))

    app.run_polling()

if __name__ == "__main__":
    main()
