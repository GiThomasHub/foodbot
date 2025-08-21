
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
