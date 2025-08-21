import os
import re
import random
import pandas as pd
import gspread
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# === ENV & Sheets Setup ===
load_dotenv()
TOKEN = os.getenv("TELEGRAM_API_KEY")
SHEET_ID = "1XzhGPWz7EFJAyZzaJQhoLyl-cTFNEa0yKvst0D0yVUs"
SHEET_NAME = "Basisdaten"

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)

# Session cache: user_id → {menues: [...], aufwand: [...]}
sessions = {}

# === Menübasis laden (Spalte B/C) ===
def lade_gerichtebasis():
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    daten = sheet.get_all_values()[1:]
    gerichte = []
    for row in daten:
        if len(row) >= 3 and row[1] and row[2].isdigit():
            gerichte.append((row[1], int(row[2])))  # (Gericht, Aufwand)
    return pd.DataFrame(gerichte, columns=["Gericht", "Aufwand"]).drop_duplicates()

# === Zutatendaten (Spalte E–J) ===
def lade_zutaten():
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    daten = sheet.get_all_values()
    extrahiert = [row[4:10] for row in daten if len(row) >= 10]
    df = pd.DataFrame(extrahiert[1:], columns=["Gericht", "Zutat", "Kategorie", "Aufwand", "Menge", "Einheit"])
    df = df[df["Gericht"].notna() & df["Zutat"].notna()]
    df["Aufwand"] = df["Aufwand"].astype(int)
    df["Menge"] = pd.to_numeric(df["Menge"], errors="coerce").fillna(0)
    return df

# === /start ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Willkommen beim Menu Bot!\n"
        "/menu 3 (1,1,1) → 3 Menüs: 1x einfach, 1x mittel, 1x aufwaendig\n"
        "/tausche 2 → tauscht Menü 2\n"
        "/fertig → zeigt Einkaufsliste & Kochliste (für 2 Personen)\n"
        "/fertig 4 → zeigt Liste für 4 Personen"
    )


# === /menu ===
async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    text = update.message.text.strip()
    print(f"[INFO] /menu empfangen von {user_id}: {text}")

    try:
        match = re.match(r"/menu\s+(\d+)\s+\((\d+),(\d+),(\d+)\)", text)
        if not match:
            raise ValueError("Falsches Format.")

        total = int(match.group(1))
        a1, a2, a3 = int(match.group(2)), int(match.group(3)), int(match.group(4))
        print(f"[INFO] Verteilung erkannt: total={total}, a1={a1}, a2={a2}, a3={a3}")

        basis = lade_gerichtebasis()
        print(f"[INFO] Geladene Basisgerichte: {len(basis)}")

        bereits = set()
        ausgewaehlt = []
        aufwand_liste = []

        for stufe, menge in zip([1, 2, 3], [a1, a2, a3]):
            verfuegbar = list(set(basis[basis["Aufwand"] == stufe]["Gericht"]) - bereits)
            print(f"[DEBUG] Aufwand {stufe}: {len(verfuegbar)} verfügbar")
            zufaellig = random.sample(verfuegbar, min(len(verfuegbar), menge))
            ausgewaehlt.extend(zufaellig)
            aufwand_liste.extend([stufe] * len(zufaellig))
            bereits.update(zufaellig)

        sessions[user_id] = {"menues": ausgewaehlt, "aufwand": aufwand_liste}

        antwort = "\n".join([f"{i+1}. {g}" for i, g in enumerate(ausgewaehlt)])
        print(f"[INFO] Auswahl erfolgreich: {ausgewaehlt}")
        await update.message.reply_text(f"🎲 Deine Menüs:\n{antwort}\n\nMit /tausche 2 kannst du Menü 2 neu ziehen.")

    except Exception as e:
        import traceback
        fehlertext = "".join(traceback.format_exception(None, e, e.__traceback__))
        print(f"[ERROR] {fehlertext}")
        await update.message.reply_text(f"❌ Interner Fehler:\n```\n{fehlertext}\n```", parse_mode="Markdown")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    try:
        basis = lade_gerichtebasis()
        antwort = f"✅ Verbindung zum Google Sheet steht.\n📄 Menüs verfügbar: {len(basis)}"

        if user_id in sessions:
            menues = sessions[user_id]["menues"]
            antwort += f"\n📝 Aktuelle Auswahl:\n" + "\n".join(f"- {m}" for m in menues)
        else:
            antwort += "\nℹ️ Noch keine Menüauswahl aktiv."

        await update.message.reply_text(antwort)

    except Exception as e:
        import traceback
        fehlertext = "".join(traceback.format_exception(None, e, e.__traceback__))
        await update.message.reply_text(f"❌ Fehler bei /status:\n```\n{fehlertext}\n```", parse_mode="Markdown")



# === /tausche ===
async def tausche(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in sessions:
        await update.message.reply_text("⚠️ Bitte zuerst /menu verwenden.")
        return

    args = context.args
    if not args or not all(arg.isdigit() for arg in args):
        await update.message.reply_text("❌ Bitte gib z. B. ein: /tausche 1 3")
        return

    daten = lade_gerichtebasis()
    session = sessions[user_id]
    menues = session["menues"]
    aufwand = session["aufwand"]
    bereits = set(menues)

    for arg in args:
        i = int(arg) - 1
        if 0 <= i < len(menues):
            stufe = aufwand[i]
            verfuegbar = list(set(daten[daten["Aufwand"] == stufe]["Gericht"]) - bereits)
            if verfuegbar:
                neu = random.choice(verfuegbar)
                bereits.remove(menues[i])
                menues[i] = neu
                bereits.add(neu)

    antwort = "\n".join([f"{i+1}. {g}" for i, g in enumerate(menues)])
    await update.message.reply_text(f"🔄 Neue Menüs:\n{antwort}")


# === /fertig ===
async def fertig(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in sessions:
        await update.message.reply_text("⚠️ Du hast noch keine Menüs gewählt.")
        return

    # Personenanzahl extrahieren
    try:
        personen = int(context.args[0]) if context.args else 2
        if personen <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Ungültige Personenanzahl. Beispiel: /fertig 4")
        return

    faktor = personen / 2
    daten = lade_zutaten()
    ausgewaehlt = sessions[user_id]["menues"]

    # Nur relevante Zutaten & skalieren
    zutatenliste = daten[daten["Gericht"].isin(ausgewaehlt)].copy()
    zutatenliste["Menge"] = pd.to_numeric(zutatenliste["Menge"], errors="coerce").fillna(0)
    zutatenliste["Menge"] *= faktor

    # 🛒 Einkaufsliste
    einkaufsliste = (
        zutatenliste.groupby(["Zutat", "Kategorie", "Einheit"])["Menge"]
        .sum()
        .reset_index()
        .sort_values(by="Kategorie")
    )

    einkauf_text = f"🛒 *Einkaufsliste* für {personen} Personen:\n"
    for _, row in einkaufsliste.iterrows():
        menge = round(row['Menge'], 1) if row['Menge'] % 1 else int(row['Menge'])
        einkauf_text += f"- {row['Zutat']} ({row['Kategorie']}): {menge}{row['Einheit']}\n"

    # 🍽 Kochliste
    koch_text = "\n\n🍽 *Kochliste*:\n"
    for gericht in ausgewaehlt:
        zutaten = zutatenliste[zutatenliste["Gericht"] == gericht]
        zt = " | ".join(
            f"{z['Zutat']} {round(z['Menge'], 1) if z['Menge'] % 1 else int(z['Menge'])}{z['Einheit']}"
            for _, z in zutaten.iterrows()
        )
        koch_text += f"- *{gericht}*: {zt}\n"

    await update.message.reply_markdown(einkauf_text + koch_text)


# === /reset ===
async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id in sessions:
        del sessions[user_id]
        await update.message.reply_text("🔁 Deine Session wurde zurückgesetzt.")
    else:
        await update.message.reply_text("ℹ️ Es war keine aktive Session vorhanden.")



# === Startfunktion ===
def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", menu))
    app.add_handler(CommandHandler("fertig", fertig))
    app.add_handler(CommandHandler("tausche", tausche))
    app.add_handler(CommandHandler("reset", reset))
    app.add_handler(CommandHandler("status", status))


    print("✅ Bot läuft ...")
    
    app.add_handler(CommandHandler("favorit", favorit))
    app.add_handler(CommandHandler("meinefavoriten", meinefavoriten))
    app.add_handler(CommandHandler("delete", delete))
    app.add_handler(CommandHandler("rezept", rezept))
    app.run_polling()


if __name__ == "__main__":
    main()


# === Erweiterung: JSON Session Store ===
import json
SESSIONS_DIR = "data/sessions"
os.makedirs(SESSIONS_DIR, exist_ok=True)

def session_path(user_id):
    return os.path.join(SESSIONS_DIR, f"{user_id}.json")

def load_session(user_id):
    try:
        with open(session_path(user_id), "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"menues": [], "aufwand": [], "favoriten": []}

def save_session(user_id, session_data):
    with open(session_path(user_id), "w") as f:
        json.dump(session_data, f, indent=2)

# === Favoritenfunktionen ===
async def favorit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    session = load_session(user_id)
    try:
        nummer = int(context.args[0]) - 1
        menues = session.get("menues", [])
        if 0 <= nummer < len(menues):
            menue = menues[nummer]
            if menue not in session.get("favoriten", []):
                session["favoriten"].append(menue)
                save_session(user_id, session)
                await update.message.reply_text(f"✅ Menü {nummer+1} als Favorit gespeichert.")
            else:
                await update.message.reply_text("⚠️ Dieses Menü ist schon in deinen Favoriten.")
        else:
            await update.message.reply_text("❌ Ungültige Menü-Nummer.")
    except:
        await update.message.reply_text("❌ Format: /favorit 3")


async def meinefavoriten(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    favoriten = load_session(user_id).get("favoriten", [])
    if not favoriten:
        await update.message.reply_text("📭 Du hast noch keine Favoriten.")
        return
    msg = "🌟 Deine Favoriten:\\n" + "\\n".join(f"{i+1}. {f}" for i, f in enumerate(favoriten))

    await update.message.reply_text(msg)

async def delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    session = load_session(user_id)
    try:
        nummer = int(context.args[0]) - 1
        if 0 <= nummer < len(session["favoriten"]):
            entfernt = session["favoriten"].pop(nummer)
            save_session(user_id, session)
            await update.message.reply_text(f"🗑 Favorit „{entfernt}“ gelöscht.")
        else:
            await update.message.reply_text("❌ Ungültige Favoriten-Nummer.")
    except:
        await update.message.reply_text("❌ Format: /delete 1")

# === GPT Rezept Generator mit Cache ===
import hashlib
from openai import OpenAI
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
CACHE_DIR = "data/rezepte_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def get_gpt_rezept(menuname, zutaten):
    key = hashlib.md5((menuname + ",".join(zutaten)).encode()).hexdigest()
    path = os.path.join(CACHE_DIR, f"{key}.txt")
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read()

    prompt = f"""
Erstelle ein detailliertes Rezept für das Gericht "{menuname}" mit diesen Zutaten:
{', '.join(zutaten)}.

Das Rezept soll für genau 2 Personen sein.

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

async def rezept(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    session = load_session(user_id)
    try:
        nummer = int(context.args[0]) - 1
        menues = session.get("menues", [])
        if 0 <= nummer < len(menues):
            menue = menues[nummer]
            zutaten = lade_zutaten()
            zutatenliste = zutaten[zutaten["Gericht"] == menue]["Zutat"].tolist()
            rezept_text = get_gpt_rezept(menue, zutatenliste)
            await update.message.reply_text(f"📖 Rezept für {menue}:\n\n{rezept_text}")

        else:
            await update.message.reply_text("❌ Ungültige Menü-Nummer.")
    except:
        await update.message.reply_text("❌ Format: /rezept 2")

