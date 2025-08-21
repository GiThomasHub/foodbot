import os
import re
import json
import random
import pandas as pd
import gspread
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ConversationHandler, ContextTypes

MENU_INPUT, FERTIG_PERSONEN, REZEPT_INDEX, REZEPT_PERSONEN = range(4)

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

# === Load/Save Helpers ===
def load_json(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# Load persisted data
sessions = load_json(SESSIONS_FILE)
favorites = load_json(FAVORITES_FILE)
recipe_cache = load_json(CACHE_FILE)

# === Google Sheets Data ===

def lade_gerichtebasis():
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_GERICHTE)
    daten = sheet.get_all_values()[1:]  # Überspringe Header
    gerichte = [(row[0], int(row[1]), row[2]) for row in daten if len(row) >= 3 and row[0] and row[1].isdigit()]
    return pd.DataFrame(gerichte, columns=["Gericht", "Aufwand", "Art"]).drop_duplicates()



def lade_zutaten():
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_ZUTATEN)
    daten = sheet.get_all_values()[1:]  # Überspringe Header
    df = pd.DataFrame(daten, columns=["Gericht", "Zutat", "Kategorie", "Aufwand", "Menge", "Einheit"])
    df = df[df["Gericht"].notna() & df["Zutat"].notna()]
    df["Aufwand"] = pd.to_numeric(df["Aufwand"], errors="coerce").fillna(0).astype(int)
    df["Menge"] = pd.to_numeric(df["Menge"], errors="coerce").fillna(0)
    return df


# === Commands ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Willkommen beim Menu Bot!\n"
        "Verfügbare Befehle:\n"
        "/menu – Menüvorschläge präsentieren\n"
        "/tausche 2 – tauscht Menü 2 aus\n"
        "/fertig – Einkaufsliste & Kochliste erstellen\n"
        "/favorit 2 – speichert Menü 2 als Favorit\n"
        "/meinefavoriten – zeigt deine Favoriten\n"
        "/delete 1 – löscht Favorit 1\n"
        "/rezept – GPT-Rezept erstellen\n"
        "/status – zeigt aktuelle Auswahl\n"
        "/reset – setzt Session zurück (Favoriten bleiben)\n"
        "/setup – zeigt alle Kommandos"
    )

async def setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🛠 Kommandos im Menu Bot:\n"
        "/start – Hilfe & Einführung\n"
        "/menu 3 (1,1,1) – Wähle 3 Menüs (z. B. einfach, mittel, aufwändig)\n"
        "/tausche 2 – Tauscht Menü 2 gegen ein neues\n"
        "/fertig [x] – Einkaufsliste & Kochanleitung für x Personen (Standard: 2)\n"
        "/reset – Setzt deine Auswahl zurück\n"
        "/status – Zeigt gewählte Menüs und Sheet-Status\n\n"
        "/favorit 3 – Setzt Menü 3 als Favorit\n"
        "/meinefavoriten – Zeigt alle deine Favoriten\n"
        "/delete 1 – Entfernt Favoriten-Menü Nr. 1\n\n"
        "/rezept 2 [x] – Erstellt ein Rezept für Menü 2 via KI (optional x Personen)\n"
        "/setup – Zeigt diese Kommandoliste"
    )

async def menu_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Bitte gib Anzahl Menüs an inkl. Verteilung Aufwand (#einfach, #mittel, #aufwändig).\n"
        "Beispiel: 4 (2,1,1)"
    )
    return MENU_INPUT

async def menu_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
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

        basis = lade_gerichtebasis()
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
        reply = "🎲 Deine Menüs:\n" + "\n".join(f"{i+1}. {g}" for i, g in enumerate(ausgewaehlt))
        await update.message.reply_text(reply)
        return ConversationHandler.END
    except Exception as e:
        await update.message.reply_text(f"❌ Fehler: {e}")
        return MENU_INPUT



async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    basis = lade_gerichtebasis()
    reply = f"✅ Google Sheet OK, {len(basis)} Menüs verfügbar.\n"
    if user_id in sessions:
        reply += "📝 Aktuelle Auswahl:\n" + "\n".join(f"- {m}" for m in sessions[user_id]["menues"])
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
    daten = lade_gerichtebasis()
    sess = sessions[user_id]
    menues, aufw = sess["menues"], sess["aufwand"]
    bes = set(menues)
    for arg in args:
        idx = int(arg)-1
        if 0<=idx<len(menues):
            st = aufw[idx]
            cand = list(set(daten[daten["Aufwand"]==st]["Gericht"]) - bes)
            if cand:
                neu = random.choice(cand)
                bes.remove(menues[idx])
                menues[idx] = neu
                bes.add(neu)
    save_json(SESSIONS_FILE, sessions)
    await update.message.reply_text("🔄 Neue Menüs:\n" + "\n".join(f"{i+1}. {g}" for i,g in enumerate(menues)))

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
    df = lade_zutaten()
    ausgew = sessions[user_id]["menues"]
    zut = df[df["Gericht"].isin(ausgew)].copy()
    zut["Menge"] *= faktor
    eink = (
        zut.groupby(["Zutat", "Kategorie", "Einheit"])["Menge"]
        .sum().reset_index().sort_values("Kategorie")
    )
    eink_text = f"🛒 Einkaufsliste für {personen} Personen:\n"
    for _, r in eink.iterrows():
        m = round(r.Menge, 1) if r.Menge % 1 else int(r.Menge)
        eink_text += f"- {r.Zutat} ({r.Kategorie}): {m}{r.Einheit}\n"
    koch_text = "\n🍽 Kochliste:\n"
    for g in ausgew:
        part = zut[zut.Gericht == g]
        ze = " | ".join(f"{z.Zutat} {round(z.Menge,1) if z.Menge%1 else int(z.Menge)}{z.Einheit}" for _, z in part.iterrows())
        koch_text += f"- {g}: {ze}\n"
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
        df = lade_zutaten()
        zutaten = df[df["Gericht"] == dish].copy()
        zutaten["Menge"] *= personen / 2
        zut_text = "\n".join(
            f"- {row.Zutat}: {round(row.Menge,1) if row.Menge % 1 else int(row.Menge)} {row.Einheit}"
            for _, row in zutaten.iterrows()
        )

        basis = lade_gerichtebasis()
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
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("setup", setup))
    app.add_handler(CommandHandler("tausche", tausche))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("reset", reset))
    app.add_handler(CommandHandler("favorit", favorit))
    app.add_handler(CommandHandler("meinefavoriten", meinefavoriten))
    app.add_handler(CommandHandler("delete", delete))

    # Neue Conversations
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("menu", menu_start)],
        states={MENU_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, menu_input)]},
        fallbacks=[]
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("fertig", fertig_start)],
        states={FERTIG_PERSONEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, fertig_input)]},
        fallbacks=[]
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("rezept", rezept_start)],
        states={
            REZEPT_INDEX: [MessageHandler(filters.TEXT & ~filters.COMMAND, rezept_index)],
            REZEPT_PERSONEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, rezept_personen)],
        },
        fallbacks=[]
    ))

    print("✅ Bot läuft...")
    app.run_polling()


if __name__ == "__main__":
    main()
