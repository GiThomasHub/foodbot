import os
import re
import json
import random
import pandas as pd
import gspread
import warnings
import urllib.request
import logging
import base64, gzip, time
import httpx
import math
import asyncio
from aiohttp import web
from html import escape, unescape
from datetime import datetime
from pathlib import Path
from collections import Counter
from fpdf import FPDF                                         #könnte gelöscht werden -> ausprobieren wenn mal zeit besteht
from fpdf.enums import XPos, YPos
from dotenv import load_dotenv
from openai import OpenAI
from decimal import Decimal, ROUND_HALF_UP
from google.cloud import firestore
from telegram.constants import ParseMode
from google.oauth2.service_account import Credentials
from telegram.error import BadRequest
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
import telegram
from persistence import (
    user_key,
    get_profile as store_get_profile, set_profile as store_set_profile,
    get_favorites as store_get_favorites, set_favorites as store_set_favorites,
    # Neu für Sessions (pro Chat):
    chat_key,
    get_session as store_get_session, set_session as store_set_session, delete_session as store_delete_session,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ConversationHandler,
    ContextTypes,
    CallbackQueryHandler,
    Defaults,
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


HTTPX_TIMEOUT = httpx.Timeout(10.0, connect=3.0)  # 3s Connect, 10s gesamt
HTTPX_LIMITS  = httpx.Limits(max_connections=100, max_keepalive_connections=20)
HTTPX_CLIENT  = httpx.AsyncClient(timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS, follow_redirects=False)


MENU_INPUT, ASK_BEILAGEN, SELECT_MENUES, BEILAGEN_SELECT, ASK_FINAL_LIST, ASK_SHOW_LIST, FERTIG_PERSONEN, REZEPT_INDEX, REZEPT_PERSONEN, TAUSCHE_SELECT, TAUSCHE_CONFIRM, ASK_CONFIRM, EXPORT_OPTIONS, FAV_OVERVIEW, FAV_DELETE_SELECT, PDF_EXPORT_CHOICE, FAV_ADD_SELECT, RESTART_CONFIRM, PROFILE_CHOICE, PROFILE_NEW_A, PROFILE_NEW_B, PROFILE_NEW_C, PROFILE_OVERVIEW, QUICKONE_START, QUICKONE_CONFIRM, PERSONS_SELECTION, PERSONS_MANUAL, MENU_COUNT, MENU_AUFWAND = range(29)

# HELPER:

# === Cloud Run: Health + Telegram Webhook via aiohttp ===

def _compute_base_url():
    # 1) Falls gesetzt, einfach nehmen
    env_url = (os.getenv("PUBLIC_URL") or os.getenv("BASE_URL") or "").strip()
    if env_url:
        return env_url

    # 2) In Cloud Run: kanonische URL selbst bauen
    if os.getenv("K_SERVICE"):
        try:
            def _meta(path):
                req = urllib.request.Request(
                    f'http://metadata.google.internal/computeMetadata/v1/{path}',
                    headers={"Metadata-Flavor": "Google"}
                )
                with urllib.request.urlopen(req, timeout=2) as r:
                    return r.read().decode()

            service = os.getenv("K_SERVICE")
            project_num = _meta("project/numeric-project-id")
            region = _meta("instance/region").split("/")[-1]  # .../regions/<region>
            return f"https://{service}-{project_num}.{region}.run.app"
        except Exception as e:
            print(f"⚠️ Konnte kanonische URL nicht ermitteln: {e}")

    # 3) Fallback: leer -> Health-Server
    return ""



# === ENV & Sheets Setup ===
load_dotenv()
TOKEN = os.getenv("TELEGRAM_API_KEY")
BASE_URL = _compute_base_url()
print(f"ENV CHECK → PORT={os.getenv('PORT','8080')} BASE_URL={'gesetzt' if BASE_URL else 'leer'}")
WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # darf None sein
SHEET_ID = os.getenv("SHEET_ID", "1XzhGPWz7EFJAyZzaJQhoLyl-cTFNEa0yKvst0D0yVUs")
SHEET_GERICHTE = os.getenv("SHEET_GERICHTE", "Gerichte")
SHEET_ZUTATEN = os.getenv("SHEET_ZUTATEN", "Zutaten")
PERSISTENCE = (os.getenv("PERSISTENCE") or "json").strip().lower()
SHEETS_CACHE_TTL_SEC = int(os.getenv("SHEETS_CACHE_TTL_SEC", "3600"))
SHEETS_CACHE_NAMESPACE = os.getenv("SHEETS_CACHE_NAMESPACE", "v1")

# Firestore-Client nur nutzen, wenn PERSISTENCE=firestore (Prod)
try:
    FS = firestore.Client() if PERSISTENCE == "firestore" else None
except Exception as e:
    logging.warning("Firestore-Init fehlgeschlagen (%s) – Sheets-Cache wird deaktiviert.", e)
    FS = None


def _get_openai_client():
    """
    OpenAI nur verwenden, wenn OPENAI_API_KEY gesetzt ist. 
    Fehlerresistent initialisieren (kein Crash bei fehlendem Key/Paket).
    """
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        return None
    try:
        from openai import OpenAI
        return OpenAI(api_key=key, timeout=20, max_retries=1)
    except Exception as e:
        logging.warning("OpenAI init übersprungen: %s", e)
        return None

def _fallback_steps(dish: str, zut_text: str) -> str:
    return (
        "1) Zutaten bereitstellen und vorbereiten.\n"
        f"2) {dish} nach üblicher Methode zubereiten.\n"
        "3) Abschmecken und anrichten.\n"
        "4) Guten Appetit!"
    )

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
gc = os.getenv("GOOGLE_CRED_JSON")
if gc and gc.strip().startswith("{"):
    creds = Credentials.from_service_account_info(json.loads(gc), scopes=scope)
else:
    creds = Credentials.from_service_account_file(gc or "credentials.json", scopes=scope)
client = gspread.authorize(creds)


# === Persistence Files ===
DATA_DIR = os.getenv("DATA_DIR","/tmp")
os.makedirs(DATA_DIR, exist_ok=True)
SESSIONS_FILE = os.path.join(DATA_DIR, "sessions.json")
FAVORITES_FILE = os.path.join(DATA_DIR, "favorites.json")
CACHE_FILE = os.path.join(DATA_DIR, "recipe_cache.json")
PROFILES_FILE = os.path.join(DATA_DIR, "profiles.json")
FAV_FILE = FAVORITES_FILE
HISTORY_FILE = os.path.join(DATA_DIR, "history.json")


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
    "Fleisch & Fisch":       "🥩",    #"🥩🐟",
    "Obst & Gemüse":        "🥕",     #"🍎🥕",
    "Getränke":      "🧃",            #"🧃🍷",
    "Trockenware & Vorrat":"🥫",      #"🍝🥫",
    "Milchwaren":    "🥛",           #"🧀🥛",
    "Backwaren":     "🥖",       #"🥖🥐",
    "Kühlregal": "🥶",             #"🥶🧊",
    "Haushalt & Sonstiges": "🧻",  #"🧽🧻",

}


WEIGHT_CHOICES = {f"weight_{i}": i for i in range(1, 8)}
WEIGHT_CHOICES["weight_any"] = None          #  «Egal» = keine Einschränkung





#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# === Utilites: Load/Save Helpers ===
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Nur für Admins: Aufwand-Verteilung anzeigen
def show_debug_for(obj) -> bool:
    default_ids = {7650843881}
    raw = (os.getenv("ADMIN_IDS") or "").strip()
    try:
        env_ids = {int(x) for x in raw.split(",") if x.strip()}
    except ValueError:
        env_ids = set()
    admin_ids = env_ids or default_ids

    uid = None
    # Update
    if getattr(obj, "effective_user", None):
        uid = obj.effective_user.id
    # CallbackQuery
    elif getattr(obj, "from_user", None):
        uid = obj.from_user.id
    # Message (Fallback)
    elif getattr(obj, "message", None) and getattr(obj.message, "from_user", None):
        uid = obj.message.from_user.id

    return uid in admin_ids if uid is not None else False


# ---------- Sheets-Cache (Firestore) ----------
def _df_to_compact_json(df: pd.DataFrame) -> dict:
    """Sehr kompaktes Format: Spalten + Zeilen als Liste von Listen."""
    return {
        "cols": list(df.columns),
        "rows": df.astype(object).values.tolist(),  # vermeidet numpy-Types im JSON
    }

def _compact_json_to_df(obj: dict) -> pd.DataFrame:
    return pd.DataFrame(obj["rows"], columns=obj["cols"])

def _fs_doc_for(name: str):
    """
    name ∈ {"gerichte","beilagen","zutaten"} → Doc-Pfad:
    sheets_cache/<SHEET_ID>/<NAMESPACE>/<name>
    """
    return FS.collection("sheets_cache").document(SHEET_ID).collection(SHEETS_CACHE_NAMESPACE).document(name)

def _cache_read_if_fresh(name: str, ttl_sec: int):
    if not FS:
        return None
    try:
        doc = _fs_doc_for(name).get()
    except Exception as e:
        logging.warning("Sheets-Cache: Firestore-Read fehlgeschlagen (%s) → Fallback auf Sheets", e)
        return None

    if not doc.exists:
        return None

    d = doc.to_dict() or {}
    updated_ts = int(d.get("updated_ts", 0))
    if time.time() - updated_ts > ttl_sec:
        return None  # abgelaufen

    try:
        payload = gzip.decompress(base64.b64decode(d["payload_b64_gzip"]))
        return json.loads(payload.decode("utf-8"))
    except Exception as e:
        logging.warning("Sheets-Cache: Dekomprimieren/JSON fehlgeschlagen (%s) → Fallback auf Sheets", e)
        return None


def _cache_write(name: str, compact_obj: dict):
    if not FS:
        return
    try:
        payload = json.dumps(compact_obj, ensure_ascii=False).encode("utf-8")
        b64 = base64.b64encode(gzip.compress(payload)).decode("ascii")
        _fs_doc_for(name).set(
            {
                "payload_b64_gzip": b64,
                "updated_ts": int(time.time()),
                "ttl_sec": SHEETS_CACHE_TTL_SEC,
                "schema_version": 1,
            },
            merge=True,
        )
    except Exception as e:
        logging.warning("Sheets-Cache: Firestore-Write fehlgeschlagen (%s) – ignoriere und fahre fort", e)


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

def dishes_header(count: int, step: int | None = None) -> str:
    """
    Baut den Titel für die Gerichte-Liste.
    - count=1  → 'Deine Auswahl:'
    - count>1  → 'Deine Auswahl:'
    - step     → optionaler Debug-/Schrittzähler in Klammern, z. B. (3)
    """
    base = "Deine Auswahl" if count == 1 else "Deine Auswahl"
    suffix = f"({step})" if step is not None else ""
    return f"🥣 <u><b>{base}:</b></u>"      #falls nummerierung nach "Deine Gerichte" bspw. "Deine Gerichte(1)" nötig: return f"🥣 <u><b>{base}{suffix}:</b></u>"

# Zentral "Deine Gerichte"
async def show_final_dishes_and_ask_persons(update: Update, context: ContextTypes.DEFAULT_TYPE, *, step: int | None = None, remove_proposal: bool = True, clear_flow: bool = True,) -> int:
    """
    Zentrale Ausgabe:
      1) optional 'Vorschlag/Neuer Vorschlag' + zugehöriges Verteilungs-Debug entfernen
      2) optional Debug/flow-Messages (flow_msgs) löschen
      3) finale Gerichteliste rendern
      4) Personen-Dialog starten
    """
    uid = str(update.effective_user.id)
    chat_id = update.effective_chat.id

    # 1) Karte + Debug IMMER als Paar behandeln
    if remove_proposal:
        await delete_proposal_card(context, chat_id)

    # 2) Flow-Messages aufräumen
    if clear_flow:
        await reset_flow_state(
            update, context,
            reset_session=False, delete_messages=True,
            only_keys=["flow_msgs"]
        )

    # 3) Finale Liste
    menus = sessions.get(uid, {}).get("menues", [])
    text  = dishes_header(len(menus), step=step) + "\n"
    for dish in menus:
        sel_nums   = sessions[uid].get("beilagen", {}).get(dish, [])
        side_names = df_beilagen.loc[df_beilagen["Nummer"].isin(sel_nums), "Beilagen"].tolist()
        text      += format_hanging_line(
            escape(format_dish_with_sides(dish, side_names)),
            bullet="‣", indent_nbsp=2, wrap_at=60
        ) + "\n"

    msg = await context.bot.send_message(chat_id, pad_message(text))
    context.user_data.setdefault("flow_msgs", []).append(msg.message_id)

    # 4) Personen-Dialog
    return await ask_for_persons(update, context)



# ---- Debounced Redraw für 'Definiere Aufwand' ----
async def _debounced_aufwand_render(q, context):
    # schon ein Render geplant?
    task = context.user_data.get("aufw_render_task")
    if task and not task.done():
        # nur markieren, dass ein neuer Stand vorhanden ist
        context.user_data["aufw_dirty"] = True
        return

    context.user_data["aufw_dirty"] = True

    async def _runner():
        try:
            # kurzer Puffer für schnelle Mehrfachklicks
            await asyncio.sleep(0.12)
            if context.user_data.get("aufw_dirty"):
                context.user_data["aufw_dirty"] = False
                verteilung = context.user_data["aufwand_verteilung"]
                total = context.user_data["menu_count"]
                await q.message.edit_reply_markup(
                    reply_markup=build_aufwand_keyboard(verteilung, total)
                )
        except Exception:
            pass
        finally:
            context.user_data["aufw_render_task"] = None

    context.user_data["aufw_render_task"] = asyncio.create_task(_runner())


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

def build_swap_keyboard(menus: list[str], selected: set[int]) -> InlineKeyboardMarkup:
    """Buttons 1…N mit Toggle-Häkchen + dynamischem Footer ('(kein tausch)'/ 'Weiter')."""
    btns = []
    for idx, _g in enumerate(menus, 1):
        label = f"{'✅ ' if idx in selected else ''}{idx}"
        btns.append(InlineKeyboardButton(label, callback_data=f"swap_sel:{idx}"))
    rows = distribute_buttons_equally(btns, max_per_row=7)
    footer_label = "Weiter" if selected else "(kein Tausch)"
    rows.append([InlineKeyboardButton(footer_label, callback_data="swap_done")])
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

def ensure_profile_loaded(uid_str: str) -> bool:
    """
    Stellt sicher, dass ein Profil für uid_str im lokalen Dict 'profiles' liegt.
    Falls nicht vorhanden, wird es aus dem Persistenz-Layer (JSON/Firestore)
    nachgeladen und in 'profiles' zwischengespeichert.
    Rückgabe: True, wenn Profil vorhanden (nach dem Schritt), sonst False.
    """
    if uid_str in profiles and isinstance(profiles[uid_str], dict):
        return True
    try:
        ukey = user_key(int(uid_str))
    except Exception:
        return False
    data = store_get_profile(ukey)
    if data:
        profiles[uid_str] = data
        return True
    return False

def ensure_favorites_loaded(uid_str: str) -> None:
    """
    Stellt sicher, dass favorites[uid_str] eine Liste ist.
    Lädt sie bei Bedarf aus dem Persistenz-Layer (JSON/Firestore).
    """
    if uid_str in favorites and isinstance(favorites[uid_str], list):
        return
    try:
        ukey = user_key(int(uid_str))
        favorites[uid_str] = store_get_favorites(ukey)
        if not isinstance(favorites[uid_str], list):
            favorites[uid_str] = []
    except Exception:
        favorites.setdefault(uid_str, [])

def ensure_session_loaded_for_user_and_chat(update: Update) -> tuple[str, str]:
    """
    Lädt (falls nötig) die Chat-Session aus der Persistenz (Key = chat_id)
    und legt sie in-memory unter sessions[uid] ab (Key = user_id).
    Rückgabe: (uid_str, cid_str)
    """
    uid = str(update.effective_user.id)
    cid = str(update.effective_chat.id)

    # Falls already vorhanden & nicht leer → fertig
    if uid in sessions and isinstance(sessions[uid], dict) and sessions[uid]:
        return uid, cid

    # Aus Store pro Chat laden → unter uid ablegen
    try:
        ckey = chat_key(int(cid))
        data = store_get_session(ckey)
        sessions[uid] = data if isinstance(data, dict) else {}
    except Exception:
        sessions.setdefault(uid, {})

    return uid, cid


def persist_session(update: Update) -> None:
    """
    Persistiert die aktuelle Session (Key = user_id in-memory) unter dem
    Chat-Schlüssel (Key = chat_id) im Store.
    """
    uid = str(update.effective_user.id)
    cid = str(update.effective_chat.id)
    try:
        ckey = chat_key(int(cid))
        store_set_session(ckey, sessions.get(uid, {}))
    except Exception:
        # bewusst keine harten Fehler im Bot
        pass




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


async def cleanup_prof_loop_except_start(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """
    Löscht alle während des Profil-Wizards entstandenen Nachrichten,
    AUSGENOMMEN die gemerkte Startfrage 'Wie möchtest Du fortfahren?'.
    """
    start_id = context.user_data.get("prof_start_msg_id")
    msg_ids: list[int] = context.user_data.get("prof_msgs", [])

    for mid in msg_ids:
        if isinstance(start_id, int) and mid == start_id:
            continue
        try:
            await context.bot.delete_message(chat_id, mid)
        except Exception:
            pass

    # Liste auf die Startfrage reduzieren (falls vorhanden)
    if isinstance(start_id, int):
        context.user_data["prof_msgs"] = [start_id]
    else:
        context.user_data["prof_msgs"] = []


def pad_message(text: str, min_width: int = 35) -> str:                       # definiert breite der nachrichten bzw. min breite
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

async def safe_delete_and_untrack(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, list_key: str = "flow_msgs") -> None:
    """
    Löscht eine Nachricht tolerant und entfernt deren ID aus der gegebenen Tracking-Liste.
    """
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass
    try:
        ids = context.user_data.get(list_key, [])
        if isinstance(ids, list):
            ids[:] = [mid for mid in ids if mid != message_id]
    except Exception:
        pass


async def render_beilage_precheck_debug(update_or_query, context: ContextTypes.DEFAULT_TYPE, dishes_or_single, prefix: str = "DEBUG Beilagenvorprüfung:") -> None:
    """
    Baut und sendet den DEBUG-Text zur Beilagen-Vorprüfung für ein einzelnes Gericht oder eine Liste von Gerichten.
    Nachricht wird in flow_msgs getrackt.
    """
    if not show_debug_for(update_or_query):
        return

    try:
        # unify: ein Gericht → Liste
        if isinstance(dishes_or_single, str):
            dishes = [dishes_or_single]
        else:
            dishes = list(dishes_or_single or [])

        lines = []
        for dish in dishes:
            try:
                raw_series = df_gerichte.loc[df_gerichte["Gericht"] == dish, "Beilagen"]
                raw = str(raw_series.iloc[0]) if not raw_series.empty else "<n/a>"
            except Exception:
                raw = "<err>"
            codes = parse_codes(raw)
            nz = [c for c in codes if c != 0]
            allowed = sorted(list(allowed_sides_for_dish(dish)))[:12]
            lines.append(f"{dish}: raw='{raw}' → codes={codes} → nz={nz} → allowed={allowed} (n={len(allowed)})")

        if not lines:
            return

        dbg = prefix + "\n" + "\n".join(lines)

        # robust: update_or_query kann Update oder CallbackQuery sein
        msg_obj = getattr(update_or_query, "message", None) or getattr(update_or_query, "effective_message", None)
        if msg_obj is None:
            return
        m = await msg_obj.reply_text(dbg)
        context.user_data.setdefault("flow_msgs", []).append(m.message_id)
    except Exception:
        pass


# === Debug: Verteilungs-Message upsert (ersetzen statt neu anfügen)
async def upsert_distribution_debug(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, *, key: str = "dist_debug_msg_id") -> int:
    chat_id = update.effective_chat.id
    prev_id = context.user_data.get(key)

    # 1) Bestehende Debug-Message in-place updaten
    if isinstance(prev_id, int):
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=prev_id, text=text)
            return prev_id
        except Exception:
            # Fallback: alte Debug-Message weg
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=prev_id)
            except Exception:
                pass

    # 2) Neu senden
    msg = await context.bot.send_message(chat_id, pad_message(text))
    context.user_data[key] = msg.message_id
    return msg.message_id

async def render_proposal_with_debug(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    title: str,
    dishes: list[str],
    buttons: list[list[InlineKeyboardButton]],
    replace_old: bool = True,
) -> int:
    """
    Löscht optional das alte Paar (Debug + Vorschlag), sendet dann das Debug
    SCHÖN OBERHALB der Karte und direkt darunter die Vorschlagskarte.
    Rückgabe: message_id der Vorschlagskarte.
    """
    chat_id = update.effective_chat.id

    # 1) Altes Paar (Debug + Karte) wegräumen
    if replace_old:
        await delete_proposal_card(context, chat_id)

    # 2) Debug OBEN drüber, falls Admin
    if show_debug_for(update):
        try:
            dbg_txt = build_selection_debug_text(dishes)
            if dbg_txt:
                await upsert_distribution_debug(update, context, dbg_txt)
        except Exception:
            pass

    # 3) Karte direkt darunter senden (jetzt NICHTS mehr löschen)
    return await send_proposal_card(
        update, context,
        title=title,
        dishes=dishes,
        buttons=buttons,
        replace_old=False
    )


def format_hanging_line(text: str, *, bullet: str = "‣", indent_nbsp: int = 4, wrap_at: int = 60) -> str:
    from html import unescape as _unescape
    nbsp = "\u00A0"   # NBSP
    wj   = "\u2060"   # WORD JOINER (zero-width, verhindert Kollaps)
    fig  = "\u2007"   # FIGURE SPACE (non-collapsing, monowidth-ish)
    pipe = "│"        # optische Führung

    prefix = f"{bullet}{nbsp}"
    # Word-Joiner voranstellen, damit die folgenden Spaces nicht „verschluckt“ werden
    hang   = pipe + wj + (fig * indent_nbsp)

    words = str(text or "").split()
    if not words:
        return prefix

    lines = []
    cur = prefix + words[0]
    for w in words[1:]:
        tentative   = cur + " " + w
        visible_len = len(_unescape(tentative.replace(nbsp, " ")))
        if visible_len > wrap_at:
            lines.append(cur)
            cur = hang + w
        else:
            cur = tentative

    lines.append(cur)
    return "\n".join(lines)

def build_selection_debug_text(menues: list[str]) -> str:
    """
    Baut eine kurze DEBUG-Übersicht zur aktuellen Auswahl:
    Aufwand-, Küchen-, Typ- und Ernährungsstil-Verteilung.
    """
    if not menues:
        return ""
    sel = df_gerichte[df_gerichte["Gericht"].isin(menues)]
    if sel.empty:
        return ""

    from collections import Counter
    c_aw   = Counter(sel["Aufwand"])
    c_k    = Counter(sel["Küche"])
    c_typ  = Counter(sel["Typ"])
    c_erna = Counter(sel["Ernährungsstil"])

    # feste Reihenfolge für Aufwand
    aufwand_text = ", ".join(f"{c_aw.get(i,0)} x {i}" for i in (1, 2, 3))
    kitchen_text = ", ".join(f"{v} x {k}" for k, v in c_k.items())
    typ_text     = ", ".join(f"{v} x {k}" for k, v in c_typ.items())
    einschr_text = ", ".join(f"{v} x {k}" for k, v in c_erna.items())

    lines = [
        f"📊 Aufwand-Verteilung: {aufwand_text}",
        f"🎨 Küche-Verteilung: {kitchen_text}",
        f"⚙️ Typ-Verteilung: {typ_text}",
        f"🥗 Ernährungsstil-Verteilung: {einschr_text}",
    ]
    return "\n".join(lines)


# NEW: track IDs von Nachrichten, die wir beim Neustart gezielt löschen wollen
def _track_export_msg(context: "ContextTypes.DEFAULT_TYPE", msg_id: int) -> None:
    if not isinstance(msg_id, int):
        return
    context.user_data.setdefault("export_msgs", []).append(msg_id)


def build_new_run_banner() -> str:
    """Erzeugt die Statuszeile 'Neuer Lauf: Wochentag, TT.MM.YY, HH:MM Uhr' (deutsche Wochentage)."""
    now = datetime.now()
    wdays = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
    wday = wdays[now.weekday()]
    stamp = now.strftime("%d. %b %Y")
    return f"🔄 <u><b>Neustart: {wday}, {stamp}</b></u>"

##### 3 Helper für Optimierung Nachrichtenlöschung -> Zentral und nicht mehr in den Funktionen einzeln

# ===== Zentraler Flow-Reset & Mini-Helper =====

def track_msg(context: ContextTypes.DEFAULT_TYPE, key: str, mid: int) -> None:
    if isinstance(mid, int):
        context.user_data.setdefault(key, []).append(mid)

async def delete_proposal_card(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    # Vorschlagskarte
    pid = context.user_data.pop("proposal_msg_id", None)
    if isinstance(pid, int):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=pid)
        except Exception:
            pass

    # Verteilungs-Debug
    did = context.user_data.pop("dist_debug_msg_id", None)
    if isinstance(did, int):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=did)
        except Exception:
            pass


async def send_proposal_card(update: Update, context: ContextTypes.DEFAULT_TYPE, title: str, dishes: list[str] | None = None, *, buttons: list[list[InlineKeyboardButton]] | None = None, replace_old: bool = True,) -> int:
    uid = str(update.effective_user.id)
    chat_id = update.effective_chat.id
    if replace_old:
        await delete_proposal_card(context, chat_id)

    if dishes is None:
        dishes = sessions.get(uid, {}).get("menues", [])

    header = f"🥣 <u><b>{title}</b></u>"
    lines = [
        format_hanging_line(escape(str(g)), bullet="‣", indent_nbsp=2, wrap_at=60)
        for g in (dishes or [])
    ]
    body = "\n".join(lines)

    kb     = InlineKeyboardMarkup(buttons) if buttons else None

    msg = await context.bot.send_message(chat_id, pad_message(f"{header}\n{body}"), reply_markup=kb)
    context.user_data["proposal_msg_id"] = msg.message_id
    return msg.message_id

async def ask_beilagen_yes_no(anchor_msg, context: ContextTypes.DEFAULT_TYPE) -> int:
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Ja", callback_data="ask_yes"),
         InlineKeyboardButton("Nein", callback_data="ask_no")]
    ])
    m = await anchor_msg.reply_text(pad_message("Möchtest Du Beilagen hinzufügen?"), reply_markup=kb)
    track_msg(context, "flow_msgs", m.message_id)
    return ASK_BEILAGEN

def build_beilage_keyboard(allowed_codes: set[int], selected: list[int]) -> InlineKeyboardMarkup:
    dfb = df_beilagen.copy()
    dfb["Nummer"] = dfb["Nummer"].astype(int)
    btns = []
    for _, r in dfb[dfb["Nummer"].isin(allowed_codes)].iterrows():
        code = int(r["Nummer"])
        name = str(r["Beilagen"])
        label = f"{'✅ ' if code in selected else ''}{name}"
        btns.append(InlineKeyboardButton(label, callback_data=f"beilage_{code}"))
    rows = distribute_buttons_equally(btns, max_per_row=3)
    footer = InlineKeyboardButton("Fertig" if selected else "Weiter ohne Beilage", callback_data="beilage_done")
    rows.append([footer])
    return InlineKeyboardMarkup(rows)

EFFORT_LABELS = {1: "(<30min)", 2: "(30-60min)", 3: "(>60min)"}
def effort_label(lvl: int | None) -> str:
    return EFFORT_LABELS.get(lvl or 0, "")

def normalize_link(v: str) -> str:
    v = (v or "").strip()
    if not v:
        return ""
    return v if v.startswith(("http://","https://")) else "https://" + v


async def reset_flow_state(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    reset_session: bool = True,
    delete_messages: bool = True,
    only_keys: list[str] | None = None,
    skip_keys: list[str] | None = None,
) -> None:
    """
    Zentraler Hook, um UI-Nachrichten zu löschen und ephemere User-States zu resetten.
    - reset_session=True: löscht die Gerichtesession (menus/beilagen) aus 'sessions' + Persistenz.
    - delete_messages=True: löscht getrackte Nachrichten aus den bekannten Message-Listen.
    - only_keys: löscht NUR die angegebenen Message-Listen (keine ephemeren States).
    - skip_keys: überspringt bestimmte Message-Listen.
    """
    uid = str(update.effective_user.id) if update.effective_user else None
    chat_id = update.effective_chat.id if update.effective_chat else None

    # 1) Nachrichtenlisten: bekannte Keys
    msg_keys_all = ["flow_msgs", "prof_msgs", "fav_msgs", "fav_add_msgs", "export_msgs"]
    if only_keys is not None:
        msg_keys = [k for k in msg_keys_all if k in only_keys]
        clear_ephemeral = False  # bei only_keys keine States löschen
    else:
        msg_keys = msg_keys_all.copy()
        clear_ephemeral = True
    if skip_keys:
        msg_keys = [k for k in msg_keys if k not in skip_keys]

    if delete_messages and chat_id is not None:
        for key in msg_keys:
            ids = context.user_data.get(key, [])
            for mid in ids:
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=mid)
                except Exception:
                    pass
            context.user_data[key] = []

    # 2) Ephemere User-States (nur wenn kein only_keys gesetzt ist)
    if clear_ephemeral:
        EPHEMERAL_KEYS = {
            # Menü-Anzahl & Aufwand
            "menu_count_sel", "menu_count", "menu_count_page", "aufwand_verteilung",
            # Personen
            "temp_persons", "persons_page", "personen",
            # Beilagen/Mehrfachauswahl
            "allowed_beilage_codes", "menu_list", "to_process", "menu_idx",
            # Tauschen
            "swap_candidates", "swapped_indices",
            # QuickOne
            "quickone_remaining", "quickone_side_pools",
            # Favoriten-Flows
            "fav_total", "fav_del_sel", "fav_sel_sel", "fav_add_sel",
            # Profil-Wizard
            "new_profile",
            # Ergebnis/Exporte
            "final_list", "einkaufsliste_df", "kochliste_text",
        }
        for k in EPHEMERAL_KEYS:
            context.user_data.pop(k, None)

    # 3) Gerichtesession (menus/beilagen) & Persistenz
    if reset_session and uid and (uid in sessions or update.effective_chat):
        try:
            if uid in sessions:
                del sessions[uid]
            if update.effective_chat:
                ckey = chat_key(int(update.effective_chat.id))
                store_delete_session(ckey)
        except Exception:
            pass

# ___________________________

async def ask_for_persons(update: Update, context: ContextTypes.DEFAULT_TYPE, page: str = "low") -> int:
    """
    Paginiertes Auswahl-Keyboard für 1–6 / 7–12 Personen mit 'Fertig'.
    Zahl klickt nur Auswahl (✅), weiter geht es erst mit 'Fertig'.
    """
    q = update.callback_query
    data = q.data if q else None

    # Fresh entry (kein reiner Seitenwechsel): alte Auswahl löschen
    if not (q and (data in ("persons_page_low", "persons_page_high"))):
        context.user_data.pop("temp_persons", None)
        context.user_data["persons_page"] = "low"

    # State: Seite & Auswahl (temp_persons hält die Auswahl bis 'Fertig')
    sel = context.user_data.get("temp_persons")
    if data in ("persons_page_low", "persons_page_high"):
        page = "high" if data == "persons_page_high" else "low"
    context.user_data["persons_page"] = page

    if page == "low":
        nums = range(1, 7)
        nav_btn = InlineKeyboardButton("Mehr ➡️", callback_data="persons_page_high")
    else:
        nums = range(7, 13)
        nav_btn = InlineKeyboardButton("⬅️ Weniger", callback_data="persons_page_low")

    row_numbers = [
        InlineKeyboardButton(f"{n} ✅" if sel == n else f"{n}", callback_data=f"persons_{n}")
        for n in nums
    ]
    done_label = "✔️ Weiter" if isinstance(sel, int) else "(wähle oben)"
    footer = [nav_btn, InlineKeyboardButton(done_label, callback_data="persons_done")]
    kb = InlineKeyboardMarkup([row_numbers, footer])
    prompt = "Für wie viele Personen soll die Einkaufs- und Kochliste erstellt werden?"

    # a) Bei echtem Seitenwechsel nur das Keyboard updaten
    if q and data in ("persons_page_low", "persons_page_high"):
        await q.edit_message_reply_markup(reply_markup=kb)
        return PERSONS_SELECTION

    # b) Initial/sonst: neue Nachricht senden
    msg = await update.effective_message.reply_text(prompt, reply_markup=kb)
    context.user_data.setdefault("flow_msgs", []).append(msg.message_id)
    return PERSONS_SELECTION








# ============================================================================================
# ===================================== FAVORITEN–HELPER =====================================
# ============================================================================================


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
    if total == 0:
        return []  # <- verhindert Division durch 0

    rows_needed = math.ceil(total / max_per_row)
    per_row = total // rows_needed
    extra = total % rows_needed

    rows, index = [], 0
    for r in range(rows_needed):
        count = per_row + (1 if r < extra else 0)
        rows.append(buttons[index:index + count])
        index += count
    return rows


def _build_numbers_keyboard(prefix: str, total: int, selected: set[int], max_per_row: int, done_cb: str, *, done_label_empty: str = "Fertig", done_label_some:  str = "✔️ Fertig",) -> InlineKeyboardMarkup:
    """
    Generischer Zahlen-Keyboard-Builder.
    prefix:  'fav_del_' | 'fav_add_' | 'fav_sel_'
    done_cb: 'fav_add_done' | 'fav_sel_done' | 'fav_del_done'
    """
    btns = [
        InlineKeyboardButton(
            f"{'✅ ' if i in selected else ''}{i}",
            callback_data=f"{prefix}{i}"
        )
        for i in range(1, total + 1)
    ]
    rows = distribute_buttons_equally(btns, max_per_row=max_per_row)
    footer_label = done_label_some if selected else done_label_empty
    rows.append([InlineKeyboardButton(footer_label, callback_data=done_cb)])
    return InlineKeyboardMarkup(rows)


def build_fav_numbers_keyboard(total: int, selected: set[int]) -> InlineKeyboardMarkup:
    """Zahlen-Buttons (max. 8 pro Zeile) für Entfernen-Modus + 'Zurück'/'✔️ Fertig'."""
    return _build_numbers_keyboard(prefix="fav_del_", total=total, selected=selected, max_per_row=7, done_cb="fav_del_done", done_label_empty="Zurück",done_label_some="✔️ Fertig")


# NEW — Text abkürzen (ASCII-„...“), feste maximale Länge
def _truncate_label(text: str, max_len: int) -> str:
    text = str(text or "")
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return text[:max_len - 3].rstrip() + "..."

# NEW — Einspaltige Buttons mit Gerichtsnamen (⭐ am rechten Ende, ✅ vorn bei Auswahl)
def build_fav_add_keyboard_dishes(
    dishes: list[str],
    selected: set[int],
    existing_favs: set[str],
    max_len: int = 35
) -> InlineKeyboardMarkup:
    rows = []
    for i, name in enumerate(dishes, start=1):
        is_fav = name in existing_favs
        avail = max_len - (1 if is_fav else 0)
        base = _truncate_label(name, avail)
        label = base + ("⭐" if is_fav else "")
        if i in selected:
            label = "✅ " + label
        rows.append([InlineKeyboardButton(label, callback_data=f"fav_add_{i}")])

    # Footer-Button: keine Auswahl → "Keines", sonst "✔️ Weiter/Fertig"
    footer_label = "✖️ Keines" if not selected else "✔️ Fertig"
    rows.append([InlineKeyboardButton(footer_label, callback_data="fav_add_done")])
    return InlineKeyboardMarkup(rows)

def build_menu_select_keyboard_for_sides(dishes: list[str], selected_zero_based: set[int], *, max_len: int = 35) -> InlineKeyboardMarkup:
    """
    Einspaltige Buttons mit Gerichtsnamen für den Beilagen-Preselect-Schritt.
    - zeigt nur Gerichte, die überhaupt Beilagen erlauben
    - markiert selektierte mit '✅ ' vor dem Namen
    - Footer: 'Fertig' / '✔️ Fertig' (wenn >=1 ausgewählt)
    Hinweis: selected_zero_based enthält 0-basierte Indizes.
    """
    rows = []
    for i, name in enumerate(dishes, start=1):
        if not allowed_sides_for_dish(name):
            continue
        label_base = _truncate_label(name, max_len)
        label = ("✅ " if (i - 1) in selected_zero_based else "") + label_base
        rows.append([InlineKeyboardButton(label, callback_data=f"select_{i}")])

    footer_label = "✔️ Fertig" if selected_zero_based else "keine Beilagen"
    rows.append([InlineKeyboardButton(footer_label, callback_data="select_done")])
    return InlineKeyboardMarkup(rows)

# ============================================================================================

async def send_main_buttons(msg):
    """Hauptmenü-Buttons erneut anzeigen (z. B. bei leerer Favoritenliste)."""
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🍲 Menü",      callback_data="start_menu")],
        [InlineKeyboardButton("⚡ QuickOne",     callback_data="start_quickone")],
        [InlineKeyboardButton("🔖 Favoriten", callback_data="start_favs"),
        InlineKeyboardButton("🛠️ Übersicht",     callback_data="start_setup"),
    ]])
    await msg.reply_text(pad_message("➡️ Wähle eine Option:"), reply_markup=kb)

# ============================================================================================

async def send_action_menu(msg, context: ContextTypes.DEFAULT_TYPE):
    """
    Zeigt die drei Haupt-Export/Restart-Buttons mit Frage an,
    tracked die Nachricht (für späteres Löschen) und gibt sie zurück.
    """
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔖 Gerichte zu Favoriten hinzufügen", callback_data="favoriten")],
        [InlineKeyboardButton("🛒 Einkaufsliste in Bring! exportieren", callback_data="export_bring")],
        [InlineKeyboardButton("📄 Als PDF exportieren", callback_data="export_pdf")],
        [InlineKeyboardButton("🔄 Das passt so. Neustart!", callback_data="restart")],
    ])
    out = await msg.reply_text(pad_message("Was steht als nächstes an?"), reply_markup=kb)
    _track_export_msg(context, out.message_id)
    return out

# Load persisted data (env-aware: avoid JSON preload when Firestore is enabled)
if (os.getenv("PERSISTENCE") or "json").strip().lower() == "firestore":
    sessions = {}
    favorites = {}
    profiles = {}
    recipe_cache = {}
else:
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

def parse_codes(s: str) -> list[int]:
    if s is None:
        return []
    return [int(m) for m in re.findall(r"\d+", str(s))]

def allowed_sides_for_dish(dish: str) -> set[int]:
    """
    Liefert die final erlaubten Beilagen-Nummern für ein Gericht.
    Nutzt 99/88/77 (Kategorien) korrekt und filtert 0 heraus.
    """
    try:
        raw_series = df_gerichte.loc[df_gerichte["Gericht"] == dish, "Beilagen"]
        raw = str(raw_series.iloc[0]) if not raw_series.empty else ""
    except Exception:
        raw = ""
    base = [c for c in parse_codes(raw) if c != 0]

    # Kategorienlisten aus df_beilagen
    nums = df_beilagen["Nummer"].astype(int)
    kh   = set(nums[df_beilagen["Kategorie"] == "Kohlenhydrate"].tolist())
    gv   = set(nums[df_beilagen["Kategorie"] == "Gemüse"].tolist())
    all_nums = set(nums.tolist())

    allowed: set[int] = set()
    if 99 in base:
        allowed |= (kh | gv)
    else:
        if 88 in base:
            allowed |= kh
        if 77 in base:
            allowed |= gv
        # explizite Nummern, die real existieren
        allowed |= set(x for x in base if x not in (88, 77, 99) and x in all_nums)
    return allowed



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


def _load_sheets_via_cache(ttl_sec: int = SHEETS_CACHE_TTL_SEC):
    """
    1) Frischen Snapshot aus Firestore holen (60min TTL)
    2) Falls leer/abgelaufen → direkt aus Sheets laden, transformieren, in Firestore ablegen
    3) DataFrames zurückgeben
    """
    # 1) Versuche Firestore-Cache (frisch)
    if FS:
        cg = _cache_read_if_fresh("gerichte", ttl_sec)
        cb = _cache_read_if_fresh("beilagen", ttl_sec)
        cz = _cache_read_if_fresh("zutaten",  ttl_sec)
        if cg and cb and cz:
            logging.info("Sheets-Cache: HIT (Firestore, frisch)")
            return (
                _compact_json_to_df(cg),
                _compact_json_to_df(cb),
                _compact_json_to_df(cz),
            )
        else:
            logging.info("Sheets-Cache: MISS/EXPIRED → lade aus Google Sheets")

    # 2) Aus Google Sheets laden (deine bestehenden Loader)
    df_g = lade_gerichtebasis()
    df_b = lade_beilagen()
    df_z = lade_zutaten()

    # 3) In Firestore als kompaktes JSON ablegen (nur wenn FS aktiv)
    if FS:
        try:
            _cache_write("gerichte", _df_to_compact_json(df_g))
            _cache_write("beilagen", _df_to_compact_json(df_b))
            _cache_write("zutaten",  _df_to_compact_json(df_z))
            logging.info("Sheets-Cache: Snapshot in Firestore aktualisiert")
        except Exception as e:
            logging.warning("Sheets-Cache: Write-Fehler: %s", e)

    return df_g, df_b, df_z

df_gerichte, df_beilagen, df_zutaten = _load_sheets_via_cache()



# --- Normalisierung: Typ immer als "1"/"2"/"3" ---
df_gerichte["Typ"] = (
    pd.to_numeric(df_gerichte["Typ"], errors="coerce")
      .astype("Int64")
      .map({1: "1", 2: "2", 3: "3"})
      .fillna("2")
)

# --- Schnell-Indizes für häufige Lookups (robust) ---
_G_COLS = ["Beilagen", "Aufwand", "Typ", "Link"]
_present_cols = [c for c in _G_COLS if c in df_gerichte.columns]

try:
    _G_INDEX = (
        df_gerichte
        .set_index("Gericht")[_present_cols]
        .to_dict(orient="index")
    )
except Exception as e:
    logging.warning("Gerichte-Index konnte nicht aufgebaut werden (%s) – arbeite ohne Schnell-Index.", e)
    _G_INDEX = {}

def gi(name: str):
    """Schneller Zugriff auf Gerichte-Zeile als dict (oder None)."""
    try:
        return _G_INDEX.get(name)
    except Exception:
        return None


def get_aufwand_for(dish: str):
    """Aufwand eines Gerichts als int (1/2/3) oder None."""
    row = gi(dish)
    if not row:
        return None
    try:
        return int(pd.to_numeric(row.get("Aufwand"), errors="coerce"))
    except Exception:
        return None

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

    nL = min(len(df_light),  target["light"])
    nM = min(len(df_medium), target["medium"])
    nH = min(len(df_heavy),  target["heavy"])

    chosen = {
        "light":  (df_light.sample(
                      n=nL, replace=False,
                      weights=pd.to_numeric(df_light["Gewicht"], errors="coerce").fillna(1.0)
                  ) if nL > 0 and not df_light.empty else df_light.iloc[:0].copy()),
        "medium": (df_medium.sample(
                      n=nM, replace=False,
                      weights=pd.to_numeric(df_medium["Gewicht"], errors="coerce").fillna(1.0)
                  ) if nM > 0 and not df_medium.empty else df_medium.iloc[:0].copy()),
        "heavy":  (df_heavy.sample(
                      n=nH, replace=False,
                      weights=pd.to_numeric(df_heavy["Gewicht"], errors="coerce").fillna(1.0)
                  ) if nH > 0 and not df_heavy.empty else df_heavy.iloc[:0].copy()),
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


# ===== Zentrale Texte & UI für Start/Übersicht =====
def get_welcome_text() -> str:
    return (
        "👋 Willkommen!\n\n"
        "Hier ein paar Infos zum Bot:\n\n"
        "Du kannst Vorschläge für leckere Gerichte erstellen. Nur 1 Gericht oder gleich mehrere für die ganze Woche. Die sortierte Einkaufsliste hilft Dir im Laden Zeit zu sparen.\n\n"
            )
def get_overview_text() -> str:
    return (
        "<u>Übersicht der Befehle:</u>\n\n"
        "🍲 Lass Dir leckere Gerichte vorschlagen\n\n"
        "⚡ Ein Gericht - Wenns schnell geht!\n\n"
        "🔖 Deine Lieblingsgerichte\n\n"
        "🛠️ Nützliche Infos und Hilfen\n\n"
        "🔄️ Starte jederzeit neu"
    )

def build_main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🍲 Menü",      callback_data="start_menu"),
            InlineKeyboardButton("⚡ QuickOne",  callback_data="start_quickone"),
        ],
        [
            InlineKeyboardButton("🔖 Favoriten", callback_data="start_favs"),
            InlineKeyboardButton("🛠️ Übersicht", callback_data="start_setup"),
            InlineKeyboardButton("🔄 Restart",   callback_data="restart_ov"),
        ],
    ])

async def send_overview(chat_id: int, context: ContextTypes.DEFAULT_TYPE, edit_message=None):
    text = get_overview_text()
    kb = build_main_menu_keyboard()
    if edit_message is not None:
        await edit_message.edit_text(text, reply_markup=kb)
    else:
        await context.bot.send_message(chat_id, text, reply_markup=kb)

async def send_welcome_then_overview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    # 1) Sofort: kurzer Willkommens-Text
    await context.bot.send_message(chat_id, get_welcome_text())
    # 2) Nach 3 Sekunden: Übersicht + Buttons
    await asyncio.sleep(3)
    await send_overview(chat_id, context)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_welcome_then_overview(update, context)

async def setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🛠 <u>Übersicht der Funktionen:</u>\n"
        #"/start – Hilfe & Einführung\n"
        #"/menu – generiere Gerichtevorschläge\n"
        #"/meinefavoriten – Übersicht deiner Favoriten\n"
        #"/meinProfil – Übersicht Deiner Favoriten\n"
        "/status – zeigt aktuelle Gerichtewahl\n"
        "/reset – setzt Session zurück (Favoriten bleiben)\n"
        "/setup – zeigt alle Funktionen\n"
        #"/neustart – Startet neuen Prozess (Favoriten bleiben)\n"
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
    context.user_data["prof_start_msg_id"] = sent.message_id

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
    context.user_data["prof_start_msg_id"] = sent.message_id
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
        if ensure_profile_loaded(uid):
            # In-place Edit der Startfrage → "Wie viele Gerichte...?"
            context.user_data.pop("menu_count_sel", None)
            context.user_data["menu_count_page"] = "low"

            kb = build_menu_count_inline_kb(None, "low")
            try:
                await q.message.edit_text(
                    pad_message("Wie viele Gerichte soll ich vorschlagen?"),
                    reply_markup=kb
                )
            except Exception:
                # Fallback: wenn Edit nicht möglich, neue Nachricht senden
                msg = await q.message.reply_text(
                    pad_message("Wie viele Gerichte soll ich vorschlagen?"),
                    reply_markup=kb
                )
                # Tracking: neu gesendete Nachricht in flow_msgs
                context.user_data.setdefault("flow_msgs", []).append(msg.message_id)
            else:
                # Tracking: die ehem. Startfrage von prof_msgs → flow_msgs verschieben
                try:
                    context.user_data.get("prof_msgs", []).remove(q.message.message_id)
                except Exception:
                    pass
                context.user_data.setdefault("flow_msgs", []).append(q.message.message_id)
                # Startfrage-ID aufräumen, weil jetzt Flow-Nachricht
                if context.user_data.get("prof_start_msg_id") == q.message.message_id:
                    context.user_data.pop("prof_start_msg_id", None)

            return MENU_COUNT

        # Kein Profil ⇒ Hinweis + Wizard starten (unverändert)
        async def send_and_log(text: str, *, store_id: bool = True, **kwargs):
            msg = await q.message.reply_text(text, **kwargs)
            if store_id:
                context.user_data.setdefault("prof_msgs", []).append(msg.message_id)
            return msg

        await send_and_log("Es besteht noch kein Profil. Erstelle eines!")
        context.user_data["new_profile"] = {"styles": set()}
        await send_and_log(
            "Ernährungsstil:",
            reply_markup=build_restriction_keyboard()
        )
        return PROFILE_NEW_A


    # ===== 2)  Ohne Einschränkung =========================================
    if choice == "prof_nolim":
        # In-place Edit der Startfrage → "Wie viele Gerichte...?"
        context.user_data.pop("menu_count_sel", None)
        context.user_data["menu_count_page"] = "low"

        kb = build_menu_count_inline_kb(None, "low")
        try:
            await q.message.edit_text(
                pad_message("Wie viele Gerichte soll ich vorschlagen?"),
                reply_markup=kb
            )
        except Exception:
            # Fallback: wenn Edit nicht möglich, neue Nachricht senden
            msg = await q.message.reply_text(
                pad_message("Wie viele Gerichte soll ich vorschlagen?"),
                reply_markup=kb
            )
            context.user_data.setdefault("flow_msgs", []).append(msg.message_id)
        else:
            # Tracking: die ehem. Startfrage von prof_msgs → flow_msgs verschieben
            try:
                context.user_data.get("prof_msgs", []).remove(q.message.message_id)
            except Exception:
                pass
            context.user_data.setdefault("flow_msgs", []).append(q.message.message_id)
            # Startfrage-ID aufräumen, weil jetzt Flow-Nachricht
            if context.user_data.get("prof_start_msg_id") == q.message.message_id:
                context.user_data.pop("prof_start_msg_id", None)

        return MENU_COUNT

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
        if ensure_profile_loaded(uid):
            await send_and_log(
                profile_overview_text(profiles[uid]),
                reply_markup=build_profile_overview_keyboard(),
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
    # Persistentes Speichern (JSON oder Firestore – je nach PERSISTENCE)
    store_set_profile(user_key(int(uid)), profiles[uid])


    # Übersicht + Buttons
    await q.message.edit_text(
        profile_overview_text(profiles[uid]),
        reply_markup=build_profile_overview_keyboard()
    )
    return PROFILE_OVERVIEW


async def profile_overview_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    choice = q.data  # 'prof_overwrite' oder 'prof_back'

    if choice == "prof_overwrite":
        # Wizard neu starten
        context.user_data["new_profile"] = {"styles": set()}
        sent = await q.message.edit_text(
            pad_message("Ernährungsstil:"),
            reply_markup=build_restriction_keyboard(),
        )
        if sent.message_id not in context.user_data.get("prof_msgs", []):
            context.user_data.setdefault("prof_msgs", []).append(sent.message_id)
        return PROFILE_NEW_A

    if choice == "prof_back":
        # Alle Profil-/Wizard-Nachrichten löschen, aber die Startfrage stehen lassen
        await cleanup_prof_loop_except_start(context, q.message.chat.id)
        # Zurück in den Auswahl-State; die (bestehende) Startfrage bleibt sichtbar/benutzbar
        return PROFILE_CHOICE

    # Fallback (sollte nie erreicht werden)
    return PROFILE_OVERVIEW


def build_menu_count_inline_kb(selected: int | None, page: str = "low") -> InlineKeyboardMarkup:
    if page == "low":
        nums = range(1, 7)
        nav_btn = InlineKeyboardButton("Mehr ➡️", callback_data="menu_count_page_high")
    else:
        nums = range(7, 13)
        nav_btn = InlineKeyboardButton("⬅️ Weniger", callback_data="menu_count_page_low")

    row_numbers = [
        InlineKeyboardButton(f"{n} ✅" if selected == n else f"{n}", callback_data=f"menu_count_{n}")
        for n in nums
    ]
    done_label = "✔️ Weiter" if isinstance(selected, int) else "(wähle oben)"
    footer = [nav_btn, InlineKeyboardButton(done_label, callback_data="menu_count_done")]
    return InlineKeyboardMarkup([row_numbers, footer])

async def ask_menu_count(update: Update, context: ContextTypes.DEFAULT_TYPE, page: str = "low"):
    """Zahlenauswahl 1–12 mit Umschaltung und 'Fertig'. Auswahl nur markieren (✅),
    weiter geht es erst mit 'Fertig'. Der 'Fertig'-Button zeigt einen grünen Haken,
    sobald eine Zahl gewählt ist."""
    q = update.callback_query
    if not (q and (q.data in ("menu_count_page_high", "menu_count_page_low"))):
        context.user_data.pop("menu_count_sel", None)
    data = q.data if q else None

    # State: aktuelle Seite & Auswahl merken
    sel = context.user_data.get("menu_count_sel")
    if data in ("menu_count_page_high", "menu_count_page_low"):
        page = "high" if data == "menu_count_page_high" else "low"
    context.user_data["menu_count_page"] = page

    # Zahlen & Navigation je Seite
    if page == "low":
        nums = range(1, 7)
        nav_btn = InlineKeyboardButton("Mehr ➡️", callback_data="menu_count_page_high")
    else:
        nums = range(7, 13)
        nav_btn = InlineKeyboardButton("⬅️ Weniger", callback_data="menu_count_page_low")

    row_numbers = [
        InlineKeyboardButton(f"{n} ✅" if sel == n else f"{n}", callback_data=f"menu_count_{n}")
        for n in nums
    ]
    done_label = "✔️ Weiter" if isinstance(sel, int) else "(wähle oben)"
    footer = [nav_btn, InlineKeyboardButton(done_label, callback_data="menu_count_done")]
    kb = InlineKeyboardMarkup([row_numbers, footer])
    text = pad_message("Wie viele Gerichte soll ich vorschlagen?")

    # a) Bei echtem Seitenwechsel: nur ReplyMarkup editen
    if q and data in ("menu_count_page_high", "menu_count_page_low"):
        await q.edit_message_reply_markup(reply_markup=kb)
        return MENU_COUNT

    # b) Initial oder sonst: Nachricht mit Tastatur senden/editen (Layout bleibt gleich)
    if q:
        msg = await q.message.reply_text(text, reply_markup=kb)
        context.user_data.setdefault("flow_msgs", []).append(msg.message_id)
    elif update.message:
        msg = await update.message.reply_text(text, reply_markup=kb)
        context.user_data.setdefault("flow_msgs", []).append(msg.message_id)
    else:
        chat_id = update.effective_chat.id
        msg = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)
        context.user_data.setdefault("flow_msgs", []).append(msg.message_id)

    return MENU_COUNT


async def menu_count_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data

    # 1) Seitenwechsel
    if data in ("menu_count_page_high", "menu_count_page_low"):
        return await ask_menu_count(update, context, page="high" if data.endswith("high") else "low")

    # 2) Zahl angeklickt -> nur markieren (✅), nicht fortfahren
    if data.startswith("menu_count_") and data != "menu_count_done":
        try:
            sel = int(data.rsplit("_", 1)[-1])
        except ValueError:
            return MENU_COUNT
        context.user_data["menu_count_sel"] = sel

        # Tastatur mit ✅ neu aufbauen (Layout unverändert)
        page = context.user_data.get("menu_count_page", "low")
        if page == "low":
            nums = range(1, 7)
            nav_btn = InlineKeyboardButton("Mehr ➡️", callback_data="menu_count_page_high")
        else:
            nums = range(7, 13)
            nav_btn = InlineKeyboardButton("⬅️ Weniger", callback_data="menu_count_page_low")

        row_numbers = [
            InlineKeyboardButton(f"{n} ✅" if sel == n else f"{n}", callback_data=f"menu_count_{n}")
            for n in nums
        ]
        done_label = "✔️ Weiter" if isinstance(sel, int) else "Weiter"
        footer = [nav_btn, InlineKeyboardButton(done_label, callback_data="menu_count_done")]
        await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([row_numbers, footer]))
        return MENU_COUNT

    # 3) Fertig -> jetzt geht's weiter
    if data == "menu_count_done":
        sel = context.user_data.get("menu_count_sel")
        if not isinstance(sel, int):
            await q.answer("Bitte zuerst eine Zahl auswählen.", show_alert=True)
            return MENU_COUNT

        context.user_data["menu_count"] = sel
        context.user_data["aufwand_verteilung"] = {"light": 0, "medium": 0, "heavy": 0}
        await q.message.edit_text(
            f"Du suchst <b>{sel}</b> Gerichte 👍\n\nDefiniere deren Aufwand:",
            reply_markup=build_aufwand_keyboard(context.user_data["aufwand_verteilung"], sel)
        )
        return MENU_AUFWAND

    return MENU_COUNT



async def start_menu_count_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Startet den Menü-Auswahl-Flow (1–12) und setzt vorher UI & lokale Auswahl zurück."""
    # Nur UI-Messages der Start-/Profil-Phase löschen, Session behalten
    await reset_flow_state(update, context, reset_session=False, delete_messages=True, only_keys=["flow_msgs", "prof_msgs"])

    # Auswahl & Seite sicher zurücksetzen
    context.user_data.pop("menu_count_sel", None)
    context.user_data.pop("menu_count", None)
    context.user_data["menu_count_page"] = "low"

    return await ask_menu_count(update, context, page="low")


async def menu_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Erwartet Eingabe:  <gesamt> (<einfach>,<mittel>,<aufwändig>)
    Beispiel: 4 (2,1,1)
    """
    try:
        final_gerichte: list[str] = []
        final_aufwand:  list[int] = []        
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
            # Profil ist optional; ohne Profil = keine Einschränkung
            if not profile:
                profile = None  # apply_profile_filters() kommt damit klar


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
            persist_session(update)


            await render_proposal_with_debug(
                update, context,
                title="Vorschlag:",
                dishes=final_gerichte,
                buttons=[
                    [InlineKeyboardButton("Passt", callback_data="confirm_yes"),
                     InlineKeyboardButton("Austauschen", callback_data="confirm_no")]
                ],
                replace_old=True
            )
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
        persist_session(update)

        await render_proposal_with_debug(
            update, context,
            title="Vorschlag:",
            dishes=ausgewaehlt,
            buttons=[
                [InlineKeyboardButton("Passt", callback_data="confirm_yes"),
                 InlineKeyboardButton("Austauschen", callback_data="confirm_no")]
            ],
            replace_old=True
        )
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
    ensure_session_loaded_for_user_and_chat(update)
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat.id
    uid     = str(query.from_user.id)

    # CODE START — menu_confirm_cb : confirm_yes ohne Ja/Nein-Umschalten
    if query.data == "confirm_yes":
        # Buttons der Vorschlagskarte sofort entfernen (kein Umschalten auf "Ja/Nein")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        # 3) Menüs und Beilagen-fähige Menüs ermitteln
        menus = sessions[uid]["menues"]
        side_menus = [idx for idx, dish in enumerate(menus) if allowed_sides_for_dish(dish)]

        # erst jetzt: wenn KEINE Beilagen möglich sind -> direkt weiter, ohne Debug
        if not side_menus:
            return await show_final_dishes_and_ask_persons(update, context, step=2)
    
        await render_beilage_precheck_debug(update, context, menus, prefix="DEBUG Beilagenvorprüfung:")

        # 4b) >0 Beilagen-Menüs: zuerst fragen, ob Beilagen überhaupt gewünscht sind
        return await ask_beilagen_yes_no(query.message, context)
    

    if query.data == "confirm_no":
        # Buttons der Vorschlagskarte sofort entfernen (kein Umschalten auf "Ja/Nein")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
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
        return await ask_for_persons(update, context, page="high" if data == "persons_page_high" else "low")

    # 2) Zahl gewählt -> nur markieren (✅), noch NICHT weiter
    if data.startswith("persons_") and data != "persons_done":
        try:
            sel = int(data.split("_")[1])
        except Exception:
            return PERSONS_SELECTION

        context.user_data["temp_persons"] = sel

        # Tastatur mit Haken neu aufbauen (Layout unverändert)
        page = context.user_data.get("persons_page", "low")
        if page == "low":
            nums = list(range(1, 7))
            nav_label, nav_data = "Mehr ➡️", "persons_page_high"
        else:
            nums = list(range(7, 13))
            nav_label, nav_data = "⬅️ Weniger", "persons_page_low"

        row_numbers = [
            InlineKeyboardButton(f"{n} ✅" if sel == n else f"{n}", callback_data=f"persons_{n}")
            for n in nums
        ]
        done_label = "✔️ Weiter" if isinstance(sel, int) else "Weiter"
        footer = [
            InlineKeyboardButton(nav_label, callback_data=nav_data),
            InlineKeyboardButton(done_label, callback_data="persons_done"),
        ]
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([row_numbers, footer]))
        return PERSONS_SELECTION

    # 3) Fertig -> jetzt weiter
    if data == "persons_done":
        sel = context.user_data.get("temp_persons")
        if not isinstance(sel, int):
            await query.answer("Bitte zuerst eine Zahl auswählen.", show_alert=True)
            return PERSONS_SELECTION

        # Auswahl als finale Personenanzahl übernehmen
        context.user_data["personen"] = sel

        # weiter im Flow:
        return await fertig_input(update, context)

    return PERSONS_SELECTION


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
    ensure_session_loaded_for_user_and_chat(update)
    uid = str(update.effective_user.id)
    chat_id = update.effective_chat.id

    # 🔧 Wichtig: Alte Vorschlagskarte (falls noch vorhanden) IMMER vorher entfernen
    await delete_proposal_card(context, chat_id)

    # 1) Flow-UI zurücksetzen (nur Nachrichtenliste), Pools nicht mehr nötig
    context.user_data["flow_msgs"] = []
    context.user_data.pop("quickone_side_pools", None)  # nicht mehr genutzt

    # 2) Gerichtspool initialisieren oder weiterverwenden
    all_dishes = [d for d in df_gerichte["Gericht"].tolist() if isinstance(d, str) and d.strip()]
    remaining = context.user_data.get("quickone_remaining")

    # Nur wenn der Pool noch nie existierte → mit allen Gerichten befüllen
    if remaining is None:
        remaining = all_dishes.copy()

    # Wenn der Pool existiert, aber leer ist → Durchgang zu Ende
    if not remaining:
        msg = await context.bot.send_message(
            chat_id,
            pad_message("⚠️ Es sind keine neuen Gerichte mehr im aktuellen Durchgang.\n"
                        "Starte bitte neu mit »🔄 Restart«.")
        )
        context.user_data.setdefault("flow_msgs", []).append(msg.message_id)
        return QUICKONE_CONFIRM

    # Favoriten (3x) × Aktiv-Gewicht
    user_favs = favorites.get(uid, [])
    wmap      = df_gerichte.set_index("Gericht")["Gewicht"].to_dict()
    weights   = [(3 if d in user_favs else 1) * float(wmap.get(d, 1.0)) for d in remaining]

    dish = random.choices(remaining, weights=weights, k=1)[0]
    remaining.remove(dish)
    context.user_data["quickone_remaining"] = remaining

    # 3) Session setzen – KEINE Beilagen mehr vorwählen
    sessions[uid] = {
        "menues":  [dish],
        "aufwand": [int(df_gerichte.loc[df_gerichte["Gericht"] == dish, "Aufwand"].iloc[0]) if not df_gerichte.loc[df_gerichte["Gericht"] == dish, "Aufwand"].empty else 0],
        "beilagen": {}
    }
    persist_session(update)

    # bevorzugt Session-Aufwand, sonst df
    try:
        lvl = sessions[uid]["aufwand"][0]
    except Exception:
        lvl = get_aufwand_for(dish)

    label_txt = effort_label(lvl)
    aufwand_label = f" <i>{escape(label_txt)}</i>" if label_txt else ""

    # 4) Vorschlag + Buttons (in *derselben* Nachricht)
    text = pad_message(f"🥣 <u><b>Vorschlag:</b></u>\n\n{escape(dish)} {aufwand_label}")
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("✔️ Passt", callback_data="quickone_passt"),
        InlineKeyboardButton("🔁 Neu",   callback_data="quickone_neu"),
    ]])
    msg = await context.bot.send_message(chat_id, text=text, reply_markup=markup)
    context.user_data["proposal_msg_id"] = msg.message_id  # gezielt als Vorschlagskarte tracken

    return QUICKONE_CONFIRM



# ─────────── quickone_confirm_cb ───────────
async def quickone_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_session_loaded_for_user_and_chat(update)
    q = update.callback_query
    await q.answer()
    uid = str(update.effective_user.id)
    chat_id = q.message.chat.id
    data = q.data

    if data == "quickone_passt":
        # Buttons am Vorschlag entfernen
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        dish = sessions[uid]["menues"][0]
        allowed = allowed_sides_for_dish(dish)

        # ⚠️ WICHTIG: wie im Menü-Loop — wenn KEINE Beilagen möglich sind,
        # KEIN Debug rendern, sondern direkt weiter.
        if not allowed:
            return await show_final_dishes_and_ask_persons(update, context, step=2)

        # Ab hier gibt es Beilagen → Debug jetzt (erst) anzeigen
        await render_beilage_precheck_debug(update, context, dish, prefix="DEBUG Beilagenvorprüfung:")

        kb = InlineKeyboardMarkup([[ 
            InlineKeyboardButton("Ja",   callback_data="quickone_ask_yes"),
            InlineKeyboardButton("Nein", callback_data="quickone_ask_no"),
        ]])
        msg = await q.message.reply_text(
            pad_message("Möchtest Du Beilagen hinzufügen?"),
            reply_markup=kb
        )
        context.user_data.setdefault("flow_msgs", []).append(msg.message_id)
        return QUICKONE_CONFIRM




    if data == "quickone_neu":
        # Keinen kompletten Neustart; ersetze die bestehende Vorschlagskarte in-place
        remaining = context.user_data.get("quickone_remaining")
        all_dishes = [d for d in df_gerichte["Gericht"].tolist() if isinstance(d, str) and d.strip()]

        if remaining is None:
            remaining = all_dishes.copy()

        if not remaining:
            try:
                await q.answer("Keine neuen Gerichte mehr im aktuellen Durchgang. Bitte »🔄 Restart«.", show_alert=True)
            except Exception:
                pass
            return QUICKONE_CONFIRM

        # Favoriten (3x) × Aktiv-Gewicht
        user_favs = favorites.get(uid, [])
        wmap = df_gerichte.set_index("Gericht")["Gewicht"].to_dict()
        weights = [(3 if d in user_favs else 1) * float(wmap.get(d, 1.0)) for d in remaining]

        dish = random.choices(remaining, weights=weights, k=1)[0]
        try:
            remaining.remove(dish)
        except ValueError:
            pass
        context.user_data["quickone_remaining"] = remaining

        # Session aktualisieren (ein Gericht)
        sessions[uid] = {
            "menues": [dish],
            "aufwand": [int(df_gerichte.loc[df_gerichte["Gericht"] == dish, "Aufwand"].iloc[0]) if not df_gerichte.loc[df_gerichte["Gericht"] == dish, "Aufwand"].empty else 0],
            "beilagen": {}
        }
        persist_session(update)

        # Aufwand-Label
        try:
            lvl = sessions[uid]["aufwand"][0]
        except Exception:
            lvl = get_aufwand_for(dish)
        label_txt = effort_label(lvl)
        aufwand_label = f" <i>{escape(label_txt)}</i>" if label_txt else ""

        # Text der bestehenden Vorschlagskarte ersetzen
        title = "Neuer Vorschlag:"
        text = pad_message(f"🥣 <u><b>{title}</b></u>\n\n{escape(dish)} {aufwand_label}")

        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✔️ Passt", callback_data="quickone_passt"),
            InlineKeyboardButton("🔁 Neu",   callback_data="quickone_neu"),
        ]])

        pid = context.user_data.get("proposal_msg_id")
        if isinstance(pid, int):
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=pid,
                    text=text,
                    reply_markup=kb
                )
            except Exception:
                # Fallback: wenn Edit scheitert, Karte neu senden und ID aktualisieren
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=pid)
                except Exception:
                    pass
                msg_new = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)
                context.user_data["proposal_msg_id"] = msg_new.message_id
        else:
            msg_new = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)
            context.user_data["proposal_msg_id"] = msg_new.message_id

        return QUICKONE_CONFIRM


    if data == "quickone_ask_no":
        # Beilagen-Frage (diese Nachricht) entfernen
        await safe_delete_and_untrack(context, chat_id, q.message.message_id, "flow_msgs")

        # Direkt zur Personen-Auswahl (Vorschlag bleibt sichtbar)
        return await show_final_dishes_and_ask_persons(update, context, step=2)


    if data == "quickone_ask_yes":
        # Beilagen-Frage (diese Nachricht) entfernen
        await safe_delete_and_untrack(context, chat_id, q.message.message_id, "flow_msgs")

        # QuickOne hat exakt 1 Gericht → direkt in die Beilagen-Auswahl für dieses Gericht
        dish = sessions[uid]["menues"][0]
        context.user_data["menu_list"] = [dish]
        context.user_data["to_process"] = [0]
        context.user_data["menu_idx"]   = 0

        return await ask_beilagen_for_menu(q, context)

    return ConversationHandler.END


async def ask_beilagen_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_session_loaded_for_user_and_chat(update)
    query = update.callback_query
    await query.answer()
    uid = str(query.from_user.id)

    if query.data == "ask_no":
        # Beilagenfrage selbst entfernen bleibt korrekt:
        try:
            await context.bot.delete_message(chat_id=query.message.chat.id, message_id=query.message.message_id)
        except Exception:
            pass
        return await show_final_dishes_and_ask_persons(update, context, step=2)


    if query.data == "ask_yes":
        # Beilagen-Frage sofort entfernen
        await safe_delete_and_untrack(context, query.message.chat.id, query.message.message_id, "flow_msgs")

        menus = sessions[uid]["menues"]
        side_menus = [idx for idx, dish in enumerate(menus) if allowed_sides_for_dish(dish)]

        if len(side_menus) == 0:
            return await show_final_dishes_and_ask_persons(update, context, step=3)


        if len(side_menus) == 1:
            context.user_data["menu_list"] = menus
            context.user_data["to_process"] = side_menus
            context.user_data["menu_idx"]   = 0
            return await ask_beilagen_for_menu(query, context)

        context.user_data["menu_list"] = menus
        context.user_data["selected_menus"] = set()  # 0-basierte Indizes
        kb = build_menu_select_keyboard_for_sides(menus, context.user_data["selected_menus"], max_len=35)
        msg = await query.message.reply_text(pad_message("Für welche Gerichte?"), reply_markup=kb)
        context.user_data.setdefault("flow_msgs", []).append(msg.message_id)
        return SELECT_MENUES




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

    if show_debug_for(update_or_query):
        try:
            raw_series = df_gerichte.loc[df_gerichte["Gericht"] == gericht, "Beilagen"]
            raw = str(raw_series.iloc[0]) if not raw_series.empty else "<n/a>"
        except Exception:
            raw = "<err>"
        codes = parse_codes(raw)
        allowed = sorted(list(allowed_sides_for_dish(gericht)))
        msg_dbg = await update_or_query.message.reply_text(
            f"DEBUG {gericht}: raw='{raw}' → codes={codes} → allowed={allowed}"
        )
        context.user_data.setdefault("flow_msgs", []).append(msg_dbg.message_id)

    # 2) Beilage-Codes aus df_gerichte lesen und parsen
    raw = df_gerichte.loc[df_gerichte["Gericht"] == gericht, "Beilagen"].iloc[0]
    codes = [c for c in parse_codes(raw) if c != 0]


    # 2) Erlaubte Nummern aus zentraler Funktion
    erlaubt = set(allowed_sides_for_dish(gericht))
    context.user_data["allowed_beilage_codes"] = erlaubt

    # 3) Auswahl initialisieren (falls noch nicht vorhanden)
    uid = str(update_or_query.from_user.id)
    sel = sessions.setdefault(uid, {}).setdefault("beilagen", {}).setdefault(gericht, [])

    # 4) Inline-Buttons bauen (max. 3/Zeile)
    markup = build_beilage_keyboard(erlaubt, sel)
    msg = await update_or_query.message.reply_text(
        pad_message(f"Wähle Beilagen für: <b>{escape(gericht)}</b>"),
        reply_markup=markup,
    )
    track_msg(context, "flow_msgs", msg.message_id)


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
            return await show_final_dishes_and_ask_persons(update, context, step=5)


        context.user_data["to_process"] = sorted(sel)  # 0-basierte Indizes
        context.user_data["menu_idx"]   = 0
        return await ask_beilagen_for_menu(query, context)



    idx = int(data.split("_")[1]) - 1
    if idx in sel:
        sel.remove(idx)
    else:
        sel.add(idx)

    # Nach dem Toggle: Keyboard mit Namensbuttons neu rendern
    await query.message.edit_reply_markup(
        reply_markup=build_menu_select_keyboard_for_sides(menus, sel, max_len=35)
    )
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
    ensure_session_loaded_for_user_and_chat(update)
    query = update.callback_query
    await query.answer()
    data = query.data

    uid = str(query.from_user.id)
    idx_list = context.user_data["to_process"]
    idx = idx_list[context.user_data["menu_idx"]]
    menus = context.user_data["menu_list"]
    gericht = menus[idx]

    sel = sessions.setdefault(uid, {}).setdefault("beilagen", {}).setdefault(gericht, [])

    if data == "beilage_done":
        context.user_data["menu_idx"] += 1
        if context.user_data["menu_idx"] < len(idx_list):
            return await ask_beilagen_for_menu(query, context)

        # Alle Menüs abgearbeitet → zentrale Ausgabe
        return await show_final_dishes_and_ask_persons(update, context, step=1)



    # Toggle einer Beilage
    num = int(data.split("_")[1])
    if num in sel:
        sel.remove(num)
    else:
        sel.append(num)

    # Buttons neu zeichnen
    markup = build_beilage_keyboard(set(context.user_data.get("allowed_beilage_codes", [])), sel)
    await query.message.edit_reply_markup(markup)
    
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
    ensure_session_loaded_for_user_and_chat(update)
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
            reply += f"‣ {escape(formatted)}\n"
    else:
        reply += "ℹ️ Keine aktive Session."
    await update.message.reply_text(reply)



def build_profile_choice_keyboard() -> InlineKeyboardMarkup:
    """Inline-Buttons für die Frage ›Wie möchtest Du fortfahren?‹"""
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("...mit bestehendem Profil",  callback_data="prof_exist")],
        [InlineKeyboardButton("...ohne Einschränkung",  callback_data="prof_nolim"),],
        [
            InlineKeyboardButton("✍️ Profil erstellen",        callback_data="prof_new"),
            InlineKeyboardButton("👀 Profil anzeigen",         callback_data="prof_show"),
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
        zeile("Aufwändig", "heavy"),
    ]

    
    summe = sum(verteilung.values())
    if summe == total:
        rows.append([
            InlineKeyboardButton("🎲 Zufall", callback_data="aufwand_rand"),
            InlineKeyboardButton("✅ Weiter", callback_data="aufwand_done"),
        ])
    else:
        rows.append([
            InlineKeyboardButton("🎲 Zufall", callback_data="aufwand_rand"),
            InlineKeyboardButton(f"{summe}/{total} gewählt", callback_data="noop"),
        ])

    return InlineKeyboardMarkup(rows)




def profile_overview_text(p: dict) -> str:
    """Formatiert die Profil-Übersicht"""
    styles_str = "Alle Stile" if not p["styles"] else ", ".join(p["styles"])
    return (
        "🗂 <b>Dein Profil</b>\n"
        f"• Ernährungsstil: {escape(str(p.get('restriction', '')))}\n"
        f"• Küche: {escape(styles_str)}\n"
        f"• Typ: {escape(str(p.get('weight', 'Egal') or 'Egal'))}"
    )

def build_profile_overview_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔄 Neu anlegen", callback_data="prof_overwrite"),
            InlineKeyboardButton("🔙 Zurück",       callback_data="prof_back"),
        ]
    ])


##############################################
#>>>>>>>>>>>>TAUSCHE
##############################################

async def tausche(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_session_loaded_for_user_and_chat(update)
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

    persist_session(update)

    
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
        await upsert_distribution_debug(update, context, debug_msg)


    await update.message.reply_text(
        "🔄 Neue Menüs:\n" +
        "\n".join(f"{i+1}. {g}" for i, g in enumerate(menues))
    )

    if show_debug_for(update):
        try:
            dbg_txt = build_selection_debug_text(menues)
            if dbg_txt:
                await upsert_distribution_debug(update, context, dbg_txt)
        except Exception:
            pass


async def tausche_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_session_loaded_for_user_and_chat(update)
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
            await _debounced_aufwand_render(query, context)

        return MENU_AUFWAND

    elif data == "aufwand_rand":
        # zufällige Verteilung auf 3 Klassen, Summe = total
        total = context.user_data["menu_count"]
        picks = [random.choice(("light", "medium", "heavy")) for _ in range(total)]
        verteilung["light"]  = picks.count("light")
        verteilung["medium"] = picks.count("medium")
        verteilung["heavy"]  = picks.count("heavy")

        await query.message.edit_reply_markup(
            reply_markup=build_aufwand_keyboard(verteilung, total)
        )
        return MENU_AUFWAND

    elif data == "aufwand_done":
        if sum(verteilung.values()) != total:
            await query.answer("Noch nicht vollständig verteilt!", show_alert=True)
            return MENU_AUFWAND

        a1 = verteilung["light"]
        a2 = verteilung["medium"]
        a3 = verteilung["heavy"]
        total = a1 + a2 + a3  # sichere Gesamtmenge

        # 🚫 WICHTIG: evtl. geplanten Debounce-Render abbrechen (gegen „erst Buttons weg, dann Text weg“)
        task = context.user_data.pop("aufw_render_task", None)
        if task and not task.done():
            try:
                task.cancel()
            except Exception:
                pass
        context.user_data["aufw_dirty"] = False

        # Nachricht EINMALIG komplett löschen (Buttons + Text zusammen)
        try:
            await context.bot.delete_message(
                chat_id=query.message.chat.id,
                message_id=query.message.message_id
            )
        except Exception:
            pass

        return await menu_input_direct(f"{total} ({a1},{a2},{a3})", update, context)

    elif data == "noop":
        return MENU_AUFWAND





async def tausche_select_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_session_loaded_for_user_and_chat(update)
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

    if data == "swap_done" and not sel:
        # Tausch-Frage entfernen
        try:
            await context.bot.delete_message(
                chat_id=q.message.chat.id,
                message_id=q.message.message_id
            )
        except Exception:
            pass

        # Weiter wie 'Passt' → Beilagenfrage oder direkt Personen
        menus = sessions[uid]["menues"]
        side_menus = [i for i, dish in enumerate(menus) if allowed_sides_for_dish(dish)]

        if not side_menus:
            return await show_final_dishes_and_ask_persons(update, context, step=2)

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Ja", callback_data="ask_yes"),
             InlineKeyboardButton("Nein", callback_data="ask_no")]
        ])
        msg2 = await q.message.reply_text(pad_message("Möchtest Du Beilagen hinzufügen?"), reply_markup=kb)
        context.user_data.setdefault("flow_msgs", []).append(msg2.message_id)
        return ASK_BEILAGEN


    # 3) 'Weiter' mit Auswahl → alten Vorschlag + Tauschfrage löschen, neuen Vorschlag senden
    if data == "swap_done":
        # 1) Profil / Basis
        profile = profiles.get(uid)
        basis_df = apply_profile_filters(df_gerichte, profile)

        sessions[uid].setdefault("beilagen", {})
        menues = sessions[uid]["menues"]
        aufw = sessions[uid]["aufwand"]
        swap_history = sessions[uid].setdefault("swap_history", {1: [], 2: [], 3: []})
        if all(len(v) == 0 for v in swap_history.values()):
            for dish, lvl in zip(menues, aufw):
                swap_history[lvl].append(dish)

        swapped_slots: list[int] = []
        for idx in sorted(sel):
            slot = idx - 1
            current_dish = menues[slot]
            current_aufw = aufw[slot]
            row_cur = df_gerichte[df_gerichte["Gericht"] == current_dish].iloc[0]
            current_art = ART_ORDER.get(row_cur["Typ"], 2)

            other_sel = set(menues) - {current_dish}
            cands = set(
                basis_df[basis_df["Aufwand"] == current_aufw]["Gericht"]
            ) - {current_dish} - other_sel

            if not cands:
                for lvl in (current_aufw - 1, current_aufw + 1):
                    if 1 <= lvl <= 3:
                        fb = set(
                            basis_df[basis_df["Aufwand"] == lvl]["Gericht"]
                        ) - {current_dish} - other_sel
                        if fb:
                            cands = fb
                            break

            used = set(swap_history[current_aufw])
            pool = list(cands - used)
            if not pool:
                swap_history[current_aufw] = [
                    m for m, lv in zip(menues, aufw) if lv == current_aufw
                ]
                used = set(swap_history[current_aufw])
                pool = list(cands - used)
            if not pool:
                continue

            scored = []
            for cand in pool:
                row_c = df_gerichte[df_gerichte["Gericht"] == cand].iloc[0]
                cand_aw = int(row_c["Aufwand"])
                cand_art = ART_ORDER.get(row_c["Typ"], 2)
                d_aw = abs(current_aufw - cand_aw)
                d_art = abs(current_art - cand_art)
                scored.append((cand, (d_aw, d_art)))

            min_score = min(score for _, score in scored)
            best = [c for c, score in scored if score == min_score]

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

        persist_session(update)
        context.user_data["swapped_indices"] = swapped_slots

        # 2) Tauschfrage (diese Nachricht) entfernen + aus flow_msgs austragen
        try:
            await context.bot.delete_message(
                chat_id=q.message.chat.id,
                message_id=q.message.message_id
            )
        except Exception:
            pass
        flow_ids = context.user_data.get("flow_msgs", [])
        if isinstance(flow_ids, list):
            try:
                flow_ids.remove(q.message.message_id)
            except ValueError:
                pass

        # 3) Debug aktualisieren; falls Debug-Message fehlt, sicherer Fallback über Neu-Render
        has_debug = isinstance(context.user_data.get("dist_debug_msg_id"), int)
        if has_debug and show_debug_for(update):
            try:
                dbg_txt = build_selection_debug_text(sessions[uid]["menues"])
                if dbg_txt:
                    await upsert_distribution_debug(update, context, dbg_txt)
            except Exception:
                pass

        # 4) Debug + Karte IMMER als Paar neu rendern → Debug garantiert darüber
        dishes = sessions[uid]["menues"]
        await render_proposal_with_debug(
            update, context,
            title="Neuer Vorschlag:",
            dishes=dishes,
            buttons=[
                [InlineKeyboardButton("Passt", callback_data="swap_ok"),
                 InlineKeyboardButton("Austauschen", callback_data="swap_again")]
            ],
            replace_old=True
        )
        return TAUSCHE_CONFIRM






# ─────────── tausche_confirm_cb ───────────
async def tausche_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    ensure_session_loaded_for_user_and_chat(update)
    q = update.callback_query
    await q.answer()
    chat_id = q.message.chat.id
    uid     = str(q.from_user.id)

    if q.data == "swap_again":
        # Kein Umschalten der Buttons – nur den Auswahl-Flow neu starten
        context.user_data["swap_candidates"] = set()

        # Nur die letzte Frage löschen (nicht die Liste/den Vorschlag)
        flow = context.user_data.get("flow_msgs", [])
        if flow:
            last_id = flow.pop()
            try:
                await q.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

        kb = build_swap_keyboard(sessions[uid]["menues"], context.user_data["swap_candidates"])
        msg = await q.message.reply_text(pad_message("Welche Gerichte möchtest Du tauschen?"), reply_markup=kb)
        context.user_data["flow_msgs"].append(msg.message_id)
        return TAUSCHE_SELECT


    if q.data == "swap_ok":
        # Buttons der Vorschlagskarte entfernen (kein Umschalten auf "Ja/Nein")
        flow = context.user_data.get("flow_msgs", [])
        if flow:
            last_id = flow.pop()
            try:
                await q.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

        # 🔑 Ephemere Keys sicher resetten (sonst alte Auswahl hängen geblieben)
        for k in ("menu_list", "to_process", "menu_idx", "allowed_beilage_codes", "selected_menus"):
            context.user_data.pop(k, None)

        # jetzt gleiche Beilagen-Logik wie oben in menu_confirm_cb:
        menus = sessions[uid]["menues"]
        side_menus = [idx for idx, dish in enumerate(menus) if allowed_sides_for_dish(dish)]
        
        if not side_menus:
            return await show_final_dishes_and_ask_persons(update, context, step=2)
            
        await render_beilage_precheck_debug(update, context, menus, prefix="DEBUG Beilagenvorprüfung (nach Tausch):")

        return await ask_beilagen_yes_no(q.message, context)
# CODE SCHLUSS


    return ConversationHandler.END





##############################################
#>>>>>>>>>>>>FERTIG
##############################################

async def fertig_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_session_loaded_for_user_and_chat(update)
    if str(update.message.from_user.id) not in sessions:
        await update.message.reply_text("⚠️ Keine Menüs gewählt.")
        return ConversationHandler.END
    await update.message.reply_text(pad_message("Für wie viele Personen?"))
    return FERTIG_PERSONEN

async def fertig_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_session_loaded_for_user_and_chat(update)
    user_id = str(update.effective_user.id)
    chat_id = update.effective_chat.id

    # Personenzahl: Buttons (temp_persons) bevorzugen, sonst Text
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
    df = df_zutaten
    ausgew = sessions[user_id]["menues"]
    context.user_data["final_list"] = ausgew

    # Hauptgerichte
    zut_gericht = df[
        (df["Typ"] == "Gericht") &
        (df["Gericht"].isin(ausgew))
    ].copy()

    # Beilagen
    all_nums = sum(sessions[user_id].get("beilagen", {}).values(), [])
    beilage_names = df_beilagen.loc[df_beilagen["Nummer"].isin(all_nums), "Beilagen"].tolist()
    zut_beilage = df[
        (df["Typ"] == "Beilagen") &
        (df["Gericht"].isin(beilage_names))
    ].copy()

    # Zusammenführen + skalieren
    zut = pd.concat([zut_gericht, zut_beilage], ignore_index=True)
    zut["Menge"] *= faktor

    # Vegi-Profil: Fleisch raus
    profile = profiles.get(user_id)
    if profile and profile.get("restriction") == "Vegi":
        zut = zut[zut["Kategorie"] != "Fleisch"]

    # ---- Einkaufsliste (gruppiert) ----
    eink = (
        zut.groupby(["Zutat", "Kategorie", "Einheit"])
        .agg(Menge=("Menge", "sum"), Menge_raw=("Menge_raw", "first"))
        .reset_index()
        .sort_values(["Kategorie", "Zutat"])
    )

    eink_text = f"\n<b>🛒 <u>Einkaufsliste für {personen} Personen:</u></b>\n"
    for cat, group in eink.groupby("Kategorie"):
        emoji = CAT_EMOJI.get(cat, "")
        eink_text += f"\n{emoji} <u>{escape(str(cat))}</u>\n"
        for _, r in group.iterrows():
            raw = str(r["Menge_raw"]).strip()
            if not raw.replace(".", "").isdigit():
                txt = raw or "wenig"
                line = f"‣ {r.Zutat}: {txt}"
            else:
                amt  = format_amount(r.Menge)
                line = f"‣ {r.Zutat}: {amt} {r.Einheit}"
            eink_text += f"{line}\n"

    # --- Kochliste mit Hauptgericht- und Beilagen-Zutaten in der richtigen Reihenfolge ---
    koch_text = f"\n<b><u>🍽 Kochliste für {personen} Personen:</u></b>\n"

    # Schnelle Lookups für Link & Aufwand
    _link_by_dish    = df_gerichte.set_index("Gericht")["Link"].to_dict()
    _aufwand_by_dish = df_gerichte.set_index("Gericht")["Aufwand"].to_dict()
    # Session-Aufwand (falls vorhanden) hat Vorrang
    _aufwand_session = {}
    try:
        _aufwand_session = {d: lv for d, lv in zip(ausgew, sessions[user_id].get("aufwand", []))}
    except Exception:
        _aufwand_session = {}

    _label_map = {1: "(<30min)", 2: "(30-60min)", 3: "(>60min)"}

    for g in ausgew:
        # 1) Beilagen-Namen zum Gericht
        sel_nums       = sessions[user_id].get("beilagen", {}).get(g, [])
        beilagen_namen = df_beilagen.loc[df_beilagen["Nummer"].isin(sel_nums), "Beilagen"].tolist()

        # 2) Zutaten für Hauptgericht + Beilagen in Reihenfolge zusammenführen
        part_haupt = zut[(zut["Typ"] == "Gericht") & (zut["Gericht"] == g)]
        parts_list = [part_haupt]
        for b in beilagen_namen:
            part_b = zut[(zut["Typ"] == "Beilagen") & (zut["Gericht"] == b)]
            parts_list.append(part_b)
        part = pd.concat(parts_list, ignore_index=True)

        # 3) Zutaten-Text (HTML-escapen)
        ze_parts = []
        for _, row in part.iterrows():
            raw = str(row["Menge_raw"]).strip()
            if not raw.replace(".", "").isdigit():
                txt = raw or "wenig"
                ze_parts.append(f"{row['Zutat']} {txt}")
            else:
                amt = format_amount(row["Menge"])
                ze_parts.append(f"{row['Zutat']} {amt} {row['Einheit']}")
        ze_html = escape(", ".join(ze_parts))

        # 4) Titel: Link (falls vorhanden) + Beilagenzusatz + Aufwand-Label
        #    a) Link robust (https:// ergänzen, falls fehlt)
        raw_link = normalize_link(str(_link_by_dish.get(g, "") or ""))

        #    b) Haupttitel (Link außen, Bold innen: <a><b>…</b></a>)
        name_html = f"<b>{escape(g)}</b>"
        if raw_link:
            name_html = f'<a href="{escape(raw_link, quote=True)}"><b>{escape(g)}</b></a>'

        #    c) Zusatz hinter dem Gerichts-Namen (z.B. " mit Reis und Brokkoli")
        full_title = format_dish_with_sides(g, beilagen_namen)
        rest       = full_title[len(g):] if full_title.startswith(g) else ""
        rest_html  = f"<b>{escape(rest)}</b>" if rest else ""

        #    d) Aufwand zuerst aus der Session, sonst aus df (nur 1/2/3 zulassen)
        lvl = _aufwand_session.get(g, None)
        if lvl not in (1, 2, 3):
            try:
                lvl = int(_aufwand_by_dish.get(g, 0))
            except Exception:
                lvl = 0

        aufwand_label_html = ""
        if lvl in _label_map:
            aufwand_label_html = f"<i>{escape(_label_map[lvl])}</i>"

        display_title_html = f"{name_html}{rest_html}{(' ' + aufwand_label_html) if aufwand_label_html else ''}"
        koch_text += f"\n{display_title_html}\n{ze_html}\n"

    # Vorschlagskarte ("Mein Vorschlag" / "Neuer Vorschlag") gezielt entfernen
    await delete_proposal_card(context, chat_id)


    # ---- Flow-UI aufräumen (nur flow_msgs) ----
    await reset_flow_state(update, context, reset_session=False, delete_messages=True, only_keys=["flow_msgs"])

    # ---- Für Exporte merken ----
    context.user_data["einkaufsliste_df"] = eink
    context.user_data["kochliste_text"]   = koch_text
    
    # — Einkaufs- & Kochliste senden + Export-Buttons an dieselbe Nachricht —

    # 1) Finale Liste OHNE Buttons senden (bleibt im Chat stehen)
    sent_list = await context.bot.send_message(
        chat_id=chat_id,
        text=koch_text + eink_text,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
    # optional: für spätere Referenzen
    context.user_data["final_list_msg_id"] = sent_list.message_id

    # 2) Aktionsmenü als EIGENE Nachricht direkt darunter senden
    await send_action_menu(sent_list, context)

    return ConversationHandler.END



##############################################
#>>>>>>>>>>>>EXPORTE / FINALE
##############################################

###################---------------------- Export to Bring--------------------

async def export_to_bring(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Erstellt HTML-Recipe-Gist → Bring-Deeplink. Ersetzt das Aktionsmenü (nicht die Liste)."""
    query = update.callback_query
    await query.answer()

    eink = context.user_data.get("einkaufsliste_df")
    if eink is None:
        await query.edit_message_text("❌ Keine Einkaufsliste gefunden.")
        return ConversationHandler.END

    # --- JSON-LD vorbereiten (stabil sortiert) ---
    eink_sorted = (
        eink.copy()
        .assign(Kategorie=lambda d: d["Kategorie"].fillna("Sonstiges"))
        .sort_values(["Kategorie", "Zutat"], kind="mergesort")
    )
    recipe_ingredients = [
        (f"{format_amount(r.Menge)} {r.Einheit} {r.Zutat}").strip()
        for _, r in eink_sorted.iterrows()
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

    if not GITHUB_TOKEN:
        await query.edit_message_text(
            "❌ Kein GitHub-Token gefunden (Umgebungsvariable GITHUB_TOKEN). "
            "Ohne öffentliches Rezept kann Bring! nichts importieren."
        )
        # die editierte Nachricht ist bereits „Fehler“-Text; wir müssen sie nicht speziell tracken
        return ConversationHandler.END

    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}
    gist_payload = {
        "description": "FoodApp – temporärer Bring-Recipe-Import",
        "public": True,
        "files": {"recipe.html": {"content": html_content}},
    }

    try:
        gist_resp = await HTTPX_CLIENT.post("https://api.github.com/gists", json=gist_payload, headers=headers)
        gist_resp.raise_for_status()
        raw_url = gist_resp.json()["files"]["recipe.html"]["raw_url"]

        dl_resp = await HTTPX_CLIENT.get(
            "https://api.getbring.com/rest/bringrecipes/deeplink",
            params={"url": raw_url, "source": "web"},
            follow_redirects=False,
        )
        if dl_resp.status_code in (301, 302, 303, 307, 308):
            deeplink = dl_resp.headers.get("location")
        else:
            dl_resp.raise_for_status()
            deeplink = dl_resp.json().get("deeplink")

        if not deeplink:
            raise RuntimeError("Kein Deeplink erhalten")

        logging.info("Erhaltener Bring-Deeplink: %s", deeplink)

    except (httpx.HTTPError, RuntimeError) as err:
        logging.error("Fehler bei Bring-Export: %s", err)
        await query.edit_message_text("❌ Bring-Export fehlgeschlagen. Versuche es später erneut.")
        return ConversationHandler.END

    # Aktionsmenü-Nachricht in Bring-Button umwandeln (und ID merken fürs spätere Löschen beim Neustart)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("In Bring! importieren", url=deeplink)]])
    await query.edit_message_text("🛒 Einkaufsliste an Bring! senden:", reply_markup=kb)
    _track_export_msg(context, query.message.message_id)

    # Neues Aktionsmenü darunter erneut anbieten
    await send_action_menu(query.message, context)
    return EXPORT_OPTIONS



###################---------------------- PDF Export--------------------

async def export_to_pdf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fragt, welche Listen exportiert werden sollen. Ersetzt das Aktionsmenü (nicht die Liste).”
    """
    query = update.callback_query
    await query.answer()

    eink_df   = context.user_data.get("einkaufsliste_df")
    koch_text = context.user_data.get("kochliste_text")
    if eink_df is None or eink_df.empty or not koch_text:
        await query.edit_message_text("❌ Keine Listen zum Export gefunden.")
        return ConversationHandler.END

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Einkaufsliste", callback_data="pdf_export_einkauf")],
        [InlineKeyboardButton("Kochliste",     callback_data="pdf_export_koch")],
        [InlineKeyboardButton("Beides",        callback_data="pdf_export_beides")],
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
    pdf = PDF(date_str)  # unsere Unterklasse
    try:
        pdf.add_font("DejaVu", "",  "fonts/DejaVuSans.ttf")
        pdf.add_font("DejaVu", "B", "fonts/DejaVuSans-Bold.ttf")
        pdf.add_page()
    except Exception:
        pdf.add_page()

    # ---------- Helper: KOCHLISTE ----------
    def write_kochliste():
        pdf.set_font("DejaVu", "B", 14)
        pdf.cell(0, 10, "Kochliste", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        raw_lines = koch_text.splitlines()
        lines = []
        for l in raw_lines:
            if not l.strip():
                continue
            plain = unescape(re.sub(r"<[^>]+>", "", l))
            if "Kochliste" in plain:
                continue
            lines.append(l)

        last_title = None
        for l in lines:
            if last_title is None:
                last_title = l
                continue

            title_plain       = unescape(re.sub(r"<[^>]+>", "", last_title))
            ingredients_plain = unescape(re.sub(r"<[^>]+>", "", l))
            last_title = None

            pdf.set_x(pdf.l_margin)
            pdf.set_font("DejaVu", "B", 12)
            pdf.multi_cell(pdf.epw, 8, title_plain, align="L")

            pdf.set_x(pdf.l_margin)
            pdf.set_font("DejaVu", "", 12)
            pdf.multi_cell(pdf.epw, 8, ingredients_plain, align="L")
            pdf.ln(2)

        if last_title is not None:
            title_plain = unescape(re.sub(r"<[^>]+>", "", last_title))
            pdf.set_x(pdf.l_margin)
            pdf.set_font("DejaVu", "B", 12)
            pdf.multi_cell(pdf.epw, 8, title_plain, align="L")
            pdf.ln(2)

    # ---------- Helper: EINKAUFSLISTE ----------
    def write_einkaufsliste():
        pdf.set_font("DejaVu", "B", 14)
        pdf.cell(0, 10, "Einkaufsliste", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        col_gap = 8
        col_w   = (pdf.epw - col_gap) / 2
        left_x  = pdf.l_margin
        right_x = pdf.l_margin + col_w + col_gap
        start_y = pdf.get_y()
        col     = 0

        def current_x():
            return left_x if col == 0 else right_x

        def page_bottom():
            return pdf.h - pdf.b_margin

        def switch_column():
            nonlocal col, start_y, left_x, right_x, col_w
            if col == 0:
                col = 1
                pdf.set_xy(right_x, start_y)
            else:
                pdf.add_page()
                pdf.set_font("DejaVu", "B", 14)
                pdf.cell(0, 10, "Einkaufsliste", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                col_w   = (pdf.epw - col_gap) / 2
                left_x  = pdf.l_margin
                right_x = pdf.l_margin + col_w + col_gap
                start_y = pdf.get_y()
                col = 0
                pdf.set_xy(left_x, start_y)

        def ensure_space(height_needed: float):
            if pdf.get_y() + height_needed <= page_bottom():
                return
            switch_column()

        def calc_item_height(txt: str, line_h: float = 6.0) -> float:
            width = pdf.get_string_width(txt)
            lines = max(1, math.ceil(width / col_w)) if col_w > 0 else 1
            return lines * line_h

        pdf.set_font("DejaVu", "", 12)
        for cat, group in eink_df.sort_values(["Kategorie", "Zutat"]).groupby("Kategorie"):
            head = str(cat)
            ensure_space(8)
            pdf.set_font("DejaVu", "B", 12)
            pdf.set_x(current_x())
            pdf.multi_cell(col_w, 8, head, align="L")
            pdf.set_x(current_x())
            pdf.set_font("DejaVu", "", 12)

            for _, row in group.iterrows():
                raw = str(row["Menge_raw"]).strip()
                if not raw.replace(".", "").isdigit():
                    txt  = raw or "wenig"
                    line = f"▪ {row['Zutat']}: {txt}"
                else:
                    amt  = format_amount(row["Menge"])
                    line = f"▪ {row['Zutat']}: {amt} {row['Einheit']}"

                h = calc_item_height(line, line_h=6)
                ensure_space(h)
                pdf.set_xy(current_x(), pdf.get_y())
                pdf.multi_cell(col_w, 6, line, align="L")

            ensure_space(2)
            pdf.set_xy(current_x(), pdf.get_y() + 2)

    # --- Ausgabereihenfolge je nach Wahl ---
    if choice == "koch":
        write_kochliste()
    elif choice == "einkauf":
        write_einkaufsliste()
    else:
        write_kochliste()
        pdf.add_page()
        write_einkaufsliste()

    # --- Speichern & Senden ---
    tmp_filename = f"liste_{q.from_user.id}.pdf"   # interner Temp-Dateiname
    pdf.output(tmp_filename)

    # Aktionsmenü-Nachricht in "Hier ist dein PDF:" umwandeln und ID merken
    await q.edit_message_text("📄 Hier ist dein PDF:")
    _track_export_msg(context, q.message.message_id)

    # Download-Name wie gewünscht: "Foodylenko - TT.MM.YY.pdf"
    date_disp = datetime.now().strftime("%d.%m.%y")
    with open(tmp_filename, "rb") as f:
        pdf_msg = await q.message.reply_document(document=f, filename=f"Foodylenko - {date_disp}.pdf")
    os.remove(tmp_filename)

    # Auch die PDF-Dokument-Nachricht fürs spätere Löschen merken
    _track_export_msg(context, pdf_msg.message_id)

    # Danach neues Aktionsmenü
    await send_action_menu(q.message, context)
    return EXPORT_OPTIONS



###################---------------------- NEUSTART FLOW--------------------


async def restart_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry-Point für Neustart-Button: fragt nach Bestätigung.
       Lässt das Aktionsmenü stehen und sendet die Bestätigungsfrage darunter.
    """
    q = update.callback_query
    await q.answer()

    text = pad_message("🔄 Bist Du sicher?")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Ja",   callback_data="restart_yes"),
         InlineKeyboardButton("Nein", callback_data="restart_no")]
    ])
    confirm = await context.bot.send_message(
        chat_id=q.message.chat.id,
        text=text,
        reply_markup=kb
    )
    # ID merken, damit wir bei "Nein" nur diese Frage löschen können
    context.user_data["restart_confirm_msg_id"] = confirm.message_id
    return RESTART_CONFIRM



async def restart_start_ov(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Restart-Bestätigung aus der ÜBERSICHT.
    Wichtig: Übersicht NICHT editieren – neue Nachricht als Reply zur Übersicht posten.
    """
    q = update.callback_query
    await q.answer()
    chat_id = q.message.chat.id

    confirm_text = pad_message("🔄 Bist Du sicher?")
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Ja",   callback_data="restart_yes_ov"),
        InlineKeyboardButton("Nein", callback_data="restart_no_ov"),
    ]])

    await context.bot.send_message(
        chat_id=chat_id,
        text=confirm_text,
        reply_markup=kb
    )
    return ConversationHandler.END




async def restart_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Bestätigung für 'Das passt so. Neustart!' am Prozessende."""
    q = update.callback_query
    await q.answer()
    chat_id = q.message.chat.id
    data = q.data  # 'restart_yes' | 'restart_no'

    # ggf. zuvor gesendete Bestätigungsfrage entfernen
    confirm_id = context.user_data.pop("restart_confirm_msg_id", None)

    if data == "restart_no":
        if confirm_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=confirm_id)
            except Exception:
                pass
        # Aktionsmenü bleibt stehen; keine neuen Buttons senden
        return EXPORT_OPTIONS

    # === restart_yes ===
    if confirm_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=confirm_id)
        except Exception:
            pass

    # 1) Aktions-/Export-Nachrichten entfernen
    for mid in context.user_data.get("export_msgs", []):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass
    context.user_data["export_msgs"] = []

    # 2) Vorschlagskarte gezielt entfernen
    await delete_proposal_card(context, chat_id)

    # 3) Alle bekannten UI-Listen JETZT leeren (damit später nichts „nachträglich“ löscht)
    for key in ["flow_msgs", "prof_msgs", "fav_msgs", "fav_add_msgs"]:
        ids = context.user_data.get(key, [])
        for mid in ids:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            except Exception:
                pass
        context.user_data[key] = []
    # Ein paar Marker zurücksetzen
    for key in ["proposal_msg_id", "final_list_msg_id"]:
        context.user_data.pop(key, None)

    # 4) Session wirklich zurücksetzen (neuer Lauf!)
    uid = str(update.effective_user.id)
    if uid in sessions:
        del sessions[uid]
    try:
        ckey = chat_key(int(update.effective_chat.id))
        store_delete_session(ckey)
    except Exception:
        pass

    # 5) QuickOne-Pool beenden
    context.user_data.pop("quickone_remaining", None)

    # 6) kurzer Abschiedsgruß → danach IN-PLACE in Neustart-Banner verwandeln
    try:
        bye = await context.bot.send_message(chat_id, pad_message("Super, bis bald!👋"))
        await asyncio.sleep(1.2)

        banner = build_new_run_banner()
        # Wichtiges Caveat: Diese Nachricht NICHT in export_msgs tracken!
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=bye.message_id,
            text=pad_message(banner),
        )
        context.user_data.pop("quickone_remaining", None)  # Pool des Durchgangs beenden
        await asyncio.sleep(1.0)
    except Exception:
        # Fallback: wenn Edit scheitert → Bye (falls möglich) löschen und Banner frisch posten
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=bye.message_id)
        except Exception:
            pass
        try:
            banner = build_new_run_banner()
            await context.bot.send_message(chat_id, pad_message(banner))
            await asyncio.sleep(1.0)
        except Exception:
            pass


    await send_overview(chat_id, context)
    return ConversationHandler.END




async def restart_confirm_ov(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Bestätigung für '🔄 Restart' aus der Übersicht."""
    q = update.callback_query
    await q.answer()
    chat_id = q.message.chat.id
    data = q.data  # 'restart_yes_ov' | 'restart_no_ov'

    # Bestätigungsfrage entfernen
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=q.message.message_id)
    except Exception:
        pass

    if data == "restart_yes_ov":
        # gleiche Aufräumlogik wie im anderen Restart
        await delete_proposal_card(context, chat_id)

        for key in ["flow_msgs", "prof_msgs", "fav_msgs", "fav_add_msgs", "export_msgs"]:
            ids = context.user_data.get(key, [])
            for mid in ids:
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=mid)
                except Exception:
                    pass
            context.user_data[key] = []
        for key in ["proposal_msg_id", "final_list_msg_id"]:
            context.user_data.pop(key, None)

        uid = str(update.effective_user.id)
        if uid in sessions:
            del sessions[uid]
        try:
            ckey = chat_key(int(update.effective_chat.id))
            store_delete_session(ckey)
        except Exception:
            pass

        context.user_data.pop("quickone_remaining", None)

        # Abschiedsgruß → danach IN-PLACE in Neustart-Banner verwandeln
        try:
            bye = await context.bot.send_message(chat_id, pad_message("Super, bis bald!👋"))
            await asyncio.sleep(1.3)

            banner = build_new_run_banner()
            # Wichtiges Caveat: Diese Nachricht NICHT in export_msgs tracken!
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=bye.message_id,
                text=pad_message(banner),
            )
            context.user_data.pop("quickone_remaining", None)  # Pool des Durchgangs beenden
            await asyncio.sleep(1.0)
        except Exception:
            # Fallback: wenn Edit scheitert → Bye (falls möglich) löschen und Banner frisch posten
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=bye.message_id)
            except Exception:
                pass
            try:
                banner = build_new_run_banner()
                await context.bot.send_message(chat_id, pad_message(banner))
                await asyncio.sleep(1.0)
            except Exception:
                pass


        await send_overview(chat_id, context)
        return ConversationHandler.END

    # data == 'restart_no_ov'
    return ConversationHandler.END




##############################################
#>>>>>>>>>>>>FAVORITEN
##############################################

async def favorit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_session_loaded_for_user_and_chat(update)
    user_id = str(update.message.from_user.id)
    if user_id not in sessions:
        return await update.message.reply_text("⚠️ Bitte erst /menu.")
    if not context.args or not context.args[0].isdigit():
        return await update.message.reply_text("❌ Nutzung: /favorit 2")
    idx = int(context.args[0]) - 1
    menues = sessions[user_id]["menues"]
    if 0<=idx<len(menues):
        fav = menues[idx]
        ensure_favorites_loaded(user_id)
        favorites.setdefault(user_id, []).append(fav)
        store_set_favorites(user_key(int(user_id)), favorites[user_id])
        await update.message.reply_text(f"❤️ '{fav}' als Favorit gespeichert.")
    else:
        await update.message.reply_text("❌ Ungültiger Index.")


# ===================================== FAVORITEN–FLOW (anschauen & löschen)=============================

def build_fav_overview_text_for(uid: str) -> str:
    """Erzeugt den Text der initialen Favoriten-Übersicht (wie fav_start)."""
    _label_map = {1: "(<30min)", 2: "(30-60min)", 3: "(>60min)"}
    _aufwand_by_dish = df_gerichte.set_index("Gericht")["Aufwand"].to_dict()

    # Session-Aufwand hat Vorrang
    try:
        sess = sessions.get(uid, {})
        _aufwand_session = {d: lv for d, lv in zip(sess.get("menues", []), sess.get("aufwand", []))}
    except Exception:
        _aufwand_session = {}

    def _effort_level_for(d: str) -> int | None:
        lvl = _aufwand_session.get(d)
        if lvl in (1, 2, 3):
            return int(lvl)
        try:
            lvl = int(_aufwand_by_dish.get(d, 0))
            return lvl if lvl in (1, 2, 3) else None
        except Exception:
            return None

    favs = favorites.get(uid, []) or []
    groups = {1: [], 2: [], 3: []}
    for d in favs:
        lvl = _effort_level_for(d)
        if lvl in (1, 2, 3):
            groups[lvl].append(d)

    for lvl in (1, 2, 3):
        groups[lvl].sort(key=lambda s: s.casefold())

    sections = []
    for lvl in (1, 2, 3):
        if not groups[lvl]:
            continue
        header = f"<u>Aufwand: {escape(_label_map[lvl])}</u>"
        lines  = "\n".join(f"‣ {escape(d)}" for d in groups[lvl])
        sections.append(f"{header}\n{lines}")

    txt = "⭐ <u>Deine Favoriten:</u>\n" + ("\n\n".join(sections) if sections else "(keine Favoriten)")
    return txt


async def fav_render_overview_in_place(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Aktualisiert die bestehende Favoriten-Übersicht (list/menu) in-place.
    Falls die alten Messages nicht mehr editierbar sind, fällt auf fav_start() zurück.
    """
    q = update.callback_query
    uid = str(q.from_user.id)
    chat_id = q.message.chat.id
    ids = context.user_data.get("fav_overview_ids") or {}
    list_id = ids.get("list")
    menu_id = ids.get("menu")

    # Fallback: keine bekannten Overview-IDs -> neu aufbauen
    if not list_id or not menu_id:
        await fav_start(update, context)
        return

    txt = build_fav_overview_text_for(uid)

    action_text = (
        "<u>Was möchtest Du machen?</u>\n\n"
        "✔️ <b>Selektiere</b> Gerichte für die Auswahl\n\n"
        "✖️ Favoriten aus Liste <b>entfernen</b>\n\n"
        "🔙 <b>Zurück</b> zum Hauptmenü"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✔️ Selektieren", callback_data="fav_action_select"),
        InlineKeyboardButton("✖️ Entfernen",   callback_data="fav_action_remove"),
        InlineKeyboardButton("🔙 Zurück",      callback_data="fav_action_back"),
    ]])

    # Liste editieren (oder bei Fehler neu senden und IDs aktualisieren)
    try:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=list_id, text=pad_message(txt))
    except Exception:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=list_id)
        except Exception:
            pass
        m1 = await context.bot.send_message(chat_id, pad_message(txt))
        context.user_data.setdefault("fav_overview_ids", {})["list"] = m1.message_id

    # Action-Menü editieren (oder bei Fehler neu senden und IDs aktualisieren)
    try:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=menu_id, text=action_text, reply_markup=kb)
    except Exception:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=menu_id)
        except Exception:
            pass
        m2 = await context.bot.send_message(chat_id, action_text, reply_markup=kb)
        context.user_data.setdefault("fav_overview_ids", {})["menu"] = m2.message_id

async def fav_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry-Point für /meinefavoriten oder Button „Favoriten“."""
    msg = update.message or update.callback_query.message
    user_id = str(update.effective_user.id)
    ensure_favorites_loaded(user_id)
    favs = favorites.get(user_id, [])
    # IDs aller Loop-Nachrichten sammeln
    context.user_data["fav_msgs"] = []

    if not favs:
        warn = await msg.reply_text("Keine Favoriten vorhanden. Füge diese später hinzu!")
        await asyncio.sleep(2)
        try:
            await context.bot.delete_message(chat_id=msg.chat.id, message_id=warn.message_id)
        except:
            pass
        return ConversationHandler.END

    txt = build_fav_overview_text_for(user_id)

    m1 = await msg.reply_text(pad_message(txt))


    
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✔️ Selektieren", callback_data="fav_action_select"),
        InlineKeyboardButton("✖️ Entfernen",   callback_data="fav_action_remove"),
        InlineKeyboardButton("🔙 Zurück",      callback_data="fav_action_back")
    ]])
    m2 = await msg.reply_text(
        "<u>Was möchtest Du machen?</u>\n\n"
        "✔️ <b>Selektiere</b> Gerichte für die Auswahl\n\n"
        "✖️ Favoriten aus Liste <b>entfernen</b>\n\n"
        "🔙 <b>Zurück</b> zum Hauptmenü",
        reply_markup=kb
    )
    context.user_data["fav_msgs"].extend([m1.message_id, m2.message_id])
    context.user_data["fav_overview_ids"] = {"list": m1.message_id, "menu": m2.message_id}
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

# === Favoriten: Gruppieren + fortlaufend nummerieren (für Select/Remove) ===
def _effort_level_for_fav(uid: str, dish: str) -> int | None:
    """
    Einheitliche Aufwandsbestimmung:
    1) Session-Aufwand (falls vorhanden) hat Vorrang
    2) sonst Aufwand aus df_gerichte["Aufwand"]
    """
    try:
        sess = sessions.get(uid, {})
        for d, lvl in zip(sess.get("menues", []), sess.get("aufwand", [])):
            if d == dish and lvl in (1, 2, 3):
                return int(lvl)
    except Exception:
        pass
    try:
        lvl = int(df_gerichte.set_index("Gericht").at[dish, "Aufwand"])
        return lvl if lvl in (1, 2, 3) else None
    except Exception:
        return None

def _build_numbered_grouped_favs(uid: str) -> tuple[str, dict[int, str]]:
    """
    Baut den Text für die Select/Remove-Ansichten:
      - nach Aufwand gruppiert (1..3)
      - innerhalb der Gruppen alphabetisch
      - fortlaufend nummeriert 1..N über alle Gruppen
    Gibt (text_html, index_map) zurück, wobei index_map[i] -> Gericht
    """
    favs = favorites.get(uid, []) or []
    groups = {1: [], 2: [], 3: []}
    for d in favs:
        lvl = _effort_level_for_fav(uid, d)
        if lvl in (1, 2, 3):
            groups[lvl].append(d)

    # alphabetisch in jeder Gruppe
    for lvl in (1, 2, 3):
        groups[lvl].sort(key=lambda s: s.casefold())

    _label_map = {1: "(<30min)", 2: "(30-60min)", 3: "(>60min)"}
    lines = []
    idx_map: dict[int, str] = {}
    running = 1
    for lvl in (1, 2, 3):
        if not groups[lvl]:
            continue
        lines.append(f"<u>Aufwand: {escape(_label_map[lvl])}</u>")
        for dish in groups[lvl]:
            lines.append(f"{running}. {escape(dish)}")
            idx_map[running] = dish
            running += 1
        lines.append("")  # Leerzeile zwischen Gruppen

    text = "⭐ <u><b>Deine Favoriten (Auswahl):</b></u>\n" + "\n".join(lines).strip()
    return text, idx_map


async def fav_action_choice_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    ensure_favorites_loaded(uid)
    msg = q.message

    if q.data == "fav_action_back":
        chat_id = q.message.chat.id

        # 1) Arbeits-UI (Listen/Keyboards der Unter-Loops) entfernen
        for mid in context.user_data.get("fav_work_ids", []):
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            except Exception:
                pass
        context.user_data["fav_work_ids"] = []

        # 2) Die beiden Overview-Nachrichten (Liste + "Was möchtest Du machen?") zuverlässig entfernen
        ids = context.user_data.get("fav_overview_ids") or {}
        for key in ("list", "menu"):
            mid = ids.get(key)
            if mid:
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=mid)
                except Exception:
                    pass
        context.user_data.pop("fav_overview_ids", None)

        # 3) Fallback/Alt: evtl. noch in fav_msgs getrackte IDs auch säubern
        for mid in context.user_data.get("fav_msgs", []):
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            except Exception:
                pass
        context.user_data["fav_msgs"] = []

        # 4) Conversation beenden
        return ConversationHandler.END


    if q.data == "fav_action_remove":
        favs = favorites.get(uid, [])
        if not favs:
            await msg.reply_text("Du hast aktuell keine Favoriten gespeichert.")
            return ConversationHandler.END

        text, idx_map = _build_numbered_grouped_favs(uid)
        total = len(idx_map)

        # State für diesen Remove-Loop
        context.user_data["fav_total"] = total
        context.user_data["fav_del_sel"] = set()
        context.user_data["fav_del_index_map"] = idx_map  # Nummer → Gericht

        # Eine Nachricht mit gruppierter, nummerierter Liste + Zahlen-Keyboard
        list_msg = await msg.reply_text(
            pad_message(text),
            reply_markup=build_fav_numbers_keyboard(total, set())
        )
        context.user_data.setdefault("fav_work_ids", []).append(list_msg.message_id)
        return FAV_DELETE_SELECT

    if q.data == "fav_action_select":
        favs = favorites.get(uid, [])
        if not favs:
            await msg.reply_text("Keine Favoriten vorhanden.")
            return ConversationHandler.END

        text, idx_map = _build_numbered_grouped_favs(uid)
        total = len(idx_map)

        # State für diesen Select-Loop
        context.user_data["fav_total"] = total
        context.user_data["fav_sel_sel"] = set()
        context.user_data["fav_sel_index_map"] = idx_map  # Nummer → Gericht

        # Eine Nachricht mit gruppierter, nummerierter Liste + Zahlen-Keyboard
        list_msg = await msg.reply_text(
            pad_message(text),
            reply_markup=build_fav_selection_keyboard(total, set())
        )
        context.user_data.setdefault("fav_work_ids", []).append(list_msg.message_id)
        return FAV_ADD_SELECT


async def fav_selection_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    chat_id = q.message.chat.id

    sel = sorted(context.user_data.get("fav_sel_sel", set()))
    idx_map: dict[int, str] = context.user_data.get("fav_sel_index_map", {}) or {}

    # Auswahl anhand der fortlaufenden Nummern auflösen
    selected = [idx_map[i] for i in sel if i in idx_map]

    # Arbeitsnachrichten (Liste + Keyboard) wegräumen
    for mid in context.user_data.get("fav_work_ids", []):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass
    context.user_data["fav_work_ids"] = []
    context.user_data.pop("fav_sel_sel", None)
    context.user_data.pop("fav_sel_index_map", None)

    # Auswahl (falls vorhanden) speichern + kurze Info, die wir nach 1.2s wieder löschen
    if selected:
        context.user_data["fav_selection"] = selected
        info = await q.message.reply_text("✅ Auswahl gespeichert. Klicke nun auf <b>Zurück</b> und starte den normalen Suchlauf über <b>Menü</b>")
        try:
            await asyncio.sleep(1.8)
        finally:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=info.message_id)
            except Exception:
                pass

    # Zurück in den Favoriten-Overview-State (keine neue Übersicht posten!)
    return FAV_OVERVIEW


async def fav_del_number_toggle_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    idx = int(q.data.split("_")[-1])
    sel = context.user_data.setdefault("fav_del_sel", set())
    sel.symmetric_difference_update({idx})
    total = context.user_data["fav_total"]
    await q.edit_message_reply_markup(build_fav_numbers_keyboard(total, sel))
    #return FAV_ADD_SELECT
    return FAV_DELETE_SELECT


async def fav_del_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    chat_id = q.message.chat.id

    # Auswahl lesen (fortlaufende Nummern)
    sel = sorted(context.user_data.get("fav_del_sel", set()))
    idx_map: dict[int, str] = context.user_data.get("fav_del_index_map", {}) or {}

    ensure_favorites_loaded(uid)
    favs = favorites.get(uid, [])[:]

    # Welche Gerichte sollen entfernt werden?
    to_remove = {idx_map[i] for i in sel if i in idx_map}

    removed = 0
    if to_remove:
        # Reihenfolge der übrigen Favoriten beibehalten
        favs = [d for d in favs if d not in to_remove]
        favorites[uid] = favs
        store_set_favorites(user_key(int(uid)), favorites[uid])
        removed = len(to_remove)

    # Arbeitsnachrichten (Liste + Keyboard) entfernen
    for mid in context.user_data.get("fav_work_ids", []):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass
    context.user_data["fav_work_ids"] = []
    context.user_data.pop("fav_del_sel", None)
    context.user_data.pop("fav_del_index_map", None)

    # Kein neues Overview posten!
    # Wenn etwas entfernt wurde: in-place aktualisieren + Info kurz zeigen (und wieder entfernen)
    if removed > 0:
        await fav_render_overview_in_place(update, context)
        info = await q.message.reply_text(f"✅ Du hast {removed} Favorit{'en' if removed != 1 else ''} entfernt.")
        await asyncio.sleep(1.5)
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=info.message_id)
        except Exception:
            pass

    # Ansonsten (Zurück ohne Auswahl): einfach wieder im Overview-State landen
    return FAV_OVERVIEW



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
    """Zahlen-Buttons (max. 7 pro Zeile) für Selektions-Modus + 'Zurück'/'✔️ Fertig'."""
    return _build_numbers_keyboard(prefix="fav_sel_", total=total, selected=selected, max_per_row=7, done_cb="fav_sel_done", done_label_empty="Zurück", done_label_some="✔️ Fertig")



async def fav_selection_toggle_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    idx = int(q.data.split("_")[-1])
    sel = context.user_data.setdefault("fav_sel_sel", set())
    sel.symmetric_difference_update({idx})
    total = context.user_data.get("fav_total", 0)
    await q.edit_message_reply_markup(reply_markup=build_fav_selection_keyboard(total, sel))
    return FAV_ADD_SELECT



# ===================================== FAVORITEN–FLOW (hinzufügen)=============================

async def fav_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    msg = q.message

    # Liste der Gerichte aus user_data holen
    dishes = context.user_data.get("final_list", [])
    if not dishes:
        await msg.edit_text("ℹ️ Keine Gerichte verfügbar.")
        return ConversationHandler.END

    # Auswahl initialisieren
    context.user_data["fav_add_sel"]  = set()
    context.user_data["fav_add_msgs"] = []

    # bestehende Favoriten des Users
    user_id       = str(q.from_user.id)
    ensure_favorites_loaded(user_id)
    existing_favs = set(favorites.get(user_id, []))

    header_text = pad_message(
        "Welche(s) Gericht(e) möchtest du deinen Favoriten hinzufügen?\n"
        "<i>(*Bestehende Favoriten gekennzeichnet)</i>"
    )
    kb = build_fav_add_keyboard_dishes(dishes, set(), existing_favs, max_len=35)

    # Aktionsmenü in Kopfzeile + Buttons verwandeln (ersetzt)
    await msg.edit_text(header_text, reply_markup=kb)
    context.user_data["fav_add_msgs"].append(msg.message_id)

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

    dishes = context.user_data.get("final_list", [])
    user_id = str(q.from_user.id)
    ensure_favorites_loaded(user_id)
    existing_favs = set(favorites.get(user_id, []))

    await q.edit_message_reply_markup(
        reply_markup=build_fav_add_keyboard_dishes(dishes, sel, existing_favs, max_len=35)
    )
    return FAV_ADD_SELECT


async def fav_add_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    sel    = sorted(context.user_data.get("fav_add_sel", []))
    dishes = context.user_data.get("final_list", [])
    user_id = str(q.from_user.id)

    # In Favoriten speichern
    ensure_favorites_loaded(user_id)
    favs = favorites.get(user_id, [])
    for i in sel:
        if 1 <= i <= len(dishes):
            dish = dishes[i-1]
            if dish not in favs:
                favs.append(dish)
    favorites[user_id] = favs
    store_set_favorites(user_key(int(user_id)), favorites[user_id])

    # Alle Loop-Messages löschen
    msg = q.message
    for mid in context.user_data.get("fav_add_msgs", []):
        try:
            await context.bot.delete_message(chat_id=msg.chat.id, message_id=mid)
        except:
            pass

    # Favoriten-Übersicht senden
    txt = "⭐ Deine aktualisierte Favoritenliste:\n" + "\n".join(f"‣ {d}" for d in favs)
    favlist_msg = await msg.reply_text(txt)
    _track_export_msg(context, favlist_msg.message_id)

    # Zurück ins Aktions-Menu
    await send_action_menu(msg, context)
    return EXPORT_OPTIONS




# ============================================================================================


##############################################
#>>>>>>>>>>>>CANCEL / RESET / RESTART
##############################################

async def delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    ensure_favorites_loaded(user_id)
    favs = favorites.get(user_id, [])
    if not context.args or not context.args[0].isdigit():
        return await update.message.reply_text("❌ Nutzung: /delete 1")
    idx = int(context.args[0]) - 1
    if 0<=idx<len(favs):
        rem = favs.pop(idx)
        store_set_favorites(user_key(int(user_id)), favorites[user_id])
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

    # 2) Offene Menü-Session löschen (lokal + Persistenz)
    if uid in sessions:
        del sessions[uid]
    try:
        store_delete_session(chat_key(int(update.effective_chat.id)))
    except Exception:
        pass

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
    ensure_session_loaded_for_user_and_chat(update)
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
    ensure_session_loaded_for_user_and_chat(update)
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
            f"‣ {row.Zutat}: {format_amount(row.Menge)} {row.Einheit}"
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
            _client = _get_openai_client()
            if _client:
                try:
                    resp = _client.chat.completions.create(
                        model=os.getenv("OPENAI_MODEL", "gpt-3.5-turbo"),
                        messages=[{"role": "user", "content": prompt}],
                    )
                    steps = resp.choices[0].message.content.strip()
                except Exception as e:
                    logging.warning("OpenAI-Fehler, nutze Fallback: %s", e)
                    steps = _fallback_steps(dish, zut_text)
            else:
                steps = _fallback_steps(dish, zut_text)

            recipe_cache[cache_key] = steps
            save_json(CACHE_FILE, recipe_cache)

        msg = (
            f"📖 Rezept für <b>{escape(dish)}</b> für <b>{personen}</b> Personen:\n\n"
            f"<b>Zutaten:</b>\n{escape(zut_text)}\n\n"
            f"<b>Zubereitungszeit:</b> ca. {escape(time_str)}\n\n"
            f"<b>Anleitung:</b>\n{escape(steps)}"
        )
        await update.message.reply_text(msg)
        
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
    print("BUILD_MARK = FIX_WEBHOOK_", __import__("datetime").datetime.utcnow().isoformat())
    app = ApplicationBuilder().token(TOKEN).defaults(Defaults(parse_mode=ParseMode.HTML, disable_web_page_preview=True)).build()
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
            PROFILE_CHOICE: [CallbackQueryHandler(profile_choice_cb, pattern=r"^prof_(?:exist|nolim|new|show)$")],
            PROFILE_NEW_A: [CallbackQueryHandler(profile_new_a_cb, pattern="^res_")],
            PROFILE_NEW_B: [CallbackQueryHandler(profile_new_b_cb, pattern="^style_")],
            PROFILE_NEW_C: [CallbackQueryHandler(profile_new_c_cb, pattern="^weight_")],
            PROFILE_OVERVIEW: [CallbackQueryHandler(profile_overview_cb, pattern=r"^prof_(?:overwrite|back)$")],
            MENU_INPUT:    [MessageHandler(filters.TEXT & ~filters.COMMAND, menu_input)],
            ASK_CONFIRM:   [CallbackQueryHandler(menu_confirm_cb, pattern="^confirm_")],
            ASK_BEILAGEN:  [CallbackQueryHandler(ask_beilagen_cb)],
            SELECT_MENUES:   [CallbackQueryHandler(select_menus_cb)],
            BEILAGEN_SELECT: [
                CallbackQueryHandler(restart_start,           pattern="^restart$"),
                CallbackQueryHandler(beilage_select_cb,       pattern=r"^beilage_(\d+|done)$"),
            ],
            ASK_FINAL_LIST:  [CallbackQueryHandler(ask_final_list_cb)],
            ASK_SHOW_LIST:   [CallbackQueryHandler(ask_showlist_cb)],
            PERSONS_SELECTION: [
                        CallbackQueryHandler(persons_selection_cb, pattern="^persons_page_(low|high)$"),
                        CallbackQueryHandler(persons_selection_cb, pattern="^persons_(\d+|done)$"),
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
                        CallbackQueryHandler(fav_add_done_cb,          pattern=r"^fav_add_done$"),
                        CallbackQueryHandler(fav_selection_toggle_cb,   pattern=r"^fav_sel_\d+$"),
                        CallbackQueryHandler(fav_selection_done_cb,     pattern="^fav_sel_done$")
            ],
            
        },
        fallbacks=[cancel_handler, reset_handler],
        allow_reentry=True
    ))

    #### ---- Globale Handler ----

    #app.add_handler(CallbackQueryHandler(start_favs_cb,   pattern="^start_favs$"))
    app.add_handler(CallbackQueryHandler(start_setup_cb, pattern=r"^(start|restart)_setup$"))
    app.add_handler(CallbackQueryHandler(setup_ack_cb,    pattern="^setup_ack$"))
    app.add_handler(CallbackQueryHandler(fav_add_start,    pattern="^favoriten$"))
    app.add_handler(CallbackQueryHandler(export_to_bring,  pattern="^export_bring$"))
    app.add_handler(CallbackQueryHandler(export_to_pdf,    pattern="^export_pdf$"))
    app.add_handler(CallbackQueryHandler(process_pdf_export_choice, pattern="^pdf_export_"))
    app.add_handler(CallbackQueryHandler(restart_start,    pattern="^restart$"))
    app.add_handler(CallbackQueryHandler(restart_start_ov, pattern="^restart_ov$"))
    app.add_handler(CallbackQueryHandler(restart_confirm_cb,  pattern="^restart_(yes|no)$"))
    app.add_handler(CallbackQueryHandler(restart_confirm_ov,  pattern="^restart_(yes|no)_ov$"))
    app.add_handler(CallbackQueryHandler(fav_add_number_toggle_cb, pattern=r"^fav_add_\d+$"))
    #app.add_handler(CallbackQueryHandler(fav_add_done_cb,          pattern="^fav_add_done$"))    am 23/9 gelöscht




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
            BEILAGEN_SELECT:   [CallbackQueryHandler(beilage_select_cb,  pattern=r"^beilage_(\d+|done)$")],  # ← NEU
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
            #CallbackQueryHandler(fav_add_start, pattern="^favoriten$")   gelöscht am 23/9, weil im globalen handler bereits drin...: "Behalte die Verarbeitung im Haupt-ConversationHandler (weil sie logisch dort hingehört) und lösche die Entry-Point-Zeile im Favoriten-Conversation."
        ],
        states={
            FAV_OVERVIEW: [
                CallbackQueryHandler(fav_overview_cb, pattern="^fav_edit_yes$"),
                CallbackQueryHandler(fav_overview_cb, pattern="^fav_edit_no$"),
                CallbackQueryHandler(fav_action_choice_cb, pattern="^fav_action_")
            ],
            FAV_DELETE_SELECT: [
                CallbackQueryHandler(fav_number_toggle_cb, pattern=r"^fav_del_\d+$"),
                CallbackQueryHandler(fav_del_done_cb,      pattern="^fav_del_done$"),
                CallbackQueryHandler(fav_overview_cb,      pattern="^fav_edit_yes$"),
                CallbackQueryHandler(fav_overview_cb,      pattern="^fav_edit_no$")
            ],
            # neu: Favoriten hinzufügen-Loop
            FAV_ADD_SELECT: [
                CallbackQueryHandler(fav_add_number_toggle_cb, pattern=r"^fav_add_\d+$"),
                CallbackQueryHandler(fav_add_done_cb,          pattern=r"^fav_add_done$"),
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




    # (Cloud Run start moved into main())


    print("✅ Bot läuft...")
    port = int(os.getenv("PORT", "8080"))
    url_path = f"webhook/{(WEBHOOK_SECRET or 'hook')[:16]}"

    from aiohttp import web

    async def _health_route(_request):
        return web.Response(text="OK")  # 200

    async def _telegram_webhook(request):
        # Telegram schickt POST JSON; an PTB weiterreichen
        data = await request.json()
        update = Update.de_json(data, app.bot)
        await app.process_update(update)
        return web.Response(text="OK")

    path = "/" + url_path.lstrip("/")

    if os.getenv("K_SERVICE"):
        # Cloud Run: öffentliche URL bestimmen (wie bisher)
        base = BASE_URL or _compute_base_url() or ""
        if not base:
            base = _compute_base_url() or ""
        webhook_url = f"{base.rstrip('/')}{path}"
        print(f"▶️ Cloud Run Webhook auf :{port} → {webhook_url}")

        aio = web.Application()
        aio.router.add_get("/", _health_route)
        aio.router.add_get("/webhook/health", _health_route)
        aio.router.add_post(path, _telegram_webhook)

        async def _on_startup(_app):
            try:
                await app.initialize()
            except Exception as e:
                print(f"❌ app.initialize() fehlgeschlagen: {e}")
                # HTTP-Server trotzdem starten, damit Health/Logs verfügbar sind
                return
            try:
                await app.bot.set_webhook(url=webhook_url, secret_token=WEBHOOK_SECRET)
                print("✅ set_webhook OK")
            except Exception as e:
                print(f"⚠️ set_webhook failed: {e} — continuing without blocking startup")
            await app.start()


        async def _on_cleanup(_app):
            await app.stop()
            await app.shutdown()
            try:
                await HTTPX_CLIENT.aclose()
            except Exception:
                pass

        aio.on_startup.append(_on_startup)
        aio.on_cleanup.append(_on_cleanup)
        web.run_app(aio, host="0.0.0.0", port=port)

    elif BASE_URL:
        # Lokal (ngrok o.ä.)
        webhook_url = f"{BASE_URL.rstrip('/')}{path}"
        print(f"▶️ Lokaler Webhook auf :{port} → {webhook_url}")

        aio = web.Application()
        aio.router.add_get("/webhook/health", _health_route)
        aio.router.add_post(path, _telegram_webhook)

        async def _on_startup(_app):
            await app.initialize()
            try:
                await app.bot.set_webhook(url=webhook_url, secret_token=WEBHOOK_SECRET)
                print("✅ set_webhook OK")
            except Exception as e:
                print(f"⚠️ set_webhook failed: {e} — continuing without blocking startup")
            await app.start()

        async def _on_cleanup(_app):
            await app.stop()
            await app.shutdown()
            try:
                await HTTPX_CLIENT.aclose()
            except Exception:
                pass
                
        aio.on_startup.append(_on_startup)
        aio.on_cleanup.append(_on_cleanup)
        web.run_app(aio, host="0.0.0.0", port=port)

    else:
        print("⚠️ Keine PUBLIC_URL → starte Polling (nur lokal geeignet).")
        app.run_polling()
    # --- ersetzen Ende ---

if __name__ == "__main__":
    main()
