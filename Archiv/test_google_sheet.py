import os
print("Aktueller Pfad:", os.getcwd())
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SHEET_ID = "1XzhGPWz7EFJAyZzaJQhoLyl-cTFNEa0yKvst0D0yVUs"
SHEET_NAME = "Basisdaten"

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

try:
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    data = sheet.get_all_values()
    print(f"✅ Verbindung erfolgreich. {len(data)} Zeilen gelesen.")

except Exception as e:
    print("❌ Fehler beim Zugriff auf Google Sheets:", e)
