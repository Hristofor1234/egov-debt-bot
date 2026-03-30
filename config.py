from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
INCOMING_DIR = DATA_DIR / "incoming"
OUTPUT_DIR = DATA_DIR / "output"
DB_PATH = DATA_DIR / "bot.db"

for path in (DATA_DIR, INCOMING_DIR, OUTPUT_DIR):
    path.mkdir(parents=True, exist_ok=True)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"