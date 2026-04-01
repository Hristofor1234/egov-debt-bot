from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
INCOMING_DIR = DATA_DIR / "incoming"
OUTPUT_DIR = DATA_DIR / "output"
DEBUG_DIR = DATA_DIR / "debug"
DB_PATH = DATA_DIR / "bot.db"

for path in (DATA_DIR, INCOMING_DIR, OUTPUT_DIR, DEBUG_DIR):
    path.mkdir(parents=True, exist_ok=True)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
HEADLESS = os.getenv("HEADLESS", "true").strip().lower() == "true"

MIN_DELAY_SECONDS = int(os.getenv("MIN_DELAY_SECONDS", "10"))
MAX_DELAY_SECONDS = int(os.getenv("MAX_DELAY_SECONDS", "18"))

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
BATCH_PAUSE_SECONDS = int(os.getenv("BATCH_PAUSE_SECONDS", "240"))

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
MAX_CONSECUTIVE_ERRORS = int(os.getenv("MAX_CONSECUTIVE_ERRORS", "4"))

MAX_INCOMING_FILES = int(os.getenv("MAX_INCOMING_FILES", "3"))
MAX_OUTPUT_FILES = int(os.getenv("MAX_OUTPUT_FILES", "3"))