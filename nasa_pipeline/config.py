import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
QUARANTINE_DIR = DATA_DIR / "quarantine"
PROCESSED_DIR = DATA_DIR / "processed"
LOGS_DIR = PROJECT_ROOT / "logs"

for directory in (RAW_DIR, STAGING_DIR, QUARANTINE_DIR, PROCESSED_DIR, LOGS_DIR):
    directory.mkdir(parents=True, exist_ok=True)

PROCESSED_DB_PATH = PROCESSED_DIR / "weather.db"
PROCESSED_TABLE = "processed_weather"

NASA_INSIGHT_WEATHER_URL = "https://api.nasa.gov/insight_weather/"
NASA_API_KEY = os.getenv("API-Key")

REQUEST_PARAMS = {
    "api_key": NASA_API_KEY,
    "feedtype": "json",
    "ver": "1.0",
}
