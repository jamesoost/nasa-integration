import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests

from nasa_pipeline.config import NASA_INSIGHT_WEATHER_URL, RAW_DIR, REQUEST_PARAMS

logger = logging.getLogger(__name__)


def fetch_weather_data(url: str = NASA_INSIGHT_WEATHER_URL, params: dict | None = None) -> dict:
    response = requests.get(url, params=params or REQUEST_PARAMS, timeout=30)
    response.raise_for_status()
    return response.json()


def save_raw(payload: dict, raw_dir: Path = RAW_DIR) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_path = raw_dir / f"insight_weather_{timestamp}.json"
    raw_path.write_text(json.dumps(payload, indent=2))
    logger.info("Saved raw payload to %s", raw_path)
    return raw_path


def run_extract() -> Path:
    payload = fetch_weather_data()
    return save_raw(payload)


def get_latest_raw_path(raw_dir: Path = RAW_DIR) -> Path:
    raw_files = sorted(raw_dir.glob("insight_weather_*.json"))
    if not raw_files:
        raise FileNotFoundError(f"No raw payload files found in {raw_dir}")
    return raw_files[-1]
