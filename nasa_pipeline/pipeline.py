import argparse
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame

from nasa_pipeline.config import LOGS_DIR
from nasa_pipeline.extract import get_latest_raw_path, run_extract
from nasa_pipeline.load import write_processed
from nasa_pipeline.spark_session import get_spark_session
from nasa_pipeline.transform import (
    flatten_insight_json,
    log_validation_metrics,
    read_staging,
    stage,
    validate,
    write_quarantine,
)

_LOG_TIMESTAMP = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOGS_DIR / f"pipeline_{_LOG_TIMESTAMP}.log"),
    ],
)
logger = logging.getLogger(__name__)


def extract_stage() -> dict:
    raw_path = run_extract()
    return json.loads(raw_path.read_text())


def transform_stage(spark, raw_json: dict | None = None) -> DataFrame:
    if raw_json is None:
        latest_raw_path = get_latest_raw_path()
        logger.info("Transform stage reading latest raw file from %s", latest_raw_path)
        raw_json = json.loads(latest_raw_path.read_text())
    rows = flatten_insight_json(raw_json)
    staged_df = stage(spark, rows)
    processed_df, quarantine_df = validate(staged_df)
    log_validation_metrics(processed_df, quarantine_df)
    write_quarantine(quarantine_df)
    return processed_df


def load_stage(spark, processed_df: DataFrame | None = None) -> DataFrame:
    if processed_df is None:
        logger.info("Load stage running standalone: reading and validating staging snapshot")
        staged_df = read_staging(spark)
        processed_df, quarantine_df = validate(staged_df)
        log_validation_metrics(processed_df, quarantine_df)
    write_processed(processed_df)
    return processed_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run NASA InSight ETL stages")
    parser.add_argument(
        "--stage",
        choices=["all", "extract", "transform", "load"],
        default="all",
        help="Run full pipeline or a single stage",
    )
    return parser.parse_args()


def main(stage: str = "all") -> None:
    if stage == "extract":
        extract_stage()
        return

    spark = get_spark_session()
    try:
        if stage == "transform":
            transform_stage(spark)
            return

        if stage == "load":
            load_stage(spark)
            return

        raw_json = extract_stage()
        processed_df = transform_stage(spark, raw_json)
        load_stage(spark, processed_df)
    finally:
        spark.stop()


if __name__ == "__main__":
    args = parse_args()
    main(stage=args.stage)
