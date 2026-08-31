import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, trim, when
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from nasa_pipeline.config import QUARANTINE_DIR, STAGING_DIR

logger = logging.getLogger(__name__)

STAGING_SCHEMA = StructType(
    [
        StructField("sol", StringType(), True),
        StructField("season", StringType(), True),
        StructField("first_utc", StringType(), True),
        StructField("last_utc", StringType(), True),
        StructField("at_average_temp", DoubleType(), True),
        StructField("at_count", DoubleType(), True),
        StructField("at_min_temp", DoubleType(), True),
        StructField("at_max_temp", DoubleType(), True),
        StructField("hws_average_wind_speed", DoubleType(), True),
        StructField("hws_count", DoubleType(), True),
        StructField("hws_min_wind_speed", DoubleType(), True),
        StructField("hws_max_wind_speed", DoubleType(), True),
        StructField("pre_average_pressure", DoubleType(), True),
        StructField("pre_count", DoubleType(), True),
        StructField("pre_min_pressure", DoubleType(), True),
        StructField("pre_max_pressure", DoubleType(), True),
    ]
)


def _as_float(value) -> float | None:
    return None if value is None else float(value)


def flatten_insight_json(raw_json: dict) -> list[dict]:
    sol_keys = raw_json.get("sol_keys", [])
    rows = []
    for sol in sol_keys:
        sol_data = raw_json.get(sol, {}) or {}
        at = sol_data.get("AT", {}) or {}
        hws = sol_data.get("HWS", {}) or {}
        pre = sol_data.get("PRE", {}) or {}
        rows.append(
            {
                "sol": sol,
                "season": sol_data.get("Season"),
                "first_utc": sol_data.get("First_UTC"),
                "last_utc": sol_data.get("Last_UTC"),
                "at_average_temp": _as_float(at.get("av")),
                "at_count": _as_float(at.get("ct")),
                "at_min_temp": _as_float(at.get("mn")),
                "at_max_temp": _as_float(at.get("mx")),
                "hws_average_wind_speed": _as_float(hws.get("av")),
                "hws_count": _as_float(hws.get("ct")),
                "hws_min_wind_speed": _as_float(hws.get("mn")),
                "hws_max_wind_speed": _as_float(hws.get("mx")),
                "pre_average_pressure": _as_float(pre.get("av")),
                "pre_count": _as_float(pre.get("ct")),
                "pre_min_pressure": _as_float(pre.get("mn")),
                "pre_max_pressure": _as_float(pre.get("mx")),
            }
        )
    return rows


def stage(spark: SparkSession, rows: list[dict], staging_dir=STAGING_DIR) -> DataFrame:
    df = spark.createDataFrame(rows, schema=STAGING_SCHEMA)
    cleaned = df.withColumn("sol", trim(col("sol"))).dropDuplicates(["sol"])
    cleaned.write.mode("overwrite").option("header", True).csv(str(staging_dir))
    logger.info("Staged %s rows to %s", cleaned.count(), staging_dir)
    return cleaned


def read_staging(spark: SparkSession, staging_dir=STAGING_DIR) -> DataFrame:
    """Read the current staging CSV zone back into a DataFrame (used when a
    stage is run standalone instead of being fed an in-memory DataFrame)."""
    return (
        spark.read.format("csv")
        .option("header", True)
        .schema(STAGING_SCHEMA)
        .load(str(staging_dir))
    )


def validate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    reject_reason = (
        when(col("sol").isNull() | (trim(col("sol")) == ""), lit("missing sol id"))
        .when(col("at_average_temp").isNull(), lit("missing AT sensor reading"))
        .when(col("hws_average_wind_speed").isNull(), lit("missing HWS sensor reading"))
        .when(col("pre_average_pressure").isNull(), lit("missing PRE sensor reading"))
        .otherwise(lit(None))
    )
    tagged = df.withColumn("reject_reason", reject_reason)
    quarantine_df = tagged.filter(col("reject_reason").isNotNull())
    processed_df = tagged.filter(col("reject_reason").isNull()).drop("reject_reason")
    return processed_df, quarantine_df


def log_validation_metrics(processed_df: DataFrame, quarantine_df: DataFrame) -> dict:
    """Log data quality metrics and return them for orchestration decisions."""
    processed_count = processed_df.count()
    quarantine_count = quarantine_df.count()
    total_count = processed_count + quarantine_count
    acceptance_rate = (processed_count / total_count) if total_count else 0.0

    reason_rows = quarantine_df.groupBy("reject_reason").count().collect()
    reason_breakdown = {row["reject_reason"]: row["count"] for row in reason_rows}

    logger.info(
        "Validation summary total=%s processed=%s quarantined=%s acceptance_rate=%.2f",
        total_count,
        processed_count,
        quarantine_count,
        acceptance_rate,
    )
    if reason_breakdown:
        logger.info("Quarantine reason breakdown: %s", reason_breakdown)

    return {
        "total_count": total_count,
        "processed_count": processed_count,
        "quarantine_count": quarantine_count,
        "acceptance_rate": acceptance_rate,
        "reason_breakdown": reason_breakdown,
    }


def write_quarantine(df: DataFrame, quarantine_dir=QUARANTINE_DIR) -> None:
    df.write.mode("overwrite").option("header", True).csv(str(quarantine_dir))
    logger.info("Wrote %s quarantined rows to %s", df.count(), quarantine_dir)
