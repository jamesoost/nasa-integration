import logging
import sqlite3
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, StringType
from nasa_pipeline.config import PROCESSED_DB_PATH, PROCESSED_TABLE

logger = logging.getLogger(__name__)

_SQLITE_TYPE_MAP = {StringType: "TEXT", DoubleType: "REAL"}
_UPSERT_BATCH_SIZE = 100


def write_processed(
    df: DataFrame, db_path: Path = PROCESSED_DB_PATH, table_name: str = PROCESSED_TABLE
) -> None:
    """Upsert validated records into SQLite, keyed by sol."""
    fields = df.schema.fields
    col_names = [f.name for f in fields]
    col_defs = ", ".join(
        f'"{f.name}" {_SQLITE_TYPE_MAP.get(type(f.dataType), "TEXT")}'
        + (" PRIMARY KEY" if f.name == "sol" else "")
        for f in fields
    )
    quoted_col_names = ", ".join(f'"{c}"' for c in col_names)
    placeholders = ", ".join(["?"] * len(fields))
    update_clause = ", ".join(
        f'"{c}" = excluded."{c}"' for c in col_names if c != "sol"
    )
    rows = [tuple(row) for row in df.collect()]

    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')
        if not rows:
            logger.info("No processed rows to upsert for table=%s", table_name)
            return

        upsert_sql = (
            f'INSERT INTO "{table_name}" ({quoted_col_names}) VALUES ({placeholders}) '
            f'ON CONFLICT("sol") DO UPDATE SET {update_clause}'
        )
        for idx in range(0, len(rows), _UPSERT_BATCH_SIZE):
            conn.executemany(upsert_sql, rows[idx : idx + _UPSERT_BATCH_SIZE])
        conn.commit()
    logger.info("Upserted %s processed rows into %s (table=%s)", len(rows), db_path, table_name)
