from nasa_pipeline.load import write_processed
from nasa_pipeline.transform import flatten_insight_json, stage

SAMPLE_RAW_JSON = {
    "sol_keys": ["100"],
    "100": {
        "AT": {"av": -62.0, "ct": 100, "mn": -90.0, "mx": -30.0},
        "HWS": {"av": 5.0, "ct": 100, "mn": 0.1, "mx": 12.0},
        "PRE": {"av": 750.0, "ct": 100, "mn": 700.0, "mx": 800.0},
        "First_UTC": "2019-04-13T00:00:00Z",
        "Last_UTC": "2019-04-14T00:00:00Z",
        "Season": "fall",
    },
}


def test_write_processed_upserts_without_duplicating_rows(spark, tmp_path):
    db_path = tmp_path / "weather.db"
    rows = flatten_insight_json(SAMPLE_RAW_JSON)
    df = stage(spark, rows, staging_dir=tmp_path / "staging")

    write_processed(df, db_path=db_path, table_name="processed_weather")
    write_processed(df, db_path=db_path, table_name="processed_weather")  # re-run, same sol

    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM processed_weather").fetchone()[0]
        at_avg = conn.execute(
            "SELECT at_average_temp FROM processed_weather WHERE sol = '100'"
        ).fetchone()[0]
    finally:
        conn.close()

    assert count == 1
    assert at_avg == -62.0
