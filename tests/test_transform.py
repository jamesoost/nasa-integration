from nasa_pipeline.transform import flatten_insight_json, stage, validate

SAMPLE_RAW_JSON = {
    "sol_keys": ["100", "101"],
    "100": {
        "AT": {"av": -62.0, "ct": 100, "mn": -90.0, "mx": -30.0},
        "HWS": {"av": 5.0, "ct": 100, "mn": 0.1, "mx": 12.0},
        "PRE": {"av": 750.0, "ct": 100, "mn": 700.0, "mx": 800.0},
        "First_UTC": "2019-04-13T00:00:00Z",
        "Last_UTC": "2019-04-14T00:00:00Z",
        "Season": "fall",
    },
    "101": {
        # missing PRE value entirely -> should be quarantined
        "AT": {"av": -60.0, "ct": 90, "mn": -88.0, "mx": -28.0},
        "HWS": {"av": 4.5, "ct": 90, "mn": 0.2, "mx": 11.0},
        "PRE": {},
        "First_UTC": "2019-04-14T00:00:00Z",
        "Last_UTC": "2019-04-15T00:00:00Z",
        "Season": "fall",
    },
}


def test_flatten_insight_json_produces_one_row_per_sol():
    rows = flatten_insight_json(SAMPLE_RAW_JSON)
    assert len(rows) == 2
    assert rows[0]["sol"] == "100"
    assert rows[0]["at_average_temp"] == -62.0


def test_validate_splits_valid_and_invalid_rows(spark, tmp_path):
    rows = flatten_insight_json(SAMPLE_RAW_JSON)
    staging_df = stage(spark, rows, staging_dir=tmp_path / "staging")
    processed_df, quarantine_df = validate(staging_df)

    assert processed_df.count() == 1
    assert processed_df.collect()[0]["sol"] == "100"

    assert quarantine_df.count() == 1
    quarantined_row = quarantine_df.collect()[0]
    assert quarantined_row["sol"] == "101"
    assert quarantined_row["reject_reason"] == "missing PRE sensor reading"
