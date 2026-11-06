"""Every report query runs against the fixture and returns what its page reads."""

import datetime as dt

import pytest

import lake

COLUMNS = {
    "freshness": ["bronze_ingested_at", "silver_bronze_through", "silver_finished_at", "built_at"],
    "runs": ["finished_at", "bronze_rows", "lag_minutes", "seconds"],
    "appended": ["hour", "table", "rows"],
    "rejects": ["day", "reason", "rows"],
}


@pytest.fixture(autouse=True)
def fixture_lake(monkeypatch):
    monkeypatch.setenv("BSKY_REPORTS_FIXTURE", "1")


@pytest.mark.parametrize("name", sorted(COLUMNS))
def test_query_columns(name):
    assert lake.query(name).column_names == COLUMNS[name]


def test_every_sql_file_is_covered():
    assert sorted(p.stem for p in lake.SQL.glob("*.sql")) == sorted(COLUMNS)


def test_freshness_is_one_row_of_utc_iso_times():
    (row,) = lake.query("freshness").to_pylist()
    for value in row.values():
        assert dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    # The fixture's newest bronze row is 4 minutes old and silver ran this hour.
    assert row["bronze_ingested_at"] > row["silver_bronze_through"]


def test_appended_unpacks_every_table_in_counts():
    tables = set(lake.query("appended").column("table").to_pylist())
    assert {"posts", "likes", "reposts", "post_edges"} <= tables
