"""Every report query runs against the fixture and returns what its page reads."""

import datetime as dt

import pytest

import fixture
import lake

COLUMNS = {
    "freshness": ["bronze_ingested_at", "silver_bronze_through", "silver_finished_at", "built_at"],
    "runs": ["finished_at", "bronze_rows", "lag_minutes", "seconds"],
    "appended": ["hour", "table", "rows"],
    "rejects": ["day", "reason", "rows"],
    "activity": ["day", "kind", "rows"],
    "languages": ["day", "lang", "posts"],
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


def test_activity_counts_current_posts_over_the_recent_window():
    rows = lake.query("activity").to_pylist()
    posts = sum(r["rows"] for r in rows if r["kind"] == "posts")
    days = {r["day"] for r in rows}
    assert len(days) == lake.RECENT_DAYS
    # fixture.POSTS_PER_DAY a day; edits collapse to one row; deletes in the window drop out.
    deleted_in_window = sum(1 for i in fixture.DELETED if i % fixture.POST_DAYS < lake.RECENT_DAYS)
    assert posts == fixture.POSTS_PER_DAY * lake.RECENT_DAYS - deleted_in_window
    assert sum(r["rows"] for r in rows if r["kind"] == "likes") == 1000 * lake.RECENT_DAYS


def test_languages_by_primary_subtag_with_none():
    langs = set(lake.query("languages").column("lang").to_pylist())
    assert {"en", "ja", "es", "de", "pt", "(none)"} <= langs
    assert all(lang == lang.lower() and "-" not in lang for lang in langs)
