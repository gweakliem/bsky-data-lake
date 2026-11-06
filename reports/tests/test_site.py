"""Checks on the built site (npm test builds it from the fixture first)."""

import csv
from pathlib import Path

import pytest

DIST = Path(__file__).resolve().parents[1] / "dist"
# Everything in a data file ships to the browser, so it must stay aggregated.
MAX_ROWS = 5000


@pytest.fixture(scope="module")
def data_files():
    files = sorted((DIST / "_file" / "data").glob("*"))
    if not files:
        pytest.skip("no built site: run `npm test` (or `npm run build:fixture`) first")
    return files


def test_pages_built():
    if not DIST.exists():
        pytest.skip("no built site")
    assert (DIST / "index.html").exists()


def test_data_files_stay_small(data_files):
    for path in data_files:
        if path.suffix == ".csv":
            with path.open() as f:
                assert sum(1 for _ in csv.reader(f)) <= MAX_ROWS + 1, path.name
        assert path.stat().st_size < 1_000_000, path.name
