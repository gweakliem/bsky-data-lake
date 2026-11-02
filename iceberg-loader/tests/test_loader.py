import datetime as dt

from conftest import utc, write_raw_file


def rows(loader):
    return loader.spark.table(loader.table).orderBy("received_at").collect()


def test_appends_with_utc_day_and_hour(loader, tmp_path):
    uri = write_raw_file(
        tmp_path / "src/f1.parquet",
        [utc(2026, 10, 22, 23, 59, 59), utc(2026, 10, 23, 0, 0, 1)],
    )

    assert loader.load([uri]) == [uri]

    result = rows(loader)
    assert [(r.day, r.hour) for r in result] == [
        (dt.date(2026, 10, 22), 23),
        (dt.date(2026, 10, 23), 0),
    ]
    assert {r.source_file for r in result} == {uri}
    assert all(r.ingested_at is not None for r in result)
    assert result[0].payload == '{"n":0}'


def test_partitions_by_day_and_hour(loader, tmp_path):
    uri = write_raw_file(tmp_path / "src/f1.parquet", [utc(2026, 10, 22, 6, 30)])
    loader.load([uri])

    data_dirs = {p.parent.relative_to(tmp_path / "table/data").as_posix() for p in (tmp_path / "table/data").rglob("*.parquet")}
    assert data_dirs == {"day=2026-10-22/hour=6"}


def test_multiple_files_in_one_commit(loader, tmp_path):
    a = write_raw_file(tmp_path / "src/a.parquet", [utc(2026, 10, 22, 6, 0)])
    b = write_raw_file(tmp_path / "src/b.parquet", [utc(2026, 10, 22, 7, 0), utc(2026, 10, 22, 7, 1)])

    assert loader.load([a, b]) == [a, b]
    assert [r.source_file for r in rows(loader)] == [a, b, b]
    snapshots = list((tmp_path / "table/metadata").glob("snap-*.avro"))
    assert len(snapshots) == 1


def test_one_data_file_per_hour_per_commit(loader, tmp_path):
    # Five source files, the middle one spanning the hour boundary.
    uris = [
        write_raw_file(tmp_path / f"src/{i}.parquet", times)
        for i, times in enumerate(
            [
                [utc(2026, 10, 22, 6, 50)],
                [utc(2026, 10, 22, 6, 55)],
                [utc(2026, 10, 22, 6, 59), utc(2026, 10, 22, 7, 0, 1)],
                [utc(2026, 10, 22, 7, 5)],
                [utc(2026, 10, 22, 7, 10)],
            ]
        )
    ]
    loader.load(uris)

    data = tmp_path / "table/data"
    per_hour = {}
    for p in data.rglob("*.parquet"):
        hour = p.parent.relative_to(data).as_posix()
        per_hour[hour] = per_hour.get(hour, 0) + 1
    assert per_hour == {"day=2026-10-22/hour=6": 1, "day=2026-10-22/hour=7": 1}
    assert len(rows(loader)) == 6


def test_skips_files_already_loaded(loader, tmp_path):
    a = write_raw_file(tmp_path / "src/a.parquet", [utc(2026, 10, 22, 6, 0)])
    b = write_raw_file(tmp_path / "src/b.parquet", [utc(2026, 10, 22, 6, 5)])

    loader.load([a])
    assert loader.load([a, b]) == [b]
    assert loader.load([a, b]) == []
    assert [r.source_file for r in rows(loader)] == [a, b]


def test_empty_load_is_noop(loader):
    assert loader.load([]) == []
    assert rows(loader) == []


def test_ensure_table_is_idempotent(loader, tmp_path):
    uri = write_raw_file(tmp_path / "src/a.parquet", [utc(2026, 10, 22, 6, 0)])
    loader.load([uri])
    loader.ensure_table()
    assert len(rows(loader)) == 1
