"""bsky-migrate: copy the lake into the member account (WP M5, phase E).

    # At the freeze (after the old silver's last run), on the laptop with the old account's credentials:
    bsky-migrate plan --dest-warehouse <new warehouse bucket> --exclude bsky_silver.post_edges_backfill > plan.json

    # In the new account's cluster (the migration role), with plan.json mounted:
    bsky-migrate tables plan.json [--only bsky_silver.posts ...]
    bsky-migrate objects plan.json
    bsky-migrate verify plan.json

The plan names the old buckets, so it stays out of the repo. Each command ends
with one `run {json}` log line. `tables` and `objects` resume where a previous
run stopped.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

from bsky_migrate import objects, tables

log = logging.getLogger("bsky_migrate")
NAMESPACES = ("bsky_raw", "bsky_silver")
BRONZE = "bsky_raw.jetstream"
BRONZE_TIME = "received_at"
DEFAULT_OBJECTS = ("lance/", "reports/inputs/")
# Bronze's partitions are hours; the rest are days or unpartitioned.


def _catalog():
    from bsky_enrich import incremental as inc

    return inc.catalog()


def build_plan(cat, dest_warehouse: str, bronze_before: str | None, object_prefixes: list[str],
               exclude: set[str]) -> dict:
    entries = []
    source_bucket = None
    for ns in NAMESPACES:
        for ident in sorted(cat.list_tables(ns)):
            name = ".".join(ident)
            if name in exclude:
                continue
            t = cat.load_table(ident)
            source_bucket = source_bucket or urlparse(t.metadata_location).netloc
            entry = {"identifier": name, "metadata_location": t.metadata_location, "before": None}
            if name == BRONZE and bronze_before:
                entry["before"] = [BRONZE_TIME, tables.utc_iso(bronze_before)]
            entries.append(entry)
    if source_bucket is None:
        raise SystemExit("no tables found")
    return {
        "dest_iceberg": f"s3://{dest_warehouse}/iceberg",
        "tables": entries,
        "objects": [{"source": f"s3://{source_bucket}/{p}", "dest": f"s3://{dest_warehouse}/{p}"}
                    for p in object_prefixes],
    }


def _io_properties() -> dict[str, str]:
    return {"client.region": os.environ.get("AWS_REGION", "us-west-2")}


def run_tables(plan: dict, only: set[str], rows_per_commit: int, cat=None) -> list[dict]:
    cat = cat or _catalog()
    out = []
    for e in plan["tables"]:
        if only and e["identifier"] not in only:
            continue
        ns, name = e["identifier"].split(".")
        source = tables.open_source(e["metadata_location"], _io_properties())
        dest = tables.ensure_destination(source, cat, e["identifier"], f"{plan['dest_iceberg']}/{ns}/{name}")
        before = tuple(e["before"]) if e.get("before") else None
        r = tables.copy_table(source, dest, before, rows_per_commit)
        log.info("table %s", json.dumps(dataclasses.asdict(r)))
        out.append(dataclasses.asdict(r))
    return out


def run_objects(plan: dict, workers: int, s3=None) -> list[dict]:
    if s3 is None:
        import boto3

        s3 = boto3.client("s3")
    out = []
    for o in plan["objects"]:
        r = objects.copy_prefix(s3, o["source"], o["dest"], workers)
        log.info("objects %s", json.dumps(dataclasses.asdict(r)))
        out.append(dataclasses.asdict(r))
    return out


def run_verify(plan: dict, cat=None) -> dict:
    cat = cat or _catalog()
    report = {}
    for e in plan["tables"]:
        source = tables.open_source(e["metadata_location"], _io_properties())
        try:
            dest = cat.load_table(e["identifier"])
        except Exception as exc:  # noqa: BLE001 - reported, not raised
            report[e["identifier"]] = {"ok": False, "error": repr(exc)}
            continue
        before = tuple(e["before"]) if e.get("before") else None
        report[e["identifier"]] = tables.compare(source, dest, before)
    return report


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(prog="bsky-migrate")
    sub = p.add_subparsers(dest="cmd", required=True)
    pl = sub.add_parser("plan", help="write the plan (old account's credentials)")
    pl.add_argument("--dest-warehouse", required=True, help="the new account's warehouse bucket")
    pl.add_argument("--bronze-before", help="optional: copy only bronze rows received before this ISO time (with a zone); "
                    "M5 copies all of bronze (migrate/CLAUDE.md)")
    pl.add_argument("--objects", nargs="*", default=list(DEFAULT_OBJECTS), help="warehouse prefixes to copy as objects")
    pl.add_argument("--exclude", nargs="*", default=[], help="tables to leave out, e.g. bsky_silver.post_edges_backfill")
    t = sub.add_parser("tables", help="copy the tables")
    t.add_argument("plan", type=Path)
    t.add_argument("--only", nargs="*", default=[])
    t.add_argument("--rows-per-commit", type=int, default=tables.ROWS_PER_COMMIT)
    o = sub.add_parser("objects", help="copy the object prefixes")
    o.add_argument("plan", type=Path)
    o.add_argument("--workers", type=int, default=16)
    v = sub.add_parser("verify", help="compare row counts per partition")
    v.add_argument("plan", type=Path)
    a = p.parse_args(argv)

    if a.cmd == "plan":
        plan = build_plan(_catalog(), a.dest_warehouse, a.bronze_before, a.objects, set(a.exclude))
        json.dump(plan, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return
    plan = json.loads(a.plan.read_text())
    if a.cmd == "tables":
        result = {"tables": run_tables(plan, set(a.only), a.rows_per_commit)}
    elif a.cmd == "objects":
        result = {"objects": run_objects(plan, a.workers)}
    else:
        report = run_verify(plan)
        result = {"verify": report, "ok": all(r["ok"] for r in report.values())}
    log.info("run %s", json.dumps({"cmd": a.cmd, **result}, default=str))
    if a.cmd == "verify" and not result["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
