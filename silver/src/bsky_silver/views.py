"""Current-state views over the silver tables, as portable SQL.

The views are SELECTs in `silver/sql/`, not catalog views: Glue/Athena views
can't be read by other engines, while these run on Sail (Spark SQL) and DuckDB
alike. A reader creates them as temporary views in its own session:

    for statement in statements("glue.bsky_silver"):
        spark.sql(statement)          # or duckdb_connection.execute(statement)

`python -m bsky_silver.views glue.bsky_silver` prints the statements, e.g. to
pipe into the DuckDB CLI. The files ship with the repo, not the package.
"""

from __future__ import annotations

import sys
from pathlib import Path

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"

# Each view and the views it reads, in an order that creates dependencies first.
VIEWS: dict[str, list[str]] = {
    "posts_current": [],
    "post_edges_current": [],
    "post_classifications_current": ["posts_current"],
    "post_embeddings_current": ["posts_current"],
}

# The active spec versions (see docs/plans/data-pipeline.md, "Versioning").
CLASSIFICATION_SPEC = "v1"
EMBEDDING_SPEC = "v1"


def statements(
    namespace: str,
    names: list[str] | None = None,
    classification_spec: str = CLASSIFICATION_SPEC,
    embedding_spec: str = EMBEDDING_SPEC,
    sql_dir: Path = SQL_DIR,
) -> list[str]:
    """CREATE OR REPLACE TEMPORARY VIEW statements for `names` (default: all) and
    the views they read. A view over a table that doesn't exist yet (the support
    tables before C2/E2) fails to create, so leave it out until it does."""
    wanted = set(names or VIEWS)
    for name in list(wanted):
        wanted.update(VIEWS[name])
    params = {"silver": namespace, "classification_spec": classification_spec, "embedding_spec": embedding_spec}
    return [
        f"CREATE OR REPLACE TEMPORARY VIEW {name} AS\n" + (sql_dir / f"{name}.sql").read_text().format(**params)
        for name in VIEWS
        if name in wanted
    ]


if __name__ == "__main__":
    print("\n".join(s.rstrip() + ";\n" for s in statements(sys.argv[1], sys.argv[2:] or None)))
