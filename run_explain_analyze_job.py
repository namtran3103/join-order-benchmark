#!/usr/bin/env python3
"""
Run EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) for each JOB query in this repo and save the JSON.

- Plan tree: Node Type, Relation Name, Actual Rows, Actual Total Time, Plans (children), etc.
- Actual row counts and timings per node (for T3 cardinalities / pipeline times).
- COSTS, VERBOSE, BUFFERS for costs, verbose output, and I/O stats.

Queries: *.sql in this directory, excluding schema/fkindexes/load/analyze scripts.
Output: pg_explain_job/<name>.json (e.g. 1a.sql -> pg_explain_job/1a.json)
Database: imdbload (default)

Usage:
  pip install -r requirements.txt
  python run_explain_analyze_job.py [options]
"""

import argparse
import json
import re
import sys
from pathlib import Path

try:
    import psycopg2
except ImportError:
    print("Install psycopg2: pip install -r requirements.txt", file=sys.stderr)
    sys.exit(1)

# Setup/schema scripts in this repo - not JOB queries.
EXCLUDE_SQL = {"schema.sql", "fkindexes.sql", "load_imdb.sql", "analyze_table.sql"}


def job_query_order(path: Path) -> tuple:
    """Sort JOB query files: 1a, 1b, ..., 1d, 2a, ..., 33c."""
    name = path.stem
    m = re.match(r"^(\d+)([a-z])$", name, re.I)
    if m:
        return (int(m.group(1)), m.group(2).lower())
    return (9999, name)


def main():
    repo_root = Path(__file__).resolve().parent
    default_output = repo_root / "pg_explain_job"

    parser = argparse.ArgumentParser(
        description="Run EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) for each JOB query and save JSON."
    )
    parser.add_argument(
        "--queries-dir",
        type=Path,
        default=repo_root,
        help="Directory containing JOB .sql files (default: this repo)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_output,
        help="Directory to write JSON files (default: pg_explain_job)",
    )
    parser.add_argument(
        "--dbname",
        default="imdbload",
        help="PostgreSQL database name (default: imdbload)",
    )
    parser.add_argument(
        "--user",
        default=None,
        help="PostgreSQL user (default: current OS user)",
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="PostgreSQL host (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5432,
        help="PostgreSQL port (default: 5432)",
    )
    parser.add_argument(
        "--password",
        default=None,
        help="PostgreSQL password (or set PGPASSWORD in the environment)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only list queries that would be run, do not connect or write",
    )
    args = parser.parse_args()

    queries_dir = args.queries_dir.resolve()
    if not queries_dir.is_dir():
        print(f"Queries directory not found: {queries_dir}", file=sys.stderr)
        sys.exit(1)

    # JOB query files: *.sql in repo, excluding setup scripts.
    all_sql = list(queries_dir.glob("*.sql"))
    sql_files = [p for p in all_sql if p.name not in EXCLUDE_SQL]
    sql_files.sort(key=job_query_order)

    if not sql_files:
        print(f"No JOB .sql files in {queries_dir} (excluding {EXCLUDE_SQL})", file=sys.stderr)
        sys.exit(1)

    args.output_dir = args.output_dir.resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        print(f"Would run EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) for {len(sql_files)} JOB queries.")
        print(f"Database: {args.dbname}")
        for p in sql_files:
            print(f"  {p.name} -> {args.output_dir / p.stem}.json")
        return

    conn = psycopg2.connect(
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port,
    )
    conn.set_session(autocommit=True)

    for sql_path in sql_files:
        stem = sql_path.stem
        out_path = args.output_dir / f"{stem}.json"
        query_text = sql_path.read_text().strip()
        if not query_text:
            print(f"Skip (empty): {sql_path.name}")
            continue
        explain_sql = f"EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) {query_text}"
        try:
            with conn.cursor() as cur:
                cur.execute(explain_sql)
                row = cur.fetchone()
            if row is None:
                print(f"No result: {sql_path.name}")
                continue
            plan_json = row[0]
            if isinstance(plan_json, str):
                plan_json = json.loads(plan_json)
            out_path.write_text(json.dumps(plan_json, indent=2), encoding="utf-8")
            print(f"Wrote: {out_path.relative_to(repo_root)}")
        except Exception as e:
            print(f"Error {sql_path.name}: {e}", file=sys.stderr)

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
