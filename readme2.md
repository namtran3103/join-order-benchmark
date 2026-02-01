# JOB: EXPLAIN ANALYZE and usage

## Summary

- **Script:** `run_explain_analyze_job.py` runs `EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON)` for each JOB query in this repo and saves the JSON.
- **Queries:** All `*.sql` in this directory except `schema.sql`, `fkindexes.sql`, `load_imdb.sql`, `analyze_table.sql` (JOB queries 1a–33c).
- **Output:** One JSON per query in `pg_explain_job/<name>.json` (e.g. `1a.sql` → `pg_explain_job/1a.json`).
- **Database:** PostgreSQL database `imdbload` (default).
- **Plan data:** Each JSON contains the plan tree (Node Type, Relation Name, Actual Rows, Actual Total Time, Plans, etc.) for use with T3 or other tools.

## Usage

```bash
cd join-order-benchmark
pip install -r requirements.txt
python run_explain_analyze_job.py
```

With a different PostgreSQL user or password:

```bash
python run_explain_analyze_job.py --user namtran --password YOUR_PASSWORD
```

Or set `PGPASSWORD` in the environment (psycopg2 uses it automatically):

```bash
export PGPASSWORD=YOUR_PASSWORD
python run_explain_analyze_job.py --user namtran
```

Dry run (list queries and output paths only):

```bash
python run_explain_analyze_job.py --dry-run
```

Optional arguments: `--queries-dir`, `--output-dir`, `--dbname`, `--user`, `--password`, `--host`, `--port`.
