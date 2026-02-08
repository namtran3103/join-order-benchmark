#!/usr/bin/env python3
"""
Extended plan collection for JOB: replace selectivity heuristics with ground-truth side-queries.

Follows the same logic as run_explain_analyze_tpch_extended.py:

- Base cardinality: SELECT count(*) FROM <relation> for every Seq Scan / Index Scan → actual_scan_in_card.
- Filter funnel: split Filter into parts, run cumulative WHERE probes, store actual_funnel_counts
  and component selectivity (step_n_count / step_n-1_count).
- Exact byte widths: query pg_catalog for each Output column → "ius" section (replacing Plan Width).
- Zero-result pipelines: if any side-query returns 0 → T3_empty_output: true.
- Schema/aliasing: resolve Relation Name and Schema; use FROM schema.relation AS alias so filter
  strings work; strip or map alias in filter when needed.

Queries: *.sql in repo (JOB), excluding schema/fkindexes/load/analyze scripts.
Output: pg_explain_job/extended/<name>.json (e.g. 1a.sql -> pg_explain_job/extended/1a.json)
Database: imdbload (default)

Usage:
  pip install -r requirements.txt
  python run_explain_analyze_job_extended.py [options]
"""

# Minimum bytes per column for T3: avoid 0 so tuple size reflects register/cache-line usage.
MIN_COLUMN_BYTES = 8

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Optional

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:
    psycopg2 = None

# Setup/schema scripts in this repo - not JOB queries.
EXCLUDE_SQL = {"schema.sql", "fkindexes.sql", "load_imdb.sql", "analyze_table.sql"}


def job_query_order(path: Path) -> tuple:
    """Sort JOB query files: 1a, 1b, ..., 1d, 2a, ..., 33c."""
    name = path.stem
    m = re.match(r"^(\d+)([a-z])$", name, re.I)
    if m:
        return (int(m.group(1)), m.group(2).lower())
    return (9999, name)


def split_filter_and_parts(filter_str: str) -> list:
    """
    Split a Filter string into top-level AND parts (respecting parentheses).
    Returns list of predicate strings, e.g. ["(a > 0)", "(b = 1)"].
    """
    if not filter_str or not filter_str.strip():
        return []
    s = filter_str.strip()
    # Strip balanced outer parentheses so "( (a) AND (b) )" becomes "(a) AND (b)"
    while s.startswith("(") and s.endswith(")"):
        inner = s[1:-1].strip()
        depth = 0
        for c in inner:
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            if depth < 0:
                break
        if depth != 0:
            break
        s = inner
    parts = []
    depth = 0
    start = 0
    i = 0
    n = len(s)
    while i < n:
        if i + 5 <= n and depth == 0 and s[i : i + 5] == " AND ":
            part = s[start:i].strip()
            if part:
                parts.append(part)
            start = i + 5
            i += 5
            continue
        if s[i] == "(":
            depth += 1
            i += 1
            continue
        if s[i] == ")":
            depth -= 1
            i += 1
            continue
        i += 1
    part = s[start:].strip()
    if part:
        parts.append(part)
    return parts


def classify_filter_part_to_t3_feature(part: str) -> str:
    """
    Classify a filter component string into a T3 feature name for selectivity.
    Check in order: BETWEEN, LIKE, then comparison operators.
    Returns key suffix for selectivity, e.g. 'between_selectivity', 'like_selectivity', 'compare_selectivity'.
    """
    u = part.upper()
    if "BETWEEN" in u:
        return "between_selectivity"
    if "LIKE" in u:
        return "like_selectivity"
    # Comparison operators (with space or as token to avoid matching inside identifiers)
    if any(
        op in part
        for op in (" > ", " < ", " = ", ">=", "<=", "<>", "!=", " IS NOT ", " IS ")
    ):
        return "compare_selectivity"
    return "expression_selectivity"


def run_base_count(cur, schema: str, relation: str) -> int:
    """Execute SELECT count(*) FROM schema.relation; return row count."""
    quoted_schema = sql.Identifier(schema)
    quoted_rel = sql.Identifier(relation)
    q = sql.SQL("SELECT count(*) FROM {}.{}").format(quoted_schema, quoted_rel)
    cur.execute(q)
    return int(cur.fetchone()[0])


def run_filter_funnel_probes(
    cur, schema: str, relation: str, alias: str, filter_parts: list[str], base_count: int
) -> tuple[list[int], dict[str, float], bool]:
    """
    Run cumulative WHERE probes: WHERE part1, WHERE part1 AND part2, ...
    Classify each component by T3 feature (BETWEEN, LIKE, comparison) before running the count.
    Returns (list of counts per step, dict of T3 feature name -> selectivity, has_zero).
    Keys: between_selectivity, like_selectivity, compare_selectivity, expression_selectivity;
    duplicate keys get _2, _3 suffix.
    """
    quoted_schema = sql.Identifier(schema)
    quoted_rel = sql.Identifier(relation)
    from_clause = sql.SQL("FROM {}.{} AS {}").format(
        quoted_schema, quoted_rel, sql.Identifier(alias)
    )
    counts = [base_count]
    selectivity_by_feature = {}
    key_use_count = {}
    has_zero = base_count == 0
    prev_count = base_count
    cumulative_where = None
    for part in filter_parts:
        if cumulative_where is None:
            where_expr = part
        else:
            where_expr = f"({cumulative_where}) AND ({part})"
        # Build query by concatenation so filter text (may contain {}) is not interpreted
        q = sql.SQL("SELECT count(*) ") + from_clause + sql.SQL(" WHERE ") + sql.SQL(where_expr)
        try:
            cur.execute(q)
            c = int(cur.fetchone()[0])
        except Exception:
            c = -1
        counts.append(c)
        if c == 0:
            has_zero = True
        if prev_count > 0 and c >= 0:
            sel = c / prev_count
        else:
            sel = 0.0 if (c == 0 or prev_count == 0) else -1.0
        prev_count = c
        cumulative_where = where_expr
        # Assign selectivity to T3 feature key (unique with _2, _3 if duplicate)
        base_key = classify_filter_part_to_t3_feature(part)
        n = key_use_count.get(base_key, 0) + 1
        key_use_count[base_key] = n
        key = base_key if n == 1 else f"{base_key}_{n}"
        selectivity_by_feature[key] = sel
    return counts, selectivity_by_feature, has_zero


def get_relation_column_byte_sizes(cur, schema: str, relation: str) -> dict[str, int]:
    """
    Query pg_catalog for each column of the relation; return dict attname -> byte size.
    Variable-length (attlen = -1) and any 0 result use MIN_COLUMN_BYTES so T3 sees nonzero tuple size.
    """
    cur.execute(
        """
        SELECT a.attname,
               CASE WHEN a.attlen > 0 THEN a.attlen
                    WHEN a.attlen = -1 THEN 0
                    ELSE a.attlen END AS len
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = %s AND c.relname = %s
          AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (schema, relation),
    )
    return {
        row[0]: max(row[1], MIN_COLUMN_BYTES) if row[1] is not None else MIN_COLUMN_BYTES
        for row in cur.fetchall()
    }


def output_to_column_name(output_item: str) -> Optional[str]:
    """
    Extract base column name from Output entry for lookups.
    E.g. 'customer_1.c_custkey' -> 'c_custkey', 'l_returnflag' -> 'l_returnflag'.
    """
    s = output_item.strip()
    if s.startswith("(") or "(" in s:
        return None
    if "." in s:
        return s.split(".", 1)[1]
    return s


def build_ius_for_node(
    cur,
    node: dict,
    schema: Optional[str],
    relation: Optional[str],
    col_sizes: Optional[dict[str, int]],
) -> list:
    """
    Build "ius" list for a node: one entry per Output with name and bytes.
    For scan nodes we use col_sizes from the relation; otherwise fallback to Plan Width / n.
    """
    output = node.get("Output") or []
    if not output:
        return []
    plan_width = node.get("Plan Width") or 0
    fallback_bytes = (plan_width // len(output)) if output else 0
    ius = []
    for out in output:
        col = output_to_column_name(out)
        if col_sizes is not None and col is not None and col in col_sizes:
            size = col_sizes[col]
        else:
            size = fallback_bytes
        ius.append({"name": out, "bytes": max(size, MIN_COLUMN_BYTES)})
    return ius


def filter_contains_subplan(filter_str: str) -> bool:
    """Return True if filter references InitPlan or SubPlan (cannot run standalone probe)."""
    if not filter_str:
        return False
    return "InitPlan" in filter_str or "SubPlan" in filter_str


def enrich_plan_node(
    cur,
    node: dict,
    schema: str,
    relation: Optional[str],
    alias: Optional[str],
    parent_schema: Optional[str],
    parent_relation: Optional[str],
) -> None:
    """
    Mutate node in place: add actual_scan_in_card, actual_funnel_counts, component_selectivity,
    ius, T3_empty_output. Resolve schema/relation from node or parent (for non-scan nodes).
    """
    node_type = node.get("Node Type", "")
    rel_name = node.get("Relation Name") or parent_relation
    rel_schema = node.get("Schema") or parent_schema or "public"
    rel_alias = node.get("Alias") or alias or (rel_name if rel_name else None)

    # Base cardinality for Seq Scan and Index Scan
    base_count = None
    if node_type in ("Seq Scan", "Index Scan", "Index Only Scan") and rel_name:
        try:
            base_count = run_base_count(cur, rel_schema, rel_name)
            node["actual_scan_in_card"] = base_count
            if base_count == 0:
                node["T3_empty_output"] = True
        except Exception as e:
            node["actual_scan_in_card"] = -1
            node["_scan_in_card_error"] = str(e)

        # Filter funnel (cumulative WHERE probes; first step selectivity = count(P1)/base_count)
        filter_str = node.get("Filter")
        if filter_str and not filter_contains_subplan(filter_str) and base_count is not None:
            parts = split_filter_and_parts(filter_str)
            if parts:
                try:
                    counts, selectivity_by_feature, has_zero = run_filter_funnel_probes(
                        cur, rel_schema, rel_name, rel_alias or rel_name, parts, base_count
                    )
                    node["actual_funnel_counts"] = counts
                    node["component_selectivity"] = selectivity_by_feature
                    if has_zero:
                        node["T3_empty_output"] = True
                except Exception as e:
                    node["_funnel_error"] = str(e)

        # IUs: exact byte widths for Output columns from this relation
        try:
            col_sizes = get_relation_column_byte_sizes(cur, rel_schema, rel_name)
            node["ius"] = build_ius_for_node(cur, node, rel_schema, rel_name, col_sizes)
        except Exception as e:
            node["ius"] = build_ius_for_node(cur, node, None, None, None)
            node["_ius_error"] = str(e)
    else:
        # Non-scan node: optional ius from Plan Width fallback
        node["ius"] = build_ius_for_node(cur, node, None, None, None)

    # Recurse into Plans
    for child in node.get("Plans") or []:
        enrich_plan_node(
            cur,
            child,
            rel_schema,
            rel_name,
            rel_alias,
            rel_schema,
            rel_name,
        )


def enrich_plan_tree(cur, plan_root: dict) -> None:
    """Entry point: enrich the top-level Plan dict (and its Schema/Relation if present)."""
    schema = plan_root.get("Schema") or "public"
    relation = plan_root.get("Relation Name")
    alias = plan_root.get("Alias")
    enrich_plan_node(cur, plan_root, schema, relation, alias, None, None)


def main():
    # Script lives in pg_explain_job/extended/; repo root is parent of pg_explain_job
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent.parent
    default_queries_dir = repo_root
    default_output = script_dir  # pg_explain_job/extended

    parser = argparse.ArgumentParser(
        description="Run EXPLAIN (ANALYZE, ...) for JOB queries and enrich plans with ground-truth side-queries (cardinality, funnel, ius)."
    )
    parser.add_argument(
        "--queries-dir",
        type=Path,
        default=default_queries_dir,
        help="Directory containing JOB .sql files (default: repo root)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_output,
        help="Directory to write JSON files (default: pg_explain_job/extended)",
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
        print(f"Would run extended EXPLAIN for {len(sql_files)} JOB queries -> {args.output_dir}")
        print(f"Database: {args.dbname}")
        for p in sql_files:
            print(f"  {p.name} -> {args.output_dir / p.stem}.json")
        return

    if psycopg2 is None:
        print("Install psycopg2: pip install -r requirements.txt", file=sys.stderr)
        sys.exit(1)

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

            # EXPLAIN returns a single-element list with { "Plan": {...}, ... }
            if isinstance(plan_json, list) and len(plan_json) > 0:
                first = plan_json[0]
                plan_root = first.get("Plan")
                if plan_root:
                    with conn.cursor() as cur:
                        enrich_plan_tree(cur, plan_root)

            out_path.write_text(json.dumps(plan_json, indent=2), encoding="utf-8")
            print(f"Wrote: {out_path.relative_to(repo_root)}")
        except Exception as e:
            print(f"Error {sql_path.name}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
