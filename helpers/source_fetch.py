import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

from psycopg2.extras import RealDictCursor

from .db import table_ref
from .reports import trace

_SOURCE_STATS: Dict[str, Dict[str, int]] = {}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def parse_checkpoint(raw: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not raw:
        return (None, None)
    if "|" in raw:
        dt, row_id = raw.split("|", 1)
        return (dt or None, row_id or None)
    return (None, raw)


def format_checkpoint(dt_iso: Optional[str], row_id: str) -> str:
    return f"{dt_iso or ''}|{row_id}"


def get_checkpoint(conn, schema: str, table: str, platform: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT "lastSourceId" FROM {table_ref(schema, table)} WHERE platform=%s',
            (platform,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def set_checkpoint(conn, schema: str, table: str, platform: str, value: str, dry_run: bool) -> None:
    if dry_run:
        return
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {table_ref(schema, table)} (platform, "lastSourceId", "updatedAt")
            VALUES (%s, %s, now())
            ON CONFLICT (platform) DO UPDATE SET
              "lastSourceId" = EXCLUDED."lastSourceId",
              "updatedAt" = now()
            """,
            (platform, value),
        )


def print_source_query(cur, label: str, query: str, params: Iterable[Any]) -> None:
    if not _env_bool("MIGRATION_LOG_SOURCE_SQL", False):
        return
    params_list = list(params)
    try:
        exact = cur.mogrify(query, params_list).decode("utf-8")
    except Exception as exc:
        exact = f"{query}\n-- PARAMS: {params_list!r}\n-- mogrify failed: {exc}"
    trace(f"\n[SOURCE QUERY][{label}] exact_query_for_psql:\n{exact.strip()}\n")


def trace_source_result(label: str, table: str, rows: int, limit: int) -> None:
    if not _env_bool("MIGRATION_LOG_SOURCE_RESULTS", True):
        return
    every = max(1, _env_int("MIGRATION_SOURCE_RESULT_EVERY", 20))
    key = label or table
    stats = _SOURCE_STATS.setdefault(key, {"batches": 0, "rows": 0})
    stats["batches"] += 1
    stats["rows"] += rows
    if stats["batches"] == 1 or stats["batches"] % every == 0 or rows < limit:
        trace(
            f"[SOURCE FETCH][{key}] batches={stats['batches']} "
            f"total_rows={stats['rows']} last_rows={rows} limit={limit}"
        )


def fetch_json_batch(
    conn,
    source_schema: str,
    table: str,
    date_expr: str,
    after_dt: Optional[str],
    after_id: Optional[str],
    limit: int,
    from_dt: Optional[str] = None,
    until_dt: Optional[str] = None,
    extra_conditions: Optional[List[str]] = None,
    extra_params: Optional[List[Any]] = None,
    label: Optional[str] = None,
) -> List[Dict[str, Any]]:
    anchor_id = after_id or ""
    conditions = ["data IS NOT NULL"]
    params: List[Any] = []

    if extra_conditions:
        conditions.extend(extra_conditions)
        params.extend(extra_params or [])

    if after_dt is not None:
        conditions.append(f"({date_expr}, id) > (%s::timestamptz, %s)")
        params.extend([after_dt, anchor_id])
    elif from_dt is not None:
        conditions.append(f"{date_expr} >= %s::timestamptz")
        params.append(from_dt)

    if until_dt is not None:
        conditions.append(f"{date_expr} < %s::timestamptz")
        params.append(until_dt)

    params.append(limit)
    query = f"""
        SELECT id, data
        FROM {table_ref(source_schema, table)}
        WHERE {' AND '.join(conditions)}
        ORDER BY {date_expr} ASC, id ASC
        LIMIT %s
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        print_source_query(cur, label or f"{table} batch", query, params)
        cur.execute(query, params)
        rows = cur.fetchall()
        trace_source_result(label or f"{table} batch", table, len(rows), limit)
    conn.rollback()
    return rows
