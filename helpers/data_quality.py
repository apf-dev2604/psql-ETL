from typing import Any, Dict, List, Optional

from psycopg2.extras import RealDictCursor

from .db import table_ref
from .players import username_key
from .reports import trace, write_csv_row
from .source_fetch import fetch_json_batch

DQ_FIELDS = ["phase", "mismatchColCount", "date-from", "date-to", "columnList"]


def _date_part(value: Optional[str]) -> str:
    if not value:
        return ""
    return str(value)[:10].replace("-", "")


def _target_player_by_username(conn, config, username: Any) -> Optional[Dict[str, Any]]:
    key = username_key(username)
    if not key:
        return None
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT id, "userName", "externalId", "mobileNumber", "emailAddress"
            FROM {table_ref(config.TARGET_SCHEMA, config.PLAYER_TABLE)}
            WHERE LOWER(TRIM(COALESCE("brandName", ''))) = LOWER(TRIM(%s))
              AND regexp_replace(LOWER(TRIM(COALESCE("userName", ''))), '\\s+', '', 'g') = %s
            ORDER BY "createdAt" DESC NULLS LAST, id DESC
            LIMIT 1
            """,
            (config.BRAND, key),
        )
        return cur.fetchone()


def run_data_quality_checks(src_conn, tgt_conn, adapter, config, report_path: str, from_dt: Optional[str], until_dt: Optional[str]) -> None:
    """Portable playerDetails DQ check aligned with normalized player matching.

    This carries over the important InplayV2 DQ fix: compare source player rows
    to target playerDetails_final using the same normalized username rule as
    player_map/fallback, instead of only LOWER(TRIM()).
    """
    trace("[DATA QUALITY] Starting multi-brand post-migration DQ checks.")
    sample_limit = int(getattr(config, "DQ_SAMPLE_LIMIT", 500))
    mismatch_columns = set()
    mismatch_count = 0
    detail_map = adapter.fetch_player_detail_map(src_conn)

    rows = fetch_json_batch(
        src_conn,
        config.SOURCE_SCHEMA,
        config.SOURCE_TABLES["players"],
        adapter.source_date_expr("data"),
        after_dt=None,
        after_id=None,
        limit=sample_limit,
        from_dt=from_dt,
        until_dt=until_dt,
        label=f"{config.BRAND_KEY} dq players",
    )

    for row in rows:
        mapped = adapter.map_player(row, detail_map, src_conn=src_conn)
        if not mapped:
            mismatch_count += 1
            mismatch_columns.add("unmappable_player")
            continue
        target = _target_player_by_username(tgt_conn, config, mapped.get("username"))
        if not target:
            mismatch_count += 1
            mismatch_columns.add("missing_target_row")
            continue
        comparisons = {
            "userName": (mapped.get("username"), target.get("userName")),
            "externalId": (mapped.get("external_id"), target.get("externalId")),
            "mobileNumber": (mapped.get("mobile_number"), target.get("mobileNumber")),
            "emailAddress": (mapped.get("email"), target.get("emailAddress")),
        }
        for column, (source_val, target_val) in comparisons.items():
            if str(source_val or "") != str(target_val or ""):
                mismatch_count += 1
                mismatch_columns.add(column)

    write_csv_row(
        report_path,
        DQ_FIELDS,
        {
            "phase": config.PLAYER_TABLE,
            "mismatchColCount": mismatch_count,
            "date-from": _date_part(from_dt),
            "date-to": _date_part(until_dt),
            "columnList": "[" + "|".join(sorted(mismatch_columns)) + "]",
        },
    )
    trace(
        f"[DATA QUALITY] Completed; mismatchColCount={mismatch_count}; csv={report_path}; sampleLimit={sample_limit}."
    )
