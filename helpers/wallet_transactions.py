from typing import Any, Dict, List, Optional, Tuple
from dataclasses import asdict, is_dataclass

from psycopg2.extras import execute_values

from .db import table_ref
from .players import to_decimal_str
from .reports import write_phase_report




def _as_mapping(value: Any) -> Dict[str, Any]:
    if is_dataclass(value):
        return asdict(value)
    return dict(value or {})

def ensure_wallet_dedupe_index(conn, config, dry_run: bool) -> None:
    if dry_run:
        return
    idx = f"ux_wallet_{config.BRAND_KEY.lower()}_reference"
    sql = f"""
    CREATE UNIQUE INDEX IF NOT EXISTS {idx}
    ON {table_ref(config.TARGET_SCHEMA, config.WALLET_TRANSACTION_TABLE)} ("platform", "referenceId")
    WHERE "platform" = %s AND "referenceId" IS NOT NULL
    """
    with conn.cursor() as cur:
        cur.execute(sql, (config.WALLET_PLATFORM,))


def insert_wallet_transactions(conn, config, rows: List[Dict[str, Any]], dry_run: bool, report_path: Optional[str] = None) -> Tuple[int, int, int]:
    values: List[Tuple[Any, ...]] = []
    skipped = 0
    seen = set()
    for raw_row in rows:
        row = _as_mapping(raw_row)
        if not row.get("reference_id") or not row.get("player_id"):
            skipped += 1
            if report_path:
                write_phase_report(report_path, issueType="wallet_missing_required_field", sourceTable=row.get("source_table"), sourceUsername=row.get("username"), sourceId=row.get("source_id"), referenceId=row.get("reference_id"), action="skipped", reason="Missing reference_id or player_id")
            continue
        key = (row.get("kind"), row.get("reference_id"))
        if key in seen:
            skipped += 1
            continue
        seen.add(key)
        status = str(row.get("status") or "").strip().lower()
        created = row.get("created")
        confirmed_dt = row.get("confirmed") if status == "confirmed" else None
        cancelled_dt = created if status == "cancelled" else None
        failed_dt = created if status == "failed" else None
        values.append((
            row.get("kind"), config.WALLET_PLATFORM, row.get("player_id"), row.get("payment_gateway") or "",
            row.get("domain") or config.DEFAULT_DOMAIN, to_decimal_str(row.get("amount")), status, None,
            created, confirmed_dt, cancelled_dt, failed_dt, row.get("reference_id"),
        ))
    if dry_run:
        return (0, 0, skipped)
    if not values:
        return (0, 0, skipped)
    sql = f"""
    INSERT INTO {table_ref(config.TARGET_SCHEMA, config.WALLET_TRANSACTION_TABLE)} (
        "transactionType", "platform", "playerId", "paymentGateway", "domain", "amount", "status",
        "bettingPhase", "createdDatetime", "confirmedDatetime", "cancelledDatetime", "failedDatetime", "referenceId"
    ) VALUES %s
    ON CONFLICT ("platform", "referenceId")
    WHERE ("platform" = '{config.WALLET_PLATFORM}' AND "referenceId" IS NOT NULL)
    DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=config.INSERT_PAGE_SIZE)
        inserted = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else len(values)
    duplicates = max(0, len(values) - int(inserted or 0))
    return (int(inserted or 0), duplicates, skipped)
