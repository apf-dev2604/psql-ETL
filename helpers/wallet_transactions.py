import re
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import asdict, is_dataclass

from psycopg2.extras import execute_values

from .db import table_ref
from .players import to_decimal_str
from .reports import write_phase_report, trace, short_text


def _as_mapping(value: Any) -> Dict[str, Any]:
    if is_dataclass(value):
        return asdict(value)
    return dict(value or {})


def _error_text(exc: Exception) -> str:
    return short_text(str(exc), 2000)


def _sqlstate(exc: Exception) -> str:
    return str(getattr(exc, "pgcode", "") or "")


def _diagnostic_detail(exc: Exception) -> str:
    diag = getattr(exc, "diag", None)
    if not diag:
        return ""
    parts = []
    for name in ("constraint_name", "table_name", "column_name", "message_detail", "message_hint"):
        value = getattr(diag, name, None)
        if value:
            parts.append(f"{name}={value}")
    return short_text("; ".join(parts), 2000)


def _diag_value(exc: Exception, name: str) -> str:
    diag = getattr(exc, "diag", None)
    return short_text(getattr(diag, name, "") if diag else "", 2000)


def _error_signature(exc: Exception) -> str:
    constraint_name = _diag_value(exc, "constraint_name") or "no_constraint"
    table_name = _diag_value(exc, "table_name") or "no_table"
    column_name = _diag_value(exc, "column_name") or "no_column"
    primary = _diag_value(exc, "message_primary") or _error_text(exc)
    detail = _diag_value(exc, "message_detail")
    redacted_detail = re.sub(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "<uuid>", detail)
    return short_text(f"sqlstate={_sqlstate(exc)} constraint={constraint_name} table={table_name} column={column_name} primary={primary} detail={redacted_detail}", 500)


def _trace_error_summary(label: str, counts: Counter, report_path: Optional[str]) -> None:
    if not counts:
        return
    total = sum(counts.values())
    top = counts.most_common(5)
    lines = [
        f"[INSERT ERROR SUMMARY][{label}]",
        f"  totalErrorRows  : {total}",
        f"  uniqueErrorTypes: {len(counts)}",
        f"  csv             : {report_path or ''}",
        "  topErrors:",
    ]
    for idx, (signature, count) in enumerate(top, start=1):
        lines.append(f"    {idx}. rows={count} {signature}")
    if len(counts) > len(top):
        lines.append(f"    ... {len(counts) - len(top)} more error type(s)")
    trace("\n".join(lines))


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


def _insert_sql(config) -> str:
    return f"""
    INSERT INTO {table_ref(config.TARGET_SCHEMA, config.WALLET_TRANSACTION_TABLE)} (
        "transactionType", "platform", "playerId", "paymentGateway", "domain", "amount", "status",
        "bettingPhase", "createdDatetime", "confirmedDatetime", "cancelledDatetime", "failedDatetime", "referenceId"
    ) VALUES %s
    ON CONFLICT ("platform", "referenceId")
    WHERE ("platform" = '{config.WALLET_PLATFORM}' AND "referenceId" IS NOT NULL)
    DO NOTHING
    """


def _write_wallet_insert_error(report_path: Optional[str], row: Dict[str, Any], exc: Exception) -> None:
    if not report_path:
        return
    detail = _diagnostic_detail(exc)
    write_phase_report(
        report_path,
        issueType="wallet_insert_error",
        sourceTable=row.get("source_table"),
        sourceUsername=row.get("username"),
        sourceId=row.get("source_id"),
        referenceId=row.get("reference_id"),
        targetPlayerId=row.get("player_id"),
        action="insert_error",
        reason=detail or "Target walletTransaction insert failed",
        sqlstate=_sqlstate(exc),
        constraintName=_diag_value(exc, "constraint_name"),
        tableName=_diag_value(exc, "table_name"),
        columnName=_diag_value(exc, "column_name"),
        messageDetail=_diag_value(exc, "message_detail"),
        messageHint=_diag_value(exc, "message_hint"),
        error=f"sqlstate={_sqlstate(exc)} error={_error_text(exc)}",
    )


def _insert_values_with_row_isolation(conn, config, entries: List[Tuple[Tuple[Any, ...], Dict[str, Any]]], report_path: Optional[str]) -> Tuple[int, int, int]:
    inserted = 0
    duplicates = 0
    errors = 0
    error_counts: Counter = Counter()
    sql = _insert_sql(config)
    with conn.cursor() as cur:
        for idx, (value_tuple, row) in enumerate(entries, start=1):
            sp = f"sp_wallet_row_{idx}"
            cur.execute(f"SAVEPOINT {sp}")
            try:
                execute_values(cur, sql, [value_tuple], page_size=1)
                rowcount = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 1
                if int(rowcount or 0) > 0:
                    inserted += int(rowcount)
                else:
                    duplicates += 1
                cur.execute(f"RELEASE SAVEPOINT {sp}")
            except Exception as exc:
                errors += 1
                error_counts[_error_signature(exc)] += 1
                cur.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                cur.execute(f"RELEASE SAVEPOINT {sp}")
                _write_wallet_insert_error(report_path, row, exc)
    _trace_error_summary(f"{config.BRAND_KEY} wallet_transactions", error_counts, report_path)
    return inserted, duplicates, errors


def insert_wallet_transactions(conn, config, rows: List[Dict[str, Any]], dry_run: bool, report_path: Optional[str] = None) -> Tuple[int, int, int, int]:
    """Insert wallet transactions.

    Returns (inserted, duplicate_skipped, skipped_before_insert, insert_errors).
    If a bulk insert fails, falls back to row-level SAVEPOINT isolation and writes
    exact database errors to the phase CSV.
    """
    entries: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = []
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
            if report_path:
                write_phase_report(report_path, issueType="wallet_duplicate_in_source_batch", sourceTable=row.get("source_table"), sourceUsername=row.get("username"), sourceId=row.get("source_id"), referenceId=row.get("reference_id"), action="skipped", reason="Duplicate reference_id within same in-memory source batch")
            continue
        seen.add(key)
        status = str(row.get("status") or "").strip().lower()
        created = row.get("created")
        confirmed_dt = row.get("confirmed") if status == "confirmed" else None
        cancelled_dt = created if status == "cancelled" else None
        failed_dt = created if status == "failed" else None
        value_tuple = (
            row.get("kind"), config.WALLET_PLATFORM, row.get("player_id"), row.get("payment_gateway") or "",
            row.get("domain") or config.DEFAULT_DOMAIN, to_decimal_str(row.get("amount")), status, None,
            created, confirmed_dt, cancelled_dt, failed_dt, row.get("reference_id"),
        )
        entries.append((value_tuple, row))
    if dry_run:
        return (0, 0, skipped, 0)
    if not entries:
        return (0, 0, skipped, 0)

    values = [entry[0] for entry in entries]
    sql = _insert_sql(config)
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT sp_wallet_bulk_insert")
        try:
            execute_values(cur, sql, values, page_size=config.INSERT_PAGE_SIZE)
            inserted = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else len(values)
            cur.execute("RELEASE SAVEPOINT sp_wallet_bulk_insert")
            duplicates = max(0, len(values) - int(inserted or 0))
            return (int(inserted or 0), duplicates, skipped, 0)
        except Exception as exc:
            cur.execute("ROLLBACK TO SAVEPOINT sp_wallet_bulk_insert")
            cur.execute("RELEASE SAVEPOINT sp_wallet_bulk_insert")
            trace(f"[WALLET BULK INSERT FALLBACK][{config.BRAND_KEY}] rows={len(values)} row_isolation=true sqlstate={_sqlstate(exc)}")

    inserted, duplicates, errors = _insert_values_with_row_isolation(conn, config, entries, report_path)
    return (inserted, duplicates, skipped, errors)
