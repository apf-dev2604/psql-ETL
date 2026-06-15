import uuid
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
    # Compact grouping key for screen summaries. UUIDs are redacted so the same
    # FK/check error groups together instead of producing thousands of buckets.
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


def normalize_game_type(raw: Optional[str]) -> str:
    if not raw:
        return "Slots"
    value = str(raw).strip().upper()
    if value in ("SLOTS", "SLOT"):
        return "Slots"
    if value in ("LIVE", "LIVE_CASINO", "CASINO"):
        return "Live"
    if value in ("SPORTS", "SPORT"):
        return "Sports"
    return str(raw).strip().title()


def get_or_create_game_provider(conn, config, provider_name: str, cache: Dict[str, uuid.UUID], dry_run: bool) -> uuid.UUID:
    name = (provider_name or "UNKNOWN").strip().upper() or "UNKNOWN"
    if name in cache:
        return cache[name]
    if dry_run:
        with conn.cursor() as cur:
            cur.execute(f'SELECT id FROM {table_ref(config.TARGET_SCHEMA, config.GAME_PROVIDER_TABLE)} WHERE "gameProvider"=%s', (name,))
            row = cur.fetchone()
        gid = row[0] if row else uuid.uuid4()
        cache[name] = gid
        return gid
    sql = f"""
    INSERT INTO {table_ref(config.TARGET_SCHEMA, config.GAME_PROVIDER_TABLE)} ("gameProvider", "isActive", "createdAt", "updatedAt")
    VALUES (%s, true, now(), now())
    ON CONFLICT ("gameProvider") DO UPDATE SET "updatedAt"=now()
    RETURNING id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (name,))
        gid = cur.fetchone()[0]
    cache[name] = gid
    return gid


def get_or_create_game_type(conn, config, game_type: str, cache: Dict[str, uuid.UUID], dry_run: bool) -> uuid.UUID:
    value = normalize_game_type(game_type)
    if value in cache:
        return cache[value]
    if dry_run:
        with conn.cursor() as cur:
            cur.execute(f'SELECT id FROM {table_ref(config.TARGET_SCHEMA, config.GAME_TYPE_TABLE)} WHERE "gameType"=%s', (value,))
            row = cur.fetchone()
        gid = row[0] if row else uuid.uuid4()
        cache[value] = gid
        return gid
    sql = f"""
    INSERT INTO {table_ref(config.TARGET_SCHEMA, config.GAME_TYPE_TABLE)} ("gameType", "isActive", "createdAt", "updatedAt")
    VALUES (%s, true, now(), now())
    ON CONFLICT ("gameType") DO UPDATE SET "updatedAt"=now()
    RETURNING id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (value,))
        gid = cur.fetchone()[0]
    cache[value] = gid
    return gid


def get_or_create_game_list(conn, config, game_name: str, provider_id: uuid.UUID, game_type_id: uuid.UUID, cache: Dict[Tuple[uuid.UUID, str], uuid.UUID], dry_run: bool) -> uuid.UUID:
    name = (game_name or "UNKNOWN").strip() or "UNKNOWN"
    key = (provider_id, name)
    if key in cache:
        return cache[key]
    if dry_run:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT id FROM {table_ref(config.TARGET_SCHEMA, config.GAME_LIST_TABLE)} WHERE "gameProviderId"=%s AND "gameName"=%s',
                (provider_id, name),
            )
            row = cur.fetchone()
        gid = row[0] if row else uuid.uuid4()
        cache[key] = gid
        return gid
    sql = f"""
    INSERT INTO {table_ref(config.TARGET_SCHEMA, config.GAME_LIST_TABLE)} (
        "gameTypeId", "gameProviderId", "gameName", "isProgressive", "isActive", "createdAt", "updatedAt", "brandName"
    ) VALUES (%s, %s, %s, false, true, now(), now(), %s)
    ON CONFLICT ("gameProviderId", "gameName") DO UPDATE SET
        "gameTypeId"=EXCLUDED."gameTypeId", "updatedAt"=now()
    RETURNING id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (game_type_id, provider_id, name, config.BRAND))
        gid = cur.fetchone()[0]
    cache[key] = gid
    return gid


def _insert_sql(config) -> str:
    return f"""
    INSERT INTO {table_ref(config.TARGET_SCHEMA, config.GAME_TRANSACTION_TABLE)} (
        "startDateTime", "providerId", "gameId", "gameTypeId", "playerId", "playerUserName", "tableRoomId",
        "sideBetAmount", "betAmount", "validBet", "payoutAmount",
        "PC1", "PC2", "PC3", "PC4", "PC5", "JW1", "JW2", "JW3", "JW4", "JW5",
        "progressionContributionPaid", "seedMoneyWon", "seedMoneyJackpotOver1000", "endDateTime",
        "externalId", "parlay", "betDetails", "betTiming", "brand", "platform", "roundId"
    ) VALUES %s
    ON CONFLICT ("externalId") DO NOTHING
    """


def _write_game_insert_error(report_path: Optional[str], config, row: Dict[str, Any], exc: Exception) -> None:
    if not report_path:
        return
    detail = _diagnostic_detail(exc)
    write_phase_report(
        report_path,
        issueType="game_insert_error",
        sourceTable=config.SOURCE_TABLES.get("game_transactions"),
        sourceUsername=row.get("username"),
        sourceId=row.get("source_id"),
        referenceId=row.get("external_id"),
        targetPlayerId=row.get("player_id"),
        action="insert_error",
        reason=detail or "Target gameTransaction insert failed",
        sqlstate=_sqlstate(exc),
        constraintName=_diag_value(exc, "constraint_name"),
        tableName=_diag_value(exc, "table_name"),
        columnName=_diag_value(exc, "column_name"),
        messageDetail=_diag_value(exc, "message_detail"),
        messageHint=_diag_value(exc, "message_hint"),
        error=f"sqlstate={_sqlstate(exc)} error={_error_text(exc)}",
    )


def _insert_values_with_row_isolation(conn, config, entries: List[Tuple[Tuple[Any, ...], Dict[str, Any]]], report_path: Optional[str]) -> Tuple[int, int, int]:
    """Retry one row at a time after a bulk insert failure.

    Uses SAVEPOINTs so one bad row does not abort the whole transaction. This is slower
    only on error paths and avoids keeping large error payloads in memory.
    Returns (inserted, duplicate_skipped, insert_errors).
    """
    inserted = 0
    duplicates = 0
    errors = 0
    error_counts: Counter = Counter()
    sql = _insert_sql(config)
    with conn.cursor() as cur:
        for idx, (value_tuple, row) in enumerate(entries, start=1):
            sp = f"sp_game_row_{idx}"
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
                _write_game_insert_error(report_path, config, row, exc)
    _trace_error_summary(f"{config.BRAND_KEY} game_transactions", error_counts, report_path)
    return inserted, duplicates, errors


def insert_game_transactions(conn, config, rows: List[Dict[str, Any]], dry_run: bool, report_path: Optional[str] = None) -> Tuple[int, int, int, int]:
    """Insert game transactions.

    Returns (inserted, duplicate_skipped, skipped_before_insert, insert_errors).
    If a bulk insert fails, the function falls back to row isolation so exact bad rows
    and exact database errors are written to the phase CSV.
    """
    entries: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = []
    skipped = 0
    provider_cache: Dict[str, uuid.UUID] = {}
    gametype_cache: Dict[str, uuid.UUID] = {}
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID] = {}

    for raw_row in rows:
        row = _as_mapping(raw_row)
        if not row.get("external_id") or not row.get("player_id"):
            skipped += 1
            if report_path:
                write_phase_report(report_path, issueType="game_missing_required_field", sourceTable=config.SOURCE_TABLES.get("game_transactions"), sourceUsername=row.get("username"), sourceId=row.get("source_id"), referenceId=row.get("external_id"), action="skipped", reason="Missing external_id or player_id")
            continue
        provider_id = get_or_create_game_provider(conn, config, row.get("provider_name"), provider_cache, dry_run)
        game_type_id = get_or_create_game_type(conn, config, row.get("game_type"), gametype_cache, dry_run)
        game_id = get_or_create_game_list(conn, config, row.get("game_name"), provider_id, game_type_id, gamelist_cache, dry_run)
        value_tuple = (
            row.get("created"), provider_id, game_id, game_type_id, row.get("player_id"), row.get("username"),
            row.get("table_room_id"), "0", to_decimal_str(row.get("bet_amount")), to_decimal_str(row.get("valid_bet", row.get("bet_amount"))),
            to_decimal_str(row.get("payout_amount")),
            to_decimal_str(row.get("pc1")), to_decimal_str(row.get("pc2")), to_decimal_str(row.get("pc3")), to_decimal_str(row.get("pc4")),
            to_decimal_str(row.get("pc5") if row.get("pc5") is not None else row.get("jackpot_contribution")),
            to_decimal_str(row.get("jw1")), to_decimal_str(row.get("jw2")), to_decimal_str(row.get("jw3")), to_decimal_str(row.get("jw4")),
            to_decimal_str(row.get("jw5") if row.get("jw5") is not None else row.get("jackpot_payout")),
            to_decimal_str(row.get("progression_contribution_paid")), to_decimal_str(row.get("seed_money_won")), int(row.get("seed_money_jackpot_over_1000") or 0),
            row.get("settled") or row.get("created"), row.get("external_id"), bool(row.get("parlay", False)),
            row.get("bet_details"), row.get("bet_timing"), config.BRAND, config.PLATFORM, row.get("round_id"),
        )
        entries.append((value_tuple, row))

    if dry_run:
        return (0, 0, skipped, 0)
    if not entries:
        return (0, 0, skipped, 0)

    values = [entry[0] for entry in entries]
    sql = _insert_sql(config)
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT sp_game_bulk_insert")
        try:
            execute_values(cur, sql, values, page_size=config.INSERT_PAGE_SIZE)
            inserted = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else len(values)
            cur.execute("RELEASE SAVEPOINT sp_game_bulk_insert")
            duplicates = max(0, len(values) - int(inserted or 0))
            return (int(inserted or 0), duplicates, skipped, 0)
        except Exception as exc:
            cur.execute("ROLLBACK TO SAVEPOINT sp_game_bulk_insert")
            cur.execute("RELEASE SAVEPOINT sp_game_bulk_insert")
            # Bulk failure is expected when a batch contains one bad row. Keep screen concise;
            # exact row-level errors are isolated below and written to CSV.
            trace(f"[GAME BULK INSERT FALLBACK][{config.BRAND_KEY}] rows={len(values)} row_isolation=true sqlstate={_sqlstate(exc)}")

    inserted, duplicates, errors = _insert_values_with_row_isolation(conn, config, entries, report_path)
    return (inserted, duplicates, skipped, errors)
