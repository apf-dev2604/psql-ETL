from typing import Any, Dict, List, Optional, Tuple

from psycopg2.extras import RealDictCursor

from .db import table_ref
from .players import username_key
from .reports import trace, write_reconciliation
from .source_fetch import fetch_json_batch


def _norm_username_sql(column_sql: str) -> str:
    return f"regexp_replace(LOWER(TRIM(COALESCE({column_sql}, ''))), '\\s+', '', 'g')"


def _target_player_lookup(conn, config, username: Any = None, external_id: Any = None) -> Dict[str, Any]:
    """Classify whether a player exists in target by normalized username/externalId/brand.

    This mirrors the latest InplayV2 reconciliation fix: do not label a player as
    missing just because the simple LOWER(TRIM()) join failed. Check normalized
    username, externalId, and different-brand presence separately.
    """
    uname_key = username_key(username)
    ext = str(external_id or "").strip()
    result = {
        "by_username": None,
        "by_external_id": None,
        "diff_brand_by_username": None,
        "diff_brand_by_external_id": None,
    }
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if uname_key:
            cur.execute(
                f"""
                SELECT id, "userName", "externalId", "brandName"
                FROM {table_ref(config.TARGET_SCHEMA, config.PLAYER_TABLE)}
                WHERE LOWER(TRIM(COALESCE("brandName", ''))) = LOWER(TRIM(%s))
                  AND {_norm_username_sql('"userName"')} = %s
                ORDER BY "createdAt" DESC NULLS LAST, id DESC
                LIMIT 1
                """,
                (config.BRAND, uname_key),
            )
            result["by_username"] = cur.fetchone()

            cur.execute(
                f"""
                SELECT id, "userName", "externalId", "brandName"
                FROM {table_ref(config.TARGET_SCHEMA, config.PLAYER_TABLE)}
                WHERE LOWER(TRIM(COALESCE("brandName", ''))) <> LOWER(TRIM(%s))
                  AND {_norm_username_sql('"userName"')} = %s
                ORDER BY "createdAt" DESC NULLS LAST, id DESC
                LIMIT 1
                """,
                (config.BRAND, uname_key),
            )
            result["diff_brand_by_username"] = cur.fetchone()

        if ext:
            cur.execute(
                f"""
                SELECT id, "userName", "externalId", "brandName"
                FROM {table_ref(config.TARGET_SCHEMA, config.PLAYER_TABLE)}
                WHERE LOWER(TRIM(COALESCE("brandName", ''))) = LOWER(TRIM(%s))
                  AND COALESCE("externalId"::text, '') = %s
                ORDER BY "createdAt" DESC NULLS LAST, id DESC
                LIMIT 1
                """,
                (config.BRAND, ext),
            )
            result["by_external_id"] = cur.fetchone()

            cur.execute(
                f"""
                SELECT id, "userName", "externalId", "brandName"
                FROM {table_ref(config.TARGET_SCHEMA, config.PLAYER_TABLE)}
                WHERE LOWER(TRIM(COALESCE("brandName", ''))) <> LOWER(TRIM(%s))
                  AND COALESCE("externalId"::text, '') = %s
                ORDER BY "createdAt" DESC NULLS LAST, id DESC
                LIMIT 1
                """,
                (config.BRAND, ext),
            )
            result["diff_brand_by_external_id"] = cur.fetchone()
    return result


def _game_target_lookup(conn, config, external_id: Any) -> Optional[Dict[str, Any]]:
    if not external_id:
        return None
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT id, "externalId", "playerId", "playerUserName", "brand"
            FROM {table_ref(config.TARGET_SCHEMA, config.GAME_TRANSACTION_TABLE)}
            WHERE "externalId" = %s
              AND LOWER(TRIM(COALESCE("brand", ''))) = LOWER(TRIM(%s))
            LIMIT 1
            """,
            (str(external_id), config.BRAND),
        )
        return cur.fetchone()


def _wallet_target_lookup(conn, config, kind: str, reference_id: Any) -> Optional[Dict[str, Any]]:
    if not reference_id:
        return None
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT wt.id, wt."referenceId", wt."playerId", pd."userName" AS "playerUserName", wt.platform
            FROM {table_ref(config.TARGET_SCHEMA, config.WALLET_TRANSACTION_TABLE)} wt
            LEFT JOIN {table_ref(config.TARGET_SCHEMA, config.PLAYER_TABLE)} pd
              ON pd.id = wt."playerId"
            WHERE wt.platform = %s
              AND wt."transactionType" = %s
              AND wt."referenceId" = %s
            LIMIT 1
            """,
            (config.WALLET_PLATFORM, kind, str(reference_id)),
        )
        return cur.fetchone()


def _iter_source_rows(src_conn, adapter, config, source_key: str, from_dt: Optional[str], until_dt: Optional[str], label: str):
    """Iterate source rows in the same PHT-aware date window used by migration."""
    batch_size = int(getattr(config, "RECON_BATCH_SIZE", 10000))
    max_scan = int(getattr(config, "RECON_MAX_SCAN_ROWS", 0) or 0)
    scanned = 0
    after_dt = None
    after_id = None
    table = config.SOURCE_TABLES[source_key]
    while True:
        if max_scan and scanned >= max_scan:
            break
        limit = min(batch_size, max_scan - scanned) if max_scan else batch_size
        rows = fetch_json_batch(
            src_conn,
            config.SOURCE_SCHEMA,
            table,
            adapter.source_date_expr("data"),
            after_dt=after_dt,
            after_id=after_id,
            limit=limit,
            from_dt=from_dt,
            until_dt=until_dt,
            label=label,
        )
        if not rows:
            break
        for row in rows:
            yield row
        scanned += len(rows)
        last = rows[-1]
        data = adapter.as_dict(last.get("data"))
        after_id = str(last.get("id"))
        after_dt = adapter.source_created_value(data)


def run_player_reconciliation(conn_src, conn_tgt, adapter, config, report_path: str, from_dt: Optional[str], until_dt: Optional[str]) -> int:
    trace("[RECONCILIATION] Starting playerDetails detailed checks.")
    rows_written = 0
    detail_limit = int(getattr(config, "RECON_DETAIL_LIMIT", 5000))
    detail_map = adapter.fetch_player_detail_map(conn_src)

    for row in _iter_source_rows(conn_src, adapter, config, "players", from_dt, until_dt, f"{config.BRAND_KEY} recon players"):
        mapped = adapter.map_player(row, detail_map, src_conn=conn_src)
        source_id = row.get("id")
        if not mapped:
            write_reconciliation(
                report_path,
                checkName="1_player_reconciliation_detail",
                recordType="detail",
                status="source_missing_username_or_unmappable_player",
                sourceTable=config.SOURCE_TABLES["players"],
                sourceId=source_id,
                targetTable=config.PLAYER_TABLE,
                reason="Source player row could not be mapped to a valid player payload.",
            )
            rows_written += 1
        else:
            username = mapped.get("username")
            external_id = mapped.get("external_id")
            lookup = _target_player_lookup(conn_tgt, config, username=username, external_id=external_id)
            by_user = lookup.get("by_username")
            by_ext = lookup.get("by_external_id")
            diff_user = lookup.get("diff_brand_by_username")
            diff_ext = lookup.get("diff_brand_by_external_id")
            status = None
            target = by_user or by_ext
            if not username:
                status = "source_missing_username"
            elif by_user and by_ext and str(by_user.get("id")) != str(by_ext.get("id")):
                status = "username_and_external_id_match_different_target_rows"
                target = by_user
            elif by_user and str(by_user.get("externalId") or "") != str(external_id or ""):
                status = "external_id_mismatch"
                target = by_user
            elif (not by_user) and by_ext:
                status = "exists_by_external_id_only_username_mismatch"
                target = by_ext
            elif (not by_user) and (not by_ext) and diff_user:
                status = "exists_under_different_brand_by_username"
                target = diff_user
            elif (not by_user) and (not by_ext) and diff_ext:
                status = "exists_under_different_brand_by_external_id"
                target = diff_ext
            elif not by_user and not by_ext:
                status = "source_player_missing_in_target_truly_missing"

            if status:
                write_reconciliation(
                    report_path,
                    checkName="1_player_reconciliation_detail",
                    recordType="detail",
                    status=status,
                    sourceTable=config.SOURCE_TABLES["players"],
                    sourceId=source_id,
                    sourceUsername=username,
                    sourceExternalId=external_id,
                    targetTable=config.PLAYER_TABLE,
                    targetId=(target or {}).get("id"),
                    targetUsername=(target or {}).get("userName"),
                    targetExternalId=(target or {}).get("externalId"),
                    reason="Player source-to-target reconciliation detail using normalized username, externalId, and brand-aware classification.",
                )
                rows_written += 1
        if rows_written >= detail_limit:
            break

    trace(f"Player reconciliation detail rows written={rows_written}.")
    return rows_written


def _classify_missing_tx_player(conn_tgt, config, username: Any, member_external_id: Any, prefix: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    lookup = _target_player_lookup(conn_tgt, config, username=username, external_id=member_external_id)
    if lookup.get("by_external_id"):
        return (f"{prefix}_player_exists_by_member_id_should_investigate", lookup["by_external_id"])
    if lookup.get("by_username"):
        return (f"{prefix}_player_exists_by_username_should_investigate", lookup["by_username"])
    if lookup.get("diff_brand_by_external_id"):
        return (f"{prefix}_player_exists_under_different_brand_by_member_id", lookup["diff_brand_by_external_id"])
    if lookup.get("diff_brand_by_username"):
        return (f"{prefix}_player_exists_under_different_brand_by_username", lookup["diff_brand_by_username"])
    return (f"{prefix}_player_not_found_expected_skip", None)


def run_game_reconciliation(conn_src, conn_tgt, adapter, config, report_path: str, from_dt: Optional[str], until_dt: Optional[str]) -> int:
    trace("[RECONCILIATION] Starting gameTransaction detailed checks.")
    rows_written = 0
    detail_limit = int(getattr(config, "RECON_DETAIL_LIMIT", 5000))
    for row in _iter_source_rows(conn_src, adapter, config, "game_transactions", from_dt, until_dt, f"{config.BRAND_KEY} recon game"):
        mapped = adapter.map_game_transaction(row)
        if not mapped:
            write_reconciliation(
                report_path,
                checkName="2_game_reconciliation_detail",
                recordType="detail",
                status="source_game_unmappable",
                sourceTable=config.SOURCE_TABLES["game_transactions"],
                sourceId=row.get("id"),
                targetTable=config.GAME_TRANSACTION_TABLE,
                reason="Source game transaction row could not be mapped.",
            )
            rows_written += 1
        else:
            target = _game_target_lookup(conn_tgt, config, mapped.get("external_id"))
            status = None
            target_player = None
            if not mapped.get("username"):
                status = "source_missing_username"
            elif not mapped.get("external_id"):
                status = "source_missing_external_id"
            elif not target:
                status, target_player = _classify_missing_tx_player(
                    conn_tgt,
                    config,
                    mapped.get("username"),
                    mapped.get("member_external_id"),
                    "source_game_missing_in_target",
                )
            if status:
                write_reconciliation(
                    report_path,
                    checkName="2_game_reconciliation_detail",
                    recordType="detail",
                    status=status,
                    sourceTable=config.SOURCE_TABLES["game_transactions"],
                    sourceId=mapped.get("source_id") or row.get("id"),
                    sourceUsername=mapped.get("username"),
                    sourceExternalId=mapped.get("external_id"),
                    targetTable=config.GAME_TRANSACTION_TABLE,
                    targetId=(target or {}).get("id"),
                    targetUsername=(target or {}).get("playerUserName") or (target_player or {}).get("userName"),
                    targetExternalId=(target or {}).get("externalId") or (target_player or {}).get("externalId"),
                    referenceType="externalId",
                    referenceValue=mapped.get("external_id"),
                    reason="Game source-to-target reconciliation detail with player existence classification.",
                )
                rows_written += 1
        if rows_written >= detail_limit:
            break
    trace(f"Game reconciliation detail rows written={rows_written}.")
    return rows_written


def run_wallet_reconciliation(conn_src, conn_tgt, adapter, config, report_path: str, kind: str, from_dt: Optional[str], until_dt: Optional[str]) -> int:
    source_key = "deposits" if kind == "deposit" else "withdrawals"
    trace(f"[RECONCILIATION] Starting walletTransaction.{kind} detailed checks.")
    rows_written = 0
    detail_limit = int(getattr(config, "RECON_DETAIL_LIMIT", 5000))
    for row in _iter_source_rows(conn_src, adapter, config, source_key, from_dt, until_dt, f"{config.BRAND_KEY} recon {kind}"):
        mapped = adapter.map_wallet(row, kind)
        if not mapped:
            write_reconciliation(
                report_path,
                checkName=f"3_wallet_{kind}_reconciliation_detail",
                recordType="detail",
                status="source_wallet_unmappable",
                sourceTable=config.SOURCE_TABLES[source_key],
                sourceId=row.get("id"),
                targetTable=config.WALLET_TRANSACTION_TABLE,
                reason="Source wallet row could not be mapped.",
            )
            rows_written += 1
        else:
            target = _wallet_target_lookup(conn_tgt, config, kind, mapped.get("reference_id"))
            status = None
            target_player = None
            if not mapped.get("username"):
                status = "source_missing_username"
            elif not mapped.get("reference_id"):
                status = "source_missing_reference_id"
            elif not target:
                status, target_player = _classify_missing_tx_player(
                    conn_tgt,
                    config,
                    mapped.get("username"),
                    mapped.get("member_external_id"),
                    "source_wallet_missing_in_target",
                )
            if status:
                write_reconciliation(
                    report_path,
                    checkName=f"3_wallet_{kind}_reconciliation_detail",
                    recordType="detail",
                    status=status,
                    sourceTable=config.SOURCE_TABLES[source_key],
                    sourceId=mapped.get("source_id") or row.get("id"),
                    sourceUsername=mapped.get("username"),
                    sourceExternalId=mapped.get("reference_id"),
                    targetTable=config.WALLET_TRANSACTION_TABLE,
                    targetId=(target or {}).get("id"),
                    targetUsername=(target or {}).get("playerUserName") or (target_player or {}).get("userName"),
                    targetExternalId=(target or {}).get("referenceId") or (target_player or {}).get("externalId"),
                    referenceType="referenceId",
                    referenceValue=mapped.get("reference_id"),
                    reason=f"Wallet {kind} source-to-target reconciliation detail with player existence classification.",
                )
                rows_written += 1
        if rows_written >= detail_limit:
            break
    trace(f"Wallet {kind} reconciliation detail rows written={rows_written}.")
    return rows_written


def run_target_summary(conn, config, report_path: str, from_dt: Optional[str], until_dt: Optional[str]) -> None:
    checks = [
        ("gameTransaction", config.GAME_TRANSACTION_TABLE, "brand", config.BRAND, "startDateTime"),
        ("walletTransaction", config.WALLET_TRANSACTION_TABLE, "platform", config.WALLET_PLATFORM, "createdDatetime"),
        ("playerDetails", config.PLAYER_TABLE, "brandName", config.BRAND, "registrationDate"),
    ]
    for record_type, table, brand_col, brand_val, date_col in checks:
        conditions = [f'"{brand_col}"=%s']
        params: List[Any] = [brand_val]
        if from_dt:
            conditions.append(f'"{date_col}" >= %s::timestamptz')
            params.append(from_dt)
        if until_dt:
            conditions.append(f'"{date_col}" < %s::timestamptz')
            params.append(until_dt)
        sql = f'SELECT COUNT(*) AS rows FROM {table_ref(config.TARGET_SCHEMA, table)} WHERE ' + " AND ".join(conditions)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone() or {}
        write_reconciliation(
            report_path,
            checkName="target_summary",
            recordType=record_type,
            status="summary",
            targetTable=table,
            metric="rows",
            value=row.get("rows", 0),
            reason="Target rows in configured PHT-aware window",
        )


def run_detailed_reconciliation(src_conn, tgt_conn, adapter, config, report_path: str, from_dt: Optional[str], until_dt: Optional[str]) -> None:
    """Run brand-portable reconciliation with latest InplayV2-style classifications."""
    trace("[RECONCILIATION] Starting detailed multi-brand checks.")
    run_player_reconciliation(src_conn, tgt_conn, adapter, config, report_path, from_dt, until_dt)
    if "game_transactions" in config.SOURCE_TABLES:
        run_game_reconciliation(src_conn, tgt_conn, adapter, config, report_path, from_dt, until_dt)
    if "deposits" in config.SOURCE_TABLES:
        run_wallet_reconciliation(src_conn, tgt_conn, adapter, config, report_path, "deposit", from_dt, until_dt)
    if "withdrawals" in config.SOURCE_TABLES:
        run_wallet_reconciliation(src_conn, tgt_conn, adapter, config, report_path, "withdrawal", from_dt, until_dt)
    run_target_summary(tgt_conn, config, report_path, from_dt, until_dt)
    trace("[RECONCILIATION] Detailed multi-brand checks completed.")


# Backward-compatible name used by the first refactor package.
def run_basic_reconciliation(conn, config, report_path: str, from_dt: Optional[str], until_dt: Optional[str]) -> None:
    run_target_summary(conn, config, report_path, from_dt, until_dt)
