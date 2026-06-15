import argparse
import importlib
from typing import Any, Dict, List, Optional

from .dates import business_window_bounds, parse_date_arg
from .context import MigrationContext
from .models import BrandConfig, NormalizedGameTransaction, NormalizedPlayer, NormalizedWalletTransaction
from .validators import validate_adapter, validate_brand_config
from .db import connect
from .players import build_player_map, lookup_player_id_by_username, upsert_player, username_key
from .reports import configure_logging, make_report_paths, trace, initialize_report_files, write_summary_report, write_phase_report, emit_final_error_summary
from .source_fetch import fetch_json_batch, format_checkpoint, get_checkpoint, parse_checkpoint, set_checkpoint
from .game_transactions import insert_game_transactions
from .wallet_transactions import ensure_wallet_dedupe_index, insert_wallet_transactions
from .reconciliation import run_detailed_reconciliation
from .data_quality import run_data_quality_checks

PUBLIC_BRANDS = {
    "inplay": "inplay",
    "inplayv1": "inplayv1",
    "instaplay": "instaplay",
    "1play": "oneplay",
}


def new_stats(phase: str) -> Dict[str, int]:
    return {
        "phase": phase,
        "sourceRows": 0,
        "mappedRows": 0,
        "insertAttemptRows": 0,
        "insertedRows": 0,
        "updatedRows": 0,
        "duplicateSkippedRows": 0,
        "skippedRows": 0,
        "missingPlayerRows": 0,
        "missingUsernameRows": 0,
        "missingRequiredRows": 0,
        "mappingErrorRows": 0,
        "insertErrorRows": 0,
        "dataBatches": 0,
    }


def add_insert_stats(stats: Dict[str, int], result) -> None:
    """Accept insert helper result as either:
    - (inserted, duplicates, skipped)
    - (inserted, duplicates, skipped, insert_errors)
    """
    try:
        if len(result) >= 4:
            inserted, duplicates, skipped, insert_errors = result[:4]
        else:
            inserted, duplicates, skipped = result
            insert_errors = 0
    except Exception:
        inserted, duplicates, skipped, insert_errors = (0, 0, 0, 0)
    inserted = int(inserted or 0)
    duplicates = int(duplicates or 0)
    skipped = int(skipped or 0)
    insert_errors = int(insert_errors or 0)
    stats["insertAttemptRows"] += inserted + duplicates + insert_errors
    stats["insertedRows"] += inserted
    stats["duplicateSkippedRows"] += duplicates
    stats["skippedRows"] += skipped
    stats["missingRequiredRows"] += skipped
    stats["insertErrorRows"] += insert_errors




def total_error_rows(stats: Dict[str, int]) -> int:
    """Return a compact summary count of rows that had an error/issue.

    This is intentionally an aggregate for screen/summary CSV only. Row-level
    details remain in the phase CSVs. The count includes categorized pre-insert
    skips plus target insert errors, while preventing generic skippedRows from
    hiding uncategorized skips.
    """
    categorized_pre_insert = (
        int(stats.get("missingPlayerRows") or 0)
        + int(stats.get("missingUsernameRows") or 0)
        + int(stats.get("missingRequiredRows") or 0)
        + int(stats.get("mappingErrorRows") or 0)
    )
    uncategorized_skipped = max(0, int(stats.get("skippedRows") or 0) - categorized_pre_insert)
    return categorized_pre_insert + uncategorized_skipped + int(stats.get("insertErrorRows") or 0)

def emit_phase_summary(ctx: MigrationContext, args, stats: Dict[str, int], notes: str = "") -> None:
    total_errors = total_error_rows(stats)
    msg = "\n".join([
        f"[PHASE SUMMARY][{ctx.config.BRAND_KEY} {stats['phase']}]",
        f"  sourceRows            : {stats['sourceRows']}",
        f"  mappedRows            : {stats['mappedRows']}",
        f"  insertAttemptRows     : {stats['insertAttemptRows']}",
        f"  insertedRows          : {stats['insertedRows']}",
        f"  updatedRows           : {stats['updatedRows']}",
        f"  duplicateSkippedRows  : {stats['duplicateSkippedRows']}",
        f"  skippedRows           : {stats['skippedRows']}",
        f"  missingPlayerRows     : {stats['missingPlayerRows']}",
        f"  missingUsernameRows   : {stats['missingUsernameRows']}",
        f"  missingRequiredRows   : {stats['missingRequiredRows']}",
        f"  mappingErrorRows      : {stats['mappingErrorRows']}",
        f"  insertErrorRows       : {stats['insertErrorRows']}",
        f"  totalErrorRows        : {total_errors}",
        f"  dataBatches           : {stats['dataBatches']}",
    ])
    trace(msg)
    write_summary_report(
        ctx.paths.summary,
        brand=ctx.config.BRAND_KEY,
        phase=stats["phase"],
        sourceRows=stats["sourceRows"],
        mappedRows=stats["mappedRows"],
        insertAttemptRows=stats["insertAttemptRows"],
        insertedRows=stats["insertedRows"],
        updatedRows=stats["updatedRows"],
        duplicateSkippedRows=stats["duplicateSkippedRows"],
        skippedRows=stats["skippedRows"],
        missingPlayerRows=stats["missingPlayerRows"],
        missingUsernameRows=stats["missingUsernameRows"],
        missingRequiredRows=stats["missingRequiredRows"],
        mappingErrorRows=stats["mappingErrorRows"],
        insertErrorRows=stats["insertErrorRows"],
        totalErrorRows=total_errors,
        dataBatches=stats["dataBatches"],
        dryRun=args.dry_run,
        dateFrom=ctx.from_dt,
        dateTo=ctx.until_dt,
        notes=notes,
    )


def load_brand(brand_key: str):
    public_key = str(brand_key).strip().lower()
    if public_key not in PUBLIC_BRANDS:
        allowed = ", ".join(sorted(PUBLIC_BRANDS))
        raise SystemExit(f"Unsupported --brand {brand_key!r}. Allowed values: {allowed}")
    module_key = PUBLIC_BRANDS[public_key]
    adapter_module = importlib.import_module(f"helpers.brands.{module_key}.adapter")
    config_module = importlib.import_module(f"helpers.brands.{module_key}.config")
    config = BrandConfig.from_module(config_module)
    adapter = adapter_module.Adapter()
    validate_brand_config(config)
    validate_adapter(adapter, config.SOURCE_MODE)
    return adapter, config


def resolve_player_id(tgt_conn, config, player_map: Dict[str, Any], username: str) -> Optional[Any]:
    key = username_key(username)
    player_id = player_map.get(key)
    if player_id:
        return player_id
    player_id = lookup_player_id_by_username(tgt_conn, config, username)
    if player_id:
        player_map[key] = player_id
    return player_id


def source_date_expr(adapter, source_key: str) -> str:
    if hasattr(adapter, "source_date_expr_for_table"):
        return adapter.source_date_expr_for_table(source_key, "data")
    return adapter.source_date_expr("data")


def source_created_value(adapter, data: Dict[str, Any], source_key: str) -> Optional[str]:
    try:
        return adapter.source_created_value(data, source_key)
    except TypeError:
        return adapter.source_created_value(data)


def fetch_detail_map(adapter, src_conn, from_dt=None, until_dt=None):
    try:
        return adapter.fetch_player_detail_map(src_conn, from_dt=from_dt, until_dt=until_dt)
    except TypeError:
        try:
            return adapter.fetch_player_detail_map(src_conn, date_from=from_dt, date_to=until_dt)
        except TypeError:
            return adapter.fetch_player_detail_map(src_conn)


def process_table_batch_brand(src_conn, tgt_conn, ctx: MigrationContext, args) -> int:
    adapter = ctx.adapter
    config = ctx.config
    paths = ctx.paths
    from_dt = ctx.from_dt
    until_dt = ctx.until_dt
    ensure_wallet_dedupe_index(tgt_conn, config, args.dry_run)
    detail_map = fetch_detail_map(adapter, src_conn, from_dt, until_dt)
    total = 0

    stats = new_stats("players")
    phase = "players"
    after_dt, after_id = (None, None) if args.date_from or args.date_to else parse_checkpoint(get_checkpoint(tgt_conn, config.TARGET_SCHEMA, config.CHECKPOINT_TABLE, config.checkpoint_key(phase)) if args.resume else None)
    while True:
        rows = fetch_json_batch(src_conn, config.SOURCE_SCHEMA, config.SOURCE_TABLES["players"], source_date_expr(adapter, "players"), after_dt, after_id, args.batch_size, from_dt, until_dt, label=f"{config.BRAND_KEY} players")
        if not rows:
            break
        stats["dataBatches"] += 1
        stats["sourceRows"] += len(rows)
        for row in rows:
            try:
                if hasattr(adapter, "ensure_outlet_from_player_row"):
                    adapter.ensure_outlet_from_player_row(tgt_conn, row, args.dry_run)
                mapped = adapter.map_player(row, detail_map)
                if not mapped:
                    stats["skippedRows"] += 1
                    stats["missingUsernameRows"] += 1
                    continue
                stats["mappedRows"] += 1
                _, status = upsert_player(tgt_conn, config, NormalizedPlayer.from_mapping(mapped), args.dry_run, return_status=True)
                stats["insertAttemptRows"] += 1
                if status in ("inserted", "insertable"):
                    stats["insertedRows"] += 1
                else:
                    stats["updatedRows"] += 1
            except Exception as exc:
                stats["skippedRows"] += 1
                stats["insertErrorRows"] += 1
                trace(f"[PLAYER ERROR][{config.BRAND_KEY}] sourceId={row.get('id')} error={exc}")
                if not args.dry_run:
                    tgt_conn.rollback()
        last = rows[-1]
        last_data = adapter.as_dict(last.get("data"))
        after_id = str(last["id"])
        after_dt = source_created_value(adapter, last_data, "players")
        set_checkpoint(tgt_conn, config.TARGET_SCHEMA, config.CHECKPOINT_TABLE, config.checkpoint_key(phase), format_checkpoint(after_dt, after_id), args.dry_run)
        total += len(rows)
        if not args.dry_run:
            tgt_conn.commit()
    emit_phase_summary(ctx, args, stats)

    player_map = build_player_map(tgt_conn, config)

    stats = new_stats("game_transactions")
    phase = "game_transactions"
    after_dt, after_id = (None, None) if args.date_from or args.date_to else parse_checkpoint(get_checkpoint(tgt_conn, config.TARGET_SCHEMA, config.CHECKPOINT_TABLE, config.checkpoint_key(phase)) if args.resume else None)
    while True:
        rows = fetch_json_batch(src_conn, config.SOURCE_SCHEMA, config.SOURCE_TABLES["game_transactions"], source_date_expr(adapter, "game_transactions"), after_dt, after_id, args.batch_size, from_dt, until_dt, label=f"{config.BRAND_KEY} game")
        if not rows:
            break
        stats["dataBatches"] += 1
        stats["sourceRows"] += len(rows)
        mapped_rows: List[Any] = []
        for row in rows:
            try:
                mapped = adapter.map_game_transaction(row)
                if not mapped:
                    stats["skippedRows"] += 1
                    stats["mappingErrorRows"] += 1
                    write_phase_report(paths.game, issueType="game_mapping_error", sourceTable=config.SOURCE_TABLES.get("game_transactions"), sourceId=row.get("id"), action="skipped", reason="Adapter returned no mapped game transaction")
                    continue
                username = mapped.get("username") or ""
                if not username:
                    stats["skippedRows"] += 1
                    stats["missingUsernameRows"] += 1
                    write_phase_report(paths.game, issueType="missing_username", sourceTable=config.SOURCE_TABLES.get("game_transactions"), sourceId=row.get("id"), referenceId=mapped.get("external_id"), action="skipped", reason="Missing username/member.name in source game transaction")
                    continue
                player_id = resolve_player_id(tgt_conn, config, player_map, username)
                if player_id:
                    mapped["player_id"] = player_id
                    mapped_rows.append(NormalizedGameTransaction.from_mapping(mapped))
                    stats["mappedRows"] += 1
                else:
                    stats["skippedRows"] += 1
                    stats["missingPlayerRows"] += 1
                    write_phase_report(paths.game, issueType="player_not_in_player_map", sourceTable=config.SOURCE_TABLES.get("game_transactions"), sourceUsername=username, sourceId=row.get("id"), referenceId=mapped.get("external_id"), action="skipped", reason="Unable to process no playerRecord; username not found in player_map/playerDetails_final. No shadow/ghost player was created.")
            except Exception as exc:
                stats["skippedRows"] += 1
                stats["mappingErrorRows"] += 1
                write_phase_report(paths.game, issueType="game_mapping_exception", sourceTable=config.SOURCE_TABLES.get("game_transactions"), sourceId=row.get("id"), action="skipped", reason="Game transaction mapping exception", error=exc)
                trace(f"[GAME MAP ERROR][{config.BRAND_KEY}] sourceId={row.get('id')} error={exc}")
        try:
            add_insert_stats(stats, insert_game_transactions(tgt_conn, config, mapped_rows, args.dry_run, paths.game))
        except Exception as exc:
            stats["insertErrorRows"] += len(mapped_rows) or 1
            trace(f"[GAME INSERT ERROR][{config.BRAND_KEY}] rows={len(mapped_rows)} error={exc}")
            if not args.dry_run:
                tgt_conn.rollback()
        last = rows[-1]
        last_data = adapter.as_dict(last.get("data"))
        after_id = str(last["id"])
        after_dt = source_created_value(adapter, last_data, "game_transactions")
        set_checkpoint(tgt_conn, config.TARGET_SCHEMA, config.CHECKPOINT_TABLE, config.checkpoint_key(phase), format_checkpoint(after_dt, after_id), args.dry_run)
        total += len(rows)
        if not args.dry_run:
            tgt_conn.commit()
    emit_phase_summary(ctx, args, stats)

    for kind, source_key, report_path in (("deposit", "deposits", paths.deposits), ("withdrawal", "withdrawals", paths.withdrawals)):
        stats = new_stats(source_key)
        phase = source_key
        after_dt, after_id = (None, None) if args.date_from or args.date_to else parse_checkpoint(get_checkpoint(tgt_conn, config.TARGET_SCHEMA, config.CHECKPOINT_TABLE, config.checkpoint_key(phase)) if args.resume else None)
        while True:
            rows = fetch_json_batch(src_conn, config.SOURCE_SCHEMA, config.SOURCE_TABLES[source_key], source_date_expr(adapter, source_key), after_dt, after_id, args.batch_size, from_dt, until_dt, label=f"{config.BRAND_KEY} {kind}")
            if not rows:
                break
            stats["dataBatches"] += 1
            stats["sourceRows"] += len(rows)
            mapped_rows = []
            for row in rows:
                try:
                    mapped = adapter.map_wallet(row, kind)
                    if not mapped:
                        stats["skippedRows"] += 1
                        stats["mappingErrorRows"] += 1
                        write_phase_report(report_path, issueType=f"{kind}_mapping_error", sourceTable=config.SOURCE_TABLES.get(source_key), sourceId=row.get("id"), action="skipped", reason="Adapter returned no mapped wallet transaction")
                        continue
                    username = mapped.get("username") or ""
                    if not username:
                        stats["skippedRows"] += 1
                        stats["missingUsernameRows"] += 1
                        write_phase_report(report_path, issueType="missing_username", sourceTable=config.SOURCE_TABLES.get(source_key), sourceId=row.get("id"), referenceId=mapped.get("reference_id"), action="skipped", reason=f"Missing username/member.name in source {kind} transaction")
                        continue
                    player_id = resolve_player_id(tgt_conn, config, player_map, username)
                    if player_id:
                        mapped["player_id"] = player_id
                        mapped_rows.append(NormalizedWalletTransaction.from_mapping(mapped))
                        stats["mappedRows"] += 1
                    else:
                        stats["skippedRows"] += 1
                        stats["missingPlayerRows"] += 1
                        write_phase_report(report_path, issueType="player_not_in_player_map", sourceTable=config.SOURCE_TABLES.get(source_key), sourceUsername=username, sourceId=row.get("id"), referenceId=mapped.get("reference_id"), action="skipped", reason="Unable to process no playerRecord; username not found in player_map/playerDetails_final. No shadow/ghost player was created.")
                except Exception as exc:
                    stats["skippedRows"] += 1
                    stats["mappingErrorRows"] += 1
                    write_phase_report(report_path, issueType=f"{kind}_mapping_exception", sourceTable=config.SOURCE_TABLES.get(source_key), sourceId=row.get("id"), action="skipped", reason="Wallet transaction mapping exception", error=exc)
                    trace(f"[WALLET MAP ERROR][{config.BRAND_KEY} {kind}] sourceId={row.get('id')} error={exc}")
            try:
                add_insert_stats(stats, insert_wallet_transactions(tgt_conn, config, mapped_rows, args.dry_run, report_path))
            except Exception as exc:
                stats["insertErrorRows"] += len(mapped_rows) or 1
                trace(f"[WALLET INSERT ERROR][{config.BRAND_KEY} {kind}] rows={len(mapped_rows)} error={exc}")
                if not args.dry_run:
                    tgt_conn.rollback()
            last = rows[-1]
            last_data = adapter.as_dict(last.get("data"))
            after_id = str(last["id"])
            after_dt = source_created_value(adapter, last_data, source_key)
            set_checkpoint(tgt_conn, config.TARGET_SCHEMA, config.CHECKPOINT_TABLE, config.checkpoint_key(phase), format_checkpoint(after_dt, after_id), args.dry_run)
            total += len(rows)
            if not args.dry_run:
                tgt_conn.commit()
        emit_phase_summary(ctx, args, stats)
    return total


def process_flat_table_batch_brand(src_conn, tgt_conn, ctx: MigrationContext, args) -> int:
    adapter = ctx.adapter
    config = ctx.config
    paths = ctx.paths
    ensure_wallet_dedupe_index(tgt_conn, config, args.dry_run)
    detail_map = fetch_detail_map(adapter, src_conn, ctx.from_dt, ctx.until_dt)
    total = 0

    stats = new_stats("players")
    phase = "players"
    after_id = args.start_after_id or None
    if after_id is None and args.resume:
        raw = get_checkpoint(tgt_conn, config.TARGET_SCHEMA, config.CHECKPOINT_TABLE, config.checkpoint_key(phase))
        _, after_id = parse_checkpoint(raw)
    while True:
        rows = adapter.fetch_player_rows(src_conn, int(after_id or 0), args.batch_size, ctx.from_dt, ctx.until_dt)
        if not rows:
            break
        stats["dataBatches"] += 1
        stats["sourceRows"] += len(rows)
        for row in rows:
            try:
                if hasattr(adapter, "ensure_outlet_from_player_row"):
                    adapter.ensure_outlet_from_player_row(tgt_conn, row, args.dry_run)
                mapped = adapter.map_player(row, detail_map)
                if not mapped:
                    stats["skippedRows"] += 1
                    stats["missingUsernameRows"] += 1
                    continue
                stats["mappedRows"] += 1
                _, status = upsert_player(tgt_conn, config, NormalizedPlayer.from_mapping(mapped), args.dry_run, return_status=True)
                stats["insertAttemptRows"] += 1
                if status in ("inserted", "insertable"):
                    stats["insertedRows"] += 1
                else:
                    stats["updatedRows"] += 1
            except Exception as exc:
                stats["skippedRows"] += 1
                stats["insertErrorRows"] += 1
                trace(f"[PLAYER ERROR][{config.BRAND_KEY}] sourceId={row.get('IDX') or row.get('id')} error={exc}")
                if not args.dry_run:
                    tgt_conn.rollback()
        after_id = str(rows[-1].get("IDX") or rows[-1].get("id") or after_id or 0)
        set_checkpoint(tgt_conn, config.TARGET_SCHEMA, config.CHECKPOINT_TABLE, config.checkpoint_key(phase), after_id, args.dry_run)
        total += len(rows)
        if not args.dry_run:
            tgt_conn.commit()
    emit_phase_summary(ctx, args, stats)

    player_map = build_player_map(tgt_conn, config)

    stats = new_stats("game_transactions")
    phase = "game_transactions"
    after_id = args.start_after_id or None
    if after_id is None and args.resume:
        raw = get_checkpoint(tgt_conn, config.TARGET_SCHEMA, config.CHECKPOINT_TABLE, config.checkpoint_key(phase))
        _, after_id = parse_checkpoint(raw)
    while True:
        rows = adapter.fetch_game_rows(src_conn, int(after_id or 0), args.batch_size, ctx.from_dt, ctx.until_dt)
        if not rows:
            break
        stats["dataBatches"] += 1
        stats["sourceRows"] += len(rows)
        mapped_rows = []
        for row in rows:
            try:
                mapped = adapter.map_game_transaction(row)
                if not mapped:
                    stats["skippedRows"] += 1
                    stats["mappingErrorRows"] += 1
                    continue
                username = mapped.get("username") or ""
                if not username:
                    stats["skippedRows"] += 1
                    stats["missingUsernameRows"] += 1
                    continue
                player_id = resolve_player_id(tgt_conn, config, player_map, username)
                if player_id:
                    mapped["player_id"] = player_id
                    mapped_rows.append(NormalizedGameTransaction.from_mapping(mapped))
                    stats["mappedRows"] += 1
                else:
                    stats["skippedRows"] += 1
                    stats["missingPlayerRows"] += 1
            except Exception as exc:
                stats["skippedRows"] += 1
                stats["mappingErrorRows"] += 1
                trace(f"[GAME MAP ERROR][{config.BRAND_KEY}] sourceId={row.get('IDX') or row.get('id')} error={exc}")
        try:
            add_insert_stats(stats, insert_game_transactions(tgt_conn, config, mapped_rows, args.dry_run, paths.game))
        except Exception as exc:
            stats["insertErrorRows"] += len(mapped_rows) or 1
            trace(f"[GAME INSERT ERROR][{config.BRAND_KEY}] rows={len(mapped_rows)} error={exc}")
            if not args.dry_run:
                tgt_conn.rollback()
        after_id = str(rows[-1].get("IDX") or rows[-1].get("id") or after_id or 0)
        set_checkpoint(tgt_conn, config.TARGET_SCHEMA, config.CHECKPOINT_TABLE, config.checkpoint_key(phase), after_id, args.dry_run)
        total += len(rows)
        if not args.dry_run:
            tgt_conn.commit()
    emit_phase_summary(ctx, args, stats)

    stats = new_stats("wallet_transactions")
    phase = "wallet_transactions"
    after_id = args.start_after_id or None
    if after_id is None and args.resume:
        raw = get_checkpoint(tgt_conn, config.TARGET_SCHEMA, config.CHECKPOINT_TABLE, config.checkpoint_key(phase))
        _, after_id = parse_checkpoint(raw)
    while True:
        rows = adapter.fetch_wallet_rows(src_conn, int(after_id or 0), args.batch_size, ctx.from_dt, ctx.until_dt)
        if not rows:
            break
        stats["dataBatches"] += 1
        stats["sourceRows"] += len(rows)
        mapped_rows = []
        for row in rows:
            try:
                mapped = adapter.map_wallet(row, "wallet")
                if not mapped:
                    stats["skippedRows"] += 1
                    stats["mappingErrorRows"] += 1
                    continue
                username = mapped.get("username") or ""
                if not username:
                    stats["skippedRows"] += 1
                    stats["missingUsernameRows"] += 1
                    continue
                player_id = resolve_player_id(tgt_conn, config, player_map, username)
                if player_id:
                    mapped["player_id"] = player_id
                    mapped_rows.append(NormalizedWalletTransaction.from_mapping(mapped))
                    stats["mappedRows"] += 1
                else:
                    stats["skippedRows"] += 1
                    stats["missingPlayerRows"] += 1
            except Exception as exc:
                stats["skippedRows"] += 1
                stats["mappingErrorRows"] += 1
                trace(f"[WALLET MAP ERROR][{config.BRAND_KEY}] sourceId={row.get('IDX') or row.get('id')} error={exc}")
        try:
            add_insert_stats(stats, insert_wallet_transactions(tgt_conn, config, mapped_rows, args.dry_run, paths.deposits))
        except Exception as exc:
            stats["insertErrorRows"] += len(mapped_rows) or 1
            trace(f"[WALLET INSERT ERROR][{config.BRAND_KEY}] rows={len(mapped_rows)} error={exc}")
            if not args.dry_run:
                tgt_conn.rollback()
        after_id = str(rows[-1].get("IDX") or rows[-1].get("id") or after_id or 0)
        set_checkpoint(tgt_conn, config.TARGET_SCHEMA, config.CHECKPOINT_TABLE, config.checkpoint_key(phase), after_id, args.dry_run)
        total += len(rows)
        if not args.dry_run:
            tgt_conn.commit()
    emit_phase_summary(ctx, args, stats)
    return total


def process_member_driven_brand(src_conn, tgt_conn, ctx: MigrationContext, args) -> int:
    adapter = ctx.adapter
    config = ctx.config
    paths = ctx.paths
    from_dt = ctx.from_dt
    until_dt = ctx.until_dt
    ensure_wallet_dedupe_index(tgt_conn, config, args.dry_run)
    detail_cache = {}
    player_map = build_player_map(tgt_conn, config)
    processed = 0
    player_stats = new_stats("players")
    game_stats = new_stats("game_transactions")
    wallet_stats = new_stats("wallet_transactions")
    after_dt, after_id = (None, None) if args.date_from or args.date_to else parse_checkpoint(get_checkpoint(tgt_conn, config.TARGET_SCHEMA, config.CHECKPOINT_TABLE, config.checkpoint_key("players")) if args.resume else None)

    while True:
        rows = fetch_json_batch(src_conn, config.SOURCE_SCHEMA, config.SOURCE_TABLES["players"], source_date_expr(adapter, "players"), after_dt, after_id, args.batch_size, from_dt, until_dt, label=f"{config.BRAND_KEY} member batch")
        if not rows:
            break
        player_stats["dataBatches"] += 1
        player_stats["sourceRows"] += len(rows)
        for row in rows:
            try:
                player = adapter.map_player(row, detail_cache, src_conn=src_conn)
                if not player:
                    player_stats["skippedRows"] += 1
                    player_stats["missingUsernameRows"] += 1
                    continue
                player_model = NormalizedPlayer.from_mapping(player)
                player_id, status = upsert_player(tgt_conn, config, player_model, args.dry_run, return_status=True)
                player_stats["mappedRows"] += 1
                player_stats["insertAttemptRows"] += 1
                if status in ("inserted", "insertable"):
                    player_stats["insertedRows"] += 1
                else:
                    player_stats["updatedRows"] += 1
                player_map[username_key(player_model.username)] = player_id
                member_id = player_model.external_id
                username = player_model.username

                game_rows = adapter.fetch_member_game_rows(src_conn, member_id, username, args.tx_limit, from_dt, until_dt)
                game_stats["sourceRows"] += len(game_rows)
                mapped_games = []
                for game_row in game_rows:
                    mapped = adapter.map_game_transaction(game_row)
                    if mapped:
                        mapped["player_id"] = player_id
                        mapped_games.append(NormalizedGameTransaction.from_mapping(mapped))
                        game_stats["mappedRows"] += 1
                    else:
                        game_stats["skippedRows"] += 1
                        game_stats["mappingErrorRows"] += 1
                add_insert_stats(game_stats, insert_game_transactions(tgt_conn, config, mapped_games, args.dry_run, paths.game))

                wallet_rows = []
                for kind in ("deposit", "withdrawal"):
                    rows_for_kind = adapter.fetch_member_wallet_rows(src_conn, kind, member_id, args.wallet_limit, from_dt, until_dt)
                    wallet_stats["sourceRows"] += len(rows_for_kind)
                    for wallet_row in rows_for_kind:
                        mapped = adapter.map_wallet(wallet_row, kind)
                        if mapped:
                            mapped["player_id"] = player_id
                            wallet_rows.append(NormalizedWalletTransaction.from_mapping(mapped))
                            wallet_stats["mappedRows"] += 1
                        else:
                            wallet_stats["skippedRows"] += 1
                            wallet_stats["mappingErrorRows"] += 1
                add_insert_stats(wallet_stats, insert_wallet_transactions(tgt_conn, config, wallet_rows, args.dry_run, paths.deposits))
                processed += 1
                if processed % args.commit_every == 0:
                    if args.dry_run:
                        tgt_conn.rollback()
                    else:
                        tgt_conn.commit()
            except Exception as exc:
                player_stats["skippedRows"] += 1
                player_stats["insertErrorRows"] += 1
                trace(f"[MEMBER LOOP ERROR][{config.BRAND_KEY}] sourceId={row.get('id')} error={exc}")
                if not args.dry_run:
                    tgt_conn.rollback()
        last = rows[-1]
        last_data = adapter.as_dict(last.get("data"))
        after_id = str(last["id"])
        after_dt = source_created_value(adapter, last_data, "players")
        set_checkpoint(tgt_conn, config.TARGET_SCHEMA, config.CHECKPOINT_TABLE, config.checkpoint_key("players"), format_checkpoint(after_dt, after_id), args.dry_run)
        if not args.dry_run:
            tgt_conn.commit()
    emit_phase_summary(ctx, args, player_stats)
    emit_phase_summary(ctx, args, game_stats)
    emit_phase_summary(ctx, args, wallet_stats)
    return processed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--brand", required=True, help="Brand key: inplay, inplayv1, instaplay, or 1play")
    parser.add_argument("--migrate-all", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--date-from", type=parse_date_arg, default=None)
    parser.add_argument("--date-to", type=parse_date_arg, default=None)
    parser.add_argument("--batch-size", type=int, default=100, help="Low-memory default for 4GB hosts; increase only after observing free RAM")
    parser.add_argument("--commit-every", type=int, default=500, help="Low-memory default to avoid long transactions and memory pressure")
    parser.add_argument("--resume", action="store_true", default=True)
    parser.add_argument("--no-resume", dest="resume", action="store_false")
    parser.add_argument("--start-after-id", default=None)
    parser.add_argument("--tx-limit", type=int, default=50)
    parser.add_argument("--wallet-limit", type=int, default=50)
    parser.add_argument("--run-recon", action="store_true", help="Run reconciliation after migration. Off by default to protect small EC2 hosts.")
    parser.add_argument("--run-dq", action="store_true", help="Run data-quality checks after migration. Off by default to protect small EC2 hosts.")
    parser.add_argument("--recon-only", action="store_true", help="Run reconciliation only; do not migrate rows.")
    parser.add_argument("--dq-only", action="store_true", help="Run data-quality checks only; do not migrate rows.")
    args = parser.parse_args()

    if not args.migrate_all and not args.recon_only and not args.dq_only:
        raise SystemExit("Use --migrate-all, --recon-only, or --dq-only.")

    adapter, config = load_brand(args.brand)
    paths = make_report_paths(config.BRAND_KEY, args.date_from, args.date_to)
    configure_logging(paths.log)
    initialize_report_files(paths)
    from_dt, until_dt = business_window_bounds(args.date_from, args.date_to, config.BUSINESS_WINDOW_START_HOUR, config.BUSINESS_TZ)
    trace(f"[DATE WINDOW][{config.BUSINESS_TZ_NAME}] source_from_inclusive={from_dt} source_to_exclusive={until_dt}")
    trace(f"[RESOURCE MODE] batch_size={args.batch_size} commit_every={args.commit_every} insert_page_size={config.INSERT_PAGE_SIZE}")
    ctx = MigrationContext(adapter=adapter, config=config, paths=paths, from_dt=from_dt, until_dt=until_dt, dry_run=args.dry_run)

    src = connect(config.SOURCE_DB_NAME, env_prefix="SOURCE")
    tgt = connect(config.TARGET_DB_NAME, env_prefix="TARGET")
    try:
        total = 0
        if args.migrate_all:
            if config.SOURCE_MODE == "member_driven":
                total = process_member_driven_brand(src, tgt, ctx, args)
            elif config.SOURCE_MODE == "flat_table_batch":
                total = process_flat_table_batch_brand(src, tgt, ctx, args)
            else:
                total = process_table_batch_brand(src, tgt, ctx, args)
        if args.run_recon or args.recon_only:
            run_detailed_reconciliation(src, tgt, ctx.adapter, ctx.config, ctx.paths.reconciliation, ctx.from_dt, ctx.until_dt)
        if args.run_dq or args.dq_only:
            run_data_quality_checks(src, tgt, ctx.adapter, ctx.config, ctx.paths.data_quality, ctx.from_dt, ctx.until_dt)
        if args.dry_run:
            tgt.rollback()
        else:
            tgt.commit()
        emit_final_error_summary(config.BRAND, paths)
        trace("\n".join([
            f"[RUN SUMMARY][{config.BRAND}]",
            f"  sourceProcessed : {total}",
            f"  dryRun          : {args.dry_run}",
            f"  reconOnly       : {args.recon_only}",
            f"  dqOnly          : {args.dq_only}",
            "  reports:",
            f"    summary        : {paths.summary}",
            f"    error_summary  : {paths.error_summary}",
            f"    players        : {paths.players}",
            f"    game           : {paths.game}",
            f"    deposits       : {paths.deposits}",
            f"    withdrawals    : {paths.withdrawals}",
            f"    reconciliation : {paths.reconciliation}",
            f"    data_quality   : {paths.data_quality}",
        ]))
    finally:
        try:
            src.close()
        finally:
            tgt.close()


if __name__ == "__main__":
    main()
