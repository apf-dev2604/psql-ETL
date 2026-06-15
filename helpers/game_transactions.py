import uuid
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


def insert_game_transactions(conn, config, rows: List[Dict[str, Any]], dry_run: bool, report_path: Optional[str] = None) -> Tuple[int, int, int]:
    values: List[Tuple[Any, ...]] = []
    skipped = 0
    provider_cache: Dict[str, uuid.UUID] = {}
    gametype_cache: Dict[str, uuid.UUID] = {}
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID] = {}

    for raw_row in rows:
        row = _as_mapping(raw_row)
        if not row.get("external_id") or not row.get("player_id"):
            skipped += 1
            if report_path:
                write_phase_report(report_path, issueType="game_missing_required_field", sourceTable=config.SOURCE_TABLES["game_transactions"], sourceUsername=row.get("username"), sourceId=row.get("source_id"), referenceId=row.get("external_id"), action="skipped", reason="Missing external_id or player_id")
            continue
        provider_id = get_or_create_game_provider(conn, config, row.get("provider_name"), provider_cache, dry_run)
        game_type_id = get_or_create_game_type(conn, config, row.get("game_type"), gametype_cache, dry_run)
        game_id = get_or_create_game_list(conn, config, row.get("game_name"), provider_id, game_type_id, gamelist_cache, dry_run)
        values.append((
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
        ))

    if dry_run:
        return (0, 0, skipped)
    if not values:
        return (0, 0, skipped)

    sql = f"""
    INSERT INTO {table_ref(config.TARGET_SCHEMA, config.GAME_TRANSACTION_TABLE)} (
        "startDateTime", "providerId", "gameId", "gameTypeId", "playerId", "playerUserName", "tableRoomId",
        "sideBetAmount", "betAmount", "validBet", "payoutAmount",
        "PC1", "PC2", "PC3", "PC4", "PC5", "JW1", "JW2", "JW3", "JW4", "JW5",
        "progressionContributionPaid", "seedMoneyWon", "seedMoneyJackpotOver1000", "endDateTime",
        "externalId", "parlay", "betDetails", "betTiming", "brand", "platform", "roundId"
    ) VALUES %s
    ON CONFLICT ("externalId") DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=config.INSERT_PAGE_SIZE)
        inserted = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else len(values)
    duplicates = max(0, len(values) - int(inserted or 0))
    return (int(inserted or 0), duplicates, skipped)
