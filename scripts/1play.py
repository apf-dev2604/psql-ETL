#!/usr/bin/env python3
"""
Migrate 1Play data from iestdl -> iestdbrds

Source (iestdl):
- PlayerDetails1play              (canonical identity: LOGIN_NAME)
- GameTransaction1play            (join key: PLAYER_ACCOUNT = PlayerDetails1play.LOGIN_NAME)
- PlayerCashTransactions1play     (join key: PLAYER_NAME   = PlayerDetails1play.LOGIN_NAME)
- GameProviders1play              (optional seed; we also upsert from tx rows)

Target (iestdbrds):
- playerDetails
- gameProvider
- gameType
- gameList
- gameTransaction
- walletTransaction
- migrationCheckpoint  (platform PK)

Key notes:
- Wallet "PLAYER_NAME" is a LOGIN_NAME-like identifier.
- GameTransaction1play has UNIQUE TRANSACTION_ID; we use that as gameTransaction.externalId.
- PlayerCashTransactions1play has UNIQUE TRANSACTION_ID; we use that as walletTransaction.referenceId.

Checkpointing:
- Uses 3 rows in migrationCheckpoint:
    platform = '1Play_playerDetails'     lastSourceId = last processed PlayerDetails1play.IDX
    platform = '1Play_gameTransaction'   lastSourceId = last processed GameTransaction1play.IDX
    platform = '1Play_walletTransaction' lastSourceId = last processed PlayerCashTransactions1play.IDX

UTC handling:
- Source tables use timestamp without time zone (naive). We treat them as Asia/Manila, then convert to UTC.
  (If your source is already UTC-but-naive, set ASSUME_SOURCE_TZ="UTC" below.)
"""

import os
import re
import uuid
import argparse
import socket
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values, register_uuid

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# If you ever need to flip behavior:
# - "Asia/Manila" (default): treat naive timestamps as PH local time -> convert to UTC
# - "UTC": treat naive timestamps as UTC (no conversion)
ASSUME_SOURCE_TZ = os.getenv("ASSUME_SOURCE_TZ", "Asia/Manila")


# ----------------------------
# Helpers
# ----------------------------

def probe_host_port(host: str, port: int, timeout_sec: int = 5) -> None:
    print(f"Probing TCP {host}:{port} ...", flush=True)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout_sec)
    try:
        s.connect((host, port))
        print("TCP reachable", flush=True)
    except Exception as e:
        print(f"TCP probe failed: {e}", flush=True)
    finally:
        try:
            s.close()
        except Exception:
            pass


def connect(dbname: str):
    host = os.getenv("RDS_HOST", "iest-db-postgresql.cvmg4ca8uhd2.ap-southeast-1.rds.amazonaws.com")
    user = os.getenv("RDS_USER", "root")
    password = os.getenv("RDS_PASSWORD", "2pzm0z0LvJxyfGqDtAp")
    port = int(os.getenv("RDS_PORT", "5432"))

    probe_host_port(host, port, timeout_sec=5)

    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        port=port,
        dbname=dbname,
        connect_timeout=45,
        sslmode=os.getenv("RDS_SSLMODE", "require"),
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    register_uuid(conn)
    conn.autocommit = False
    return conn


def to_decimal_str(x: Any) -> str:
    if x is None:
        return "0"
    if isinstance(x, (int, float)):
        return str(x)
    s = str(x).strip()
    return s if s else "0"


def digits_only(s: str) -> str:
    return "".join(c for c in (s or "") if c.isdigit())


def safe_email(email: Any) -> str:
    e = (str(email or "")).strip().lower()
    e = e.rstrip(".,;:")
    if not e:
        return "unknown@example.com"
    if re.match(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$", e):
        return e
    return "unknown@example.com"


def safe_mobile_10(mobile: Any) -> str:
    d = digits_only(str(mobile or ""))
    if len(d) >= 10:
        return d[-10:]
    return "0000000000"


def to_timestamptz(dt: Any) -> Optional[datetime]:
    """
    Intentionally NO timezone conversion.
    Raw iestdl timestamps are stored as-is in iestdbrds.
    """
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt
    try:
        return datetime.fromisoformat(str(dt))
    except Exception:
        return None


def normalize_game_type(raw: Optional[str]) -> str:
    if not raw:
        return "Slots"
    r = raw.strip().upper()
    if r in ("SLOTS", "SLOT"):
        return "Slots"
    if r in ("LIVE", "LIVE_CASINO", "CASINO"):
        return "Live"
    if r in ("SPORTS", "SPORT"):
        return "Sports"
    if r in ("FISHING",):
        return "Fishing"
    if r in ("ARCADE",):
        return "Arcade"
    return raw.strip().title()


def normalize_wallet_type(raw: Any) -> str:
    s = (str(raw or "").strip() or "unknown").lower()
    s = re.sub(r"\s+", "_", s)
    return s

def ensure_outlet_1play(
    tgt_conn,
    outlet_code: str,            # outlet_code: Outlet1Play.OUTLET_CODE -> outletList.outletCode
    outlet_name: str,            # outlet_name: Outlet1Play.SITE_NAME   -> outletList.outletName (recommended)
    site_name: Optional[str],    # Outlet1Play.SITE_NAME -> outletList.operator
    created_at: Optional[datetime],
    dry_run: bool,
) -> None:
    if not outlet_code:
        return

    outlet_code = str(outlet_code).strip()
    if not outlet_code:
        return

    outlet_name = (str(outlet_name).strip() if outlet_name is not None else "")
    operator = (str(site_name).strip() if site_name is not None else "")

    # Required NOT NULL fields in outletList that we don't have: store empty strings
    street = ""
    brgy = ""
    city = ""
    prov = ""

    if dry_run:
        return

    with tgt_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO "outletList" (
                "outletCode",
                "outletName",
                "streetAddress",
                "barangayAddress",
                "cityAddress",
                "provinceAddress",
                "outletShare",
                "operator",
                "isActive",
                "lastUpdateDatetime",
                "createdAt",
                "updatedAt",
                "brand"
            )
            VALUES (%s,%s,%s,%s,%s,%s,0.00,%s,true,now(),COALESCE(%s, now()),now(),%s)
            ON CONFLICT ("outletCode") DO UPDATE SET
                "outletName" = EXCLUDED."outletName",
                "operator" = EXCLUDED."operator",
                "brand" = EXCLUDED."brand",
                "updatedAt" = now(),
                "lastUpdateDatetime" = now()
            """,
            (
                outlet_code,
                outlet_name or outlet_code,  # outletName cannot be NULL/blank? still allowed, but keep sane fallback
                street,
                brgy,
                city,
                prov,
                operator,
                created_at,
                "1Play",
            ),
        )

# ----------------------------
# Checkpointing
# ----------------------------

def ck_platform_key(phase: str) -> str:
    return f"1Play_{phase}"


def checkpoint_get(tgt_conn, phase: str) -> Optional[str]:
    key = ck_platform_key(phase)
    with tgt_conn.cursor() as cur:
        cur.execute(
            'SELECT "lastSourceId" FROM "migrationCheckpoint" WHERE platform=%s',
            (key,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def checkpoint_set(tgt_conn, phase: str, last_source_id: str, dry_run: bool) -> None:
    if dry_run:
        return
    key = ck_platform_key(phase)
    with tgt_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO "migrationCheckpoint" (platform, "lastSourceId", "updatedAt")
            VALUES (%s, %s, now())
            ON CONFLICT (platform) DO UPDATE SET
              "lastSourceId" = EXCLUDED."lastSourceId",
              "updatedAt" = now()
            """,
            (key, str(last_source_id)),
        )


# ----------------------------
# Target: dimension upserts (cached)
# ----------------------------

def get_or_create_game_provider(
    tgt_conn,
    provider_name: str,
    cache: Dict[str, uuid.UUID],
    dry_run: bool
) -> uuid.UUID:
    name = (provider_name or "UNKNOWN").strip().upper() or "UNKNOWN"
    if name in cache:
        return cache[name]

    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute('SELECT id FROM "gameProvider" WHERE "gameProvider"=%s', (name,))
            row = cur.fetchone()
            gid = row[0] if row else uuid.uuid4()
            cache[name] = gid
            return gid

    sql = """
    INSERT INTO "gameProvider" ("gameProvider","isActive","createdAt","updatedAt")
    VALUES (%s, true, now(), now())
    ON CONFLICT ("gameProvider") DO UPDATE SET "updatedAt"=now()
    RETURNING id
    """
    with tgt_conn.cursor() as cur:
        cur.execute(sql, (name,))
        gid = cur.fetchone()[0]
        cache[name] = gid
        return gid


def get_or_create_game_type(
    tgt_conn,
    game_type: str,
    cache: Dict[str, uuid.UUID],
    dry_run: bool
) -> uuid.UUID:
    gt = normalize_game_type(game_type)
    if gt in cache:
        return cache[gt]

    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute('SELECT id FROM "gameType" WHERE "gameType"=%s', (gt,))
            row = cur.fetchone()
            gid = row[0] if row else uuid.uuid4()
            cache[gt] = gid
            return gid

    sql = """
    INSERT INTO "gameType" ("gameType","isActive","createdAt","updatedAt")
    VALUES (%s, true, now(), now())
    ON CONFLICT ("gameType") DO UPDATE SET "updatedAt"=now()
    RETURNING id
    """
    with tgt_conn.cursor() as cur:
        cur.execute(sql, (gt,))
        gid = cur.fetchone()[0]
        cache[gt] = gid
        return gid


def get_or_create_game_list(
    tgt_conn,
    game_name: str,
    provider_id: uuid.UUID,
    game_type_id: uuid.UUID,
    cache: Dict[Tuple[uuid.UUID, str], uuid.UUID],
    dry_run: bool
) -> uuid.UUID:
    gname = (game_name or "UNKNOWN").strip() or "UNKNOWN"
    key = (provider_id, gname)
    if key in cache:
        return cache[key]

    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute(
                'SELECT id FROM "gameList" WHERE "gameProviderId"=%s AND "gameName"=%s',
                (provider_id, gname),
            )
            row = cur.fetchone()
            gid = row[0] if row else uuid.uuid4()
            cache[key] = gid
            return gid

    sql = """
    INSERT INTO "gameList" (
        "gameTypeId","gameProviderId","gameName",
        "isProgressive","isActive","createdAt","updatedAt"
    )
    VALUES (%s,%s,%s,false,true,now(),now())
    ON CONFLICT ("gameProviderId","gameName") DO UPDATE SET
        "gameTypeId"=EXCLUDED."gameTypeId",
        "updatedAt"=now()
    RETURNING id
    """
    with tgt_conn.cursor() as cur:
        cur.execute(sql, (game_type_id, provider_id, gname))
        gid = cur.fetchone()[0]
        cache[key] = gid
        return gid


# ----------------------------
# Target: player upsert (NEW builder)
# ----------------------------

def upsert_player_1play(
    tgt_conn,
    login_name: str,
    src_row: Dict[str, Any],
    dry_run: bool
) -> uuid.UUID:
    login_name = (login_name or "").strip()

    # Prefer REG for split name; fallback details; fallback login
    reg_full_name = (src_row.get("REG_FULL_NAME") or "").strip()
    details_player_name = (src_row.get("DETAILS_PLAYER_NAME") or "").strip()
    name_for_split = reg_full_name or details_player_name or login_name

    parts = [p for p in name_for_split.split() if p.strip()]
    if len(parts) == 0:
        first, middle, last = "", "", ""
    elif len(parts) == 1:
        first, middle, last = parts[0], "", ""
    elif len(parts) == 2:
        first, middle, last = parts[0], "", parts[1]
    else:
        first, middle, last = parts[0], " ".join(parts[1:-1]), parts[-1]

    # If anything ever yields "Unknown", store empty string instead
    if last.strip().upper() == "UNKNOWN":
        last = ""

    # Mobile: prefer details.mobile; fallback reg.phone
    mobile_raw = src_row.get("DETAILS_MOBILE_NUMBER")
    if mobile_raw is None or str(mobile_raw).strip() == "":
        mobile_raw = src_row.get("REG_PHONE_NUMBER")
    mobile_10 = safe_mobile_10(mobile_raw)

    # Email: prefer details.email; fallback reg.email
    raw_email = src_row.get("DETAILS_EMAIL")
    if raw_email is None or str(raw_email).strip() == "":
        raw_email = src_row.get("REG_EMAIL")
    email = safe_email(raw_email)

    # Dates: registrationDate prefer reg.registration_date; fallback details.start_date; else now()
    reg_dt = (
        to_timestamptz(src_row.get("REGISTRATION_DATE"))
        or to_timestamptz(src_row.get("DETAILS_START_DATE"))
        or datetime.now(timezone.utc)
    )
    last_login = to_timestamptz(src_row.get("DETAILS_LAST_LOGIN_DATE"))

    # last_login_ip (strip /mask)
    last_ip_raw = src_row.get("DETAILS_LAST_IP")
    last_login_ip = None
    if last_ip_raw is not None:
        s = str(last_ip_raw).strip()
        if s:
            last_login_ip = s.split("/")[0]

    # Verification
    ver = (str(src_row.get("DETAILS_VERIFICATION_STATUS") or "")).strip().upper()
    is_verified = ver in ("VERIFIED", "APPROVED")

    # Active (either details or reg says ACTIVE)
    det_status = (str(src_row.get("DETAILS_STATUS") or "")).strip().upper()
    reg_status = (str(src_row.get("REG_STATUS") or "")).strip().upper()
    is_active = (det_status == "ACTIVE") or (reg_status == "ACTIVE")

    # Address policy: write raw iestdl address into addressProvince ONLY (no parsing).
    addr_raw = (src_row.get("DETAILS_ADDRESS") or "").strip()
    if not addr_raw:
        addr_raw = (src_row.get("DETAILS_PERMANENT_ADDRESS") or "").strip()

    address_street = ""
    address_barangay = ""
    address_city = ""
    address_province = addr_raw or "N/A"  # target requires NOT NULL

    # incomeSource/industry: keep "N/A" if missing (target requires NOT NULL)
    income_source = (str(src_row.get("DETAILS_INCOME") or "")).strip() or "N/A"
    industry = (str(src_row.get("DETAILS_INDUSTRY") or "")).strip() or "N/A"

    # Birthdate
    dob = to_timestamptz(src_row.get("DETAILS_DATE_OF_BIRTH"))
    birthdate = dob.date() if dob else None

    # Outlet mapping:
    # join key = first 7 chars of LOGIN_NAME (-> Outlet1Play.IW1P_CODE)
    # store     = Outlet1Play.OUTLET_CODE into playerDetails.outletCode
    outlet_code = (src_row.get("OUTLET_CODE") or "").strip() or None            # ✅ real outlet code
    outlet_name = (src_row.get("OUTLET_NAME") or "").strip() or outlet_code     # SITE_NAME preferred as display name
    site_name = (src_row.get("SITE_NAME") or "").strip() or None
    created_at = to_timestamptz(src_row.get("OUTLET_CREATED_AT"))

    if outlet_code:
        ensure_outlet_1play(
            tgt_conn=tgt_conn,
            outlet_code=outlet_code,            # ✅ outletList.outletCode
            outlet_name=outlet_name or "",      # outletList.outletName
            site_name=site_name,                # outletList.operator (if you still want this)
            created_at=created_at,
            dry_run=dry_run
        )

    # externalId policy: username
    external_id = login_name

    wallet_balance = to_decimal_str(src_row.get("REG_BALANCE"))

    params = (
        first, middle, last,
        mobile_10,
        email,
        login_name,
        reg_dt,
        "1Play",
        is_verified,
        is_active,
        last_login,
        last_login_ip,
        outlet_code,
        address_street, address_barangay, address_city, address_province,
        income_source, industry,
        wallet_balance,
        external_id,
        birthdate,
    )

    sql = """
    INSERT INTO "playerDetails" (
        "firstName","middleName","lastName",
        "mobileNumber","mobileNumberVerified",
        "emailAddress","emailVerified",
        "userName",
        "registrationDate","registrationIp","registrationReferrer",
        "brandName",
        "isVerified","isBlocked","blockedDatetime","isActive",
        "lastLogin","lastLoginIp",
        "outletCode","affiliateCode",
        "addressStreet","addressBarangay","addressCity","addressProvince",
        "incomeSource","industry",
        "walletBalance",
        "externalId",
        "birthdate",
        "createdAt","updatedAt"
    )
    VALUES (
        %s,%s,%s,
        %s,false,
        %s,false,
        %s,
        %s,NULL,NULL,
        %s,
        %s,false,NULL,%s,
        %s,%s,
        %s,NULL,
        %s,%s,%s,%s,
        %s,%s,
        %s,
        %s,
        %s,
        now(),now()
    )
    ON CONFLICT ("userName") DO UPDATE SET
        "firstName"=EXCLUDED."firstName",
        "middleName"=EXCLUDED."middleName",
        "lastName"=EXCLUDED."lastName",
        "mobileNumber"=EXCLUDED."mobileNumber",
        "emailAddress"=EXCLUDED."emailAddress",
        "brandName"=EXCLUDED."brandName",
        "isVerified"=EXCLUDED."isVerified",
        "isActive"=EXCLUDED."isActive",
        "lastLogin"=EXCLUDED."lastLogin",
        "lastLoginIp"=EXCLUDED."lastLoginIp",
        "outletCode"=EXCLUDED."outletCode",
        "addressStreet"=EXCLUDED."addressStreet",
        "addressBarangay"=EXCLUDED."addressBarangay",
        "addressCity"=EXCLUDED."addressCity",
        "addressProvince"=EXCLUDED."addressProvince",
        "incomeSource"=EXCLUDED."incomeSource",
        "industry"=EXCLUDED."industry",
        "walletBalance"=EXCLUDED."walletBalance",
        "externalId"=EXCLUDED."externalId",
        "birthdate"=EXCLUDED."birthdate",
        "updatedAt"=now()
    RETURNING id
    """

    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute('SELECT id FROM "playerDetails" WHERE "userName"=%s', (login_name,))
            row = cur.fetchone()
            return row[0] if row else uuid.uuid4()

    with tgt_conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def build_player_map(tgt_conn) -> Dict[str, uuid.UUID]:
    m: Dict[str, uuid.UUID] = {}
    with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, "userName"
            FROM "playerDetails"
            WHERE "brandName"='1Play'
            """
        )
        for r in cur.fetchall():
            m[str(r["userName"])] = r["id"]
    return m


# ----------------------------
# Source fetchers
# ----------------------------

def fetch_player_details_batch(src_conn, after_idx: int, batch_size: int, max_rows: Optional[int]) -> List[Dict[str, Any]]:
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
              pd."IDX"                       AS "IDX",
              pd."LOGIN_NAME"                AS "LOGIN_NAME",

              -- Details (PlayerDetails1play)
              pd."PLAYER_NAME"               AS "DETAILS_PLAYER_NAME",
              pd."START_DATE"                AS "DETAILS_START_DATE",
              pd."LAST_LOGIN_DATE"           AS "DETAILS_LAST_LOGIN_DATE",
              pd."LAST_IP"                   AS "DETAILS_LAST_IP",
              pd."STATUS"                    AS "DETAILS_STATUS",
              pd."MOBILE_NUMBER"             AS "DETAILS_MOBILE_NUMBER",
              pd."EMAIL"                     AS "DETAILS_EMAIL",
              pd."DATE_OF_BIRTH"             AS "DETAILS_DATE_OF_BIRTH",
              pd."VERIFICATION_STATUS"       AS "DETAILS_VERIFICATION_STATUS",
              pd."VERIFICATION_DATE"         AS "DETAILS_VERIFICATION_DATE",
              pd."ADDRESS"                   AS "DETAILS_ADDRESS",
              pd."PERMANENT_ADDRESS"         AS "DETAILS_PERMANENT_ADDRESS",
              pd."INCOME"                    AS "DETAILS_INCOME",
              pd."INDUSTRY"                  AS "DETAILS_INDUSTRY",

              -- Registration (PlayerRegistrations1play)
              pr."NAME"                      AS "REG_FULL_NAME",
              pr."PHONE_NUMBER"              AS "REG_PHONE_NUMBER",
              pr."EMAIL"                     AS "REG_EMAIL",
              pr."STATUS"                    AS "REG_STATUS",
              pr."REGISTRATION_DATE"         AS "REGISTRATION_DATE",
              pr."BALANCE"                   AS "REG_BALANCE",

              -- Outlet (Outlet1Play): match first 7 chars of username -> IW1P_CODE
              o."IW1P_CODE"      AS "OUTLET_IW1P_CODE",  -- keep for debugging if you want
              o."OUTLET_CODE"    AS "OUTLET_CODE",       -- ✅ this is the real outlet code to store
              o."SITE_NAME"      AS "OUTLET_NAME",       -- use SITE_NAME as display name (or swap if you prefer)
              o."SITE_NAME"      AS "SITE_NAME",
              o."DATE_CREATED"   AS "OUTLET_CREATED_AT"

            FROM "PlayerDetails1play" pd
            LEFT JOIN "PlayerRegistrations1play" pr
              ON pr."PLAYER_NAME" = pd."LOGIN_NAME"
            LEFT JOIN "Outlet1Play" o
              ON o."IW1P_CODE" = LEFT(pd."LOGIN_NAME", 7)

            WHERE pd."IDX" > %s
            ORDER BY pd."IDX"
            LIMIT %s
            """,
            (after_idx, max_rows if max_rows is not None else batch_size),
        )
        rows = cur.fetchall()
    # Keep source reads in short transactions during long-running migrations.
    src_conn.rollback()
    return rows


def fetch_game_tx_batch(src_conn, after_idx: int, batch_size: int, max_rows: Optional[int]) -> List[Dict[str, Any]]:
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
              "IDX","GAME_PROVIDER","TRANSACTION_ID","SESSION_ID","GAME_DATE","OUTLET",
              "PLAYER_ACCOUNT","GAME_NAME",
              "TOTAL_STAKES","TOTAL_WINS",
              "PC1","PC2","PC3","PC4","PC5",
              "JW1","JW2","JW3","JW4","JW5",
              "UPDATE_DATE_TIME",
              "PROGRESSIVE_CONTRIBUTION_PAID","SEED_MONEY_WON","SEED_MONEY_JACKPOT_WON_OVER_1000"
            FROM "GameTransaction1play"
            WHERE "IDX" > %s
            ORDER BY "IDX"
            LIMIT %s
            """,
            (after_idx, max_rows if max_rows is not None else batch_size),
        )
        rows = cur.fetchall()
    src_conn.rollback()
    return rows


def fetch_wallet_batch(src_conn, after_idx: int, batch_size: int, max_rows: Optional[int]) -> List[Dict[str, Any]]:
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
              "IDX","PLAYER_NAME","TRANSACTION_DATE","TRANSACTION_ID","TRANSACTION_TYPE",
              "AMOUNT","BONUS","FEE",
              "BANK","ACCOUNT_NAME","ACCOUNT_NUMBER",
              "IP"
            FROM "PlayerCashTransactions1play"
            WHERE "IDX" > %s
            ORDER BY "IDX"
            LIMIT %s
            """,
            (after_idx, max_rows if max_rows is not None else batch_size),
        )
        rows = cur.fetchall()
    src_conn.rollback()
    return rows


# ----------------------------
# Target inserts (bulk)
# ----------------------------

def insert_game_transactions_batch(
    tgt_conn,
    rows: List[Dict[str, Any]],
    player_map: Dict[str, uuid.UUID],
    provider_cache: Dict[str, uuid.UUID],
    gametype_cache: Dict[str, uuid.UUID],
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID],
    dry_run: bool
) -> Tuple[int, int, int]:
    values: List[Tuple[Any, ...]] = []
    missing_player = 0

    game_type_id = get_or_create_game_type(tgt_conn, "Slots", gametype_cache, dry_run)

    for r in rows:
        external_id = str(r.get("TRANSACTION_ID") or "").strip()
        if not external_id:
            continue

        login = str(r.get("PLAYER_ACCOUNT") or "").strip()
        player_id = player_map.get(login)
        if not player_id:
            missing_player += 1
            continue

        provider_name = str(r.get("GAME_PROVIDER") or "UNKNOWN")
        provider_id = get_or_create_game_provider(tgt_conn, provider_name, provider_cache, dry_run)

        game_name = str(r.get("GAME_NAME") or "UNKNOWN")
        game_id = get_or_create_game_list(tgt_conn, game_name, provider_id, game_type_id, gamelist_cache, dry_run)

        game_dt = to_timestamptz(r.get("GAME_DATE")) or datetime.now(timezone.utc)

        total_stakes = to_decimal_str(r.get("TOTAL_STAKES"))
        total_wins = to_decimal_str(r.get("TOTAL_WINS"))

        pc1 = to_decimal_str(r.get("PC1"))
        pc2 = to_decimal_str(r.get("PC2"))
        pc3 = to_decimal_str(r.get("PC3"))
        pc4 = to_decimal_str(r.get("PC4"))
        pc5 = to_decimal_str(r.get("PC5"))

        jw1 = to_decimal_str(r.get("JW1"))
        jw2 = to_decimal_str(r.get("JW2"))
        jw3 = to_decimal_str(r.get("JW3"))
        jw4 = to_decimal_str(r.get("JW4"))
        jw5 = to_decimal_str(r.get("JW5"))

        # Mapping you asked about:
        # - payoutAmount <- TOTAL_WINS
        # - progressionContributionPaid <- PROGRESSIVE_CONTRIBUTION_PAID
        prog_paid = to_decimal_str(r.get("PROGRESSIVE_CONTRIBUTION_PAID"))
        seed_won = to_decimal_str(r.get("SEED_MONEY_WON"))

        over_1000_raw = r.get("SEED_MONEY_JACKPOT_WON_OVER_1000")
        try:
            over_1000 = 1 if (over_1000_raw is not None and float(over_1000_raw) > 0) else 0
        except Exception:
            over_1000 = 0

        session_id = r.get("SESSION_ID")
        outlet = r.get("OUTLET")

        values.append((
            game_dt,                 # startDateTime
            provider_id,
            game_id,
            game_type_id,
            player_id,
            login,                   # playerUserName
            str(outlet) if outlet is not None else None,   # tableRoomId
            "0",                     # sideBetAmount
            total_stakes,            # betAmount
            total_stakes,            # validBet
            total_wins,              # payoutAmount
            pc1, pc2, pc3, pc4, pc5,
            jw1, jw2, jw3, jw4, jw5,
            prog_paid,
            seed_won,
            over_1000,
            game_dt,                 # endDateTime
            external_id,             # externalId
            False,                   # parlay
            None,                    # betDetails
            None,                    # betTiming
            "1Play",                 # brand
            "Online",                # platform
            str(session_id) if session_id is not None else None,  # roundId
        ))

    if dry_run:
        return (0, len(values), missing_player)

    if not values:
        return (0, 0, missing_player)

    sql = """
    INSERT INTO "gameTransaction" (
        "startDateTime",
        "providerId",
        "gameId",
        "gameTypeId",
        "playerId",
        "playerUserName",
        "tableRoomId",
        "sideBetAmount",
        "betAmount",
        "validBet",
        "payoutAmount",
        "PC1","PC2","PC3","PC4","PC5",
        "JW1","JW2","JW3","JW4","JW5",
        "progressionContributionPaid",
        "seedMoneyWon",
        "seedMoneyJackpotOver1000",
        "endDateTime",
        "externalId",
        "parlay",
        "betDetails",
        "betTiming",
        "brand",
        "platform",
        "roundId"
    )
    VALUES %s
    ON CONFLICT ("externalId") DO NOTHING
    """
    with tgt_conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=1000)

    return (len(values), 0, missing_player)


def insert_wallet_transactions_batch(
    tgt_conn,
    rows: List[Dict[str, Any]],
    player_map: Dict[str, uuid.UUID],
    dry_run: bool
) -> Tuple[int, int, int]:
    values: List[Tuple[Any, ...]] = []
    missing_player = 0

    for r in rows:
        login = str(r.get("PLAYER_NAME") or "").strip()
        player_id = player_map.get(login)
        if not player_id:
            missing_player += 1
            continue

        created_dt = to_timestamptz(r.get("TRANSACTION_DATE")) or datetime.now(timezone.utc)

        tx_type = normalize_wallet_type(r.get("TRANSACTION_TYPE"))
        amount = to_decimal_str(r.get("AMOUNT"))

        # paymentGateway rules:
        # - NULL -> ""
        # - empty string -> ""
        # - "N/A" stays "N/A"
        raw_bank = r.get("BANK")
        if raw_bank is None:
            bank = ""
        else:
            bank = str(raw_bank).strip()

        ref = r.get("TRANSACTION_ID")
        reference_id = (str(ref).strip() if ref is not None else None) or None

        status = "confirmed"

        values.append((
            tx_type,
            "1Play",          # platform
            player_id,
            bank,             # paymentGateway
            "1Play",          # domain
            amount,
            status,
            None,             # bettingPhase
            created_dt,       # createdDatetime
            created_dt,       # confirmedDatetime
            None,             # cancelledDatetime
            None,             # failedDatetime
            reference_id,
        ))

    if dry_run:
        return (0, len(values), missing_player)

    if not values:
        return (0, 0, missing_player)

    sql = """
    INSERT INTO "walletTransaction" (
        "transactionType",
        "platform",
        "playerId",
        "paymentGateway",
        "domain",
        "amount",
        "status",
        "bettingPhase",
        "createdDatetime",
        "confirmedDatetime",
        "cancelledDatetime",
        "failedDatetime",
        "referenceId"
    )
    VALUES %s
    ON CONFLICT ("platform", "referenceId")
    WHERE ("platform" = '1Play' AND "referenceId" IS NOT NULL)
    DO NOTHING
    """
    with tgt_conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=1000)
        inserted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    skipped_conflict = len(values) - inserted
    return (inserted, skipped_conflict, missing_player)


# ----------------------------
# Delete-first
# ----------------------------

def _parse_date_arg(s: str) -> datetime:
    """Parse YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS into a UTC-aware datetime (for CLI args)."""
    s = s.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    import argparse as _ap
    raise _ap.ArgumentTypeError(
        f"Invalid date: {s!r}. Expected YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS (interpreted as UTC)"
    )


def delete_1play_target_data(
    tgt_conn,
    dry_run: bool,
    keep_from: Optional[datetime] = None,
    keep_to: Optional[datetime] = None,
) -> None:
    """
    Delete all 1Play data from the target DB.

    If keep_from / keep_to are provided, records whose date falls within
    [keep_from, keep_to] are preserved (everything outside that window is deleted).
    Checkpoints are only deleted when no date range is specified (full wipe).
    """
    has_range = keep_from is not None or keep_to is not None

    def _exclusion(col: str, params: list) -> str:
        if not has_range:
            return ""
        parts: List[str] = []
        if keep_from is not None:
            parts.append(f'"{col}" < %s')
            params.append(keep_from)
        if keep_to is not None:
            parts.append(f'"{col}" > %s')
            params.append(keep_to)
        return " AND (" + " OR ".join(parts) + ")"

    if dry_run:
        msg = "[DRY-RUN] would delete target 1Play rows (gameTransaction, walletTransaction, playerDetails)"
        if has_range:
            msg = f"[DRY-RUN] would delete 1Play data (keeping records between {keep_from} and {keep_to})"
        print(msg)
        return

    with tgt_conn.cursor() as cur:
        gt_params: List[Any] = ["1Play"]
        cur.execute(
            f'DELETE FROM "gameTransaction" WHERE "brand"=%s{_exclusion("startDateTime", gt_params)}',
            gt_params,
        )
        gt = cur.rowcount

        wt_params: List[Any] = ["1Play"]
        cur.execute(
            f'DELETE FROM "walletTransaction" WHERE "platform"=%s{_exclusion("createdDatetime", wt_params)}',
            wt_params,
        )
        wt = cur.rowcount

        pd_params: List[Any] = ["1Play"]
        cur.execute(
            f'DELETE FROM "playerDetails" WHERE "brandName"=%s{_exclusion("registrationDate", pd_params)}',
            pd_params,
        )
        pl = cur.rowcount

        if not has_range:
            cur.execute('DELETE FROM "migrationCheckpoint" WHERE platform LIKE %s', ("1Play_%",))
            ck = cur.rowcount
        else:
            ck = 0

    tgt_conn.commit()
    print(f"Deleted: gameTransaction={gt}, walletTransaction={wt}, playerDetails={pl}, checkpoints={ck}")


# ----------------------------
# Single user migration
# ----------------------------

def migrate_single_user(src_conn, tgt_conn, username: str, dry_run: bool) -> None:
    username = username.strip()

    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
              pd."IDX"                       AS "IDX",
              pd."LOGIN_NAME"                AS "LOGIN_NAME",

              pd."PLAYER_NAME"               AS "DETAILS_PLAYER_NAME",
              pd."START_DATE"                AS "DETAILS_START_DATE",
              pd."LAST_LOGIN_DATE"           AS "DETAILS_LAST_LOGIN_DATE",
              pd."LAST_IP"                   AS "DETAILS_LAST_IP",
              pd."STATUS"                    AS "DETAILS_STATUS",
              pd."MOBILE_NUMBER"             AS "DETAILS_MOBILE_NUMBER",
              pd."EMAIL"                     AS "DETAILS_EMAIL",
              pd."DATE_OF_BIRTH"             AS "DETAILS_DATE_OF_BIRTH",
              pd."VERIFICATION_STATUS"       AS "DETAILS_VERIFICATION_STATUS",
              pd."VERIFICATION_DATE"         AS "DETAILS_VERIFICATION_DATE",
              pd."ADDRESS"                   AS "DETAILS_ADDRESS",
              pd."PERMANENT_ADDRESS"         AS "DETAILS_PERMANENT_ADDRESS",
              pd."INCOME"                    AS "DETAILS_INCOME",
              pd."INDUSTRY"                  AS "DETAILS_INDUSTRY",

              pr."NAME"                      AS "REG_FULL_NAME",
              pr."PHONE_NUMBER"              AS "REG_PHONE_NUMBER",
              pr."EMAIL"                     AS "REG_EMAIL",
              pr."STATUS"                    AS "REG_STATUS",
              pr."REGISTRATION_DATE"         AS "REGISTRATION_DATE",
              pr."BALANCE"                   AS "REG_BALANCE",

              o."IW1P_CODE"    AS "OUTLET_IW1P_CODE",
              o."OUTLET_CODE"  AS "OUTLET_CODE",
              o."SITE_NAME"    AS "OUTLET_NAME",
              o."DATE_CREATED" AS "OUTLET_CREATED_AT"

            FROM "PlayerDetails1play" pd
            LEFT JOIN "PlayerRegistrations1play" pr
              ON pr."PLAYER_NAME" = pd."LOGIN_NAME"
            LEFT JOIN "Outlet1Play" o
              ON o."IW1P_CODE" = LEFT(pd."LOGIN_NAME", 7)
            WHERE pd."LOGIN_NAME"=%s
            LIMIT 1
            """,
            (username,),
        )
        player_row = cur.fetchone()

    if not player_row:
        raise RuntimeError(f"No PlayerDetails1play found for LOGIN_NAME={username}")

    player_id = upsert_player_1play(tgt_conn, username, player_row, dry_run=dry_run)
    print(f"playerDetails upserted: userName={username} id={player_id}")

    player_map = build_player_map(tgt_conn) if not dry_run else {username: player_id}

    provider_cache: Dict[str, uuid.UUID] = {}
    gametype_cache: Dict[str, uuid.UUID] = {}
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID] = {}

    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
              "IDX","GAME_PROVIDER","TRANSACTION_ID","SESSION_ID","GAME_DATE","OUTLET",
              "PLAYER_ACCOUNT","GAME_NAME",
              "TOTAL_STAKES","TOTAL_WINS",
              "PC1","PC2","PC3","PC4","PC5",
              "JW1","JW2","JW3","JW4","JW5",
              "PROGRESSIVE_CONTRIBUTION_PAID","SEED_MONEY_WON","SEED_MONEY_JACKPOT_WON_OVER_1000"
            FROM "GameTransaction1play"
            WHERE "PLAYER_ACCOUNT"=%s
            ORDER BY "IDX"
            """,
            (username,),
        )
        tx_rows = cur.fetchall()

    ins, skip, missing = insert_game_transactions_batch(
        tgt_conn, tx_rows, player_map, provider_cache, gametype_cache, gamelist_cache, dry_run=dry_run
    )
    print(f"gameTransaction: attempted={ins or skip} missing_player={missing}")

    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
              "IDX","PLAYER_NAME","TRANSACTION_DATE","TRANSACTION_ID","TRANSACTION_TYPE",
              "AMOUNT","BANK"
            FROM "PlayerCashTransactions1play"
            WHERE "PLAYER_NAME"=%s
            ORDER BY "IDX"
            """,
            (username,),
        )
        w_rows = cur.fetchall()

    ins2, skip2, missing2 = insert_wallet_transactions_batch(tgt_conn, w_rows, player_map, dry_run=dry_run)
    print(f"walletTransaction: attempted={ins2 or skip2} missing_player={missing2}")

    if dry_run:
        tgt_conn.rollback()
        print("[DRY-RUN] rolled back.")
    else:
        tgt_conn.commit()
        print("Committed.")


# ----------------------------
# Migrate-all (phased, checkpointed)
# ----------------------------

def migrate_all(
    src_conn,
    tgt_conn,
    dry_run: bool,
    batch_size: int,
    commit_every: int,
    resume: bool,
    start_after_id: Optional[int],
    max_members: Optional[int],
) -> int:
    provider_cache: Dict[str, uuid.UUID] = {}
    gametype_cache: Dict[str, uuid.UUID] = {}
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID] = {}

    # Phase 1: Players
    phase = "playerDetails"
    if start_after_id is not None:
        after = int(start_after_id)
    elif resume:
        ck = checkpoint_get(tgt_conn, phase)
        after = int(ck) if ck and str(ck).isdigit() else 0
    else:
        after = 0

    processed_players = 0
    last_idx = after

    while True:
        remaining = None
        if max_members is not None:
            remaining = max_members - processed_players
            if remaining <= 0:
                break

        rows = fetch_player_details_batch(
            src_conn,
            last_idx,
            batch_size,
            remaining if remaining is not None and remaining < batch_size else None
        )
        if not rows:
            break

        for r in rows:
            login = str(r.get("LOGIN_NAME") or "").strip()
            if not login:
                last_idx = int(r["IDX"])
                continue

            _ = upsert_player_1play(tgt_conn, login, r, dry_run=dry_run)
            processed_players += 1
            last_idx = int(r["IDX"])

            if (not dry_run) and (processed_players % commit_every == 0):
                checkpoint_set(tgt_conn, phase, str(last_idx), dry_run=dry_run)
                tgt_conn.commit()
                print(f"Progress players: processed={processed_players} lastIDX={last_idx}", flush=True)

    if not dry_run:
        checkpoint_set(tgt_conn, phase, str(last_idx), dry_run=dry_run)
        tgt_conn.commit()
        print(f"Completed players phase. processed={processed_players} lastIDX={last_idx}", flush=True)

    player_map = build_player_map(tgt_conn)

    # Phase 2: GameTransaction
    phase = "gameTransaction"
    if start_after_id is not None:
        after = int(start_after_id)
    elif resume:
        ck = checkpoint_get(tgt_conn, phase)
        after = int(ck) if ck and str(ck).isdigit() else 0
    else:
        after = 0

    processed_gt = 0
    missing_player_gt = 0
    last_idx = after

    while True:
        rows = fetch_game_tx_batch(src_conn, last_idx, batch_size, None)
        if not rows:
            break

        _, _, missing = insert_game_transactions_batch(
            tgt_conn, rows, player_map,
            provider_cache, gametype_cache, gamelist_cache,
            dry_run=dry_run
        )
        processed_gt += len(rows)
        missing_player_gt += missing
        last_idx = int(rows[-1]["IDX"])

        if not dry_run:
            checkpoint_set(tgt_conn, phase, str(last_idx), dry_run=dry_run)
            if (processed_gt % commit_every) < batch_size:
                tgt_conn.commit()
                print(f"Progress gameTx: processed={processed_gt} lastIDX={last_idx} missingPlayer={missing_player_gt}", flush=True)

    if not dry_run:
        tgt_conn.commit()
        print(f"Completed gameTx phase. processed={processed_gt} lastIDX={last_idx} missingPlayer={missing_player_gt}", flush=True)

    # Phase 3: WalletTransaction
    phase = "walletTransaction"
    if start_after_id is not None:
        after = int(start_after_id)
    elif resume:
        ck = checkpoint_get(tgt_conn, phase)
        after = int(ck) if ck and str(ck).isdigit() else 0
    else:
        after = 0

    processed_wt = 0
    missing_player_wt = 0
    last_idx = after

    while True:
        rows = fetch_wallet_batch(src_conn, last_idx, batch_size, None)
        if not rows:
            break

        _, _, missing = insert_wallet_transactions_batch(
            tgt_conn, rows, player_map, dry_run=dry_run
        )
        processed_wt += len(rows)
        missing_player_wt += missing
        last_idx = int(rows[-1]["IDX"])

        if not dry_run:
            checkpoint_set(tgt_conn, phase, str(last_idx), dry_run=dry_run)
            if (processed_wt % commit_every) < batch_size:
                tgt_conn.commit()
                print(f"Progress walletTx: processed={processed_wt} lastIDX={last_idx} missingPlayer={missing_player_wt}", flush=True)

    if not dry_run:
        tgt_conn.commit()
        print(f"Completed walletTx phase. processed={processed_wt} lastIDX={last_idx} missingPlayer={missing_player_wt}", flush=True)

    if dry_run:
        tgt_conn.rollback()
        print("[DRY-RUN] rolled back all writes.", flush=True)

    return processed_players + processed_gt + processed_wt


# ----------------------------
# Main
# ----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--username", help="1Play LOGIN_NAME (single-user mode). Example: IW1PLAY2132928")
    ap.add_argument("--migrate-all", action="store_true", help="Migrate all 1Play rows with checkpoints")
    ap.add_argument("--dry-run", action="store_true", help="No writes (rollback at end)")
    ap.add_argument("--delete", action="store_true", help="Delete all 1Play data from the target DB and exit")
    ap.add_argument("--delete-first", action="store_true", help="Delete target 1Play facts/players and checkpoints first")
    ap.add_argument(
        "--keep-from",
        type=_parse_date_arg,
        default=None,
        metavar="YYYY-MM-DD",
        help="With --delete/--delete-first: keep records whose date is on or after this UTC date",
    )
    ap.add_argument(
        "--keep-to",
        type=_parse_date_arg,
        default=None,
        metavar="YYYY-MM-DD",
        help="With --delete/--delete-first: keep records whose date is on or before this UTC date",
    )

    ap.add_argument("--batch-size", type=int, default=1000, help="Batch size for source scans")
    ap.add_argument("--commit-every", type=int, default=5000, help="Commit+checkpoint after this many processed rows per phase")
    ap.add_argument("--resume", type=lambda x: str(x).lower() not in ("0", "false", "no"), default=True, help="Resume from migrationCheckpoint (default true)")
    ap.add_argument("--start-after-id", type=int, default=None, help="Override checkpoint and start from IDX > this value (applies to each phase)")
    ap.add_argument("--max-members", type=int, default=None, help="Stop after migrating N player rows (players phase only)")
    ap.add_argument("--loop", action="store_true", help="Run --migrate-all in a loop for 9 hours; sleep 30s when nothing new is found")

    args = ap.parse_args()

    if not args.migrate_all and not args.username and not args.delete:
        raise SystemExit("Either provide --username (single-user), --migrate-all, or --delete.")

    src = connect("iestdl")
    tgt = connect("iestdbrds")

    try:
        print("\n=== 1Play migration ===")
        print(f"dry_run: {args.dry_run}")
        print(f"migrate_all: {args.migrate_all}")
        print(f"delete_first: {args.delete_first}")
        print(f"batch_size: {args.batch_size} commit_every: {args.commit_every}")
        print(f"resume: {args.resume} start_after_id: {args.start_after_id} max_members: {args.max_members}")
        print(f"ASSUME_SOURCE_TZ: {ASSUME_SOURCE_TZ}")
        if args.username:
            print(f"username: {args.username}")
        print("")

        if args.delete:
            delete_1play_target_data(tgt, dry_run=args.dry_run, keep_from=args.keep_from, keep_to=args.keep_to)
            return

        if args.delete_first:
            delete_1play_target_data(tgt, dry_run=args.dry_run, keep_from=args.keep_from, keep_to=args.keep_to)

        if args.migrate_all:
            if args.loop:
                loop_end = time.time() + 9 * 3600
                iteration = 0
                while time.time() < loop_end:
                    iteration += 1
                    remaining_sec = loop_end - time.time()
                    print(f"\n[LOOP iter={iteration} remaining={remaining_sec/3600:.2f}h]", flush=True)
                    total = migrate_all(
                        src_conn=src,
                        tgt_conn=tgt,
                        dry_run=args.dry_run,
                        batch_size=args.batch_size,
                        commit_every=args.commit_every,
                        resume=True,
                        start_after_id=args.start_after_id if iteration == 1 else None,
                        max_members=args.max_members,
                    )
                    if total == 0:
                        print("[LOOP] Nothing new found, sleeping 30s ...", flush=True)
                        time.sleep(30)
                print("[LOOP] 9-hour window elapsed, exiting.", flush=True)
            else:
                migrate_all(
                    src_conn=src,
                    tgt_conn=tgt,
                    dry_run=args.dry_run,
                    batch_size=args.batch_size,
                    commit_every=args.commit_every,
                    resume=args.resume,
                    start_after_id=args.start_after_id,
                    max_members=args.max_members,
                )
        else:
            migrate_single_user(src, tgt, args.username, dry_run=args.dry_run)

    except Exception:
        try:
            tgt.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            src.close()
        except Exception:
            pass
        try:
            tgt.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
