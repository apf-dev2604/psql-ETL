#!/usr/bin/env python3
"""
Migrate 88Play data from iestdl -> iestdbrds.

Key mapping (per your verified iestdl sample):
- Source base: PlayerRegistrations88Play.data (registration)
- Supplement:  PlayerDetail88Play.data (detail)

playerDetails fields:
- userName        = registration.name
- externalId      = registration.id  (the member id, e.g. hd8HqxFdK6yvXvf4P)
- outletCode      = registration.branchCode (ensure outletList row exists)
- first/middle/last = split registration.realName (fallback to "" if realName missing)
- mobileNumber    = last 10 digits of registration.mobileNumber (e.g. +639215... -> 9215...)
- emailAddress    = registration.emailAddress (EMPTY STRING if null/blank)
- registrationDate = registration.dateTimeCreated (timestamptz)
- lastLogin       = registration.dateTimeLastActive (fallback: dateTimeLastAndroidLogIn)
- lastLoginIp     = registration.ipAddress (NULL if null/blank)
- isVerified      = TRUE if (registration.verificationStatus == VERIFIED) OR (detail.verification.status == APPROVED)
- isActive        = TRUE if registration.status == ACTIVE (else FALSE)
- addressProvince = detail.verification.address (fallback permanentAddress) (EMPTY STRING if missing)
- incomeSource    = detail.verification.sourceOfIncome (EMPTY STRING if missing)
- industry        = detail.verification.natureOfWork (EMPTY STRING if missing)
- walletBalanceDatetime = registration.dateTimeLastUpdated/dateTimeLastActive (defaults to 1900-01-01 if missing)
- birthdate       = registration.birthDay (date)

Crash/Resume:
- Uses iestdbrds.migrationCheckpoint_dev keyed by platform='88Play'
- Default behavior: resume from last checkpoint (unless --start-after-id is provided)

Performance:
- Member iteration is batched: WHERE id > last_id ORDER BY id LIMIT batch_size
- Wallet fetch uses JSON path filter: data->'member'->>'id' = %s
"""

import os
import json
import uuid
import argparse
import socket
import time
from datetime import datetime, timezone, date
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values, register_uuid


# ----------------------------
# Helpers
# ----------------------------

def to_decimal_str(x: Any) -> str:
    if x is None:
        return "0"
    if isinstance(x, (int, float)):
        return str(x)
    s = str(x).strip()
    return s if s else "0"


def parse_iso_dt(s: Any) -> Optional[datetime]:
    """
    Parses ISO8601-ish strings into tz-aware datetimes.
    - If string has 'Z', treat as UTC.
    - If string is naive, assume UTC (safe for your iestdl JSON which uses Z).
    """
    if not s:
        return None
    if isinstance(s, datetime):
        return s if s.tzinfo else s.replace(tzinfo=timezone.utc)

    txt = str(s).strip()
    if not txt:
        return None

    txt = txt.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(txt)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(txt, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return None


def parse_iso_date(s: Any) -> Optional[date]:
    if not s:
        return None
    if isinstance(s, date) and not isinstance(s, datetime):
        return s
    txt = str(s).strip()
    if not txt:
        return None
    try:
        return date.fromisoformat(txt[:10])
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
    return raw.strip().title()


def digits_only(s: str) -> str:
    return "".join(c for c in (s or "") if c.isdigit())


def email_or_empty(email: Any) -> str:
    e = (str(email or "")).strip()
    return e if e else ""


def safe_mobile_10(mobile: Any) -> str:
    """
    88Play: store last 10 digits (no country code).
    If missing, return "" (not a fake number).
    """
    d = digits_only(str(mobile or ""))
    if len(d) >= 10:
        return d[-10:]
    return ""


def pick_first(*vals: Any) -> Optional[Any]:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def as_dict(x: Any) -> Dict[str, Any]:
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    if isinstance(x, str) and x.strip():
        try:
            return json.loads(x)
        except Exception:
            return {}
    return {}


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


def split_name(full_name: str) -> Tuple[str, str, str]:
    """
    Split into first/middle/last. If cannot split -> ("", "", "")
    - If 2 parts: first, last
    - If 3+ parts: first, middle=join(parts[1:-1]), last=parts[-1]
    """
    s = (full_name or "").strip()
    if not s:
        return ("", "", "")
    parts = [p for p in s.split() if p.strip()]
    if len(parts) == 0:
        return ("", "", "")
    if len(parts) == 1:
        return (parts[0], "", "")
    if len(parts) == 2:
        return (parts[0], "", parts[1])
    return (parts[0], " ".join(parts[1:-1]), parts[-1])

SENTINEL_DT_UTC = datetime(1900, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

def normalize_end_datetime(tx: Dict[str, Any], start_dt: datetime) -> datetime:
    """
    Normalize gameTransaction endDateTime:

    - Use explicit settled/end timestamps if provided.
    - If missing or invalid, default endDateTime to startDateTime.
    """
    # preferred explicit end/settled fields
    end_dt = (
        parse_iso_dt(tx.get("dateTimeSettled"))
        or parse_iso_dt(tx.get("dateTimeEnded"))
        or parse_iso_dt(tx.get("endDateTime"))
    )

    if end_dt and end_dt >= start_dt:
        return end_dt

    return start_dt

# ----------------------------
# DB
# ----------------------------

def connect(dbname: str):
    host = os.getenv("RDS_HOST", "iest-db-postgresql.cvmg4ca8uhd2.ap-southeast-1.rds.amazonaws.com")
    user = os.getenv("RDS_USER", "")
    password = os.getenv("RDS_PASSWORD", "")
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


# ----------------------------
# Source fetch (iestdl)
# ----------------------------

def fetch_member_registration_88play(src_conn, username: str) -> Optional[Dict[str, Any]]:
    """
    Single-user fetch by registration.name (qaekatestacc).
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, data
            FROM "PlayerRegistrations88Play"
            WHERE (data->>'name') = %s
            LIMIT 1
            """,
            (username,),
        )
        row = cur.fetchone()
        if not row:
            return None
        data = row["data"]
        d = data if isinstance(data, dict) else json.loads(data)
        d["_player_reg_source_id"] = row["id"]
        return d


def parse_88play_checkpoint(raw: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse a checkpoint value into (after_dt, after_id).
    New format: "2025-03-12T10:00:00Z|hd8HqxFdK6yvXvf4P"
    Legacy format (id only, no pipe): treated as (None, id) so we start from the beginning date-wise.
    """
    if not raw:
        return (None, None)
    if "|" in raw:
        dt_str, id_str = raw.split("|", 1)
        return (dt_str or None, id_str or None)
    # Legacy checkpoint stored only the raw id — can't recover the date, restart from epoch for that id
    return (None, raw)


def format_88play_checkpoint(dt_iso: Optional[str], raw_id: str) -> str:
    return f"{dt_iso or ''}|{raw_id}"


def fetch_member_batch_88play(
    src_conn,
    after_dt: Optional[str],
    after_id: Optional[str],
    limit: int,
    from_dt: Optional[str] = None,
    until_dt: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Returns rows from PlayerRegistrations88Play ordered by (dateTimeCreated, id).
    Uses a (timestamptz, text) tuple cursor so ordering is chronological, not lexicographic.
    Lexicographic ordering on alphanumeric IDs like "hd8HqxFdK6yvXvf4P" would skip records
    whose IDs sort before the checkpoint after a resume.

    from_dt: >= filter applied only on first call (after_dt is None) to start at a specific date.
    until_dt: <= filter applied on every call to cap at an upper date bound.
    """
    anchor_id = after_id or ""
    date_col = "(data->>'dateTimeCreated')::timestamptz"

    conditions = ["data IS NOT NULL"]
    params: List[Any] = []

    if after_dt is not None:
        conditions.append(f"({date_col}, id) > (%s::timestamptz, %s)")
        params.extend([after_dt, anchor_id])
    elif from_dt is not None:
        conditions.append(f"{date_col} >= %s::timestamptz")
        params.append(from_dt)

    if until_dt is not None:
        conditions.append(f"{date_col} <= %s::timestamptz")
        params.append(until_dt)

    params.append(limit)
    where_clause = " AND ".join(conditions)

    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT id, data
            FROM "PlayerRegistrations88Play"
            WHERE {where_clause}
            ORDER BY {date_col} ASC, id ASC
            LIMIT %s
            """,
            params,
        )
        rows = cur.fetchall()
    # Keep source reads in short transactions during long-running migrations.
    src_conn.rollback()

    out: List[Dict[str, Any]] = []
    for r in rows:
        d = r["data"] if isinstance(r["data"], dict) else json.loads(r["data"])
        d["_player_reg_source_id"] = r["id"]  # checkpoint anchor
        out.append(d)
    return out


def fetch_game_transactions_88play(src_conn, username: str, limit: int) -> List[Dict[str, Any]]:
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, data
            FROM "GameTransaction88Play"
            WHERE data IS NOT NULL
              AND (data->'member'->>'name') = %s
            ORDER BY id
            LIMIT %s
            """,
            (username, limit),
        )
        rows = cur.fetchall()
    src_conn.rollback()

    out: List[Dict[str, Any]] = []
    for r in rows:
        d = r["data"] if isinstance(r["data"], dict) else json.loads(r["data"])
        d["_source_id"] = r["id"]
        out.append(d)
    return out


def fetch_wallet_rows_by_member_id(src_conn, table_name: str, member_id: str, limit: int) -> List[Dict[str, Any]]:
    """
    FAST + SAFE:
      data->'member'->>'id' = %s
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT id, data
            FROM "{table_name}"
            WHERE data IS NOT NULL
              AND (data->'member'->>'id') = %s
            ORDER BY id
            LIMIT %s
            """,
            (member_id, limit),
        )
        rows = cur.fetchall()
    src_conn.rollback()

    out: List[Dict[str, Any]] = []
    for r in rows:
        d = r["data"] if isinstance(r["data"], dict) else json.loads(r["data"])
        d["_source_id"] = r["id"]
        out.append(d)
    return out


def fetch_member_detail_88play(src_conn, member_id: str) -> Optional[Dict[str, Any]]:
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, data
            FROM "PlayerDetail88Play"
            WHERE id = %s
            LIMIT 1
            """,
            (member_id,),
        )
        row = cur.fetchone()
        if not row or row["data"] is None:
            return None
        return row["data"] if isinstance(row["data"], dict) else json.loads(row["data"])


# ----------------------------
# Target: checkpointing (iestdbrds)
# ----------------------------

#CHECKPOINT_PLATFORM = "Powerplay"
CHECKPOINT_PLATFORM = "Instaplay"
def ensure_checkpoint_table(tgt_conn) -> None:
    with tgt_conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS kemet."migrationCheckpoint_dev" (
              "platform" TEXT PRIMARY KEY,
              "lastSourceId" TEXT,
              "updatedAt" TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )


def get_checkpoint(tgt_conn) -> Optional[str]:
    with tgt_conn.cursor() as cur:
        cur.execute(
            """
            SELECT "lastSourceId"
            FROM kemet."migrationCheckpoint_dev"
            WHERE "platform" = %s
            """,
            (CHECKPOINT_PLATFORM,),
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def set_checkpoint(tgt_conn, last_source_id: str) -> None:
    with tgt_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO kemet."migrationCheckpoint_dev" ("platform","lastSourceId","updatedAt")
            VALUES (%s, %s, now())
            ON CONFLICT ("platform") DO UPDATE SET
              "lastSourceId" = EXCLUDED."lastSourceId",
              "updatedAt" = now()
            """,
            (CHECKPOINT_PLATFORM, last_source_id),
        )


# ----------------------------
# Enroll missing outlet code
# ----------------------------

def ensure_outlet_exists(
    tgt_conn,
    outlet_code: str,
    #brand: str = "Powerplay",
    brand: str = "Instaplay",
    operator: str = "Instaplay",
    #operator: str = "Powerplay",
    dry_run: bool = False,
) -> None:
    """
    Satisfy FK:
      playerDetails.outletCode -> outletList.outletCode
    """
    code = (outlet_code or "").strip()
    if not code:
        return

    if dry_run:
        return

    sql = """
    INSERT INTO kemet."outletList_final" (
        "outletCode",
        "outletName",
        "streetAddress",
        "barangayAddress",
        "cityAddress",
        "provinceAddress",
        "outletShare",
        "operator",
        "isActive",
        "brand",
        "createdAt",
        "updatedAt",
        "lastUpdateDatetime"
    )
    VALUES (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        0.00,
        %s,
        true,
        %s,
        now(),
        now(),
        now()
    )
    ON CONFLICT ("outletCode") DO NOTHING
    """
    params = (
        code,
        code,     # keep it simple/consistent for placeholders
        "",       # streetAddress
        "",       # barangayAddress
        "",       # cityAddress
        "",       # provinceAddress
        operator,
        brand,
    )
    with tgt_conn.cursor() as cur:
        cur.execute(sql, params)


# ----------------------------
# Target upserts (iestdbrds)
# ----------------------------

def upsert_player_details_88play(tgt_conn, member: Dict[str, Any], dry_run: bool) -> uuid.UUID:
    detail = as_dict(member.get("_detail"))
    verification = as_dict(detail.get("verification"))

    username = member.get("name")
    if not username:
        raise RuntimeError("Member record missing 'name'")

    # ----- NAME -----
    real_name = (member.get("realName") or "").strip()
    first = middle = last = ""
    if real_name:
        parts = real_name.split()
        if len(parts) == 1:
            first = parts[0]
        elif len(parts) == 2:
            first, last = parts
        else:
            first = parts[0]
            middle = parts[1]
            last = parts[-1]

    # ----- CONTACT -----
    mobile_10 = safe_mobile_10(member.get("mobileNumber"))
    email = email_or_empty(member.get("emailAddress") or member.get("email") or member.get("email_address"))

    # ----- DATES -----
    registered_date = parse_iso_dt(member.get("dateTimeCreated"))

    wallet_balance_dt = (
        parse_iso_dt(member.get("dateTimeLastUpdated"))
        or parse_iso_dt(member.get("dateTimeLastActive"))
    )

    last_login = (
        parse_iso_dt(member.get("dateTimeLastActive"))
        or parse_iso_dt(member.get("dateTimeLastAndroidLogIn"))
    )
    last_login_ip = (str(member.get("ipAddress") or "")).strip() or None

    # ----- FLAGS -----
    reg_vs = str(member.get("verificationStatus") or "").upper()
    is_verified = (reg_vs == "VERIFIED")
    st = str(member.get("status")).upper()
    is_active = st in ("ACTIVE", "VERIFICATION_LOCKED")

    # ----- OUTLET -----
    outlet_code = (member.get("branchCode") or "").strip() or None
    if outlet_code:
        ensure_outlet_exists(
            tgt_conn,
            outlet_code,
            #brand="Powerplay",
            #operator="Powerplay",
            brand="Instaplay",
            operator="Instaplay",
            dry_run=dry_run,
        )

    # ----- ADDRESS -----
    address_province = (
        verification.get("address")
        or verification.get("permanentAddress")
        or ""
    )

    # ----- INCOME -----
    income_source = verification.get("sourceOfIncome") or ""
    industry = verification.get("natureOfWork") or ""

    # ----- WALLET -----
    wallet = as_dict(member.get("wallet"))
    wallet_balance = wallet.get("balance") or "0"

    sql = """
    INSERT INTO kemet."playerDetails_final" (
        "userName",
        "firstName","middleName","lastName",
        "mobileNumber","mobileNumberVerified",
        "emailAddress","emailVerified",
        "registrationDate",
        "brandName",
        "isVerified","isBlocked","isActive",
        "lastLogin","lastLoginIp",
        "outletCode",
        "addressStreet","addressBarangay","addressCity","addressProvince",
        "incomeSource","industry",
        "walletBalance","walletBalanceDatetime",
        "externalId",
        "birthdate",
        "createdAt","updatedAt"
    )
    VALUES (
        %s,
        %s,%s,%s,
        %s,true,
        %s,false,
        %s,
        'Instaplay',
        %s,false,%s,
        %s, NULLIF(%s, '')::inet,
        %s,
        '','','',%s,
        %s,%s,
        %s,COALESCE(%s, TIMESTAMPTZ '1900-01-01 00:00:00+00'),
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
        "isVerified"=EXCLUDED."isVerified",
        "isActive"=EXCLUDED."isActive",
        "lastLogin"=EXCLUDED."lastLogin",
        "lastLoginIp"=EXCLUDED."lastLoginIp",
        "outletCode"=EXCLUDED."outletCode",
        "addressProvince"=EXCLUDED."addressProvince",
        "incomeSource"=EXCLUDED."incomeSource",
        "industry"=EXCLUDED."industry",
        "walletBalance"=EXCLUDED."walletBalance",
        "walletBalanceDatetime"=EXCLUDED."walletBalanceDatetime",
        "externalId"=EXCLUDED."externalId",
        "birthdate"=EXCLUDED."birthdate",
        "updatedAt"=now()
    RETURNING id
    """
    # Line 640 was replaced from PowerPlay to Instaplay

    params = (
        username,
        first, middle, last,
        mobile_10,
        email,
        registered_date,
        is_verified,
        is_active,
        last_login,
        last_login_ip,
        outlet_code,
        address_province,
        income_source,
        industry,
        wallet_balance,
        wallet_balance_dt,
        member.get("id"),
        parse_iso_dt(member.get("birthDay")).date() if member.get("birthDay") else None,
    )

    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute('SELECT id FROM kemet."playerDetails_final" WHERE "userName"=%s', (username,))
            row = cur.fetchone()
            return row[0] if row else uuid.uuid4()

    with tgt_conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]

def get_or_create_game_provider(
    tgt_conn,
    provider_name: str,
    dry_run: bool,
    cache: Optional[Dict[str, uuid.UUID]] = None,
) -> uuid.UUID:
    #print("---debug gameProvider name is: "+ provider_name )
    provider_name = (provider_name or "UNKNOWN").strip().upper() or "UNKNOWN"
    if cache is not None and provider_name in cache:
        return cache[provider_name]
    sql = """
    INSERT INTO kemet."gameProvider_final" ("gameProvider","isActive","createdAt","updatedAt")
    VALUES (%s, true, now(), now())
    ON CONFLICT ("gameProvider") DO UPDATE SET "updatedAt"=now()
    RETURNING id
    """
    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute('SELECT id FROM kemet."gameProvider_final" WHERE "gameProvider"=%s', (provider_name,))
            row = cur.fetchone()
            gid = row[0] if row else uuid.uuid4()
            if cache is not None:
                cache[provider_name] = gid
            return gid
    with tgt_conn.cursor() as cur:
        cur.execute(sql, (provider_name,))
        gid = cur.fetchone()[0]
        if cache is not None:
            cache[provider_name] = gid
        return gid


def get_or_create_game_type(
    tgt_conn,
    game_type: str,
    dry_run: bool,
    cache: Optional[Dict[str, uuid.UUID]] = None,
) -> uuid.UUID:
    game_type = normalize_game_type(game_type)
    #print("---debug gameType name is: "+ game_type)
    if cache is not None and game_type in cache:
        return cache[game_type]
    sql = """
    INSERT INTO kemet."gameType_final" ("gameType","isActive","createdAt","updatedAt")
    VALUES (%s, true, now(), now())
    ON CONFLICT ("gameType") DO UPDATE SET "updatedAt"=now()
    RETURNING id
    """
    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute('SELECT id FROM kemet."gameType_final" WHERE "gameType"=%s', (game_type,))
            row = cur.fetchone()
            gid = row[0] if row else uuid.uuid4()
            if cache is not None:
                cache[game_type] = gid
            return gid
    with tgt_conn.cursor() as cur:
        cur.execute(sql, (game_type,))
        gid = cur.fetchone()[0]
        if cache is not None:
            cache[game_type] = gid
        return gid


def get_or_create_game_list(
    tgt_conn,
    game_name: str,
    provider_id: uuid.UUID,
    game_type_id: uuid.UUID,
    dry_run: bool,
    cache: Optional[Dict[Tuple[uuid.UUID, str], uuid.UUID]] = None,
) -> uuid.UUID:
    game_name = (game_name or "UNKNOWN").strip() or "UNKNOWN"
    #print("---debug gameList name is: "+ game_name)
    key = (provider_id, game_name)
    if cache is not None and key in cache:
        return cache[key]
    sql = """
    INSERT INTO kemet."gameList_final" (
        "gameTypeId","gameProviderId","gameName",
        "isProgressive","isActive","createdAt","updatedAt"
    )
    VALUES (%s,%s,%s,false,true,now(),now())
    ON CONFLICT ("gameProviderId","gameName") DO UPDATE SET
        "gameTypeId"=EXCLUDED."gameTypeId",
        "updatedAt"=now()
    RETURNING id
    """
    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute(
                'SELECT id FROM kemet."gameList_final" WHERE "gameProviderId"=%s AND "gameName"=%s',
                (provider_id, game_name),
            )
            row = cur.fetchone()
            gid = row[0] if row else uuid.uuid4()
            if cache is not None:
                cache[key] = gid
            return gid
    with tgt_conn.cursor() as cur:
        cur.execute(sql, (game_type_id, provider_id, game_name))
        gid = cur.fetchone()[0]
        if cache is not None:
            cache[key] = gid
        return gid


def _parse_date_arg(s: str) -> datetime:
    """Parse YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS into a UTC-aware datetime (for CLI args)."""
    s = s.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise argparse.ArgumentTypeError(
        f"Invalid date: {s!r}. Expected YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS (interpreted as UTC)"
    )


def delete_all_data_88play(
    tgt_conn,
    dry_run: bool,
    keep_from: Optional[datetime] = None,
    keep_to: Optional[datetime] = None,
) -> None:
    """
    Delete all Powerplay data from the target DB.

    If keep_from / keep_to are provided, records whose date falls within
    [keep_from, keep_to] are preserved (everything outside that window is deleted).
    Checkpoints are only deleted when no date range is specified (full wipe).
    """
    has_range = keep_from is not None or keep_to is not None

    def _exclusion(col: str, params: list) -> str:
        """Return SQL fragment that excludes rows inside the keep window."""
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
        msg = "[DRY-RUN] would delete Instaplay data"
        if has_range:
            msg += f" (keeping records between {keep_from} and {keep_to})"
        print(msg)
        return

    with tgt_conn.cursor() as cur:
        #gt_params: List[Any] = ["Powerplay"]
        gt_params: List[Any] = ["Instaplay"]
        cur.execute(
            f'DELETE FROM "gameTransaction" WHERE "brand"=%s{_exclusion("startDateTime", gt_params)}',
            gt_params,
        )
        gt = cur.rowcount

        #wt_params: List[Any] = ["Powerplay"]
        wt_params: List[Any] = ["Instaplay"]
        cur.execute(
            f'DELETE FROM "walletTransaction" WHERE "platform"=%s{_exclusion("createdDatetime", wt_params)}',
            wt_params,
        )
        wt = cur.rowcount

        #pd_params: List[Any] = ["Powerplay"]
        pd_params: List[Any] = ["Instaplay"]
        cur.execute(
            f'DELETE FROM "playerDetails" WHERE "brandName"=%s{_exclusion("registrationDate", pd_params)}',
            pd_params,
        )
        pl = cur.rowcount
        # replace platform value below from Powerplay to Instaplay
        if not has_range:
            cur.execute('DELETE FROM "migrationCheckpoint" WHERE "platform" = %s', ("Instaplay",))
            ck = cur.rowcount
        else:
            ck = 0

    tgt_conn.commit()
    print(f"Deleted: gameTransaction={gt}, walletTransaction={wt}, playerDetails={pl}, checkpoints={ck}")


def delete_user_data_88play(tgt_conn, username: str, dry_run: bool) -> None:
    with tgt_conn.cursor() as cur:
        cur.execute('SELECT id FROM "playerDetails" WHERE "userName"=%s', (username,))
        row = cur.fetchone()
        player_id = row[0] if row else None

    if dry_run:
        print(f"[DRY-RUN] delete-first for username={username} (playerId={player_id})")
        return

    # replace brand Values from Powerplay to Instaplay
    with tgt_conn.cursor() as cur:
        cur.execute(
            'DELETE FROM "gameTransaction" WHERE "playerUserName"=%s AND "brand"=%s',
            (username, "Instaplay"),
        )
        gt_deleted = cur.rowcount

        wt_deleted = 0
        if player_id:
            # replace brand Values from Powerplay to Instaplay
            cur.execute(
                'DELETE FROM "walletTransaction" WHERE "playerId"=%s AND "platform"=%s',
                (player_id, "Instaplay"),
            )
            wt_deleted = cur.rowcount

    tgt_conn.commit()
    print(f"Deleted gameTransaction rows: {gt_deleted}")
    print(f"Deleted walletTransaction rows: {wt_deleted}")


# ----------------------------
# Inserts
# ----------------------------

def insert_game_transactions_88play(
    tgt_conn,
    username: str,
    player_id: uuid.UUID,
    tx_rows: List[Dict[str, Any]],
    dry_run: bool,
    provider_cache: Optional[Dict[str, uuid.UUID]] = None,
    gametype_cache: Optional[Dict[str, uuid.UUID]] = None,
    gamelist_cache: Optional[Dict[Tuple[uuid.UUID, str], uuid.UUID]] = None,
) -> Tuple[int, int]:
    values = []
    for tx in tx_rows:
        member = as_dict(tx.get("member"))
        if (member.get("name") or "").strip() != username:
            continue

        game = as_dict(tx.get("game"))
        provider_name = (game.get("provider") or "UNKNOWN")
        game_name = (game.get("name") or "UNKNOWN")
        game_type_raw = (game.get("type") or "SLOTS")

        provider_id = get_or_create_game_provider(tgt_conn, provider_name, dry_run, cache=provider_cache)
        game_type_id = get_or_create_game_type(tgt_conn, game_type_raw, dry_run, cache=gametype_cache)
        game_id = get_or_create_game_list(
            tgt_conn, game_name, provider_id, game_type_id, dry_run, cache=gamelist_cache
        )

        bet_amount = to_decimal_str(tx.get("bet"))
        payout_amount = to_decimal_str(tx.get("payout"))
        pc5 = to_decimal_str(tx.get("jackpotContribution"))
        jw5 = to_decimal_str(tx.get("jackpotPayout"))

        created = parse_iso_dt(tx.get("dateTimeCreated")) or datetime.now(timezone.utc)
        settled = normalize_end_datetime(tx, created)

        meta = as_dict(tx.get("metadata"))
        raw_req = as_dict(meta.get("rawRequest"))
        round_id = pick_first(
            tx.get("vendorRoundId"),
            raw_req.get("parent_bet_id"),
            raw_req.get("roundId"),
        )

        external_id = str(tx.get("_source_id") or tx.get("id") or "").strip()
        if not external_id:
            continue

        values.append((
            external_id,
            username,
            player_id,
            provider_id,
            game_id,
            game_type_id,
            bet_amount,
            payout_amount,
            "0", "0", "0", "0", pc5,
            "0", "0", "0", "0", "0",
            "0",
            jw5,
            0,
            external_id,
            None,
            None,
            True,
            created,
            settled,
          #  "Powerplay",
            "Instaplay",
            "Online",
            round_id,
        ))

    if not values:
        return (0, 0)

    insert_sql = """
    INSERT INTO kemet."gameTransaction_final" (
        "externalId",
        "playerUserName",
        "playerId",
        "providerId",
        "gameId",
        "gameTypeId",
        "betAmount",
        "payoutAmount",
        "PC1","PC2","PC3","PC4","PC5",
        "JW1","JW2","JW3","JW4","JW5",
        "progressionContributionPaid",
        "seedMoneyWon",
        "seedMoneyJackpotOver1000",
        "tableRoomId",
        "betDetails",
        "betTiming",
        "parlay",
        "startDateTime",
        "endDateTime",
        "brand",
        "platform",
        "roundId"
    )
    VALUES %s
    ON CONFLICT ("externalId") DO NOTHING
    """

    if dry_run:
        print(f"[DRY-RUN] would insert gameTransaction rows: {len(values)}")
        return (0, len(values))

    with tgt_conn.cursor() as cur:
        execute_values(cur, insert_sql, values, page_size=500)
    return (0, 0)


def wallet_extract_common(w: Dict[str, Any]) -> Dict[str, Any]:
    d = w
    amount = pick_first(d.get("amount"), d.get("netAmount"))
    status = pick_first(d.get("status"), d.get("state"), d.get("result")) or "CONFIRMED"
    #domain = pick_first(d.get("domain"), d.get("site"), d.get("host")) or "instaplay.com.ph"
    #domain = "instaplay.com.ph"
    val = pick_first(d.get("domain"), d.get("site"), d.get("host"))
    domain = val if val == "android/o472" else "instaplay.com.ph"
    pg = pick_first(d.get("type"), d.get("paymentGateway"), d.get("gateway"), d.get("channel")) or ""
    ref = pick_first(d.get("reference"), d.get("referenceId"), d.get("reference_id"), d.get("transaction_id"), d.get("id"), d.get("_source_id"))
    created = pick_first(d.get("dateTimeCreated"), d.get("createdAt"), d.get("createdDatetime"), d.get("created_time"), d.get("timestamp"))
    confirmed = pick_first(d.get("dateTimeConfirmed"), d.get("confirmedAt"), d.get("dateTimeLastUpdated"))

    status_norm = str(status).strip().lower()
    return {
        "amount": to_decimal_str(amount),
        "status": status_norm,
        "domain": str(domain),
        "paymentGateway": str(pg),
        "referenceId": str(ref) if ref is not None else None,
        "createdDatetime": parse_iso_dt(created) or datetime.now(timezone.utc),
        "confirmedDatetime": parse_iso_dt(confirmed),
    }


def insert_wallet_transactions_88play(
    tgt_conn,
    player_id: uuid.UUID,
    deposits: List[Dict[str, Any]],
    withdrawals: List[Dict[str, Any]],
    dry_run: bool,
) -> Tuple[int, int]:
    values = []
    seen = set()

    def add_rows(rows: List[Dict[str, Any]], tx_type: str):
        for r in rows:
            x = wallet_extract_common(r)
            if not x:
                print(f"SKIPPED: wallet_extract_common returned None for {r.get('refid')}")
                continue

            dedupe_key = (tx_type, x["referenceId"], x["amount"], x["status"], x["createdDatetime"].isoformat())
            if dedupe_key in seen:
                print(f"SKIPPED: Duplicate key found for {x['referenceId']}") # DEBUG 2
                continue
            seen.add(dedupe_key)

            print(f"READY TO INSERT: {x['referenceId']}") # DEBUG 3
            values.append((
                tx_type,
                #"Powerplay",
                "Instaplay",
                player_id,
                x["paymentGateway"] or "",
                x["domain"] or "instaplay.com.ph",
                x["amount"],
                x["status"],
                None,
                x["createdDatetime"],
                x["confirmedDatetime"] if x["status"] == "confirmed" else None,
                x["createdDatetime"] if x["status"] == "cancelled" else None,
                x["createdDatetime"] if x["status"] == "failed" else None,
                x["referenceId"],
            ))

    add_rows(deposits, "deposit")
    add_rows(withdrawals, "withdrawal")

    if not values:
        return (0, 0)

    insert_sql = """
    INSERT INTO kemet."walletTransaction_final" (
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
    """
    #ON CONFLICT ("platform", "referenceId") DO NOTHING;
    if dry_run:
        print(f"[DRY-RUN] would insert walletTransaction rows: {len(values)}")
        return (0, len(values))

    with tgt_conn.cursor() as cur:
        execute_values(cur, insert_sql, values, page_size=500)
    return (0, 0)


# ----------------------------
# Orchestration
# ----------------------------

def migrate_one_member(
    src_conn,
    tgt_conn,
    reg: Dict[str, Any],
    tx_limit: int,
    wallet_limit: int,
    dry_run: bool,
    provider_cache: Optional[Dict[str, uuid.UUID]] = None,
    gametype_cache: Optional[Dict[str, uuid.UUID]] = None,
    gamelist_cache: Optional[Dict[Tuple[uuid.UUID, str], uuid.UUID]] = None,
) -> Tuple[str, str, int, int]:
    #print(f"[START] Instaplay migrate_one_member function")
    """
    Returns: (username, player_reg_source_id, tx_count, wallet_count)
    """
    username = (reg.get("name") or "").strip()
    if not username:
        src_id = str(reg.get("_player_reg_source_id") or "")
        member_id = str(reg.get("id") or "").strip()
        print(f"[SKIP] Instaplay registration missing name: source_id={src_id} member_id={member_id}")
        return ("", src_id, 0, 0)

    player_reg_source_id = str(reg.get("_player_reg_source_id") or "")
    member_id = str(reg.get("id") or "").strip()
    if not member_id:
        raise RuntimeError(f"Registration {username} missing id")

    # detail supplement
    detail = fetch_member_detail_88play(src_conn, member_id)
    if detail:
        reg["_detail"] = detail

    # facts
    txs = fetch_game_transactions_88play(src_conn, username, limit=tx_limit)
    deposits = fetch_wallet_rows_by_member_id(src_conn, "Deposits88Play", member_id, limit=wallet_limit)
    withdrawals = fetch_wallet_rows_by_member_id(src_conn, "Withdrawals88Play", member_id, limit=wallet_limit)

    player_id = upsert_player_details_88play(tgt_conn, reg, dry_run=dry_run)
    insert_game_transactions_88play(
        tgt_conn,
        username,
        player_id,
        txs,
        dry_run=dry_run,
        provider_cache=provider_cache,
        gametype_cache=gametype_cache,
        gamelist_cache=gamelist_cache,
    )
    insert_wallet_transactions_88play(tgt_conn, player_id, deposits, withdrawals, dry_run=dry_run)

    return (username, player_reg_source_id, len(txs), len(deposits) + len(withdrawals))


def main():
    ap = argparse.ArgumentParser()

    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--username", help="Migrate a single username (registration.name)")
    mode.add_argument("--migrate-all", action="store_true", help="Migrate all InstaPlay registrations")
    mode.add_argument("--delete", action="store_true", help="Delete all Instaplay data from the target DB and exit")

    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--delete-first", action="store_true")  # single-user only

    ap.add_argument(
        "--keep-from",
        type=_parse_date_arg,
        default=None,
        metavar="YYYY-MM-DD",
        help="With --delete: keep records whose date is on or after this UTC date",
    )
    ap.add_argument(
        "--keep-to",
        type=_parse_date_arg,
        default=None,
        metavar="YYYY-MM-DD",
        help="With --delete: keep records whose date is on or before this UTC date",
    )

    ap.add_argument(
        "--date-from",
        type=_parse_date_arg,
        default=None,
        metavar="YYYY-MM-DD",
        help="With --migrate-all: only migrate records whose date is on or after this UTC date (ignores checkpoint)",
    )
    ap.add_argument(
        "--date-to",
        type=_parse_date_arg,
        default=None,
        metavar="YYYY-MM-DD",
        help="With --migrate-all: only migrate records whose date is on or before this UTC date",
    )

    ap.add_argument("--tx-limit", type=int, default=500)
    ap.add_argument("--wallet-limit", type=int, default=500)

    # all-user options
    ap.add_argument("--batch-size", type=int, default=250)
    ap.add_argument("--commit-every", type=int, default=250)
    ap.add_argument("--resume", action="store_true", default=True)
    ap.add_argument("--no-resume", action="store_false", dest="resume")
    ap.add_argument("--start-after-id", type=str, default=None)
    ap.add_argument("--max-members", type=int, default=None)
    ap.add_argument("--loop", action="store_true", help="Run --migrate-all in a loop for 9 hours; sleep 30s when nothing new is found")

    args = ap.parse_args()
    dry_run = args.dry_run

    src = connect("iestdl")
    #tgt = connect("iestdbrds")
    tgt = connect("iestdl")

    try:
        ensure_checkpoint_table(tgt)

        if args.delete:
            print("\n=== Instaplay delete ===")
            print(f"dry_run: {dry_run}")
            print(f"keep_from: {args.keep_from}")
            print(f"keep_to: {args.keep_to}\n")
            delete_all_data_88play(tgt, dry_run=dry_run, keep_from=args.keep_from, keep_to=args.keep_to)
            return

        if args.username:
            username = args.username.strip()
            print("\n=== Instaplay single-user migration ===")
            print(f"username: {username}")
            print(f"dry_run: {dry_run}")
            print(f"delete_first: {args.delete_first}")
            print(f"tx_limit: {args.tx_limit} wallet_limit: {args.wallet_limit}\n")

            reg = fetch_member_registration_88play(src, username)
            if not reg:
                raise RuntimeError(f"No PlayerRegistrations88Play record found for username={username}")

            if args.delete_first:
                delete_user_data_88play(tgt, username, dry_run=dry_run)

            provider_cache: Dict[str, uuid.UUID] = {}
            gametype_cache: Dict[str, uuid.UUID] = {}
            gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID] = {}
            uname, src_id, txc, wtc = migrate_one_member(
                src, tgt, reg,
                tx_limit=args.tx_limit,
                wallet_limit=args.wallet_limit,
                dry_run=dry_run,
                provider_cache=provider_cache,
                gametype_cache=gametype_cache,
                gamelist_cache=gamelist_cache,
            )
            print(f"Done user={uname} tx={txc} wallet={wtc}")

            if dry_run:
                tgt.rollback()
                print("\n[DRY-RUN] Rolled back all target writes.")
            else:
                tgt.commit()
                print("\nCommitted all target writes.")
            return

        # ----------------------------
        # migrate-all
        # ----------------------------
        print("\n=== Instaplay migrate-all ===")
        print(f"dry_run: {dry_run}")
        print(f"tx_limit: {args.tx_limit} wallet_limit: {args.wallet_limit}")
        print(f"batch_size: {args.batch_size} commit_every: {args.commit_every}")
        print(f"resume: {args.resume}")
        print(f"start_after_id: {args.start_after_id}")
        print(f"max_members: {args.max_members}")
        print(f"date_from: {args.date_from}")
        print(f"date_to: {args.date_to}\n")

        from_dt_iso: Optional[str] = args.date_from.isoformat() if args.date_from is not None else None
        until_dt_iso: Optional[str] = args.date_to.isoformat() if args.date_to is not None else None

        provider_cache: Dict[str, uuid.UUID] = {}
        gametype_cache: Dict[str, uuid.UUID] = {}
        gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID] = {}

        loop_end = time.time() + 9 * 3600 if args.loop else None
        loop_iteration = 0

        while True:
            loop_iteration += 1

            if loop_end is not None:
                if time.time() >= loop_end:
                    print("[LOOP] 9-hour window elapsed, exiting.", flush=True)
                    break
                remaining_sec = loop_end - time.time()
                print(f"\n[LOOP iter={loop_iteration} remaining={remaining_sec/3600:.2f}h]", flush=True)

            # When date_from is specified, ignore checkpoint and start from that date
            if args.date_from is not None:
                ck_after_dt, ck_after_id = None, None
            else:
                raw_ck = args.start_after_id if loop_iteration == 1 else None
                if not raw_ck and args.resume:
                    raw_ck = get_checkpoint(tgt)
                ck_after_dt, ck_after_id = parse_88play_checkpoint(raw_ck)

            processed = 0
            cursor_dt = ck_after_dt
            cursor_id = ck_after_id

            while True:
                if args.max_members is not None and processed >= args.max_members:
                    break

                batch = fetch_member_batch_88play(src, cursor_dt, cursor_id, limit=args.batch_size, from_dt=from_dt_iso, until_dt=until_dt_iso)
                if not batch:
                    break

                since_commit = 0
                for reg in batch:
                    if args.max_members is not None and processed >= args.max_members:
                        break

                    try:
                        uname, src_id, txc, wtc = migrate_one_member(
                            src, tgt, reg,
                            tx_limit=args.tx_limit,
                            wallet_limit=args.wallet_limit,
                            dry_run=dry_run,
                            provider_cache=provider_cache,
                            gametype_cache=gametype_cache,
                            gamelist_cache=gamelist_cache,
                        )

                        if src_id and not src_id.startswith("single:"):
                            reg_dt = reg.get("dateTimeCreated") or ""
                            ck_val = format_88play_checkpoint(reg_dt, src_id)
                            print(f"if reg_date does not starts with single and  "+ str(reg_dt)  +"--" + src_id)
                            print(f"---"+ ck_val)
                            set_checkpoint(tgt, ck_val)
                            cursor_dt = reg_dt or cursor_dt
                            cursor_id = src_id

                        processed += 1
                        since_commit += 1

                        if processed % 25 == 0:
                            print(f"Progress: processed={processed} cursor_dt={cursor_dt} cursor_id={cursor_id}")

                        if since_commit >= args.commit_every:
                            if dry_run:
                                tgt.rollback()
                            else:
                                tgt.commit()
                            since_commit = 0

                    except Exception as e:
                        try:
                            tgt.rollback()
                        except Exception:
                            pass
                        print(f"ERROR on member. cursor_dt={cursor_dt} cursor_id={cursor_id}. Exception={e}")
                        raise

                if since_commit > 0:
                    if dry_run:
                        tgt.rollback()
                    else:
                        tgt.commit()

            if dry_run:
                print(f"\n[DRY-RUN] Completed migrate-all simulation. processed={processed} (rolled back each commit window).")
            else:
                print(f"\nCompleted migrate-all. processed={processed}. cursor_dt={cursor_dt} cursor_id={cursor_id}")

            if loop_end is None:
                break
            if processed == 0:
                print("[LOOP] Nothing new found, sleeping 30s ...", flush=True)
                time.sleep(30)

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