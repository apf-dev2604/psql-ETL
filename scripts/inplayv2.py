import os
import json
import re
import uuid
import argparse
import socket
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values, register_uuid

# ----------------------------
# Helpers
# ----------------------------

BRAND = "Inplay"  # playerDetails.brandName + gameTransaction.brand
PLATFORM = "Online"  # gameTransaction.platform
WALLET_PLATFORM = "Inplay"  # walletTransaction.platform

# Business reporting/migration window timezone.
# Inplay business day is evaluated in Philippine time: 06:00 PHT to before 06:00 PHT next day.
PHT_TZ = timezone(timedelta(hours=8))
PHT_TZ_NAME = "Asia/Manila"


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


def digits_only(s: str) -> str:
    return "".join(c for c in (s or "") if c.isdigit())


def safe_mobile_10(mobile: Any) -> str:
    d = digits_only(str(mobile or ""))
    if len(d) >= 10:
        return d[-10:]
    return "0000000000"


EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)


def sanitize_email(email: Any, username: str) -> str:
    """
    Must satisfy chk_player_email_format.
    If invalid, return a deterministic placeholder unique-ish per username to avoid collisions.
    """
    e = str(email or "").strip()
    e = e.rstrip(".")  # common bad data (trailing dot)
    if not e or e.lower() == "null":
        return f"{username}@unknown.local"
    if EMAIL_RE.match(e):
        return e
    # deterministic placeholder
    return f"{username}@unknown.local"


def to_decimal_str(x: Any) -> str:
    if x is None:
        return "0"
    if isinstance(x, (int, float)):
        return str(x)
    s = str(x).strip()
    return s if s else "0"


def parse_iso_dt(s: Any) -> Optional[datetime]:
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
        return None


def latest_dt(*fields: Any) -> Optional[datetime]:
    """Return the latest non-null datetime from a set of raw values."""
    candidates = [parse_iso_dt(f) for f in fields]
    valid = [dt for dt in candidates if dt is not None]
    return max(valid) if valid else None


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


def normalize_wallet_status(raw: Any) -> str:
    return (str(raw or "")).strip().lower()


def clean_username_value(value: Any) -> str:
    """
    Apply the InPlayV2 username cleanup rule before lookup/insert/reporting.
    Removes leading spaces, trailing spaces, and trapped/internal whitespace.
    Does not change letter casing.
    """
    return re.sub(r"\s+", "", str(value or "").strip())


def username_match_key(value: Any) -> str:
    """Case-insensitive username lookup key for source/target ownership matching.

    This is used only for comparisons and player_map dictionary keys.
    The clean/display username remains clean_username_value(value).
    """
    return clean_username_value(value).lower()


# ----------------------------
# Checkpointing
# ----------------------------


def ck_key(phase: str) -> str:
    return f"{BRAND}_{phase}"


def checkpoint_get(tgt_conn, phase: str) -> Optional[str]:
    key = ck_key(phase)
    with tgt_conn.cursor() as cur:
        cur.execute(
            'SELECT "lastSourceId" FROM migration_repair."migrationCheckpoint" WHERE platform=%s', (key,)
        )
        row = cur.fetchone()
        return row[0] if row else None


def checkpoint_set(tgt_conn, phase: str, last_source_id: str, dry_run: bool) -> None:
    if dry_run:
        return
    key = ck_key(phase)
    with tgt_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO migration_repair."migrationCheckpoint" (platform, "lastSourceId", "updatedAt")
            VALUES (%s, %s, now())
            ON CONFLICT (platform) DO UPDATE SET
              "lastSourceId" = EXCLUDED."lastSourceId",
              "updatedAt" = now()
            """,
            (key, str(last_source_id)),
        )


# ----------------------------
# Target: ensure unique index for wallet de-dupe
# ----------------------------


def ensure_wallet_dedupe_index(tgt_conn, dry_run: bool) -> None:
    """
    We need an ON CONFLICT target for walletTransaction to be rerunnable.
    We'll use a partial unique index on (platform, referenceId) for InPlayV2.
    """
    if dry_run:
        return
    with tgt_conn.cursor() as cur:
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_wallet_inplayv2_reference
            ON migration_repair."walletTransaction_final" ("platform", "referenceId")
            WHERE "platform" = %s AND "referenceId" IS NOT NULL
            """,
            (WALLET_PLATFORM,),
        )


# ----------------------------
# Target: dimension upserts (cached)
# ----------------------------


def get_or_create_game_provider(
    tgt_conn, provider_name: str, cache: Dict[str, uuid.UUID], dry_run: bool
) -> uuid.UUID:
    name = (provider_name or "UNKNOWN").strip().upper() or "UNKNOWN"
    if name in cache:
        return cache[name]

    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute(
                'SELECT id FROM migration_repair."gameProvider_final" WHERE "gameProvider"=%s', (name,)
            )
            row = cur.fetchone()
            gid = row[0] if row else uuid.uuid4()
            cache[name] = gid
            return gid

    sql = """
    INSERT INTO migration_repair."gameProvider_final" ("gameProvider","isActive","createdAt","updatedAt")
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
    tgt_conn, game_type: str, cache: Dict[str, uuid.UUID], dry_run: bool
) -> uuid.UUID:
    gt = normalize_game_type(game_type)
    if gt in cache:
        return cache[gt]

    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute('SELECT id FROM migration_repair."gameType_final" WHERE "gameType"=%s', (gt,))
            row = cur.fetchone()
            gid = row[0] if row else uuid.uuid4()
            cache[gt] = gid
            return gid

    sql = """
    INSERT INTO migration_repair."gameType_final" ("gameType","isActive","createdAt","updatedAt")
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
    dry_run: bool,
) -> uuid.UUID:
    gname = (game_name or "UNKNOWN").strip() or "UNKNOWN"
    key = (provider_id, gname)
    if key in cache:
        return cache[key]

    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute(
                'SELECT id FROM migration_repair."gameList_final" WHERE "gameProviderId"=%s AND "gameName"=%s',
                (provider_id, gname),
            )
            row = cur.fetchone()
            gid = row[0] if row else uuid.uuid4()
            cache[key] = gid
            return gid

    sql = """
    INSERT INTO migration_repair."gameList_final" (
        "gameTypeId","gameProviderId","gameName",
        "isProgressive","isActive","createdAt","updatedAt",
        "brandName"
    )
    VALUES (%s,%s,%s,false,true,now(),now(),%s)
    ON CONFLICT ("gameProviderId","gameName") DO UPDATE SET
        "gameTypeId"=EXCLUDED."gameTypeId",
        "brandName"=EXCLUDED."brandName",
        "updatedAt"=now()
    RETURNING id
    """
    with tgt_conn.cursor() as cur:
        cur.execute(sql, (game_type_id, provider_id, gname, BRAND))
        gid = cur.fetchone()[0]
        cache[key] = gid
        return gid


# ----------------------------
# Target: player upsert
# ----------------------------


def _player_upsert_row_from_member(
    member: Dict[str, Any], detail_map: Optional[Dict[str, Dict[str, str]]] = None
) -> Optional[Tuple[Any, ...]]:
    username = clean_username_value(member.get("name"))
    if not username:
        return None

    member_id = str(member.get("id") or "").strip() or None

    real_name = (member.get("realName") or "").strip()
    first, middle, last = "Unknown", "", "Unknown"
    if real_name:
        parts = real_name.split()
        if len(parts) == 1:
            first = parts[0]
            last = "Unknown"
        elif len(parts) == 2:
            first, last = parts[0], parts[1]
        else:
            first, middle, last = parts[0], " ".join(parts[1:-1]), parts[-1]

    mobile_10 = safe_mobile_10(member.get("mobileNumber"))
    email = sanitize_email(member.get("emailAddress"), username)
    reg_dt = parse_iso_dt(member.get("dateTimeCreated")) or datetime.now(timezone.utc)
    last_login = latest_dt(
        member.get("dateTimeLastAndroidLogIn"),
        member.get("dateTimeLastActive"),
    )
    address_street = "N/A"
    address_barangay = "N/A"
    address_city = "N/A"
    _detail = (detail_map or {}).get(member_id or "") or {}
    address_province = _detail.get("address_province") or "N/A"
    income_source = _detail.get("income_source") or "N/A"
    industry = _detail.get("industry") or "N/A"

    wallet = member.get("wallet") or {}
    if isinstance(wallet, str):
        try:
            wallet = json.loads(wallet)
        except Exception:
            wallet = {}
    wallet_balance = to_decimal_str(wallet.get("balance") or "0")
    wallet_balance_dt = reg_dt

    verification_status = str(member.get("verificationStatus") or "").upper()
    is_verified = verification_status in ("VERIFIED", "APPROVED")
    is_active = str(member.get("status") or "").upper() == "ACTIVE"

    return (
        username,
        first,
        middle,
        last,
        mobile_10,
        email,
        reg_dt,
        member.get("ipAddress"),
        BRAND,
        is_verified,
        is_active,
        last_login,
        member.get("ipAddress"),  # lastLoginIp — fallback to registrationIp
        address_street,
        address_barangay,
        address_city,
        address_province,
        income_source,
        industry,
        member_id,  # externalId = member.id
        wallet_balance,
        wallet_balance_dt,
    )


def bulk_ensure_players_from_members(
    tgt_conn,
    members: List[Dict[str, Any]],
    player_map: Dict[str, uuid.UUID],
    dry_run: bool,
    detail_map: Optional[Dict[str, Dict[str, str]]] = None,
) -> None:
    rows_by_user: Dict[str, Tuple[Any, ...]] = {}
    for m in members:
        row = _player_upsert_row_from_member(m, detail_map=detail_map)
        if not row:
            continue
        uname = str(row[0])
        if uname and username_match_key(uname) not in player_map:
            rows_by_user[uname] = row

    if not rows_by_user:
        return

    usernames = sorted(rows_by_user.keys())
    if dry_run:
        for uname in usernames:
            player_map[username_match_key(uname)] = uuid.uuid4()
        return

    sql = """
    INSERT INTO migration_repair."playerDetails_final" (
        "userName",
        "firstName","middleName","lastName",
        "mobileNumber","mobileNumberVerified",
        "emailAddress","emailVerified",
        "registrationDate",
        "registrationIp",
        "registrationReferrer",
        "brandName",
        "isVerified","isBlocked","blockedDatetime","isActive",
        "lastLogin","lastLoginIp",
        "outletCode","affiliateCode",
        "addressStreet","addressBarangay","addressCity","addressProvince",
        "incomeSource","industry",
        "externalId",
        "walletBalance","walletBalanceDatetime",
        "createdAt","updatedAt"
    )
    VALUES %s
    ON CONFLICT ("userName") DO NOTHING
    """
    values = [rows_by_user[u] for u in usernames]
    with tgt_conn.cursor() as cur:
        execute_values(
            cur,
            sql,
            values,
            template="(%s,%s,%s,%s,%s,false,%s,false,%s,%s,NULL,%s,%s,false,NULL,%s,%s,%s,NULL,NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())",
            page_size=5000,
        )

    with tgt_conn.cursor() as cur:
        cur.execute(
            'SELECT id, "userName" FROM migration_repair."playerDetails_final" WHERE LOWER(TRIM("userName")) = ANY(%s)',
            ([username_match_key(u) for u in usernames],),
        )
        for pid, uname in cur.fetchall():
            player_map[username_match_key(uname)] = pid


def upsert_player_from_member(
    tgt_conn,
    member: Dict[str, Any],
    dry_run: bool,
    detail_map: Optional[Dict[str, Dict[str, str]]] = None,
) -> uuid.UUID:
    """
    Upsert playerDetails using member object from PlayerRegistrationsInplayV2 or embedded member in tx/deposit/withdraw.
    """
    username = clean_username_value(member.get("name"))
    if not username:
        raise RuntimeError("Member missing name")

    member_id = str(member.get("id") or "").strip() or None

    real_name = (member.get("realName") or "").strip()
    first, middle, last = "Unknown", "", "Unknown"
    if real_name:
        parts = real_name.split()
        if len(parts) == 1:
            first = parts[0]
            last = "Unknown"
        elif len(parts) == 2:
            first, last = parts[0], parts[1]
        else:
            first, middle, last = parts[0], " ".join(parts[1:-1]), parts[-1]

    mobile_10 = safe_mobile_10(member.get("mobileNumber"))
    email = sanitize_email(member.get("emailAddress"), username)

    reg_dt = parse_iso_dt(member.get("dateTimeCreated")) or datetime.now(timezone.utc)
    last_login = latest_dt(
        member.get("dateTimeLastAndroidLogIn"),
        member.get("dateTimeLastActive"),
    )

    verification_status = str(member.get("verificationStatus") or "").upper()
    is_verified = verification_status in ("VERIFIED", "APPROVED")
    is_active = str(member.get("status") or "").upper() == "ACTIVE"
    outlet_code = str(member.get("branchCode") or "").strip() or None
    if outlet_code:
        ensure_outlet_code_enrolled(tgt_conn, outlet_code, dry_run)
    birthdate = parse_iso_dt(member.get("birthDay"))

    # Required NOT NULL placeholders — supplement from PlayerDetailInplayV2 where available.
    address_street = "N/A"
    address_barangay = "N/A"
    address_city = "N/A"
    _detail = (detail_map or {}).get(member_id or "") or {}
    address_province = _detail.get("address_province") or "N/A"
    income_source = _detail.get("income_source") or "N/A"
    industry = _detail.get("industry") or "N/A"

    wallet = member.get("wallet") or {}
    if isinstance(wallet, str):
        try:
            wallet = json.loads(wallet)
        except Exception:
            wallet = {}
    wallet_balance = to_decimal_str(wallet.get("balance") or "0")
    wallet_balance_dt = reg_dt

    sql = """
    INSERT INTO migration_repair."playerDetails_final" (
        "userName",
        "firstName","middleName","lastName",
        "mobileNumber","mobileNumberVerified",
        "emailAddress","emailVerified",
        "registrationDate",
        "registrationIp",
        "registrationReferrer",
        "brandName",
        "isVerified","isBlocked","blockedDatetime","isActive",
        "lastLogin","lastLoginIp",
        "outletCode","affiliateCode",
        "addressStreet","addressBarangay","addressCity","addressProvince",
        "incomeSource","industry",
        "externalId",
        "birthdate",
        "walletBalance","walletBalanceDatetime",
        "createdAt","updatedAt"
    )
    VALUES (
        %s,
        %s,%s,%s,
        %s,false,
        %s,false,
        %s,
        %s,
        NULL,
        %s,
        %s,false,NULL,%s,
        %s,%s,
        %s,NULL,
        %s,%s,%s,%s,
        %s,%s,
        %s,
        %s,
        %s,%s,
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
        "registrationDate"=LEAST(EXCLUDED."registrationDate", "playerDetails_final"."registrationDate"),
        "outletCode"=COALESCE(EXCLUDED."outletCode", "playerDetails_final"."outletCode"),
        "birthdate"=COALESCE(EXCLUDED."birthdate", "playerDetails_final"."birthdate"),
        "externalId"=EXCLUDED."externalId",
        "walletBalance"=EXCLUDED."walletBalance",
        "walletBalanceDatetime"=EXCLUDED."walletBalanceDatetime",
        "addressProvince"=CASE WHEN EXCLUDED."addressProvince" <> 'N/A' THEN EXCLUDED."addressProvince" ELSE "playerDetails_final"."addressProvince" END,
        "incomeSource"=CASE WHEN EXCLUDED."incomeSource" <> 'N/A' THEN EXCLUDED."incomeSource" ELSE "playerDetails_final"."incomeSource" END,
        "industry"=CASE WHEN EXCLUDED."industry" <> 'N/A' THEN EXCLUDED."industry" ELSE "playerDetails_final"."industry" END,
        "updatedAt"=now()
    RETURNING id
    """

    params = (
        username,
        first,
        middle,
        last,
        mobile_10,
        email,
        reg_dt,
        member.get("ipAddress"),
        BRAND,
        is_verified,
        is_active,
        last_login,
        member.get(
            "ipAddress"
        ),  # lastLoginIp — fallback to registrationIp (only IP available in source)
        outlet_code,
        address_street,
        address_barangay,
        address_city,
        address_province,
        income_source,
        industry,
        member_id,  # externalId = member.id
        birthdate,
        wallet_balance,
        wallet_balance_dt,
    )

    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute(
                'SELECT id FROM migration_repair."playerDetails_final" WHERE LOWER(TRIM("userName"))=%s',
                (username_match_key(username),),
            )
            row = cur.fetchone()
            return row[0] if row else uuid.uuid4()

    with tgt_conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def build_player_map(tgt_conn) -> Dict[str, uuid.UUID]:
    """
    Build player_map from the target final player table.

    Deployment routing:
      target DB connection: iestdl
      target player table: kemet."playerDetails_final"

    The returned id is used as the playerId for downstream game/wallet rows.
    This keeps transaction foreign-key values aligned with the target final table.

    Matching rule:
      - brandName is filtered with LOWER(TRIM()) in SQL.
      - userName is normalized by username_match_key() before becoming a player_map key.
      - game/wallet source member.name is normalized by the same username_match_key() before lookup.

    Diagnostics:
      - total_playerdetails_rows: all rows in kemet."playerDetails_final".
      - brand_matched_rows: rows that match BRAND after LOWER(TRIM()).
      - fetched_rows: rows returned by the brand-filtered player map query.
      - loaded_keys: final player_map size after username normalization.
      - blank_username_keys: brand-matched rows whose userName normalized to blank.
      - duplicate_normalized_keys: brand-matched rows that collide after normalization.
    """
    m: Dict[str, uuid.UUID] = {}
    total_playerdetails_rows = 0
    brand_matched_rows = 0
    fetched_rows = 0
    blank_username_keys = 0
    duplicate_normalized_keys = 0

    with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                COUNT(*)::bigint AS total_playerdetails_rows,
                COALESCE(SUM(
                    CASE
                        WHEN LOWER(TRIM(COALESCE("brandName", ''))) = LOWER(TRIM(%s)) THEN 1
                        ELSE 0
                    END
                ), 0)::bigint AS brand_matched_rows
            FROM migration_repair."playerDetails_final"
            """,
            (BRAND,),
        )
        count_row = cur.fetchone() or {}
        total_playerdetails_rows = int(count_row.get("total_playerdetails_rows") or 0)
        brand_matched_rows = int(count_row.get("brand_matched_rows") or 0)

        cur.execute(
            """
            SELECT id, "userName"
            FROM migration_repair."playerDetails_final"
            WHERE LOWER(TRIM(COALESCE("brandName", ''))) = LOWER(TRIM(%s))
            """,
            (BRAND,),
        )
        rows = cur.fetchall()
        fetched_rows = len(rows)

        for r in rows:
            key = username_match_key(r["userName"])
            if not key:
                blank_username_keys += 1
                continue
            if key in m:
                duplicate_normalized_keys += 1
            m[key] = r["id"]

    try:
        trace_print(
            f"[PLAYER MAP] total_playerdetails_rows={total_playerdetails_rows} "
            f"brand_matched_rows={brand_matched_rows} fetched_rows={fetched_rows} "
            f"loaded_keys={len(m)} blank_username_keys={blank_username_keys} "
            f"duplicate_normalized_keys={duplicate_normalized_keys} "
            f'table=kemet."playerDetails_final" brand={BRAND}'
        )
    except NameError:
        print(
            f"[PLAYER MAP] total_playerdetails_rows={total_playerdetails_rows} "
            f"brand_matched_rows={brand_matched_rows} fetched_rows={fetched_rows} "
            f"loaded_keys={len(m)} blank_username_keys={blank_username_keys} "
            f"duplicate_normalized_keys={duplicate_normalized_keys} "
            f'table=kemet."playerDetails_final" brand={BRAND}',
            flush=True,
        )

    return m


def lookup_player_id_by_username(tgt_conn, username: Any) -> Optional[uuid.UUID]:
    """
    Fallback target lookup for player_map misses.

    Business rule preserved:
      - This does NOT create a player.
      - This does NOT generate a new UUID.
      - It only returns an existing kemet."playerDetails_final".id UUID.
      - If no existing playerDetails row matches, caller must skip the transaction.
    """
    username_key = username_match_key(username)
    if not username_key:
        return None

    sql = """
    SELECT id, "userName"
    FROM migration_repair."playerDetails_final"
    WHERE LOWER(TRIM(COALESCE("brandName", ''))) = LOWER(TRIM(%s))
      AND regexp_replace(LOWER(TRIM(COALESCE("userName", ''))), '\\s+', '', 'g') = %s
    ORDER BY "createdAt" DESC NULLS LAST, id DESC
    LIMIT 1
    """

    with tgt_conn.cursor() as cur:
        cur.execute(sql, (BRAND, username_key))
        row = cur.fetchone()

    if not row:
        try:
            trace_print(f"[PLAYER MAP FALLBACK MISS] username={username} key={username_key}")
        except NameError:
            print(f"[PLAYER MAP FALLBACK MISS] username={username} key={username_key}", flush=True)
        return None

    player_id = row[0]
    matched_username = row[1]

    try:
        trace_print(
            f"[PLAYER MAP FALLBACK HIT] username={username} key={username_key} "
            f"matchedUserName={matched_username} playerId={player_id}"
        )
    except NameError:
        print(
            f"[PLAYER MAP FALLBACK HIT] username={username} key={username_key} "
            f"matchedUserName={matched_username} playerId={player_id}",
            flush=True,
        )

    return player_id


def ensure_outlet_code_enrolled(tgt_conn, outlet_code: str, dry_run: bool) -> None:
    """Insert outlet_code into outletList if not already present (ON CONFLICT DO NOTHING)."""
    if dry_run:
        return
    sql = """
    INSERT INTO migration_repair."outletList_final" (
        "outletCode", "outletName",
        "streetAddress", "barangayAddress", "cityAddress", "provinceAddress",
        "outletShare", "operator", "isActive", "brand",
        "createdAt", "updatedAt", "lastUpdateDatetime"
    ) VALUES (
        %s, %s, '', '', '', '',
        0.00, 'Inplay', true, %s,
        now(), now(), now()
    )
    ON CONFLICT ("outletCode") DO NOTHING
    """
    with tgt_conn.cursor() as cur:
        cur.execute(sql, (outlet_code, outlet_code, BRAND))
    tgt_conn.commit()


# ----------------------------
# Source fetchers (batched by date+id cursor)
# ----------------------------


def parse_inplayv2_checkpoint(raw: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse a checkpoint value into (after_dt, after_id).
    New format: "2025-03-12T10:00:00Z|Hmzes2p18ca8ChBnCG"
    Legacy format (id only, no pipe): IDs are alphanumeric with no recoverable date — restart from
    the beginning (safe since all inserts are idempotent ON CONFLICT).
    """
    if not raw:
        return (None, None)
    if "|" in raw:
        dt_str, id_str = raw.split("|", 1)
        return (dt_str or None, id_str or None)
    # Legacy checkpoint stored only the raw id — can't reconstruct the date cursor, so restart
    return (None, None)


def format_inplayv2_checkpoint(dt_iso: Optional[str], raw_id: str) -> str:
    return f"{dt_iso or ''}|{raw_id}"

    def fetch_json_table_batch(
        src_conn,
        table: str,
        after_dt: Optional[str],
        after_id: Optional[str],
        limit: int,
        from_dt: Optional[str] = None,
        until_dt: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        # InPlayV2 ids are random alphanumeric strings (e.g. "Hmzes2p18ca8ChBnCG") — ordering by id
        # alone is not chronological. Use (dateTimeCreated, id) tuple cursor so pagination is
        # date-ordered and records from any date range are never skipped after a checkpoint resume.
        #
        # from_dt: applied as >= on the first call (after_dt is None) to start at a specific date.
        # until_dt: applied as < on every call as an exclusive upper bound.
        date_col = "COALESCE(data->>'dateTimeCreated', data->>'createdDateTime')::timestamptz"
        anchor_id = after_id or ""

        conditions = ["data IS NOT NULL"]
        params: List[Any] = []

        if after_dt is not None:
            conditions.append(f"({date_col}, id) > (%s::timestamptz, %s)")
            params.extend([after_dt, anchor_id])
        elif from_dt is not None:
            conditions.append(f"{date_col} >= %s::timestamptz")
            params.append(from_dt)

        if until_dt is not None:
            conditions.append(f"{date_col} < %s::timestamptz")
            params.append(until_dt)

        params.append(limit)
        where_clause = " AND ".join(conditions)

        with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT id, data
                FROM "{table}"
                WHERE {where_clause}
                ORDER BY {date_col} ASC, id ASC
                LIMIT %s
                """,
                params,
            )
            rows = cur.fetchall()
        # Keep source reads in short transactions during long-running migrations.
        src_conn.rollback()
        return rows


# ----------------------------
# Source fetchers (PlayerDetailInplayV2 verification data)
# ----------------------------


def fetch_player_detail_map(src_conn) -> Dict[str, Dict[str, str]]:
    """
    Fetch all PlayerDetailInplayV2 rows that have verification data.
    Returns a dict keyed by externalId (data->id) with address/income/work values.
    """
    detail_map: Dict[str, Dict[str, str]] = {}
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT data->>'id' AS external_id,
                   data->'verification'->>'address'        AS address_province,
                   data->'verification'->>'sourceOfIncome' AS income_source,
                   data->'verification'->>'natureOfWork'   AS industry
            FROM public."PlayerDetailInplayV2"
            WHERE data->'verification' IS NOT NULL
              AND data->'verification' != 'null'::jsonb
            """)
        for row in cur.fetchall():
            eid = row["external_id"]
            if eid:
                detail_map[eid] = {
                    "address_province": row["address_province"] or "N/A",
                    "income_source": row["income_source"] or "N/A",
                    "industry": row["industry"] or "N/A",
                }
    src_conn.rollback()
    print(f"Loaded PlayerDetailInplayV2 verification map: {len(detail_map)} entries", flush=True)
    return detail_map


# ----------------------------
# Target inserts: gameTransaction
# ----------------------------


def insert_game_tx_batch(
    tgt_conn,
    rows: List[Dict[str, Any]],
    player_map: Dict[str, uuid.UUID],
    provider_cache: Dict[str, uuid.UUID],
    gametype_cache: Dict[str, uuid.UUID],
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID],
    dry_run: bool,
) -> Tuple[int, int]:
    """
    Returns: (attempted, missing_player)
    """
    values: List[Tuple[Any, ...]] = []
    missing_player = 0

    missing_members: List[Dict[str, Any]] = []
    for r in rows:
        data = as_dict(r.get("data"))
        member = as_dict(data.get("member")) if data else {}
        username = clean_username_value(member.get("name"))
        if username and username not in player_map:
            missing_members.append(member)
    bulk_ensure_players_from_members(tgt_conn, missing_members, player_map, dry_run=dry_run)

    for r in rows:
        data = as_dict(r.get("data"))
        if not data:
            continue

        tx_id = str(data.get("id") or r.get("id") or "").strip()
        if not tx_id:
            continue

        member = as_dict(data.get("member"))
        username = clean_username_value(member.get("name"))
        if not username:
            continue

        player_id = player_map.get(username_match_key(username))
        if not player_id:
            # create shadow player from embedded member (no PlayerRegistrations coverage)
            player_id = upsert_player_from_member(tgt_conn, member, dry_run=dry_run)
            player_map[username] = player_id

        game = as_dict(data.get("game"))
        provider_name = str(game.get("provider") or "UNKNOWN")
        game_name = str(game.get("name") or "UNKNOWN")
        game_type_raw = str(game.get("type") or "SLOTS")

        provider_id = get_or_create_game_provider(tgt_conn, provider_name, provider_cache, dry_run)
        game_type_id = get_or_create_game_type(tgt_conn, game_type_raw, gametype_cache, dry_run)
        game_id = get_or_create_game_list(
            tgt_conn, game_name, provider_id, game_type_id, gamelist_cache, dry_run
        )

        created = parse_iso_dt(data.get("dateTimeCreated")) or datetime.now(timezone.utc)
        settled = parse_iso_dt(data.get("dateTimeSettled")) or created

        bet_amount = to_decimal_str(data.get("bet"))
        payout_amount = to_decimal_str(data.get("payout"))
        pc5 = to_decimal_str(data.get("jackpotContribution"))
        jw5 = to_decimal_str(data.get("jackpotPayout"))

        round_id = data.get("vendorRoundId")

        values.append(
            (
                created,
                provider_id,
                game_id,
                game_type_id,
                player_id,
                username,
                None,  # tableRoomId (no source mapping)
                "0",  # sideBetAmount
                bet_amount,
                bet_amount,  # validBet
                payout_amount,
                "0",
                "0",
                "0",
                "0",
                pc5,
                "0",
                "0",
                "0",
                "0",
                jw5,
                "0",  # progressionContributionPaid
                "0",
                0,  # seedMoneyJackpotOver1000
                settled,
                tx_id,  # externalId UNIQUE
                False,
                None,
                None,
                BRAND,
                PLATFORM,
                str(round_id) if round_id is not None else None,
            )
        )

    if dry_run:
        return (len(values), missing_player)

    if not values:
        return (0, missing_player)

    sql = """
    INSERT INTO migration_repair."gameTransaction_final" (
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

    return (len(values), missing_player)


# ----------------------------
# Target inserts: walletTransaction
# ----------------------------


def wallet_row_to_values(
    tgt_conn,
    kind: str,  # "deposit" | "withdrawal"
    src_id: str,
    data: Dict[str, Any],
    player_map: Dict[str, uuid.UUID],
    dry_run: bool,
) -> Optional[Tuple[Any, ...]]:
    member = as_dict(data.get("member"))
    username = clean_username_value(member.get("name"))
    if not username:
        return None

    player_id = player_map.get(username_match_key(username))
    if not player_id:
        player_id = upsert_player_from_member(tgt_conn, member, dry_run=dry_run)
        player_map[username] = player_id

    # amounts / status / dates
    amount = to_decimal_str(data.get("netAmount") or data.get("amount"))
    status = normalize_wallet_status(data.get("status"))

    created_dt = (
        parse_iso_dt(data.get("dateTimeCreated")) or parse_iso_dt(data.get("createdDateTime"))
    ) or datetime.now(timezone.utc)
    confirmed_dt = parse_iso_dt(data.get("dateTimeConfirmed")) or created_dt

    # payment gateway / domain
    payment_gateway = str(data.get("type") or data.get("paymentGateway") or "N/A").strip() or "N/A"
    domain = (
        str(
            data.get("domain")
            or (member.get("domain") if isinstance(member, dict) else None)
            or BRAND
        ).strip()
        or BRAND
    )

    # reference id: data->id, fallback to src_id
    ref = data.get("id") or src_id
    reference_id = str(ref).strip() if ref is not None else None
    if not reference_id:
        reference_id = f"{kind}:{src_id}"

    # status date constraints
    confirmed = confirmed_dt if status == "confirmed" else None
    cancelled = created_dt if status == "cancelled" else None
    failed = created_dt if status == "failed" else None

    return (
        kind,
        WALLET_PLATFORM,
        player_id,
        payment_gateway,
        domain,
        amount,
        status,
        None,  # bettingPhase
        created_dt,
        confirmed,
        cancelled,
        failed,
        reference_id,
    )


def insert_wallet_batch(
    tgt_conn, rows: List[Dict[str, Any]], kind: str, player_map: Dict[str, uuid.UUID], dry_run: bool
) -> int:
    values: List[Tuple[Any, ...]] = []

    missing_members: List[Dict[str, Any]] = []
    for r in rows:
        data = as_dict(r.get("data"))
        member = as_dict(data.get("member")) if data else {}
        username = clean_username_value(member.get("name"))
        if username and username not in player_map:
            missing_members.append(member)
    bulk_ensure_players_from_members(tgt_conn, missing_members, player_map, dry_run=dry_run)

    for r in rows:
        src_id = str(r.get("id") or "").strip()
        data = as_dict(r.get("data"))
        if not src_id or not data:
            continue
        v = wallet_row_to_values(tgt_conn, kind, src_id, data, player_map, dry_run=dry_run)
        if v:
            values.append(v)

    if dry_run:
        return len(values)

    if not values:
        return 0

    # Requires our partial unique index. Use ON CONFLICT with WHERE matching predicate.
    sql = f"""
    INSERT INTO migration_repair."walletTransaction_final" (
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
    WHERE ("platform" = '{WALLET_PLATFORM}' AND "referenceId" IS NOT NULL)
    DO NOTHING
    """
    with tgt_conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=1000)

    return len(values)


# ----------------------------
# Delete-first
# ----------------------------


def _parse_date_arg(s: str) -> datetime:
    """Parse YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS for CLI date arguments.

    The migration source window later uses only the calendar date portion and
    applies the Inplay PHT business boundary in date_window_bounds_for_source().
    """
    s = s.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    import argparse as _ap

    raise _ap.ArgumentTypeError(
        f"Invalid date: {s!r}. Expected YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS"
    )


def date_window_bounds_for_source(
    date_from: Optional[datetime], date_to: Optional[datetime]
) -> Tuple[Optional[str], Optional[str]]:
    """Return PHT-aware source date-window bounds for dateTimeCreated.

    Business window:
      --date-from YYYY-MM-DD -> YYYY-MM-DD 06:00:00+08:00 inclusive
      --date-to   YYYY-MM-DD -> YYYY-MM-DD 06:00:00+08:00 exclusive

    Example:
      --date-from 2026-05-28 --date-to 2026-05-29
      pulls records from 2026-05-28 06:00:00 PHT up to, but not including,
      2026-05-29 06:00:00 PHT.

    PostgreSQL compares these as timestamptz values against source JSONB dates
    that contain UTC/Z timestamps. This makes the pull window business-PHT aware
    while final timestamp with time zone columns can still store/display UTC.
    """

    def _pht_boundary_text(value: datetime, hour: int, minute: int = 0, second: int = 0) -> str:
        boundary = datetime.combine(
            value.date(),
            datetime.min.time(),
            tzinfo=PHT_TZ,
        ).replace(hour=hour, minute=minute, second=second, microsecond=0)
        return boundary.isoformat(sep=" ", timespec="seconds")

    start_text: Optional[str] = None
    end_text: Optional[str] = None

    if date_from is not None:
        start_text = _pht_boundary_text(date_from, 6, 0, 0)

    if date_to is not None:
        # Exclusive upper bound: < date_to 06:00:00+08:00.
        # This is safer than <= 05:59:59 because it includes fractional seconds.
        end_text = _pht_boundary_text(date_to, 6, 0, 0)

    return start_text, end_text


def delete_inplayv2_target_data(
    tgt_conn,
    dry_run: bool,
    keep_from: Optional[datetime] = None,
    keep_to: Optional[datetime] = None,
) -> None:
    """
    Delete all Inplay data from the target DB.

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
        msg = "[DRY-RUN] would delete InPlayV2 target rows (gameTransaction, walletTransaction, playerDetails, checkpoints)"
        if has_range:
            msg = f"[DRY-RUN] would delete Inplay data (keeping records between {keep_from} and {keep_to})"
        print(msg)
        return

    with tgt_conn.cursor() as cur:
        gt_params: List[Any] = [BRAND]
        cur.execute(
            f'DELETE FROM migration_repair."gameTransaction_final" WHERE "brand"=%s{_exclusion("startDateTime", gt_params)}',
            gt_params,
        )
        gt = cur.rowcount

        wt_params: List[Any] = [WALLET_PLATFORM]
        cur.execute(
            f'DELETE FROM migration_repair."walletTransaction_final" WHERE "platform"=%s{_exclusion("createdDatetime", wt_params)}',
            wt_params,
        )
        wt = cur.rowcount

        pd_params: List[Any] = [BRAND]
        cur.execute(
            f'DELETE FROM migration_repair."playerDetails_final" WHERE "brandName"=%s{_exclusion("registrationDate", pd_params)}',
            pd_params,
        )
        pl = cur.rowcount

        if not has_range:
            cur.execute(
                'DELETE FROM migration_repair."migrationCheckpoint" WHERE platform LIKE %s',
                (f"{BRAND}_%",),
            )
            ck = cur.rowcount
        else:
            ck = 0

    tgt_conn.commit()
    print(
        f"Deleted: gameTransaction={gt}, walletTransaction={wt}, playerDetails={pl}, checkpoints={ck}",
        flush=True,
    )


# ----------------------------
# Repair existing data (fix already-migrated records)
# ----------------------------

OLD_BRAND = "InPlayV2"  # previous brand value used in migrated records


def repair_existing_data(
    src_conn,
    tgt_conn,
    dry_run: bool,
    batch_size: int,
    commit_every: int,
) -> None:
    """
    Fix records already inserted under the old BRAND='InPlayV2' mapping.

    SQL-only fixes (no source data needed):
      - Rename brand/platform/brandName/checkpoint keys: InPlayV2 → Inplay
      - gameTransaction: swap PC1↔PC5 (jackpotContribution was wrongly in PC1)
      - gameTransaction: swap JW5↔seedMoneyWon (jackpotPayout was wrongly in seedMoneyWon)
      - gameTransaction: set tableRoomId = NULL (had serialCode; mapping says no source)
      - Recreate wallet dedupe index for new platform value

    Phases requiring source data:
      - Players: re-upsert from PlayerRegistrationsInplayV2 (ON CONFLICT UPDATE fixes
        isActive, outletCode, birthdate)
      - Wallets: delete existing rows then re-insert with corrected
        createdDatetime (createdDateTime), amount (netAmount), referenceId (data->id)
    """
    print("\n=== Repair existing InPlayV2 data ===", flush=True)

    # ------------------------------------------------------------------
    # Step 1: SQL-only fixes on target DB
    # ------------------------------------------------------------------
    if dry_run:
        print(
            "[DRY-RUN] Would run SQL fixes (brand rename, PC/JW swap, tableRoomId, checkpoints)",
            flush=True,
        )
    else:
        with tgt_conn.cursor() as cur:
            # Rename brand in gameTransaction
            cur.execute(
                'UPDATE migration_repair."gameTransaction_final" SET "brand" = %s WHERE "brand" = %s',
                (BRAND, OLD_BRAND),
            )
            gt_brand = cur.rowcount
            print(f"  gameTransaction brand renamed: {gt_brand} rows", flush=True)

            # Swap PC1 (had jackpotContribution) → PC5; zero out PC1
            cur.execute(
                """
                UPDATE migration_repair."gameTransaction_final"
                SET "PC5" = "PC1", "PC1" = '0'
                WHERE "brand" = %s AND "PC1" <> '0'
                """,
                (BRAND,),
            )
            gt_pc = cur.rowcount
            print(f"  gameTransaction PC1→PC5 swap: {gt_pc} rows", flush=True)

            # Swap seedMoneyWon (had jackpotPayout) → JW5; zero out seedMoneyWon
            cur.execute(
                """
                UPDATE migration_repair."gameTransaction_final"
                SET "JW5" = "seedMoneyWon", "seedMoneyWon" = '0'
                WHERE "brand" = %s AND "seedMoneyWon" <> '0'
                """,
                (BRAND,),
            )
            gt_jw = cur.rowcount
            print(f"  gameTransaction JW5/seedMoneyWon swap: {gt_jw} rows", flush=True)

            # Clear tableRoomId (was serialCode; mapping has no source)
            cur.execute(
                'UPDATE migration_repair."gameTransaction_final" SET "tableRoomId" = NULL WHERE "brand" = %s AND "tableRoomId" IS NOT NULL',
                (BRAND,),
            )
            gt_tr = cur.rowcount
            print(f"  gameTransaction tableRoomId cleared: {gt_tr} rows", flush=True)

            # Rename platform in walletTransaction
            cur.execute(
                'UPDATE migration_repair."walletTransaction_final" SET "platform" = %s WHERE "platform" = %s',
                (WALLET_PLATFORM, OLD_BRAND),
            )
            wt_plat = cur.rowcount
            print(f"  walletTransaction platform renamed: {wt_plat} rows", flush=True)

            # Rename brandName in playerDetails
            cur.execute(
                'UPDATE migration_repair."playerDetails_final" SET "brandName" = %s WHERE "brandName" = %s',
                (BRAND, OLD_BRAND),
            )
            pd_brand = cur.rowcount
            print(f"  playerDetails brandName renamed: {pd_brand} rows", flush=True)

            # Rename migrationCheckpoint keys
            cur.execute(
                """
                UPDATE migration_repair."migrationCheckpoint"
                SET platform = REPLACE(platform, %s || '_', %s || '_')
                WHERE platform LIKE %s
                """,
                (OLD_BRAND, BRAND, f"{OLD_BRAND}_%"),
            )
            ck_renamed = cur.rowcount
            print(f"  migrationCheckpoint keys renamed: {ck_renamed} rows", flush=True)

            # Recreate wallet dedupe index for new platform value
            cur.execute("DROP INDEX IF EXISTS ux_wallet_inplayv2_reference")
            print("  Dropped old wallet dedupe index", flush=True)

        tgt_conn.commit()
        print("SQL fixes committed.", flush=True)

        # Recreate index for new WALLET_PLATFORM
        ensure_wallet_dedupe_index(tgt_conn, dry_run=False)
        tgt_conn.commit()
        print(f"  Recreated wallet dedupe index for platform='{WALLET_PLATFORM}'", flush=True)

    # ------------------------------------------------------------------
    # Step 2: Re-upsert players from source (fixes isActive, outletCode, birthdate)
    # ------------------------------------------------------------------
    print("\n[Repair] Re-processing player registrations from source...", flush=True)
    detail_map = fetch_player_detail_map(src_conn)
    processed = 0
    last_dt, last_id = None, None
    while True:
        rows = fetch_json_table_batch(
            src_conn, "PlayerRegistrationsInplayV2", last_dt, last_id, batch_size
        )
        if not rows:
            break
        for r in rows:
            rid = str(r["id"])
            data = as_dict(r.get("data"))
            if data:
                try:
                    _ = upsert_player_from_member(
                        tgt_conn, data, dry_run=dry_run, detail_map=detail_map
                    )
                except Exception as e:
                    print(f"[WARN] player upsert failed id={rid}: {e}", flush=True)
                    tgt_conn.rollback()
            processed += 1
            last_id = rid
            last_dt = str(data.get("dateTimeCreated") or "") or None
            if (not dry_run) and (processed % commit_every == 0):
                tgt_conn.commit()
                print(f"  Progress players: processed={processed} lastId={last_id}", flush=True)

    if not dry_run:
        tgt_conn.commit()
    print(f"[Repair] Player re-upsert done. processed={processed}", flush=True)

    # ------------------------------------------------------------------
    # Step 2b: Direct KYC back-fill for all 73 verified players by externalId.
    # Covers both registration-sourced players AND any shadow players whose
    # externalId was set but whose KYC fields were never populated.
    # ------------------------------------------------------------------
    print(
        "\n[Repair] Back-filling KYC fields (addressProvince/incomeSource/industry) by externalId...",
        flush=True,
    )
    if dry_run:
        print(f"[DRY-RUN] Would update KYC fields for {len(detail_map)} players", flush=True)
    else:
        kyc_updated = 0
        with tgt_conn.cursor() as cur:
            for external_id, detail in detail_map.items():
                cur.execute(
                    """
                    UPDATE migration_repair."playerDetails_final"
                    SET "addressProvince" = %s,
                        "incomeSource"    = %s,
                        "industry"        = %s,
                        "updatedAt"       = now()
                    WHERE "externalId" = %s
                      AND "brandName"  = %s
                      AND (
                          "addressProvince" = 'N/A' OR "addressProvince" IS NULL OR
                          "incomeSource"    = 'N/A' OR "incomeSource"    IS NULL OR
                          "industry"        = 'N/A' OR "industry"        IS NULL
                      )
                    """,
                    (
                        detail["address_province"],
                        detail["income_source"],
                        detail["industry"],
                        external_id,
                        BRAND,
                    ),
                )
                kyc_updated += cur.rowcount
        tgt_conn.commit()
        print(f"[Repair] KYC back-fill done. updated={kyc_updated} players", flush=True)

    # ------------------------------------------------------------------
    # Step 3: Delete and re-insert wallet transactions (fixes createdDatetime,
    #         amount→netAmount, referenceId→data->id)
    # ------------------------------------------------------------------
    print("\n[Repair] Deleting existing wallet records for re-insertion...", flush=True)
    if not dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute(
                'DELETE FROM migration_repair."walletTransaction_final" WHERE "platform" = %s',
                (WALLET_PLATFORM,),
            )
            wt_deleted = cur.rowcount
        tgt_conn.commit()
        print(f"  Deleted {wt_deleted} walletTransaction rows", flush=True)
    else:
        print("[DRY-RUN] Would delete walletTransaction rows and re-insert", flush=True)

    player_map = build_player_map(tgt_conn)

    for kind, table in [("deposit", "DepositsInplayV2"), ("withdrawal", "WithdrawalsInplayV2")]:
        print(f"[Repair] Re-inserting {kind}s from {table}...", flush=True)
        processed_w = 0
        last_dt, last_id = None, None
        while True:
            rows = fetch_json_table_batch(src_conn, table, last_dt, last_id, batch_size)
            if not rows:
                break
            insert_wallet_batch(tgt_conn, rows, kind, player_map, dry_run=dry_run)
            processed_w += len(rows)
            last_row_data = as_dict(rows[-1].get("data"))
            last_id = str(rows[-1]["id"])
            last_dt = (
                str(
                    last_row_data.get("dateTimeCreated")
                    or last_row_data.get("createdDateTime")
                    or ""
                )
                or None
            )
            if (not dry_run) and (processed_w % commit_every) < batch_size:
                tgt_conn.commit()
                print(f"  Progress {kind}s: processed={processed_w} lastId={last_id}", flush=True)
        if not dry_run:
            tgt_conn.commit()
        print(f"[Repair] {kind}s done. processed={processed_w}", flush=True)

    if dry_run:
        tgt_conn.rollback()
        print("[DRY-RUN] rolled back all writes.", flush=True)

    print("\n=== Repair complete ===", flush=True)


# ----------------------------
# Repair: fix wallet statuses from source
# ----------------------------


def repair_wallet_statuses(
    src_conn,
    tgt_conn,
    dry_run: bool,
    batch_size: int,
    commit_every: int,
) -> None:
    """
    Re-fetch the raw status from iestdl for every walletTransaction row
    and update the target if the stored value differs.
    Also corrects the constrained date columns to match the corrected status.
    """
    print("\n=== Repair wallet statuses ===", flush=True)

    for kind, table in [("deposit", "DepositsInplayV2"), ("withdrawal", "WithdrawalsInplayV2")]:
        print(f"[repair-status] Scanning {table}...", flush=True)
        processed = updated = 0
        last_dt, last_id = None, None

        while True:
            rows = fetch_json_table_batch(src_conn, table, last_dt, last_id, batch_size)
            if not rows:
                break

            for r in rows:
                src_id = str(r["id"])
                data = as_dict(r.get("data"))
                raw_status = normalize_wallet_status(data.get("status"))
                ref = data.get("id") or src_id
                reference_id = str(ref).strip() if ref is not None else f"{kind}:{src_id}"

                confirmed = None
                cancelled = None
                failed = None
                if raw_status == "confirmed":
                    confirmed_dt = parse_iso_dt(data.get("dateTimeConfirmed")) or parse_iso_dt(
                        data.get("dateTimeCreated") or data.get("createdDateTime")
                    )
                    confirmed = confirmed_dt
                elif raw_status == "cancelled":
                    created_dt = parse_iso_dt(
                        data.get("dateTimeCreated") or data.get("createdDateTime")
                    ) or datetime.now(timezone.utc)
                    cancelled = created_dt
                elif raw_status == "failed":
                    created_dt = parse_iso_dt(
                        data.get("dateTimeCreated") or data.get("createdDateTime")
                    ) or datetime.now(timezone.utc)
                    failed = created_dt

                if not dry_run:
                    with tgt_conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE migration_repair."walletTransaction_final"
                            SET status = %s,
                                "confirmedDatetime" = %s,
                                "cancelledDatetime" = %s,
                                "failedDatetime"    = %s,
                                "updatedAt"         = now()
                            WHERE platform = %s
                              AND "referenceId" = %s
                              AND "transactionType" = %s
                              AND status <> %s
                            """,
                            (
                                raw_status,
                                confirmed,
                                cancelled,
                                failed,
                                WALLET_PLATFORM,
                                reference_id,
                                kind,
                                raw_status,
                            ),
                        )
                        updated += cur.rowcount

                processed += 1

            last_row_data = as_dict(rows[-1].get("data"))
            last_id = str(rows[-1]["id"])
            last_dt = (
                str(
                    last_row_data.get("dateTimeCreated")
                    or last_row_data.get("createdDateTime")
                    or ""
                )
                or None
            )

            if not dry_run and (processed % commit_every) < batch_size:
                tgt_conn.commit()
                print(f"  Progress {kind}s: processed={processed} updated={updated}", flush=True)

        if not dry_run:
            tgt_conn.commit()
        print(f"[repair-status] {kind}s done. processed={processed} updated={updated}", flush=True)

    if dry_run:
        tgt_conn.rollback()
        print("[DRY-RUN] rolled back all writes.", flush=True)

    print("\n=== Repair wallet statuses complete ===", flush=True)


# ============================================================================
# InPlayV2 audit/reporting overlay
# - Adds InPlayV1-style live trace logging, CSV reports, reconciliation, DQ,
#   report packaging, and mailer dispatch.
# - Preserves InPlayV2 mappings/table names from the original script.
# - Per latest requirement: game/wallet phases DO NOT create shadow/ghost players
#   when playerDetails/player_map is missing. They log the skipped source row.
# ============================================================================

import csv
import logging
import zipfile

try:
    from utilities.mailer import send_migration_reports
except Exception:
    send_migration_reports = None

TIMESTAMP_STR = datetime.now().strftime("%Y%m%d%H%M%S")
LOG_FILE_PATH = f"logs/inplayv2_migration_trace_{TIMESTAMP_STR}.log"
CSV_GAMETX_PATH = f"reports/inplayv2_gameTransaction_{TIMESTAMP_STR}.csv"
CSV_DEPOSITS_PATH = f"reports/inplayv2_deposits_{TIMESTAMP_STR}.csv"
CSV_WITHDRAWALS_PATH = f"reports/inplayv2_withdrawals_{TIMESTAMP_STR}.csv"
CSV_PLAYERS_PATH = f"reports/inplayv2_players_{TIMESTAMP_STR}.csv"
CSV_RECONCILIATION_PATH = f"reports/inplayv2_reconciliation_{TIMESTAMP_STR}.csv"
CSV_DATA_QUALITY_PATH = f"reports/inplayv2_data_quality_{TIMESTAMP_STR}.csv"
REPORT_ZIP_THRESHOLD_BYTES = 17 * 1024 * 1024


def _artifact_date_part(value: Optional[datetime], fallback: str) -> str:
    """Return YYYYMMDD for report/log filenames, with an 8-digit fallback for open-ended date-window runs."""
    if value is None:
        return fallback
    return value.strftime("%Y%m%d")


def configure_run_artifact_paths(args: argparse.Namespace) -> None:
    """Set CSV and log filenames from --date-from/--date-to and the current run timestamp.

    Date-window filename shape:
      CSV: <phase>_<date-from>_<date-to>-rundate_<YYYYMMDDHHMMSS>.csv
      LOG: inplayv2_trace_<date-from>_<date-to>-rundate_<YYYYMMDDHHMMSS>.log

    Full-run filename shape, when neither --date-from nor --date-to is provided:
      CSV: <phase>_full_rundate_<YYYYMMDDHHMMSS>.csv
      LOG: inplayv2_trace_full_rundate_<YYYYMMDDHHMMSS>.log
    """
    global LOG_FILE_PATH
    global CSV_GAMETX_PATH, CSV_DEPOSITS_PATH, CSV_WITHDRAWALS_PATH
    global CSV_PLAYERS_PATH, CSV_RECONCILIATION_PATH, CSV_DATA_QUALITY_PATH

    date_from = getattr(args, "date_from", None)
    date_to = getattr(args, "date_to", None)
    if date_from is None and date_to is None:
        run_suffix = f"full_rundate_{TIMESTAMP_STR}"
    else:
        date_window = f"{_artifact_date_part(date_from, '00000000')}_{_artifact_date_part(date_to, '99999999')}"
        run_suffix = f"{date_window}-rundate_{TIMESTAMP_STR}"

    LOG_FILE_PATH = f"logs/inplayv2_trace_{run_suffix}.log"
    CSV_GAMETX_PATH = f"reports/inplayv2_gameTransaction_{run_suffix}.csv"
    CSV_DEPOSITS_PATH = f"reports/inplayv2_deposits_{run_suffix}.csv"
    CSV_WITHDRAWALS_PATH = f"reports/inplayv2_withdrawals_{run_suffix}.csv"
    CSV_PLAYERS_PATH = f"reports/inplayv2_players_{run_suffix}.csv"
    CSV_RECONCILIATION_PATH = f"reports/inplayv2_reconciliation_{run_suffix}.csv"
    if date_from is None and date_to is None:
        dq_suffix = f"full_rundate_{TIMESTAMP_STR}"
    else:
        dq_date_window = f"{_artifact_date_part(date_from, '00000000')}-{_artifact_date_part(date_to, '99999999')}"
        dq_suffix = f"{dq_date_window}-rundate_{TIMESTAMP_STR}"
    CSV_DATA_QUALITY_PATH = f"reports/dataQuality_{dq_suffix}.csv"


PLAYER_REGISTRATION_SOURCE_TABLE = "PlayerRegistrationsInplayV2"
PLAYER_DETAIL_SOURCE_TABLE = "PlayerDetailInplayV2"
GAME_TRANSACTION_SOURCE_TABLE = "GameTransactionInplayV2"
DEPOSITS_SOURCE_TABLE = "DepositsInplayV2"
WITHDRAWALS_SOURCE_TABLE = "WithdrawalsInplayV2"
SOURCE_SCHEMA = "public"

PLAYER_REPORT_COUNTS: Dict[str, int] = {}
PLAYER_REPORT_ISSUE_COUNTS: Dict[str, int] = {}
PHASE_REPORT_COUNTS: Dict[str, Dict[str, int]] = {}
PHASE_REPORT_ISSUE_COUNTS: Dict[str, Dict[str, int]] = {}
RECONCILIATION_SUMMARY_LINES: List[str] = []
DATA_QUALITY_SUMMARY_LINES: List[str] = []
SOURCE_QUERY_TRACE: Dict[str, Dict[str, Any]] = {}


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def source_table_ref(table: str) -> str:
    if SOURCE_SCHEMA:
        return f"{SOURCE_SCHEMA}.{quote_ident(table)}"
    return quote_ident(table)


# ---------------------------------------------------------------------------
# Checkpoint safety override for explicit date-window runs
# ---------------------------------------------------------------------------
def checkpoint_sort_key_v2(raw: Optional[str]) -> Tuple[datetime, str]:
    """Return comparable checkpoint key from '<sourceDateIso>|<sourceRowId>'.

    Legacy id-only checkpoints sort as the oldest possible timestamp so a real
    date|id pointer can replace them, but an older bounded date-window run will
    not rewind a newer checkpoint.
    """
    dt_raw, id_raw = parse_inplayv2_checkpoint(raw)
    dt = parse_iso_dt(dt_raw) or datetime.min.replace(tzinfo=timezone.utc)
    return (dt, str(id_raw or ""))


def checkpoint_is_greater_v2(candidate: Optional[str], current: Optional[str]) -> bool:
    """True only when candidate checkpoint pointer is strictly newer."""
    if not candidate:
        return False
    if not current:
        return True
    return checkpoint_sort_key_v2(candidate) > checkpoint_sort_key_v2(current)


def checkpoint_get(tgt_conn, phase: str) -> Optional[str]:
    """Read the current InPlayV2 checkpoint from the target checkpoint table."""
    key = ck_key(phase)
    with tgt_conn.cursor() as cur:
        cur.execute(
            'SELECT "lastSourceId" FROM migration_repair."migrationCheckpoint" WHERE platform=%s', (key,)
        )
        row = cur.fetchone()
        return row[0] if row else None


def checkpoint_set(tgt_conn, phase: str, last_source_id: str, dry_run: bool) -> None:
    """Set checkpoint only when the new date|id pointer is greater than current.

    This protects explicit --date-from/--date-to backfill runs from rewinding
    migrationcheckpoint_dev when the target checkpoint already points to a newer
    source date.
    """
    if dry_run:
        return
    key = ck_key(phase)
    candidate = str(last_source_id or "")
    with tgt_conn.cursor() as cur:
        cur.execute(
            'SELECT "lastSourceId" FROM migration_repair."migrationCheckpoint" WHERE platform=%s', (key,)
        )
        row = cur.fetchone()
        current = row[0] if row else None
        if current is not None and not checkpoint_is_greater_v2(candidate, current):
            trace_print(
                f"[CHECKPOINT SKIP][{phase}] existing checkpoint is newer or equal; current={current} candidate={candidate}"
            )
            return
        cur.execute(
            """
            INSERT INTO migration_repair."migrationCheckpoint" (platform, "lastSourceId", "updatedAt")
            VALUES (%s, %s, now())
            ON CONFLICT (platform) DO UPDATE SET
              "lastSourceId" = EXCLUDED."lastSourceId",
              "updatedAt" = now()
            """,
            (key, candidate),
        )
        trace_print(f"[CHECKPOINT UPDATE][{phase}] platform={key} lastSourceId={candidate}")


def trace_print(message: str, level: int = logging.INFO) -> None:
    print(message, flush=True)
    try:
        logging.log(level, message)
    except Exception:
        pass


def _counter_inc(counter: Dict[str, int], key: Optional[str], amount: int = 1) -> None:
    k = str(key or "unknown")
    counter[k] = int(counter.get(k) or 0) + amount


def _phase_counter_inc(
    counter: Dict[str, Dict[str, int]], phase: str, key: Optional[str], amount: int = 1
) -> None:
    bucket = counter.setdefault(phase, {})
    _counter_inc(bucket, key, amount)


def player_report_total() -> int:
    return sum(int(v or 0) for v in PLAYER_REPORT_COUNTS.values())


def player_report_counts_text() -> str:
    if not PLAYER_REPORT_COUNTS:
        return "none"
    return ", ".join(f"{k}={PLAYER_REPORT_COUNTS[k]}" for k in sorted(PLAYER_REPORT_COUNTS))


def player_report_issue_counts_text() -> str:
    if not PLAYER_REPORT_ISSUE_COUNTS:
        return "none"
    return ", ".join(
        f"{k}={PLAYER_REPORT_ISSUE_COUNTS[k]}" for k in sorted(PLAYER_REPORT_ISSUE_COUNTS)
    )


def phase_report_total(phase: str) -> int:
    return sum(int(v or 0) for v in (PHASE_REPORT_COUNTS.get(phase) or {}).values())


def phase_report_count(phase: str, action: str) -> int:
    return int((PHASE_REPORT_COUNTS.get(phase) or {}).get(action) or 0)


def phase_report_counts_text(phase: str) -> str:
    counts = PHASE_REPORT_COUNTS.get(phase) or {}
    if not counts:
        return "none"
    return ", ".join(f"{k}={counts[k]}" for k in sorted(counts))


def phase_report_issue_counts_text(phase: str) -> str:
    counts = PHASE_REPORT_ISSUE_COUNTS.get(phase) or {}
    if not counts:
        return "none"
    return ", ".join(f"{k}={counts[k]}" for k in sorted(counts))


def all_phase_duplicate_total() -> int:
    phases = ("gameTransaction", "walletTransaction.deposit", "walletTransaction.withdrawal")
    return sum(phase_report_count(p, "duplicate_key_ignored") for p in phases)


def write_skipped_to_csv(filepath: str, fieldnames: List[str], row_data: Dict[str, Any]) -> None:
    try:
        dir_name = os.path.dirname(filepath)
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
        file_exists = os.path.isfile(filepath)
        with open(filepath, mode="a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow(row_data)
    except Exception as e:
        print(f"[ERROR] CSV write failed path={filepath}: {e}", flush=True)
        try:
            logging.exception("CSV write failed path=%s", filepath)
        except Exception:
            pass


PHASE_REPORT_FIELDNAMES = [
    "issueType",
    "sourceTable",
    "sourceUsername",
    "sourceId",
    "referenceId",
    "targetId",
    "targetPlayerId",
    "targetUsername",
    "action",
    "reason",
    "error",
]


def _short_csv_text(value: Any, limit: int = 300) -> str:
    """Compact, single-line CSV text for phase exception reports."""
    if value is None:
        return ""
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) > limit:
        return text[: max(0, limit - 3)] + "..."
    return text


def _phase_issue_label(phase_prefix: str, issue_type: Optional[str]) -> str:
    issue = _short_csv_text(issue_type or "unknown_issue", limit=120)
    known_prefixes = ("player_", "game_", "gameTx_", "deposit_", "withdrawal_")
    if issue.startswith(known_prefixes):
        return issue
    return f"{phase_prefix}_{issue}"


def write_phase_report_row(
    filepath: str,
    issue_type: str,
    source_table: str,
    source_username: Any = "",
    source_id: Any = "",
    reference_id: Any = "",
    target_id: Any = "",
    target_player_id: Any = "",
    target_username: Any = "",
    action: Any = "",
    reason: Any = "",
    error: Any = "",
) -> None:
    """Write one compact phase CSV row. No payload, dry-run flag, or timestamp."""
    write_skipped_to_csv(
        filepath,
        PHASE_REPORT_FIELDNAMES,
        {
            "issueType": _short_csv_text(issue_type, limit=120),
            "sourceTable": _short_csv_text(source_table, limit=120),
            "sourceUsername": _short_csv_text(source_username, limit=120),
            "sourceId": _short_csv_text(source_id, limit=120),
            "referenceId": _short_csv_text(reference_id, limit=120),
            "targetId": _short_csv_text(target_id, limit=120),
            "targetPlayerId": _short_csv_text(target_player_id, limit=120),
            "targetUsername": _short_csv_text(target_username, limit=120),
            "action": _short_csv_text(action, limit=80),
            "reason": _short_csv_text(reason, limit=300),
            "error": _short_csv_text(error, limit=300),
        },
    )


def _csv_safe_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return str(value)


def _member_username(data: Dict[str, Any]) -> str:
    member = as_dict(data.get("member"))
    return clean_username_value(
        member.get("name")
        or member.get("username")
        or member.get("userName")
        or data.get("username")
        or data.get("name")
    )


def _source_dt_value_v2(data: Dict[str, Any]) -> Optional[str]:
    value = data.get("dateTimeCreated")
    return str(value).strip() if value is not None and str(value).strip() else None


def record_player_report(
    source_table: str,
    source_id: Any,
    username: Optional[str],
    action: str,
    issue_type: str,
    reason: str,
    dry_run: bool,
    data: Optional[Dict[str, Any]] = None,
    target_id: Any = None,
    target_username: Any = None,
    external_id: Any = None,
    error: Any = None,
) -> None:
    trace_print(
        f"[REPORT][playerDetails] sourceTable={source_table} sourceId={source_id or 'N/A'} username={username or ''} targetId={target_id or ''} action={acti
on} issueType={issue_type} reason={reason}"
        + (f" error={error}" if error else "")
    )
    _counter_inc(PLAYER_REPORT_COUNTS, action)
    _counter_inc(PLAYER_REPORT_ISSUE_COUNTS, issue_type)
    write_phase_report_row(
        CSV_PLAYERS_PATH,
        _phase_issue_label("player", issue_type),
        source_table,
        source_username=username,
        source_id=source_id,
        reference_id=external_id,
        target_id=target_id,
        target_username=target_username,
        action=action,
        reason=reason,
        error=error,
    )


def record_game_report(
    source_id: Any,
    external_id: Any,
    username: Optional[str],
    reason: str,
    dry_run: bool,
    action: str = "skipped",
    issue_type: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
    target_id: Any = None,
    target_player_id: Any = None,
    target_username: Any = None,
    provider_name: Any = None,
    game_name: Any = None,
    game_type: Any = None,
    round_id: Any = None,
    error: Any = None,
) -> None:
    phase = "gameTransaction"
    issue = issue_type or action
    label = "[REPORT][gameTransaction]" if action != "skipped" else "[SKIP][gameTransaction]"
    trace_print(
        f"{label} sourceTable={GAME_TRANSACTION_SOURCE_TABLE} sourceId={source_id or 'N/A'} externalId={external_id or ''} sourceUsername={username or ''} t
argetId={target_id or ''} targetPlayerId={target_player_id or ''} targetUsername={target_username or ''} action={action} issueType={issue} reason={reason}"
        + (f" error={error}" if error else "")
    )
    _phase_counter_inc(PHASE_REPORT_COUNTS, phase, action)
    _phase_counter_inc(PHASE_REPORT_ISSUE_COUNTS, phase, issue)
    details = []
    if provider_name:
        details.append(f"provider={provider_name}")
    if game_name:
        details.append(f"game={game_name}")
    if game_type:
        details.append(f"gameType={game_type}")
    if round_id:
        details.append(f"roundId={round_id}")
    compact_reason = reason if not details else f"{reason}; {'; '.join(details)}"
    write_phase_report_row(
        CSV_GAMETX_PATH,
        _phase_issue_label("gameTx", issue),
        GAME_TRANSACTION_SOURCE_TABLE,
        source_username=username,
        source_id=source_id,
        reference_id=external_id,
        target_id=target_id,
        target_player_id=target_player_id,
        target_username=target_username,
        action=action,
        reason=compact_reason,
        error=error,
    )


def record_wallet_report(
    kind: str,
    source_id: Any,
    username: Optional[str],
    reason: str,
    dry_run: bool,
    reference_id: Optional[Any] = None,
    action: str = "skipped",
    issue_type: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
    target_id: Any = None,
    target_player_id: Any = None,
    target_username: Any = None,
    amount: Any = None,
    status: Any = None,
    payment_gateway: Any = None,
    error: Any = None,
) -> None:
    phase = f"walletTransaction.{kind}"
    source_table = DEPOSITS_SOURCE_TABLE if kind == "deposit" else WITHDRAWALS_SOURCE_TABLE
    issue = issue_type or action
    label = (
        f"[REPORT][walletTransaction.{kind}]"
        if action != "skipped"
        else f"[SKIP][walletTransaction.{kind}]"
    )
    trace_print(
        f"{label} sourceTable={source_table} sourceId={source_id or 'N/A'} referenceId={reference_id or ''} sourceUsername={username or ''} targetId={target
_id or ''} targetPlayerId={target_player_id or ''} targetUsername={target_username or ''} action={action} issueType={issue} reason={reason}"
        + (f" error={error}" if error else "")
    )
    _phase_counter_inc(PHASE_REPORT_COUNTS, phase, action)
    _phase_counter_inc(PHASE_REPORT_ISSUE_COUNTS, phase, issue)
    target_csv = CSV_DEPOSITS_PATH if kind == "deposit" else CSV_WITHDRAWALS_PATH
    details = []
    if amount not in (None, ""):
        details.append(f"amount={amount}")
    if status:
        details.append(f"status={status}")
    if payment_gateway:
        details.append(f"paymentGateway={payment_gateway}")
    compact_reason = reason if not details else f"{reason}; {'; '.join(details)}"
    write_phase_report_row(
        target_csv,
        _phase_issue_label(kind, issue),
        source_table,
        source_username=username,
        source_id=source_id,
        reference_id=reference_id,
        target_id=target_id,
        target_player_id=target_player_id,
        target_username=target_username,
        action=action,
        reason=compact_reason,
        error=error,
    )


def write_reconciliation_row(
    check_name: str,
    record_type: str,
    status: str,
    source_table: str = "",
    source_id: Any = "",
    source_username: Any = "",
    source_external_id: Any = "",
    target_table: str = "",
    target_id: Any = "",
    target_username: Any = "",
    target_external_id: Any = "",
    reference_type: str = "",
    reference_value: Any = "",
    metric: str = "",
    value: Any = "",
    reason: str = "",
    notes: str = "",
) -> None:
    write_skipped_to_csv(
        CSV_RECONCILIATION_PATH,
        [
            "checkName",
            "recordType",
            "status",
            "sourceTable",
            "sourceId",
            "sourceUsername",
            "sourceExternalId",
            "targetTable",
            "targetId",
            "targetUsername",
            "targetExternalId",
            "referenceType",
            "referenceValue",
            "metric",
            "value",
            "reason",
            "notes",
            "timestamp",
        ],
        {
            "checkName": check_name,
            "recordType": record_type,
            "status": status,
            "sourceTable": source_table,
            "sourceId": source_id or "",
            "sourceUsername": source_username or "",
            "sourceExternalId": source_external_id or "",
            "targetTable": target_table,
            "targetId": target_id or "",
            "targetUsername": target_username or "",
            "targetExternalId": target_external_id or "",
            "referenceType": reference_type,
            "referenceValue": reference_value or "",
            "metric": metric,
            "value": "" if value is None else value,
            "reason": reason,
            "notes": notes,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )


def add_reconciliation_summary(line: str) -> None:
    RECONCILIATION_SUMMARY_LINES.append(line)
    trace_print(line)


def reconciliation_email_summary() -> str:
    if not RECONCILIATION_SUMMARY_LINES:
        return "Reconciliation Summary:\n- Reconciliation checks were not executed for this run.\n"
    return (
        "Reconciliation Summary:\n"
        + "\n".join(f"- {line}" for line in RECONCILIATION_SUMMARY_LINES)
        + "\n"
    )


def _dq_report_date_part(value: Optional[str]) -> str:
    """Return YYYYMMDD for the DQ summary CSV date columns."""
    if not value:
        return ""
    dt = parse_iso_dt(value)
    if dt is not None:
        return dt.strftime("%Y%m%d")
    text = str(value).strip()
    if len(text) >= 10:
        return text[:10].replace("-", "")
    return text


def write_data_quality_summary_row(
    phase: str,
    mismatch_col_count: int,
    date_from: Optional[str],
    date_to: Optional[str],
    columns: Iterable[Any],
) -> None:
    column_names = sorted({str(c).strip() for c in columns if c is not None and str(c).strip()})
    write_skipped_to_csv(
        CSV_DATA_QUALITY_PATH,
        ["phase", "mismatchColCount", "date-from", "date-to", "columnList"],
        {
            "phase": phase,
            "mismatchColCount": int(mismatch_col_count or 0),
            "date-from": _dq_report_date_part(date_from),
            "date-to": _dq_report_date_part(date_to),
            "columnList": "[" + "|".join(column_names) + "]",
        },
    )


def add_data_quality_summary(line: str) -> None:
    DATA_QUALITY_SUMMARY_LINES.append(line)
    trace_print(line)


def data_quality_email_summary() -> str:
    if not DATA_QUALITY_SUMMARY_LINES:
        return "Data Quality Summary:\n- Data quality checks were not executed for this run.\n"
    return (
        "Data Quality Summary:\n"
        + "\n".join(f"- {line}" for line in DATA_QUALITY_SUMMARY_LINES)
        + "\n"
    )


def package_reports_if_needed(
    file_paths: List[str], threshold_bytes: int = REPORT_ZIP_THRESHOLD_BYTES
) -> List[str]:
    existing_paths = [path for path in file_paths if path and os.path.isfile(path)]
    if not existing_paths:
        trace_print("[REPORT PACKAGING] No report files found for attachment.")
        return file_paths
    total_size = sum(os.path.getsize(path) for path in existing_paths)
    if total_size < threshold_bytes:
        trace_print(
            f"[REPORT PACKAGING] CSV reports total={total_size} bytes below threshold={threshold_bytes}; sending individual files."
        )
        return file_paths
    zip_path = f"reports/inplayv2_migration_reports_{TIMESTAMP_STR}.zip"
    os.makedirs(os.path.dirname(zip_path), exist_ok=True)
    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in existing_paths:
            zf.write(path, arcname=os.path.basename(path))
    trace_print(
        f"[REPORT PACKAGING] CSV reports total={total_size} bytes reached threshold={threshold_bytes}; created zip={zip_path}"
    )
    return [zip_path]


#def source_date_expr_v2(alias: Optional[str] = None, table: Optional[str] = None) -> str:
#    """Return the PHT-aware source date expression used for cursoring and date-window filters.
#
#    Source JSONB date values contain UTC/Z timestamps. Cast them to timestamptz
#    so PostgreSQL compares real instants against the PHT business window bounds
#    created by date_window_bounds_for_source().
#    """
#    p = f"{alias}." if alias else ""
#    return f"COALESCE({p}data->>'dateTimeCreated', {p}data->>'createdDateTime')::timestamptz"

def source_date_expr_v2(alias: Optional[str] = None, table: Optional[str] = None) -> str:
    """Return the source date expression used for cursoring and date-window filters.

    Source JSONB date values contain UTC/Z timestamps. Use immutable_json_timestamp()
    so PostgreSQL can use an expression index for large JSONB source tables.

    The PHT-aware behavior is still controlled by date_window_bounds_for_source(),
    which passes +08:00 timestamptz boundaries into the WHERE clause.
    """
    p = f"{alias}." if alias else ""
    return (
        "immutable_json_timestamp("
        f"COALESCE({p}data->>'dateTimeCreated', {p}data->>'createdDateTime')"
        ")"
    )


def print_source_query(cur, label: str, query: str, params: Iterable[Any]) -> None:
    params_list = list(params)
    try:
        exact_query = cur.mogrify(query, params_list).decode("utf-8")
    except Exception as e:
        exact_query = f"{query.strip()}\n-- PARAMS: {params_list!r}\n-- mogrify failed: {e}"

    entry = SOURCE_QUERY_TRACE.setdefault(
        label, {"count": 0, "first": None, "last": None, "rows": 0}
    )
    entry["count"] += 1
    if entry["first"] is None:
        entry["first"] = exact_query
    entry["last"] = exact_query

    # InPlayV1-style visible SQL trace: print/log the rendered SELECT before execution.
    # This runs in both live mode and --dry-run because source SELECTs are still executed in dry-run.
    trace_print(f"\n[SOURCE QUERY][{label}] exact_query_for_psql:\n{exact_query.strip()}\n")
    logging.info("[SOURCE QUERY][%s] %s", label, " ".join(exact_query.split()))


def note_source_query_result(label: str, rows: int) -> None:
    entry = SOURCE_QUERY_TRACE.setdefault(
        label, {"count": 0, "first": None, "last": None, "rows": 0}
    )
    entry["rows"] = int(entry.get("rows") or 0) + int(rows or 0)


def print_source_query_summary(
    phase: str, labels: Optional[List[str]] = None, clear: bool = True
) -> None:
    selected = labels or sorted(SOURCE_QUERY_TRACE.keys())
    trace_print(f"\n[SOURCE QUERY SUMMARY][{phase}]")
    any_printed = False
    for label in selected:
        entry = SOURCE_QUERY_TRACE.get(label)
        if not entry:
            continue
        any_printed = True
        trace_print(
            f"[SOURCE QUERY SUMMARY][{phase}][{label}] executed_selects={entry.get('count', 0)} rowsFetched={entry.get('rows', 0)}"
        )
        last_q = entry.get("last") or ""
        if last_q:
            trace_print(f"exact_query_for_psql:\n{last_q}\n")
        if clear:
            SOURCE_QUERY_TRACE.pop(label, None)
    if not any_printed:
        trace_print("No source SELECT was executed for this phase.")


def fetch_json_table_batch(
    src_conn,
    table: str,
    after_dt: Optional[str],
    after_id: Optional[str],
    limit: int,
    from_dt: Optional[str] = None,
    until_dt: Optional[str] = None,
) -> List[Dict[str, Any]]:
    date_col = source_date_expr_v2(table=table)
    anchor_id = after_id or ""
    conditions = ["data IS NOT NULL"]
    params: List[Any] = []
    if after_dt is not None:
        conditions.append(f"({date_col}, id) > (%s::timestamptz, %s)")
        params.extend([after_dt, anchor_id])
    elif from_dt is not None:
        conditions.append(f"{date_col} >= %s::timestamptz")
        params.append(from_dt)
    if until_dt is not None:
        conditions.append(f"{date_col} < %s::timestamptz")
        params.append(until_dt)
    params.append(limit)
    query = f"""
        SELECT id, data
        FROM {source_table_ref(table)}
        WHERE {' AND '.join(conditions)}
        ORDER BY {date_col} ASC, id ASC
        LIMIT %s
    """
    label = f"{table} batch"
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        print_source_query(cur, label, query, params)
        cur.execute(query, params)
        rows = cur.fetchall()
        note_source_query_result(label, len(rows))
        trace_print(f"[SOURCE QUERY RESULT][{table}] rows={len(rows)} limit={limit}")
    src_conn.rollback()
    return rows


def fetch_player_detail_map(src_conn) -> Dict[str, Dict[str, str]]:
    detail_map: Dict[str, Dict[str, str]] = {}
    query = f"""
        SELECT data->>'id' AS external_id,
               data->'verification'->>'address'        AS address_province,
               data->'verification'->>'sourceOfIncome' AS income_source,
               data->'verification'->>'natureOfWork'   AS industry
        FROM {source_table_ref(PLAYER_DETAIL_SOURCE_TABLE)}
        WHERE data->'verification' IS NOT NULL
          AND data->'verification' != 'null'::jsonb
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        print_source_query(cur, f"{PLAYER_DETAIL_SOURCE_TABLE} detail-map", query, [])
        cur.execute(query)
        rows = cur.fetchall()
        note_source_query_result(f"{PLAYER_DETAIL_SOURCE_TABLE} detail-map", len(rows))
        for row in rows:
            eid = row["external_id"]
            if eid:
                detail_map[eid] = {
                    "address_province": row["address_province"] or "N/A",
                    "income_source": row["income_source"] or "N/A",
                    "industry": row["industry"] or "N/A",
                }
    src_conn.rollback()
    trace_print(f"Loaded PlayerDetailInplayV2 verification map: {len(detail_map)} entries")
    return detail_map


# NOTE: The original player upsert mapping is preserved. Only reporting was added and
# ON CONFLICT references were aligned to the same table used by the INSERT target.
def upsert_player_from_member(
    tgt_conn,
    member: Dict[str, Any],
    dry_run: bool,
    detail_map: Optional[Dict[str, Dict[str, str]]] = None,
) -> uuid.UUID:
    username = clean_username_value(member.get("name"))
    if not username:
        record_player_report(
            PLAYER_REGISTRATION_SOURCE_TABLE,
            member.get("id"),
            None,
            "skipped",
            "missing_username",
            "Member missing name",
            dry_run,
            data=member,
        )
        raise RuntimeError("Member missing name")
    member_id = str(member.get("id") or "").strip() or None
    real_name = (member.get("realName") or "").strip()
    first, middle, last = "Unknown", "", "Unknown"
    if real_name:
        parts = real_name.split()
        if len(parts) == 1:
            first = parts[0]
        elif len(parts) == 2:
            first, last = parts[0], parts[1]
        else:
            first, middle, last = parts[0], " ".join(parts[1:-1]), parts[-1]
    mobile_10 = safe_mobile_10(member.get("mobileNumber"))
    email = sanitize_email(member.get("emailAddress"), username)
    reg_dt = parse_iso_dt(member.get("dateTimeCreated")) or datetime.now(timezone.utc)
    last_login = latest_dt(member.get("dateTimeLastAndroidLogIn"), member.get("dateTimeLastActive"))
    verification_status = str(member.get("verificationStatus") or "").upper()
    is_verified = verification_status in ("VERIFIED", "APPROVED")
    is_active = str(member.get("status") or "").upper() == "ACTIVE"
    outlet_code = str(member.get("branchCode") or "").strip() or None
    if outlet_code:
        try:
            ensure_outlet_code_enrolled(tgt_conn, outlet_code, dry_run)
        except Exception as e:
            write_reconciliation_row(
                "dimension_runtime_failure",
                "detail",
                "outlet_enrollment_failed",
                source_table=PLAYER_REGISTRATION_SOURCE_TABLE,
                source_username=username,
                source_external_id=member_id,
                target_table="outletList",
                reference_type="outletCode",
                reference_value=outlet_code,
                reason="Failed to enroll outletCode while upserting player",
                notes=str(e),
            )
            raise
    birthdate = parse_iso_dt(member.get("birthDay"))
    address_street = "N/A"
    address_barangay = "N/A"
    address_city = "N/A"
    _detail = (detail_map or {}).get(member_id or "") or {}
    address_province = _detail.get("address_province") or "N/A"
    income_source = _detail.get("income_source") or "N/A"
    industry = _detail.get("industry") or "N/A"
    wallet = member.get("wallet") or {}
    if isinstance(wallet, str):
        try:
            wallet = json.loads(wallet)
        except Exception:
            wallet = {}
    wallet_balance = to_decimal_str(wallet.get("balance") or "0")
    wallet_balance_dt = reg_dt
    sql = """
    INSERT INTO migration_repair."playerDetails_final" (
        "userName",
        "firstName",
        "middleName",
        "lastName",
        "mobileNumber",
        "mobileNumberVerified",
        "emailAddress",
        "emailVerified",
        "registrationDate",
        "registrationIp",
        "registrationReferrer",
        "brandName",
        "isVerified",
        "isBlocked",
        "blockedDatetime",
        "isActive",
        "lastLogin",
        "lastLoginIp",
        "outletCode",
        "affiliateCode",
        "addressStreet",
        "addressBarangay",
        "addressCity",
        "addressProvince",
        "incomeSource",
        "industry",
        "externalId",
        "birthdate",
        "walletBalance",
        "walletBalanceDatetime",
        "createdAt",
        "updatedAt"
    )
    VALUES (
        %s,
        %s,
        %s,
        %s,
        %s,
        false,
        %s,
        false,
        %s,
        %s,
        NULL,
        %s,
        %s,
        false,
        NULL,
        %s,
        %s,
        %s,
        %s,
        NULL,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        now(),
        now()
    )
    ON CONFLICT ("userName") DO UPDATE SET
        "firstName" = EXCLUDED."firstName",
        "middleName" = EXCLUDED."middleName",
        "lastName" = EXCLUDED."lastName",
        "mobileNumber" = EXCLUDED."mobileNumber",
        "emailAddress" = EXCLUDED."emailAddress",
        "brandName" = EXCLUDED."brandName",
        "isVerified" = EXCLUDED."isVerified",
        "isActive" = EXCLUDED."isActive",
        "lastLogin" = EXCLUDED."lastLogin",
        "lastLoginIp" = EXCLUDED."lastLoginIp",
        "registrationDate" = LEAST(
            EXCLUDED."registrationDate",
            "playerDetails_final"."registrationDate"
        ),
        "outletCode" = COALESCE(
            EXCLUDED."outletCode",
            "playerDetails_final"."outletCode"
        ),
        "birthdate" = COALESCE(
            EXCLUDED."birthdate",
            "playerDetails_final"."birthdate"
        ),
        "externalId" = EXCLUDED."externalId",
        "walletBalance" = EXCLUDED."walletBalance",
        "walletBalanceDatetime" = EXCLUDED."walletBalanceDatetime",
        "addressProvince" = CASE
            WHEN EXCLUDED."addressProvince" <> 'N/A'
                THEN EXCLUDED."addressProvince"
            ELSE "playerDetails_final"."addressProvince"
        END,
        "incomeSource" = CASE
            WHEN EXCLUDED."incomeSource" <> 'N/A'
                THEN EXCLUDED."incomeSource"
            ELSE "playerDetails_final"."incomeSource"
        END,
        "industry" = CASE
            WHEN EXCLUDED."industry" <> 'N/A'
                THEN EXCLUDED."industry"
            ELSE "playerDetails_final"."industry"
        END,
        "updatedAt" = now()
    RETURNING id, (xmax = 0) AS inserted
    """
    params = (
        username,
        first,
        middle,
        last,
        mobile_10,
        email,
        reg_dt,
        member.get("ipAddress"),
        BRAND,
        is_verified,
        is_active,
        last_login,
        member.get("ipAddress"),
        outlet_code,
        address_street,
        address_barangay,
        address_city,
        address_province,
        income_source,
        industry,
        member_id,
        birthdate,
        wallet_balance,
        wallet_balance_dt,
    )
    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute(
                'SELECT id FROM migration_repair."playerDetails_final" WHERE LOWER(TRIM("userName"))=%s',
                (username_match_key(username),),
            )
            row = cur.fetchone()
            return row[0] if row else uuid.uuid4()
    try:
        with tgt_conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            pid = row[0]
            inserted_flag = bool(row[1]) if len(row) > 1 else True
            if not inserted_flag:
                record_player_report(
                    PLAYER_REGISTRATION_SOURCE_TABLE,
                    member.get("source_id") or member.get("id"),
                    username,
                    "duplicate_key_upserted",
                    "duplicate_username_updated",
                    "Existing player row updated by ON CONFLICT",
                    dry_run,
                    data=member,
                    target_id=pid,
                    target_username=username,
                    external_id=member_id,
                )
            return pid
    except Exception as e:
        record_player_report(
            PLAYER_REGISTRATION_SOURCE_TABLE,
            member.get("source_id") or member.get("id"),
            username,
            "upsert_failed",
            "player_upsert_failed",
            "Player upsert failed",
            dry_run,
            data=member,
            external_id=member_id,
            error=e,
        )
        raise


def _get_existing_game_target(tgt_conn, external_id: str) -> Dict[str, Any]:
    try:
        with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                'SELECT gt.id, gt."playerId", gt."playerUserName", gt."externalId" FROM migration_repair."gameTransaction_final" gt WHERE gt."externalId"=%s
LIMIT 1',
                (external_id,),
            )
            return cur.fetchone() or {}
    except Exception:
        return {}


def _get_existing_wallet_target(tgt_conn, kind: str, reference_id: str) -> Dict[str, Any]:
    try:
        with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                'SELECT wt.id, wt."playerId", pd."userName", wt."referenceId" FROM migration_repair."walletTransaction_final" wt LEFT JOIN migration_repair.
"playerDetails_final" pd ON pd.id = wt."playerId" WHERE wt.platform=%s AND wt."transactionType"=%s AND wt."referenceId"=%s LIMIT 1',
                (WALLET_PLATFORM, kind, reference_id),
            )
            return cur.fetchone() or {}
    except Exception:
        return {}


def insert_game_tx_batch(
    tgt_conn,
    rows: List[Dict[str, Any]],
    player_map: Dict[str, uuid.UUID],
    provider_cache: Dict[str, uuid.UUID],
    gametype_cache: Dict[str, uuid.UUID],
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID],
    dry_run: bool,
) -> Tuple[int, int]:
    values: List[Tuple[Any, ...]] = []
    value_meta: List[Dict[str, Any]] = []
    skipped_rows = 0
    for r in rows:
        source_id = str(r.get("id") or "").strip()
        data = as_dict(r.get("data"))
        if not source_id:
            skipped_rows += 1
            record_game_report(
                None,
                None,
                None,
                "Missing source row id",
                dry_run,
                issue_type="missing_source_id",
                data=data,
            )
            continue
        if not data:
            skipped_rows += 1
            record_game_report(
                source_id,
                None,
                None,
                "Missing or invalid source JSON data",
                dry_run,
                issue_type="missing_or_invalid_json",
                data=data,
            )
            continue
        tx_id = str(data.get("id") or source_id or "").strip()
        if not tx_id:
            skipped_rows += 1
            record_game_report(
                source_id,
                None,
                None,
                "Missing game transaction id/externalId",
                dry_run,
                issue_type="missing_external_id",
                data=data,
            )
            continue
        member = as_dict(data.get("member"))
        username = clean_username_value(member.get("name"))
        if not username:
            skipped_rows += 1
            record_game_report(
                source_id,
                tx_id,
                None,
                "Missing member.name in game transaction source payload",
                dry_run,
                issue_type="missing_username",
                data=data,
            )
            continue
        username_key = username_match_key(username)
        player_id = player_map.get(username_key)
        if not player_id:
            player_id = lookup_player_id_by_username(tgt_conn, username)
            if player_id:
                player_map[username_key] = player_id
        if not player_id:
            skipped_rows += 1
            record_game_report(
                source_id,
                tx_id,
                username,
                "Unable to process no playerRecord; username not found in player_map/playerDetails_final. No shadow/ghost player was created.",
                dry_run,
                issue_type="player_not_in_player_map",
                data=data,
            )
            continue
        game = as_dict(data.get("game"))
        provider_name = str(game.get("provider") or "UNKNOWN")
        game_name = str(game.get("name") or "UNKNOWN")
        game_type_raw = str(game.get("type") or "SLOTS")
        try:
            provider_id = get_or_create_game_provider(
                tgt_conn, provider_name, provider_cache, dry_run
            )
            game_type_id = get_or_create_game_type(tgt_conn, game_type_raw, gametype_cache, dry_run)
            game_id = get_or_create_game_list(
                tgt_conn, game_name, provider_id, game_type_id, gamelist_cache, dry_run
            )
        except Exception as e:
            skipped_rows += 1
            write_reconciliation_row(
                "dimension_runtime_failure",
                "detail",
                "dimension_upsert_failed",
                source_table=GAME_TRANSACTION_SOURCE_TABLE,
                source_id=source_id,
                source_username=username,
                target_table="gameProvider/gameType/gameList",
                reference_type="game",
                reference_value=f"{provider_name}|{game_type_raw}|{game_name}",
                reason="Dimension upsert failed while preparing gameTransaction",
                notes=str(e),
            )
            record_game_report(
                source_id,
                tx_id,
                username,
                "Dimension upsert failed while preparing gameTransaction",
                dry_run,
                issue_type="dimension_upsert_failed",
                data=data,
                provider_name=provider_name,
                game_name=game_name,
                game_type=game_type_raw,
                error=e,
            )
            try:
                tgt_conn.rollback()
            except Exception:
                pass
            continue
        created = parse_iso_dt(data.get("dateTimeCreated")) or datetime.now(timezone.utc)
        settled = parse_iso_dt(data.get("dateTimeSettled")) or created
        bet_amount = to_decimal_str(data.get("bet"))
        payout_amount = to_decimal_str(data.get("payout"))
        pc5 = to_decimal_str(data.get("jackpotContribution"))
        jw5 = to_decimal_str(data.get("jackpotPayout"))
        round_id = data.get("vendorRoundId")
        values.append(
            (
                created,
                provider_id,
                game_id,
                game_type_id,
                player_id,
                username,
                None,
                "0",
                bet_amount,
                bet_amount,
                payout_amount,
                "0",
                "0",
                "0",
                "0",
                pc5,
                "0",
                "0",
                "0",
                "0",
                jw5,
                "0",
                "0",
                0,
                settled,
                tx_id,
                False,
                None,
                None,
                BRAND,
                PLATFORM,
                str(round_id) if round_id is not None else None,
            )
        )
        value_meta.append(
            {
                "sourceId": source_id,
                "externalId": tx_id,
                "username": username,
                "providerName": provider_name,
                "gameName": game_name,
                "gameType": game_type_raw,
                "roundId": round_id,
                "playerId": player_id,
                "data": data,
            }
        )
    if skipped_rows:
        trace_print(f"[SUMMARY][gameTransaction] insertable={len(values)} skipped={skipped_rows}")
    if dry_run:
        trace_print(
            f"[DRY-RUN][gameTransaction] Would insert {len(values)} rows; skipped={skipped_rows}"
        )
        return (len(values), skipped_rows)
    if not values:
        return (0, skipped_rows)
    sql = """
    INSERT INTO migration_repair."gameTransaction_final" (
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
        "PC1",
        "PC2",
        "PC3",
        "PC4",
        "PC5",
        "JW1",
        "JW2",
        "JW3",
        "JW4",
        "JW5",
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
    RETURNING "externalId"
    """
    try:
        with tgt_conn.cursor() as cur:
            inserted_rows = execute_values(cur, sql, values, page_size=1000, fetch=True)
    except Exception as e:
        try:
            tgt_conn.rollback()
        except Exception:
            pass
        for meta in value_meta:
            record_game_report(
                meta.get("sourceId"),
                meta.get("externalId"),
                meta.get("username"),
                "gameTransaction batch insert failed",
                dry_run,
                action="insert_failed",
                issue_type="game_transaction_insert_failed",
                data=meta.get("data"),
                provider_name=meta.get("providerName"),
                game_name=meta.get("gameName"),
                game_type=meta.get("gameType"),
                round_id=meta.get("roundId"),
                error=e,
            )
        return (0, skipped_rows + len(values))
    inserted_counts: Dict[str, int] = {}
    for row in inserted_rows or []:
        k = str(row[0])
        inserted_counts[k] = inserted_counts.get(k, 0) + 1
    duplicate_count = 0
    for meta in value_meta:
        ext = str(meta["externalId"])
        if inserted_counts.get(ext, 0) > 0:
            inserted_counts[ext] -= 1
            continue
        duplicate_count += 1
        existing = _get_existing_game_target(tgt_conn, ext)
        record_game_report(
            meta.get("sourceId"),
            ext,
            meta.get("username"),
            "Duplicate gameTransaction externalId encountered; target row already existed and ON CONFLICT DO NOTHING ignored this source row",
            dry_run,
            action="duplicate_key_ignored",
            issue_type="duplicate_external_id_ignored",
            data=meta.get("data"),
            target_id=existing.get("id"),
            target_player_id=existing.get("playerId"),
            target_username=existing.get("playerUserName"),
            provider_name=meta.get("providerName"),
            game_name=meta.get("gameName"),
            game_type=meta.get("gameType"),
            round_id=meta.get("roundId"),
        )
    inserted_count = len(inserted_rows or [])
    trace_print(
        f"[LIVE][gameTransaction] inserted={inserted_count} attempted={len(values)} duplicates={duplicate_count} skipped={skipped_rows}"
    )
    return (inserted_count, skipped_rows)


def wallet_row_to_values(
    tgt_conn,
    kind: str,
    src_id: str,
    data: Dict[str, Any],
    player_map: Dict[str, uuid.UUID],
    dry_run: bool,
) -> Optional[Tuple[Any, ...]]:
    member = as_dict(data.get("member"))
    username = clean_username_value(member.get("name"))
    if not username:
        record_wallet_report(
            kind,
            src_id,
            None,
            "Missing member.name in wallet source payload",
            dry_run,
            action="skipped",
            issue_type="missing_username",
            data=data,
        )
        return None
    username_key = username_match_key(username)
    player_id = player_map.get(username_key)
    if not player_id:
        player_id = lookup_player_id_by_username(tgt_conn, username)
        if player_id:
            player_map[username_key] = player_id
    if not player_id:
        record_wallet_report(
            kind,
            src_id,
            username,
            "Unable to process no playerRecord; username not found in player_map/playerDetails_final. No shadow/ghost player was created.",
            dry_run,
            action="skipped",
            issue_type="player_not_in_player_map",
            data=data,
        )
        return None
    amount_raw = data.get("netAmount") or data.get("amount")
    try:
        amount_val = abs(float(amount_raw or 0))
    except Exception:
        record_wallet_report(
            kind,
            src_id,
            username,
            f"Invalid amount value: {amount_raw!r}",
            dry_run,
            action="skipped",
            issue_type="invalid_amount",
            data=data,
        )
        return None
    amount = str(amount_val)
    status = normalize_wallet_status(data.get("status"))
    created_dt = parse_iso_dt(data.get("dateTimeCreated")) or parse_iso_dt(
        data.get("createdDateTime")
    )
    if created_dt is None:
        created_dt = datetime.now(timezone.utc)
        record_wallet_report(
            kind,
            src_id,
            username,
            "Missing dateTimeCreated/createdDateTime; defaulting createdDatetime to now()",
            dry_run,
            action="date_defaulted",
            issue_type="missing_created_datetime_defaulted",
            data=data,
            amount=amount,
            status=status,
        )
    confirmed_dt = parse_iso_dt(data.get("dateTimeConfirmed")) or created_dt
    payment_gateway = str(data.get("type") or data.get("paymentGateway") or "N/A").strip() or "N/A"
    domain = (
        str(
            data.get("domain")
            or (member.get("domain") if isinstance(member, dict) else None)
            or BRAND
        ).strip()
        or BRAND
    )
    ref = data.get("id") or src_id
    reference_id = str(ref).strip() if ref is not None else None
    if not reference_id:
        reference_id = f"{kind}:{src_id}"
        record_wallet_report(
            kind,
            src_id,
            username,
            "Missing wallet source id/reference id; using deterministic fallback",
            dry_run,
            action="reference_defaulted",
            issue_type="missing_reference_id_defaulted",
            data=data,
            reference_id=reference_id,
            amount=amount,
            status=status,
            payment_gateway=payment_gateway,
        )
    confirmed = confirmed_dt if status == "confirmed" else None
    cancelled = created_dt if status == "cancelled" else None
    failed = created_dt if status == "failed" else None
    return (
        kind,
        WALLET_PLATFORM,
        player_id,
        payment_gateway,
        domain,
        amount,
        status,
        None,
        created_dt,
        confirmed,
        cancelled,
        failed,
        reference_id,
    )


def insert_wallet_batch(
    tgt_conn, rows: List[Dict[str, Any]], kind: str, player_map: Dict[str, uuid.UUID], dry_run: bool
) -> Tuple[int, int]:
    values: List[Tuple[Any, ...]] = []
    value_meta: List[Dict[str, Any]] = []
    skipped_rows = 0
    for r in rows:
        src_id = str(r.get("id") or "").strip()
        data = as_dict(r.get("data"))
        if not src_id:
            skipped_rows += 1
            record_wallet_report(
                kind,
                None,
                None,
                "Missing source row id",
                dry_run,
                action="skipped",
                issue_type="missing_source_id",
                data=data,
            )
            continue
        if not data:
            skipped_rows += 1
            record_wallet_report(
                kind,
                src_id,
                None,
                "Missing or invalid source JSON data",
                dry_run,
                action="skipped",
                issue_type="missing_or_invalid_json",
                data=data,
            )
            continue
        v = wallet_row_to_values(tgt_conn, kind, src_id, data, player_map, dry_run=dry_run)
        if v:
            values.append(v)
            value_meta.append(
                {
                    "sourceId": src_id,
                    "referenceId": v[12],
                    "username": _member_username(data),
                    "playerId": v[2],
                    "amount": v[5],
                    "status": v[6],
                    "paymentGateway": v[3],
                    "data": data,
                }
            )
        else:
            skipped_rows += 1
    if skipped_rows:
        trace_print(
            f"[SUMMARY][walletTransaction.{kind}] insertable={len(values)} skipped={skipped_rows}"
        )
    if dry_run:
        trace_print(
            f"[DRY-RUN][walletTransaction.{kind}] Would insert {len(values)} rows; skipped={skipped_rows}"
        )
        return (len(values), skipped_rows)
    if not values:
        return (0, skipped_rows)
    sql = f"""
    INSERT INTO migration_repair."walletTransaction_final" (
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
    WHERE ("platform" = '{WALLET_PLATFORM}' AND "referenceId" IS NOT NULL)
    DO NOTHING
    RETURNING "referenceId"
    """
    try:
        with tgt_conn.cursor() as cur:
            inserted_rows = execute_values(cur, sql, values, page_size=1000, fetch=True)
    except Exception as e:
        try:
            tgt_conn.rollback()
        except Exception:
            pass
        for meta in value_meta:
            record_wallet_report(
                kind,
                meta.get("sourceId"),
                meta.get("username"),
                "walletTransaction batch insert failed",
                dry_run,
                reference_id=meta.get("referenceId"),
                action="insert_failed",
                issue_type="wallet_transaction_insert_failed",
                data=meta.get("data"),
                target_player_id=meta.get("playerId"),
                amount=meta.get("amount"),
                status=meta.get("status"),
                payment_gateway=meta.get("paymentGateway"),
                error=e,
            )
        return (0, skipped_rows + len(values))
    inserted_counts: Dict[str, int] = {}
    for row in inserted_rows or []:
        k = str(row[0])
        inserted_counts[k] = inserted_counts.get(k, 0) + 1
    duplicate_count = 0
    for meta in value_meta:
        ref = str(meta["referenceId"])
        if inserted_counts.get(ref, 0) > 0:
            inserted_counts[ref] -= 1
            continue
        duplicate_count += 1
        existing = _get_existing_wallet_target(tgt_conn, kind, ref)
        record_wallet_report(
            kind,
            meta.get("sourceId"),
            meta.get("username"),
            "Duplicate walletTransaction platform/referenceId encountered; target row already existed and ON CONFLICT DO NOTHING ignored this source row",
            dry_run,
            reference_id=ref,
            action="duplicate_key_ignored",
            issue_type="duplicate_reference_id_ignored",
            data=meta.get("data"),
            target_id=existing.get("id"),
            target_player_id=existing.get("playerId"),
            target_username=existing.get("userName"),
            amount=meta.get("amount"),
            status=meta.get("status"),
            payment_gateway=meta.get("paymentGateway"),
        )
    inserted_count = len(inserted_rows or [])
    trace_print(
        f"[LIVE][walletTransaction.{kind}] inserted={inserted_count} attempted={len(values)} duplicates={duplicate_count} skipped={skipped_rows}"
    )
    return (inserted_count, skipped_rows)




def _fetch_source_player_recon_rows(src_conn, date_from: Optional[str], date_to: Optional[str], limit: int = 0) -> List[Dict[str, Any]]:
    params: List[Any] = []
    date_sql = ""
    date_col = source_date_expr_v2("s", table=PLAYER_REGISTRATION_SOURCE_TABLE)
    if date_from is not None:
        date_sql += f" AND {date_col} >= %s::timestamptz"
        params.append(date_from)
    if date_to is not None:
        date_sql += f" AND {date_col} < %s::timestamptz"
        params.append(date_to)
    limit_sql = ""
    if limit and limit > 0:
        limit_sql = " LIMIT %s"
        params.append(limit)
    q = f"""
        SELECT
            s.id AS source_id,
            NULLIF(TRIM(s.data->>'name'), '') AS username,
            NULLIF(TRIM(s.data->>'id'), '') AS external_id
        FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)} s
        WHERE s.data IS NOT NULL{date_sql}
        ORDER BY {date_col} ASC, s.id ASC
        {limit_sql}
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    src_conn.rollback()
    return rows


def _fetch_source_game_recon_rows(src_conn, date_from: Optional[str], date_to: Optional[str], limit: int = 0) -> List[Dict[str, Any]]:
    params: List[Any] = []
    date_sql = ""
    date_col = source_date_expr_v2("s", table=GAME_TRANSACTION_SOURCE_TABLE)
    if date_from is not None:
        date_sql += f" AND {date_col} >= %s::timestamptz"
        params.append(date_from)
    if date_to is not None:
        date_sql += f" AND {date_col} < %s::timestamptz"
        params.append(date_to)
    limit_sql = ""
    if limit and limit > 0:
        limit_sql = " LIMIT %s"
        params.append(limit)
    q = f"""
        SELECT
            s.id AS source_id,
            COALESCE(NULLIF(TRIM(s.data->>'id'), ''), s.id::text) AS external_id,
            NULLIF(TRIM(s.data->'member'->>'id'), '') AS member_external_id,
            NULLIF(TRIM(s.data->'member'->>'name'), '') AS username
        FROM {source_table_ref(GAME_TRANSACTION_SOURCE_TABLE)} s
        WHERE s.data IS NOT NULL{date_sql}
        ORDER BY {date_col} ASC, s.id ASC
        {limit_sql}
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    src_conn.rollback()
    return rows


def _fetch_source_wallet_recon_rows(src_conn, table: str, date_from: Optional[str], date_to: Optional[str], limit: int = 0) -> List[Dict[str, Any]]:
    params: List[Any] = []
    date_sql = ""
    date_col = source_date_expr_v2("s", table=table)
    if date_from is not None:
        date_sql += f" AND {date_col} >= %s::timestamptz"
        params.append(date_from)
    if date_to is not None:
        date_sql += f" AND {date_col} < %s::timestamptz"
        params.append(date_to)
    limit_sql = ""
    if limit and limit > 0:
        limit_sql = " LIMIT %s"
        params.append(limit)
    q = f"""
        SELECT
            s.id AS source_id,
            COALESCE(NULLIF(TRIM(s.data->>'id'), ''), s.id::text) AS reference_id,
            NULLIF(TRIM(s.data->'member'->>'id'), '') AS member_external_id,
            NULLIF(TRIM(s.data->'member'->>'name'), '') AS username
        FROM {source_table_ref(table)} s
        WHERE s.data IS NOT NULL{date_sql}
        ORDER BY {date_col} ASC, s.id ASC
        {limit_sql}
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    src_conn.rollback()
    return rows


def _fetch_target_player_recon_maps(tgt_conn) -> Dict[str, Dict[str, Dict[str, Any]]]:
    q = """
        SELECT
            id AS target_id,
            "userName" AS username,
            "externalId"::text AS external_id,
            "brandName" AS brand_name,
            "createdAt" AS created_at
        FROM migration_repair."playerDetails_final"
    """
    with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q)
        rows = cur.fetchall()

    brand_norm: Dict[str, Dict[str, Any]] = {}
    brand_ext: Dict[str, Dict[str, Any]] = {}
    any_norm: Dict[str, Dict[str, Any]] = {}
    any_ext: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        brand_name = str(row.get("brand_name") or "").strip().lower()
        is_brand = brand_name == BRAND.strip().lower()
        uname_key = username_match_key(row.get("username"))
        ext_key = str(row.get("external_id") or "").strip()
        if is_brand:
            if uname_key and uname_key not in brand_norm:
                brand_norm[uname_key] = row
            if ext_key and ext_key not in brand_ext:
                brand_ext[ext_key] = row
        else:
            if uname_key and uname_key not in any_norm:
                any_norm[uname_key] = row
            if ext_key and ext_key not in any_ext:
                any_ext[ext_key] = row

    return {"brand_norm": brand_norm, "brand_ext": brand_ext, "any_norm": any_norm, "any_ext": any_ext}


def _fetch_target_game_external_ids(tgt_conn) -> Dict[str, Dict[str, Any]]:
    q = """
        SELECT
            id AS target_id,
            "externalId"::text AS external_id,
            "playerId" AS target_player_id,
            "playerUserName" AS target_username
        FROM migration_repair."gameTransaction_final"
        WHERE "brand" = %s
          AND "externalId" IS NOT NULL
    """
    out: Dict[str, Dict[str, Any]] = {}
    with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q, (BRAND,))
        for row in cur.fetchall():
            key = str(row.get("external_id") or "").strip()
            if key and key not in out:
                out[key] = row
    return out


def _fetch_target_wallet_reference_ids(tgt_conn, kind: str) -> Dict[str, Dict[str, Any]]:
    q = """
        SELECT
            wt.id AS target_id,
            wt."referenceId"::text AS reference_id,
            wt."playerId" AS target_player_id,
            pd."userName" AS target_username
        FROM migration_repair."walletTransaction_final" wt
        LEFT JOIN migration_repair."playerDetails_final" pd ON pd.id = wt."playerId"
        WHERE wt."platform" = %s
          AND wt."transactionType" = %s
          AND wt."referenceId" IS NOT NULL
    """
    out: Dict[str, Dict[str, Any]] = {}
    with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q, (WALLET_PLATFORM, kind))
        for row in cur.fetchall():
            key = str(row.get("reference_id") or "").strip()
            if key and key not in out:
                out[key] = row
    return out


def _target_row_value(row: Optional[Dict[str, Any]], key: str) -> Any:
    return row.get(key) if row else None


def run_player_reconciliation_checks_v2(src_conn, tgt_conn, date_from: Optional[str] = None, date_to: Optional[str] = None, dry_run: bool = False) -> None:
    """Two-connection player reconciliation.

    src_conn reads source PlayerRegistrationsInplayV2.
    tgt_conn reads target playerDetails_final.
    Comparison is done in Python, so source and target may live in different databases.
    """
    trace_print("[RECONCILIATION] Starting InPlayV2 playerDetails checks.")
    detail_limit = int(os.getenv("RECON_DETAIL_LIMIT", "5000"))
    source_rows = _fetch_source_player_recon_rows(src_conn, date_from, date_to)
    maps = _fetch_target_player_recon_maps(tgt_conn)

    written = 0
    for src in source_rows:
        if written >= detail_limit:
            break
        username = src.get("username")
        external_id = str(src.get("external_id") or "").strip()
        uname_key = username_match_key(username)
        t_norm = maps["brand_norm"].get(uname_key) if uname_key else None
        t_ext = maps["brand_ext"].get(external_id) if external_id else None
        t_any_norm = maps["any_norm"].get(uname_key) if uname_key else None
        t_any_ext = maps["any_ext"].get(external_id) if external_id else None
        target = t_norm or t_ext or t_any_norm or t_any_ext

        if not username:
            status = "source_missing_username"
            reason = "Source player has blank/missing username."
        elif t_norm and (external_id or "") != str(t_norm.get("external_id") or ""):
            status = "external_id_mismatch"
            reason = "Player exists by normalized username, but source member id/externalId differs from target externalId."
        elif t_norm:
            status = "matched"
            reason = "Player exists by normalized username."
        elif t_ext:
            status = "exists_by_external_id_only_username_mismatch"
            reason = "Player exists by source externalId/member id, but username does not match normalized username."
        elif t_any_norm:
            status = "exists_under_different_brand_by_username"
            reason = "Player exists by normalized username, but under a different brandName."
        elif t_any_ext:
            status = "exists_under_different_brand_by_external_id"
            reason = "Player exists by externalId/member id, but under a different brandName."
        else:
            status = "source_player_missing_in_target_truly_missing"
            reason = "Player was not found in playerDetails_final by normalized username, externalId, or alternate brand checks."
        if status == "matched":
            continue
        write_reconciliation_row(
            "1_player_reconciliation_detail", "detail", status,
            source_table=PLAYER_REGISTRATION_SOURCE_TABLE,
            source_id=src.get("source_id"),
            source_username=username,
            source_external_id=external_id,
            target_table="playerDetails",
            target_id=_target_row_value(target, "target_id"),
            target_username=_target_row_value(target, "username"),
            target_external_id=_target_row_value(target, "external_id"),
            reference_type="brandName",
            reference_value=_target_row_value(target, "brand_name"),
            reason=reason,
        )
        written += 1
    add_reconciliation_summary(f"Player reconciliation detail rows written={written}.")


def run_game_reconciliation_checks_v2(src_conn, tgt_conn, date_from: Optional[str] = None, date_to: Optional[str] = None, dry_run: bool = False) -> None:
    """Two-connection game reconciliation."""
    trace_print("[RECONCILIATION] Starting InPlayV2 gameTransaction checks.")
    detail_limit = int(os.getenv("RECON_DETAIL_LIMIT", "5000"))
    source_rows = _fetch_source_game_recon_rows(src_conn, date_from, date_to)
    target_games = _fetch_target_game_external_ids(tgt_conn)
    player_maps = _fetch_target_player_recon_maps(tgt_conn)
    written = 0
    for src in source_rows:
        if written >= detail_limit:
            break
        external_id = str(src.get("external_id") or "").strip()
        member_external_id = str(src.get("member_external_id") or "").strip()
        username = src.get("username")
        uname_key = username_match_key(username)
        target = target_games.get(external_id) if external_id else None
        player_by_member = player_maps["brand_ext"].get(member_external_id) if member_external_id else None
        player_by_user = player_maps["brand_norm"].get(uname_key) if uname_key else None
        resolved_player = player_by_member or player_by_user
        if not external_id:
            status = "source_missing_external_id"
            reason = "Source game transaction has blank/missing externalId."
        elif not username:
            status = "source_missing_username"
            reason = "Source game transaction has blank/missing member.name."
        elif target:
            status = "matched"
            reason = "Game transaction exists in target by externalId."
        elif player_by_member:
            status = "source_game_missing_in_target_player_exists_by_member_id_should_investigate"
            reason = "Game transaction is missing, but the player exists by member.id/externalId; investigate insert/duplicate/report logic."
        elif player_by_user:
            status = "source_game_missing_in_target_player_exists_by_username_should_investigate"
            reason = "Game transaction is missing, but the player exists by normalized username; investigate insert/duplicate/report logic."
        else:
            status = "source_game_missing_in_target_player_not_found_expected_skip"
            reason = "Game transaction is missing and player was not found by member.id/externalId or normalized username; expected skip under no-shadow-pla
yer rule."
        if status == "matched":
            continue
        write_reconciliation_row(
            "2_game_reconciliation_detail", "detail", status,
            source_table=GAME_TRANSACTION_SOURCE_TABLE,
            source_id=src.get("source_id"),
            source_username=username,
            source_external_id=external_id,
            target_table="gameTransaction",
            target_id=_target_row_value(target, "target_id"),
            target_username=_target_row_value(target, "target_username") or _target_row_value(resolved_player, "username"),
            target_external_id=member_external_id,
            reference_type="externalId",
            reference_value=external_id,
            reason=reason,
        )
        written += 1
    add_reconciliation_summary(f"Game reconciliation detail rows written={written}.")


def run_wallet_reconciliation_checks_v2(src_conn, tgt_conn, kind: str, date_from: Optional[str] = None, date_to: Optional[str] = None, dry_run: bool = False
) -> None:
    """Two-connection wallet reconciliation."""
    table = DEPOSITS_SOURCE_TABLE if kind == "deposit" else WITHDRAWALS_SOURCE_TABLE
    trace_print(f"[RECONCILIATION] Starting InPlayV2 walletTransaction.{kind} checks.")
    detail_limit = int(os.getenv("RECON_DETAIL_LIMIT", "5000"))
    source_rows = _fetch_source_wallet_recon_rows(src_conn, table, date_from, date_to)
    target_wallets = _fetch_target_wallet_reference_ids(tgt_conn, kind)
    player_maps = _fetch_target_player_recon_maps(tgt_conn)
    written = 0
    for src in source_rows:
        if written >= detail_limit:
            break
        reference_id = str(src.get("reference_id") or "").strip()
        member_external_id = str(src.get("member_external_id") or "").strip()
        username = src.get("username")
        uname_key = username_match_key(username)
        target = target_wallets.get(reference_id) if reference_id else None
        player_by_member = player_maps["brand_ext"].get(member_external_id) if member_external_id else None
        player_by_user = player_maps["brand_norm"].get(uname_key) if uname_key else None
        resolved_player = player_by_member or player_by_user
        if not reference_id:
            status = "source_missing_reference_id"
            reason = "Source wallet transaction has blank/missing reference id."
        elif not username:
            status = "source_missing_username"
            reason = "Source wallet transaction has blank/missing member.name."
        elif target:
            status = "matched"
            reason = "Wallet transaction exists in target by platform/transactionType/referenceId."
        elif player_by_member:
            status = "source_wallet_missing_in_target_player_exists_by_member_id_should_investigate"
            reason = "Wallet transaction is missing, but the player exists by member.id/externalId; investigate insert/duplicate/report logic."
        elif player_by_user:
            status = "source_wallet_missing_in_target_player_exists_by_username_should_investigate"
            reason = "Wallet transaction is missing, but the player exists by normalized username; investigate insert/duplicate/report logic."
        else:
            status = "source_wallet_missing_in_target_player_not_found_expected_skip"
            reason = "Wallet transaction is missing and player was not found by member.id/externalId or normalized username; expected skip under no-shadow-p
layer rule."
        if status == "matched":
            continue
        write_reconciliation_row(
            f"3_wallet_{kind}_reconciliation_detail", "detail", status,
            source_table=table,
            source_id=src.get("source_id"),
            source_username=username,
            source_external_id=reference_id,
            target_table="walletTransaction",
            target_id=_target_row_value(target, "target_id"),
            target_username=_target_row_value(target, "target_username") or _target_row_value(resolved_player, "username"),
            target_external_id=member_external_id,
            reference_type="referenceId",
            reference_value=reference_id,
            reason=reason,
        )
        written += 1
    add_reconciliation_summary(f"Wallet {kind} reconciliation detail rows written={written}.")


def run_dimension_reference_reconciliation_checks_v2(src_conn, tgt_conn, date_from: Optional[str] = None, date_to: Optional[str] = None, dry_run: bool = Fal
se) -> None:
    """Two-connection dimension reconciliation."""
    trace_print("[RECONCILIATION] Starting InPlayV2 dimension/reference checks.")
    params: List[Any] = []
    date_sql = ""
    date_col = source_date_expr_v2("s", table=GAME_TRANSACTION_SOURCE_TABLE)
    if date_from is not None:
        date_sql += f" AND {date_col} >= %s::timestamptz"
        params.append(date_from)
    if date_to is not None:
        date_sql += f" AND {date_col} < %s::timestamptz"
        params.append(date_to)
    q = f"""
        SELECT DISTINCT
            NULLIF(TRIM(s.data->'game'->>'provider'), '') AS provider_name,
            NULLIF(TRIM(s.data->'game'->>'type'), '') AS game_type
        FROM {source_table_ref(GAME_TRANSACTION_SOURCE_TABLE)} s
        WHERE s.data IS NOT NULL{date_sql}
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q, params)
        src_rows = cur.fetchall()
    src_conn.rollback()
    source_providers = {str(r.get("provider_name") or "").strip().upper() for r in src_rows if r.get("provider_name")}
    source_types = {normalize_game_type(str(r.get("game_type") or "")) for r in src_rows if r.get("game_type")}
    with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT "gameProvider" FROM migration_repair."gameProvider_final"')
        target_providers = {str(r.get("gameProvider") or "").strip().upper() for r in cur.fetchall()}
        cur.execute('SELECT "gameType" FROM migration_repair."gameType_final"')
        target_types = {str(r.get("gameType") or "").strip() for r in cur.fetchall()}
    written = 0
    detail_limit = int(os.getenv("RECON_DETAIL_LIMIT", "5000"))
    for provider in sorted(source_providers - target_providers):
        if written >= detail_limit:
            break
        write_reconciliation_row(
            "4_dimension_reference_detail", "detail", "missing_target_gameProvider",
            source_table=GAME_TRANSACTION_SOURCE_TABLE,
            target_table="gameProvider",
            reference_type="gameProvider",
            reference_value=provider,
            reason="Source game provider value missing from target dimension table",
        )
        written += 1
    for game_type in sorted(source_types - target_types):
        if written >= detail_limit:
            break
        write_reconciliation_row(
            "4_dimension_reference_detail", "detail", "missing_target_gameType",
            source_table=GAME_TRANSACTION_SOURCE_TABLE,
            target_table="gameType",
            reference_type="gameType",
            reference_value=game_type,
            reason="Source game type value missing from target dimension table",
        )
        written += 1
    add_reconciliation_summary(f"Dimension/reference reconciliation detail rows written={written}.")

def run_post_migration_reconciliation_checks_v2(
    src_conn,
    tgt_conn,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    run_player_reconciliation_checks_v2(src_conn, tgt_conn, date_from, date_to, dry_run)
    run_game_reconciliation_checks_v2(src_conn, tgt_conn, date_from, date_to, dry_run)
    run_wallet_reconciliation_checks_v2(src_conn, tgt_conn, "deposit", date_from, date_to, dry_run)
    run_wallet_reconciliation_checks_v2(
        src_conn, tgt_conn, "withdrawal", date_from, date_to, dry_run
    )
    run_dimension_reference_reconciliation_checks_v2(
        src_conn, tgt_conn, date_from, date_to, dry_run
    )


def run_post_migration_reconciliation_checks_v2(
    src_conn,
    tgt_conn,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    run_player_reconciliation_checks_v2(src_conn, tgt_conn, date_from, date_to, dry_run)
    run_game_reconciliation_checks_v2(src_conn, tgt_conn, date_from, date_to, dry_run)
    run_wallet_reconciliation_checks_v2(src_conn, tgt_conn, "deposit", date_from, date_to, dry_run)
    run_wallet_reconciliation_checks_v2(
        src_conn, tgt_conn, "withdrawal", date_from, date_to, dry_run
    )
    run_dimension_reference_reconciliation_checks_v2(
        src_conn, tgt_conn, date_from, date_to, dry_run
    )



def run_post_migration_data_quality_checks_v2(
    src_conn,
    tgt_conn,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """Two-connection playerDetails DQ check.

    Source sample is read through src_conn. Target playerDetails_final is read through
    tgt_conn. This preserves normalized-username DQ behavior while allowing source
    and target to live in different databases.
    """
    trace_print("[DATA QUALITY] Starting InPlayV2 post-migration DQ checks.")
    sample_limit = int(os.getenv("DQ_SAMPLE_LIMIT", "500"))
    params: List[Any] = []
    date_sql = ""
    date_col = source_date_expr_v2("s", table=PLAYER_REGISTRATION_SOURCE_TABLE)
    if date_from is not None:
        date_sql += f" AND {date_col} >= %s::timestamptz"
        params.append(date_from)
    if date_to is not None:
        date_sql += f" AND {date_col} < %s::timestamptz"
        params.append(date_to)
    params.append(sample_limit)

    source_q = f"""
        SELECT
            s.id AS source_id,
            NULLIF(TRIM(s.data->>'name'), '') AS raw_username,
            NULLIF(TRIM(s.data->>'id'), '') AS expected_external_id,
            s.data->>'mobileNumber' AS source_mobile,
            s.data->>'emailAddress' AS source_email
        FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)} s
        WHERE s.data IS NOT NULL{date_sql}
        ORDER BY {date_col} ASC, s.id ASC
        LIMIT %s
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(source_q, params)
        source_rows = cur.fetchall()
    src_conn.rollback()

    target_q = """
        SELECT
            id AS target_id,
            "userName",
            "externalId"::text AS external_id,
            "mobileNumber",
            "emailAddress"
        FROM migration_repair."playerDetails_final"
        WHERE LOWER(TRIM(COALESCE("brandName", ''))) = LOWER(TRIM(%s))
    """
    with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(target_q, (BRAND,))
        target_rows = cur.fetchall()

    target_by_username = {
        username_match_key(row.get("userName")): row
        for row in target_rows
        if username_match_key(row.get("userName"))
    }

    mismatch_count = 0
    mismatch_columns = set()
    for src in source_rows:
        raw_username = src.get("raw_username")
        expected_username = clean_username_value(raw_username)
        username_key = username_match_key(raw_username)
        expected_external_id = str(src.get("expected_external_id") or "").strip()
        expected_mobile = safe_mobile_10(src.get("source_mobile"))
        expected_email = sanitize_email(src.get("source_email"), expected_username or "unknown")
        target = target_by_username.get(username_key)
        comparisons = [
            ("userName", expected_username, target.get("userName") if target else None),
            ("externalId", expected_external_id, target.get("external_id") if target else None),
            ("mobileNumber", expected_mobile, target.get("mobileNumber") if target else None),
            ("emailAddress", expected_email, target.get("emailAddress") if target else None),
        ]
        for column_name, source_value, target_value in comparisons:
            if target is None or str(source_value or "") != str(target_value or ""):
                mismatch_count += 1
                mismatch_columns.add(column_name)

    write_data_quality_summary_row(
        "playerDetails_final",
        mismatch_count,
        date_from,
        date_to,
        mismatch_columns,
    )
    add_data_quality_summary(
        f"InPlayV2 DQ checks completed; mismatchColCount={mismatch_count}; csv={CSV_DATA_QUALITY_PATH}; sampleLimit={sample_limit}."
    )

def migrate_all(
    src_conn,
    tgt_conn,
    dry_run: bool,
    batch_size: int,
    commit_every: int,
    resume: bool,
    start_after_id: Optional[str],
    max_rows_total: Optional[int],
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
) -> int:
    ensure_wallet_dedupe_index(tgt_conn, dry_run=dry_run)
    from_dt_iso, until_dt_iso = date_window_bounds_for_source(date_from, date_to)
    date_window_requested = (date_from is not None) or (date_to is not None)
    if date_window_requested:
        trace_print(
            f"[DATE WINDOW][PHT] source_from_inclusive={from_dt_iso} "
            f"source_to_exclusive={until_dt_iso} timezone={PHT_TZ_NAME}"
        )
    if date_window_requested:
        trace_print(
            "[DATE WINDOW] --date-from/--date-to enabled. Source reads ignore migration checkpoints; checkpoint writes are monotonic and will not rewind new
er stored pointers."
        )

    def _initial_cursor(phase: str) -> Tuple[Optional[str], Optional[str]]:
        if date_window_requested:
            return (None, None)
        raw = (
            start_after_id
            if start_after_id is not None
            else (checkpoint_get(tgt_conn, phase) if resume else None)
        )
        return parse_inplayv2_checkpoint(raw)

    provider_cache: Dict[str, uuid.UUID] = {}
    gametype_cache: Dict[str, uuid.UUID] = {}
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID] = {}
    detail_map = fetch_player_detail_map(src_conn)
    print_source_query_summary("playerDetail", [f"{PLAYER_DETAIL_SOURCE_TABLE} detail-map"])
    if max_rows_total is not None:
        player_map = build_player_map(tgt_conn)
        phase = "gameTx"
        after_dt, after_id = _initial_cursor(phase)
        processed_gt = inserted_gt_total = skipped_gt_total = 0
        last_dt, last_id = after_dt, after_id or ""
        while processed_gt < max_rows_total:
            fetch_limit = min(batch_size, max_rows_total - processed_gt)
            rows = fetch_json_table_batch(
                src_conn,
                GAME_TRANSACTION_SOURCE_TABLE,
                last_dt,
                last_id or None,
                fetch_limit,
                from_dt=from_dt_iso,
                until_dt=until_dt_iso,
            )
            if not rows:
                break
            inserted, skipped = insert_game_tx_batch(
                tgt_conn,
                rows,
                player_map,
                provider_cache,
                gametype_cache,
                gamelist_cache,
                dry_run=dry_run,
            )
            inserted_gt_total += inserted
            skipped_gt_total += skipped
            processed_gt += len(rows)
            last_row_data = as_dict(rows[-1].get("data"))
            last_id = str(rows[-1]["id"])
            last_dt = _source_dt_value_v2(last_row_data)
            if not dry_run:
                checkpoint_set(
                    tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run
                )
                if (processed_gt % commit_every) < batch_size:
                    tgt_conn.commit()
            trace_print(
                f"Progress gameTx (sanity): processed={processed_gt} inserted={inserted_gt_total} skipped={skipped_gt_total} lastId={last_id}"
            )
        if dry_run:
            tgt_conn.rollback()
        else:
            tgt_conn.commit()
        print_source_query_summary("gameTransaction", [f"{GAME_TRANSACTION_SOURCE_TABLE} batch"])
        trace_print(
            f"Completed sanity gameTx run. sourceProcessed={processed_gt} inserted_or_would_insert={inserted_gt_total} skipped={skipped_gt_total}"
        )
        return processed_gt
    phase = "player"
    after_dt, after_id = _initial_cursor(phase)
    processed = player_upserted = player_skipped = 0
    last_dt, last_id = after_dt, after_id or ""
    while True:
        rows = fetch_json_table_batch(
            src_conn,
            PLAYER_REGISTRATION_SOURCE_TABLE,
            last_dt,
            last_id or None,
            batch_size,
            from_dt=from_dt_iso,
            until_dt=until_dt_iso,
        )
        if not rows:
            break
        for r in rows:
            rid = str(r["id"])
            data = as_dict(r.get("data"))
            if data:
                try:
                    data["source_id"] = rid
                    pid = upsert_player_from_member(
                        tgt_conn, data, dry_run=dry_run, detail_map=detail_map
                    )
                    player_upserted += 1 if pid else 0
                    player_skipped += 0 if pid else 1
                except Exception as e:
                    player_skipped += 1
                    trace_print(f"[WARN] player upsert failed id={rid}: {e}", level=logging.WARNING)
                    try:
                        tgt_conn.rollback()
                    except Exception:
                        pass
            else:
                player_skipped += 1
                record_player_report(
                    PLAYER_REGISTRATION_SOURCE_TABLE,
                    rid,
                    None,
                    "skipped",
                    "missing_or_invalid_json",
                    "Missing or invalid source JSON data",
                    dry_run,
                    data=data,
                )
            processed += 1
            last_id = rid
            last_dt = _source_dt_value_v2(data)
            if (not dry_run) and (processed % commit_every == 0):
                checkpoint_set(
                    tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run
                )
                tgt_conn.commit()
        trace_print(
            f"Progress players: processed={processed} upserted={player_upserted} skipped={player_skipped} lastId={last_id}"
        )
    if not dry_run:
        if processed > 0:
            checkpoint_set(
                tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run
            )
        tgt_conn.commit()
    print_source_query_summary("playerRegistration", [f"{PLAYER_REGISTRATION_SOURCE_TABLE} batch"])
    trace_print(
        f"Completed players phase. sourceProcessed={processed} inserted_or_updated={player_upserted} skipped={player_skipped}"
    )
    player_map = build_player_map(tgt_conn)
    processed_gt = inserted_gt_total = skipped_gt_total = 0
    phase = "gameTx"
    after_dt, after_id = _initial_cursor(phase)
    last_dt, last_id = after_dt, after_id or ""
    while True:
        rows = fetch_json_table_batch(
            src_conn,
            GAME_TRANSACTION_SOURCE_TABLE,
            last_dt,
            last_id or None,
            batch_size,
            from_dt=from_dt_iso,
            until_dt=until_dt_iso,
        )
        if not rows:
            break
        inserted, skipped = insert_game_tx_batch(
            tgt_conn,
            rows,
            player_map,
            provider_cache,
            gametype_cache,
            gamelist_cache,
            dry_run=dry_run,
        )
        inserted_gt_total += inserted
        skipped_gt_total += skipped
        processed_gt += len(rows)
        last_row_data = as_dict(rows[-1].get("data"))
        last_id = str(rows[-1]["id"])
        last_dt = _source_dt_value_v2(last_row_data)
        if not dry_run:
            checkpoint_set(
                tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run
            )
            if (processed_gt % commit_every) < batch_size:
                tgt_conn.commit()
        trace_print(
            f"Progress gameTx: processed={processed_gt} inserted={inserted_gt_total} skipped={skipped_gt_total} lastId={last_id}"
        )
    if not dry_run:
        tgt_conn.commit()
    print_source_query_summary("gameTransaction", [f"{GAME_TRANSACTION_SOURCE_TABLE} batch"])
    trace_print(
        f"Completed gameTx phase. sourceProcessed={processed_gt} inserted_or_would_insert={inserted_gt_total} skipped={skipped_gt_total}"
    )
    processed_dep = inserted_dep_total = skipped_dep_total = 0
    phase = "deposits"
    after_dt, after_id = _initial_cursor(phase)
    last_dt, last_id = after_dt, after_id or ""
    while True:
        rows = fetch_json_table_batch(
            src_conn,
            DEPOSITS_SOURCE_TABLE,
            last_dt,
            last_id or None,
            batch_size,
            from_dt=from_dt_iso,
            until_dt=until_dt_iso,
        )
        if not rows:
            break
        inserted, skipped = insert_wallet_batch(
            tgt_conn, rows, "deposit", player_map, dry_run=dry_run
        )
        inserted_dep_total += inserted
        skipped_dep_total += skipped
        processed_dep += len(rows)
        last_row_data = as_dict(rows[-1].get("data"))
        last_id = str(rows[-1]["id"])
        last_dt = _source_dt_value_v2(last_row_data)
        if not dry_run:
            checkpoint_set(
                tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run
            )
            if (processed_dep % commit_every) < batch_size:
                tgt_conn.commit()
        trace_print(
            f"Progress deposits: processed={processed_dep} inserted={inserted_dep_total} skipped={skipped_dep_total} lastId={last_id}"
        )
    if not dry_run:
        tgt_conn.commit()
    print_source_query_summary("walletTransaction.deposits", [f"{DEPOSITS_SOURCE_TABLE} batch"])
    trace_print(
        f"Completed deposits phase. sourceProcessed={processed_dep} inserted_or_would_insert={inserted_dep_total} skipped={skipped_dep_total}"
    )
    processed_wd = inserted_wd_total = skipped_wd_total = 0
    phase = "withdrawals"
    after_dt, after_id = _initial_cursor(phase)
    last_dt, last_id = after_dt, after_id or ""
    while True:
        rows = fetch_json_table_batch(
            src_conn,
            WITHDRAWALS_SOURCE_TABLE,
            last_dt,
            last_id or None,
            batch_size,
            from_dt=from_dt_iso,
            until_dt=until_dt_iso,
        )
        if not rows:
            break
        inserted, skipped = insert_wallet_batch(
            tgt_conn, rows, "withdrawal", player_map, dry_run=dry_run
        )
        inserted_wd_total += inserted
        skipped_wd_total += skipped
        processed_wd += len(rows)
        last_row_data = as_dict(rows[-1].get("data"))
        last_id = str(rows[-1]["id"])
        last_dt = _source_dt_value_v2(last_row_data)
        if not dry_run:
            checkpoint_set(
                tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run
            )
            if (processed_wd % commit_every) < batch_size:
                tgt_conn.commit()
        trace_print(
            f"Progress withdrawals: processed={processed_wd} inserted={inserted_wd_total} skipped={skipped_wd_total} lastId={last_id}"
        )
    if not dry_run:
        tgt_conn.commit()
    print_source_query_summary(
        "walletTransaction.withdrawals", [f"{WITHDRAWALS_SOURCE_TABLE} batch"]
    )
    trace_print(
        f"Completed withdrawals phase. sourceProcessed={processed_wd} inserted_or_would_insert={inserted_wd_total} skipped={skipped_wd_total}"
    )
    run_post_migration_reconciliation_checks_v2(
        src_conn, tgt_conn, from_dt_iso, until_dt_iso, dry_run=dry_run
    )
    run_post_migration_data_quality_checks_v2(
        src_conn, tgt_conn, from_dt_iso, until_dt_iso, dry_run=dry_run
    )
    if dry_run:
        tgt_conn.rollback()
        trace_print("[DRY-RUN] rolled back all writes.")
    total_source_processed = processed + processed_gt + processed_dep + processed_wd
    total_inserted = player_upserted + inserted_gt_total + inserted_dep_total + inserted_wd_total
    total_skipped = player_skipped + skipped_gt_total + skipped_dep_total + skipped_wd_total
    summary = f"""
[RUN SUMMARY][InPlayV2]
  playerDetails inserted_or_updated={player_upserted}, sourceProcessed={processed}, skipped={player_skipped}, reportCsvRows={player_report_total()}, reportA
ctions=[{player_report_counts_text()}], reportIssues=[{player_report_issue_counts_text()}], reportCsvPath={CSV_PLAYERS_PATH}
  gameTransaction inserted_or_would_insert={inserted_gt_total}, sourceProcessed={processed_gt}, skipped={skipped_gt_total}, duplicates={phase_report_count('
gameTransaction', 'duplicate_key_ignored')}, reportCsvRows={phase_report_total('gameTransaction')}, reportCsvPath={CSV_GAMETX_PATH}
  walletTransaction deposits inserted_or_would_insert={inserted_dep_total}, sourceProcessed={processed_dep}, skipped={skipped_dep_total}, duplicates={phase_
report_count('walletTransaction.deposit', 'duplicate_key_ignored')}, reportCsvRows={phase_report_total('walletTransaction.deposit')}, reportCsvPath={CSV_DEP
OSITS_PATH}
  walletTransaction withdrawals inserted_or_would_insert={inserted_wd_total}, sourceProcessed={processed_wd}, skipped={skipped_wd_total}, duplicates={phase_
report_count('walletTransaction.withdrawal', 'duplicate_key_ignored')}, reportCsvRows={phase_report_total('walletTransaction.withdrawal')}, reportCsvPath={C
SV_WITHDRAWALS_PATH}
  reconciliationCsvPath={CSV_RECONCILIATION_PATH}
  dataQualityCsvPath={CSV_DATA_QUALITY_PATH}
  TOTAL sourceProcessed={total_source_processed}
  TOTAL skipped={total_skipped}
  TOTAL transaction_duplicates={all_phase_duplicate_total()}
  TOTAL records inserted_or_would_insert={total_inserted}
""".strip()
    trace_print(summary)
    return total_source_processed


def migrate_single_user(
    src_conn,
    tgt_conn,
    username: str,
    dry_run: bool,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
) -> None:
    username = clean_username_value(username)
    username_key = username_match_key(username)
    from_dt_iso, until_dt_iso = date_window_bounds_for_source(date_from, date_to)
    if date_from is not None or date_to is not None:
        trace_print(
            f"[DATE WINDOW][PHT][single-user] source_from_inclusive={from_dt_iso} "
            f"source_to_exclusive={until_dt_iso} timezone={PHT_TZ_NAME}"
        )
    ensure_wallet_dedupe_index(tgt_conn, dry_run=dry_run)
    detail_map = fetch_player_detail_map(src_conn)

    def _date_filter_for(table: str, params: List[Any]) -> str:
        date_col = source_date_expr_v2(table=table)
        clause = ""
        if from_dt_iso is not None:
            clause += f" AND {date_col} >= %s::timestamptz"
            params.append(from_dt_iso)
        if until_dt_iso is not None:
            clause += f" AND {date_col} < %s::timestamptz"
            params.append(until_dt_iso)
        return clause

    reg_params: List[Any] = [username_key]
    reg_date_sql = _date_filter_for(PLAYER_REGISTRATION_SOURCE_TABLE, reg_params)
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT id, data
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE data IS NOT NULL
              AND LOWER(TRIM(data->>'name')) = %s
              {reg_date_sql}
            ORDER BY id
            LIMIT 1
            """,
            reg_params,
        )
        reg = cur.fetchone()
    player_map: Dict[str, uuid.UUID] = build_player_map(tgt_conn)
    if reg:
        member = as_dict(reg.get("data"))
        member["source_id"] = reg.get("id")
        pid = upsert_player_from_member(tgt_conn, member, dry_run=dry_run, detail_map=detail_map)
        player_map[username_key] = pid
        trace_print(f"playerDetails upserted from registration: userName={username} id={pid}")
    else:
        record_player_report(
            PLAYER_REGISTRATION_SOURCE_TABLE,
            "N/A",
            username,
            "skipped",
            "registration_not_found",
            f"No registration row found for username={username}; game/wallet will not create shadow player",
            dry_run,
        )
    provider_cache: Dict[str, uuid.UUID] = {}
    gametype_cache: Dict[str, uuid.UUID] = {}
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID] = {}
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        tx_params: List[Any] = [username_key]
        tx_date_sql = _date_filter_for(GAME_TRANSACTION_SOURCE_TABLE, tx_params)
        cur.execute(
            f"""
            SELECT id, data
            FROM {source_table_ref(GAME_TRANSACTION_SOURCE_TABLE)}
            WHERE data IS NOT NULL
              AND LOWER(TRIM(data->'member'->>'name')) = %s
              {tx_date_sql}
            ORDER BY id
            """,
            tx_params,
        )
        tx_rows = cur.fetchall()

        dep_params: List[Any] = [username_key]
        dep_date_sql = _date_filter_for(DEPOSITS_SOURCE_TABLE, dep_params)
        cur.execute(
            f"""
            SELECT id, data
            FROM {source_table_ref(DEPOSITS_SOURCE_TABLE)}
            WHERE data IS NOT NULL
              AND LOWER(TRIM(data->'member'->>'name')) = %s
              {dep_date_sql}
            ORDER BY id
            """,
            dep_params,
        )
        dep_rows = cur.fetchall()

        wd_params: List[Any] = [username_key]
        wd_date_sql = _date_filter_for(WITHDRAWALS_SOURCE_TABLE, wd_params)
        cur.execute(
            f"""
            SELECT id, data
            FROM {source_table_ref(WITHDRAWALS_SOURCE_TABLE)}
            WHERE data IS NOT NULL
              AND LOWER(TRIM(data->'member'->>'name')) = %s
              {wd_date_sql}
            ORDER BY id
            """,
            wd_params,
        )
        wd_rows = cur.fetchall()
    inserted_gt, skipped_gt = insert_game_tx_batch(
        tgt_conn,
        tx_rows,
        player_map,
        provider_cache,
        gametype_cache,
        gamelist_cache,
        dry_run=dry_run,
    )
    inserted_dep, skipped_dep = insert_wallet_batch(
        tgt_conn, dep_rows, "deposit", player_map, dry_run=dry_run
    )
    inserted_wd, skipped_wd = insert_wallet_batch(
        tgt_conn, wd_rows, "withdrawal", player_map, dry_run=dry_run
    )
    if dry_run:
        tgt_conn.rollback()
        trace_print("[DRY-RUN] rolled back.")
    else:
        tgt_conn.commit()
        trace_print("Committed.")
    trace_print(
        f"[RUN SUMMARY][single-user] player={1 if reg else 0} gameInserted={inserted_gt} gameSkipped={skipped_gt} "
        f"depositsInserted={inserted_dep} depositsSkipped={skipped_dep} "
        f"withdrawalsInserted={inserted_wd} withdrawalsSkipped={skipped_wd}"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--username", help="InPlayV2 member.name (single-user mode). Example: franztest"
    )
    ap.add_argument("--migrate-all", action="store_true")
    ap.add_argument("--repair", action="store_true")
    ap.add_argument("--repair-status", action="store_true")
    ap.add_argument("--delete", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--delete-first", action="store_true")
    ap.add_argument("--keep-from", type=_parse_date_arg, default=None)
    ap.add_argument("--keep-to", type=_parse_date_arg, default=None)
    ap.add_argument(
        "--date-from",
        type=_parse_date_arg,
        default=None,
        help="With --migrate-all only: migrate source rows on/after this UTC date",
    )
    ap.add_argument(
        "--date-to",
        type=_parse_date_arg,
        default=None,
        help="With --migrate-all only: migrate source rows on/before this UTC date",
    )
    ap.add_argument("--batch-size", type=int, default=10000)
    ap.add_argument("--commit-every", type=int, default=200000)
    ap.add_argument(
        "--resume", type=lambda x: str(x).lower() not in ("0", "false", "no"), default=True
    )
    ap.add_argument("--start-after-id", type=str, default=None)
    ap.add_argument(
        "--max-rows-total", "--max-rows-per-phase", dest="max_rows_total", type=int, default=None
    )
    ap.add_argument("--loop", action="store_true")
    args = ap.parse_args()

    if (
        not args.migrate_all
        and not args.username
        and not args.repair
        and not args.repair_status
        and not args.delete
    ):
        raise SystemExit(
            "Either provide --username, --migrate-all, --repair, --repair-status, or --delete."
        )

    configure_run_artifact_paths(args)

    os.makedirs("logs", exist_ok=True)
    os.makedirs("reports", exist_ok=True)
    logging.basicConfig(
        filename=LOG_FILE_PATH,
        filemode="a",
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=logging.INFO,
    )

    trace_print("\n=== InPlayV2 migration with audit/reporting ===")
    trace_print(
        f"dry_run={args.dry_run} migrate_all={args.migrate_all} repair={args.repair} delete={args.delete} delete_first={args.delete_first}"
    )
    trace_print(
        f"date_from={args.date_from} date_to={args.date_to} batch_size={args.batch_size} commit_every={args.commit_every} resume={args.resume} start_after_i
d={args.start_after_id} max_rows_total={args.max_rows_total}"
    )
    trace_print(
        'data_source=iestdbrds target_data_source=iestdl source_schema=public player_map=migration_repair."playerDetails_final" target_schema=migration_repa
ir target_suffix=_final'
    )
    trace_print(
        '[SCHEMA CONFIG] source JSONB tables use public."..." on iestdbrds; player_map and all migration target tables use migration_repair."..._final" on i
estdl.'
    )

    src = connect("iestdl")
    tgt = connect("iestdbrds")
    try:
        if args.delete:
            delete_inplayv2_target_data(
                tgt, dry_run=args.dry_run, keep_from=args.keep_from, keep_to=args.keep_to
            )
            return
        if args.repair:
            repair_existing_data(src, tgt, args.dry_run, args.batch_size, args.commit_every)
        elif args.repair_status:
            repair_wallet_statuses(src, tgt, args.dry_run, args.batch_size, args.commit_every)
            return
        elif args.delete_first:
            delete_inplayv2_target_data(
                tgt, dry_run=args.dry_run, keep_from=args.keep_from, keep_to=args.keep_to
            )
        if args.migrate_all and not args.repair:
            if args.loop:
                loop_end = time.time() + 9 * 3600
                iteration = 0
                while time.time() < loop_end:
                    iteration += 1
                    trace_print(
                        f"\n[LOOP iter={iteration} remaining={(loop_end - time.time())/3600:.2f}h]"
                    )
                    total = migrate_all(
                        src,
                        tgt,
                        args.dry_run,
                        args.batch_size,
                        args.commit_every,
                        True,
                        args.start_after_id if iteration == 1 else None,
                        args.max_rows_total,
                        args.date_from,
                        args.date_to,
                    )
                    if total == 0:
                        trace_print("[LOOP] Nothing new found, sleeping 30s ...")
                        time.sleep(30)
            else:
                migrate_all(
                    src,
                    tgt,
                    args.dry_run,
                    args.batch_size,
                    args.commit_every,
                    args.resume,
                    args.start_after_id,
                    args.max_rows_total,
                    args.date_from,
                    args.date_to,
                )
        elif args.username:
            migrate_single_user(
                src,
                tgt,
                args.username,
                dry_run=args.dry_run,
                date_from=args.date_from,
                date_to=args.date_to,
            )
        trace_print("[INFO] Migration operation finished. Checking report files for dispatch...")
        active_reports = package_reports_if_needed(
            [
                CSV_GAMETX_PATH,
                CSV_DEPOSITS_PATH,
                CSV_WITHDRAWALS_PATH,
                CSV_PLAYERS_PATH,
                CSV_RECONCILIATION_PATH,
                CSV_DATA_QUALITY_PATH,
            ]
        )

        trace_print("[FINAL SUMMARY] Generated report files:")
        for report_path in [
            CSV_GAMETX_PATH,
            CSV_DEPOSITS_PATH,
            CSV_WITHDRAWALS_PATH,
            CSV_PLAYERS_PATH,
            CSV_RECONCILIATION_PATH,
            CSV_DATA_QUALITY_PATH,
        ]:
            if report_path and os.path.isfile(report_path):
                try:
                    size_bytes = os.path.getsize(report_path)
                except Exception:
                    size_bytes = "unknown"
                trace_print(f"  - {report_path} size={size_bytes}")
            else:
                trace_print(f"  - {report_path} not_created")

        trace_print(reconciliation_email_summary().rstrip())
        trace_print(data_quality_email_summary().rstrip())

        email_body = f"""Hello Team,

The InPlayV2 database migration pipeline execution run has completed.
Timestamp Group Identifier: {TIMESTAMP_STR}
Dry Run Configuration Flag: {args.dry_run}
Source Schema: public

Attached are generated CSV reports for skipped, duplicate, reconciliation, and data-quality records.

{reconciliation_email_summary()}

{data_quality_email_summary()}

If the combined report payload reached 17MB, the reports were packaged into a single ZIP file.
"""

        if args.dry_run:
            trace_print("[MAILER][SKIP] --dry-run enabled; reports generated locally but email was not sent.")
        elif send_migration_reports is not None:
            to_emails = [
                x.strip()
                for x in os.getenv("MIGRATION_REPORT_TO", "allan.faylona@iest.com.ph").split(",")
                if x.strip()
            ]
            cc_emails = [
                x.strip()
                for x in os.getenv("MIGRATION_REPORT_CC", "").split(",")
                if x.strip()
            ]
            send_migration_reports(
                subject=f"[{BRAND} InPlayV2 Migration Notification] Phase Run Complete - {TIMESTAMP_STR}",
                body_text=email_body,
                to_emails=to_emails,
                cc_emails=cc_emails,
                file_paths=active_reports,
                smtp_host=os.getenv("SMTP_HOST", "smtp.gmail.com"),
                smtp_port=int(os.getenv("SMTP_PORT", "587")),
                smtp_user=os.getenv("SMTP_USER", ""),
                smtp_password=os.getenv("SMTP_PASSWORD", ""),
            )
            trace_print("[MAILER][SENT] Migration report email dispatched.")
        else:
            trace_print(
                "[MAILER][WARN] utilities.mailer.send_migration_reports not available; reports were generated but not emailed.",
                level=logging.WARNING,
            )
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