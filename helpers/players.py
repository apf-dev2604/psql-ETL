import json
import re
import uuid
from typing import Any, Dict, Optional, Tuple
from dataclasses import asdict, is_dataclass

from .db import table_ref
from .dates import parse_iso_dt
from .models import NormalizedPlayer

EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)


def as_dict(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return json.loads(value)
        except Exception:
            return {}
    return {}




def _as_mapping(value: Any) -> Dict[str, Any]:
    if is_dataclass(value):
        return asdict(value)
    return dict(value or {})

def digits_only(value: Any) -> str:
    return "".join(c for c in str(value or "") if c.isdigit())


def clean_username(value: Any, remove_internal_spaces: bool = True) -> str:
    text = str(value or "").strip()
    if remove_internal_spaces:
        text = re.sub(r"\s+", "", text)
    return text


def username_key(value: Any) -> str:
    return clean_username(value).lower()


def safe_mobile_10(value: Any, missing_value: str = "0000000000") -> str:
    digits = digits_only(value)
    if len(digits) >= 10:
        return digits[-10:]
    return missing_value


def sanitize_email(value: Any, username: str, missing_empty: bool = False) -> str:
    text = str(value or "").strip().rstrip(".")
    if EMAIL_RE.match(text):
        return text
    if missing_empty:
        return ""
    return f"{username}@unknown.local"


def split_name(full_name: Any, fallback_unknown: bool = False) -> Tuple[str, str, str]:
    text = str(full_name or "").strip()
    if not text:
        return ("Unknown", "", "Unknown") if fallback_unknown else ("", "", "")
    parts = [p for p in text.split() if p.strip()]
    if len(parts) == 1:
        return (parts[0], "", "Unknown" if fallback_unknown else "")
    if len(parts) == 2:
        return (parts[0], "", parts[1])
    return (parts[0], " ".join(parts[1:-1]), parts[-1])


def to_decimal_str(value: Any) -> str:
    if value is None:
        return "0"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).strip()
    return text if text else "0"


def ensure_outlet(conn, config, outlet_code: Optional[str], dry_run: bool) -> None:
    code = str(outlet_code or "").strip()
    if not code or dry_run:
        return
    sql = f"""
    INSERT INTO {table_ref(config.TARGET_SCHEMA, config.OUTLET_TABLE)} (
        "outletCode", "outletName",
        "streetAddress", "barangayAddress", "cityAddress", "provinceAddress",
        "outletShare", "operator", "isActive", "brand",
        "createdAt", "updatedAt", "lastUpdateDatetime"
    ) VALUES (%s, %s, '', '', '', '', 0.00, %s, true, %s, now(), now(), now())
    ON CONFLICT ("outletCode") DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(sql, (code, code, config.BRAND, config.BRAND))


def build_player_map(conn, config) -> Dict[str, uuid.UUID]:
    result: Dict[str, uuid.UUID] = {}
    sql = f"""
    SELECT id, "userName"
    FROM {table_ref(config.TARGET_SCHEMA, config.PLAYER_TABLE)}
    WHERE LOWER(TRIM(COALESCE("brandName", ''))) = LOWER(TRIM(%s))
    """
    with conn.cursor() as cur:
        cur.execute(sql, (config.BRAND,))
        for player_id, username in cur.fetchall():
            key = username_key(username)
            if key:
                result[key] = player_id
    return result


def lookup_player_id_by_username(conn, config, username: Any) -> Optional[uuid.UUID]:
    key = username_key(username)
    if not key:
        return None
    sql = f"""
    SELECT id
    FROM {table_ref(config.TARGET_SCHEMA, config.PLAYER_TABLE)}
    WHERE LOWER(TRIM(COALESCE("brandName", ''))) = LOWER(TRIM(%s))
      AND regexp_replace(LOWER(TRIM(COALESCE("userName", ''))), '\\s+', '', 'g') = %s
    ORDER BY "createdAt" DESC NULLS LAST, id DESC
    LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (config.BRAND, key))
        row = cur.fetchone()
    return row[0] if row else None


def upsert_player(conn, config, mapped: Any, dry_run: bool) -> uuid.UUID:
    mapped = _as_mapping(mapped)
    username = clean_username(mapped.get("username"))
    if not username:
        raise RuntimeError("Mapped player missing username")

    if dry_run:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT id FROM {table_ref(config.TARGET_SCHEMA, config.PLAYER_TABLE)} WHERE LOWER(TRIM("userName"))=%s',
                (username_key(username),),
            )
            row = cur.fetchone()
            return row[0] if row else uuid.uuid4()

    outlet_code = mapped.get("outlet_code")
    ensure_outlet(conn, config, outlet_code, dry_run=dry_run)

    sql = f"""
    INSERT INTO {table_ref(config.TARGET_SCHEMA, config.PLAYER_TABLE)} (
        "userName", "firstName", "middleName", "lastName",
        "mobileNumber", "mobileNumberVerified",
        "emailAddress", "emailVerified",
        "registrationDate", "registrationIp", "registrationReferrer",
        "brandName", "isVerified", "isBlocked", "blockedDatetime", "isActive",
        "lastLogin", "lastLoginIp", "outletCode", "affiliateCode",
        "addressStreet", "addressBarangay", "addressCity", "addressProvince",
        "incomeSource", "industry", "externalId", "birthdate",
        "walletBalance", "walletBalanceDatetime", "createdAt", "updatedAt"
    ) VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,%s,%s,false,NULL,%s,%s,%s,%s,NULL,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now()
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
        "outletCode"=COALESCE(EXCLUDED."outletCode", "{config.PLAYER_TABLE}"."outletCode"),
        "birthdate"=COALESCE(EXCLUDED."birthdate", "{config.PLAYER_TABLE}"."birthdate"),
        "externalId"=EXCLUDED."externalId",
        "walletBalance"=EXCLUDED."walletBalance",
        "walletBalanceDatetime"=EXCLUDED."walletBalanceDatetime",
        "addressProvince"=EXCLUDED."addressProvince",
        "incomeSource"=EXCLUDED."incomeSource",
        "industry"=EXCLUDED."industry",
        "updatedAt"=now()
    RETURNING id
    """
    params = (
        username,
        mapped.get("first_name") or "",
        mapped.get("middle_name") or "",
        mapped.get("last_name") or "",
        mapped.get("mobile_number") or config.DEFAULT_MOBILE,
        bool(mapped.get("mobile_verified", False)),
        mapped.get("email") or config.DEFAULT_EMAIL,
        bool(mapped.get("email_verified", False)),
        mapped.get("registration_date"),
        mapped.get("registration_ip"),
        config.BRAND,
        bool(mapped.get("is_verified", False)),
        bool(mapped.get("is_active", False)),
        mapped.get("last_login"),
        mapped.get("last_login_ip"),
        outlet_code,
        mapped.get("address_street") or config.DEFAULT_ADDRESS,
        mapped.get("address_barangay") or config.DEFAULT_ADDRESS,
        mapped.get("address_city") or config.DEFAULT_ADDRESS,
        mapped.get("address_province") or config.DEFAULT_ADDRESS,
        mapped.get("income_source") or config.DEFAULT_ADDRESS,
        mapped.get("industry") or config.DEFAULT_ADDRESS,
        mapped.get("external_id"),
        mapped.get("birthdate"),
        mapped.get("wallet_balance") or "0",
        mapped.get("wallet_balance_datetime") or mapped.get("registration_date"),
    )
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]
