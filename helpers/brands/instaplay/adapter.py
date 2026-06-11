from datetime import datetime, timezone, date
from typing import Any, Dict, List, Optional

from psycopg2.extras import RealDictCursor

from helpers.db import table_ref
from helpers.dates import parse_iso_dt
from helpers.players import (
    as_dict,
    clean_username,
    sanitize_email,
    safe_mobile_10,
    split_name,
    to_decimal_str,
)
from helpers.source_fetch import fetch_json_batch
from . import config


def parse_iso_date(value: Any) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except Exception:
        return None


def pick_first(*values: Any) -> Optional[Any]:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


class Adapter:
    config = config

    @staticmethod
    def as_dict(value: Any) -> Dict[str, Any]:
        return as_dict(value)

    @staticmethod
    def source_date_expr(data_ref: str = "data") -> str:
        return f"({data_ref}->>'dateTimeCreated')::timestamptz"

    @staticmethod
    def source_created_value(data: Dict[str, Any]) -> Optional[str]:
        value = data.get("dateTimeCreated")
        return str(value).strip() if value else None

    def fetch_player_detail_map(self, src_conn) -> Dict[str, Dict[str, str]]:
        # Member-driven mode fetches details by member id to avoid loading all detail rows.
        return {}

    def fetch_player_detail(self, src_conn, member_id: str) -> Dict[str, Any]:
        with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT id, data
                FROM {table_ref(config.SOURCE_SCHEMA, config.SOURCE_TABLES['player_detail'])}
                WHERE id=%s
                LIMIT 1
                """,
                (member_id,),
            )
            row = cur.fetchone()
        src_conn.rollback()
        return as_dict(row.get("data")) if row else {}

    def map_player(self, row: Dict[str, Any], detail_cache: Dict[str, Dict[str, Any]], **kwargs) -> Optional[Dict[str, Any]]:
        src_conn = kwargs.get("src_conn")
        data = as_dict(row.get("data"))
        username = clean_username(data.get("name"), remove_internal_spaces=False)
        if not username:
            return None
        external_id = str(data.get("id") or "").strip() or None
        if external_id and external_id not in detail_cache and src_conn is not None:
            detail_cache[external_id] = self.fetch_player_detail(src_conn, external_id)
        detail = as_dict(detail_cache.get(external_id or ""))
        verification = as_dict(detail.get("verification"))
        first, middle, last = split_name(data.get("realName"), fallback_unknown=False)
        wallet = as_dict(data.get("wallet"))
        registered = parse_iso_dt(data.get("dateTimeCreated"))
        wallet_dt = parse_iso_dt(data.get("dateTimeLastUpdated")) or parse_iso_dt(data.get("dateTimeLastActive"))
        verification_status = str(data.get("verificationStatus") or "").upper()
        detail_status = str(verification.get("status") or "").upper()
        return {
            "source_id": row.get("id"),
            "username": username,
            "external_id": external_id,
            "first_name": first,
            "middle_name": middle,
            "last_name": last,
            "mobile_number": safe_mobile_10(data.get("mobileNumber"), config.DEFAULT_MOBILE),
            "mobile_verified": True,
            "email": sanitize_email(data.get("emailAddress") or data.get("email"), username, missing_empty=True),
            "registration_date": registered,
            "registration_ip": data.get("ipAddress"),
            "is_verified": verification_status == "VERIFIED" or detail_status == "APPROVED",
            "is_active": str(data.get("status") or "").upper() in ("ACTIVE", "VERIFICATION_LOCKED"),
            "last_login": parse_iso_dt(data.get("dateTimeLastActive")) or parse_iso_dt(data.get("dateTimeLastAndroidLogIn")),
            "last_login_ip": str(data.get("ipAddress") or "").strip() or None,
            "outlet_code": data.get("branchCode"),
            "address_street": "",
            "address_barangay": "",
            "address_city": "",
            "address_province": verification.get("address") or verification.get("permanentAddress") or "",
            "income_source": verification.get("sourceOfIncome") or "",
            "industry": verification.get("natureOfWork") or "",
            "birthdate": parse_iso_date(data.get("birthDay")),
            "wallet_balance": to_decimal_str(wallet.get("balance")),
            "wallet_balance_datetime": wallet_dt or datetime(1900, 1, 1, tzinfo=timezone.utc),
        }

    def fetch_member_game_rows(self, src_conn, member_id: str, username: str, limit: int, from_dt: Optional[str], until_dt: Optional[str]) -> List[Dict[str, Any]]:
        return fetch_json_batch(
            src_conn,
            config.SOURCE_SCHEMA,
            config.SOURCE_TABLES["game_transactions"],
            self.source_date_expr("data"),
            after_dt=None,
            after_id=None,
            limit=limit,
            from_dt=from_dt,
            until_dt=until_dt,
            extra_conditions=["data->'member'->>'name' = %s"],
            extra_params=[username],
            label=f"{config.BRAND_KEY} game by member",
        )

    def fetch_member_wallet_rows(self, src_conn, kind: str, member_id: str, limit: int, from_dt: Optional[str], until_dt: Optional[str]) -> List[Dict[str, Any]]:
        table = config.SOURCE_TABLES["deposits" if kind == "deposit" else "withdrawals"]
        return fetch_json_batch(
            src_conn,
            config.SOURCE_SCHEMA,
            table,
            self.source_date_expr("data"),
            after_dt=None,
            after_id=None,
            limit=limit,
            from_dt=from_dt,
            until_dt=until_dt,
            extra_conditions=["data->'member'->>'id' = %s"],
            extra_params=[member_id],
            label=f"{config.BRAND_KEY} {kind} by member",
        )

    def normalize_end_datetime(self, tx: Dict[str, Any], start_dt: datetime) -> datetime:
        end_dt = parse_iso_dt(tx.get("dateTimeSettled")) or parse_iso_dt(tx.get("dateTimeEnded")) or parse_iso_dt(tx.get("endDateTime"))
        return end_dt if end_dt and end_dt >= start_dt else start_dt

    def map_game_transaction(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        tx = as_dict(row.get("data"))
        if not tx:
            return None
        member = as_dict(tx.get("member"))
        username = clean_username(member.get("name"), remove_internal_spaces=False)
        game = as_dict(tx.get("game"))
        external_id = str(row.get("id") or tx.get("id") or "").strip()
        if not username or not external_id:
            return None
        created = parse_iso_dt(tx.get("dateTimeCreated")) or datetime.now(timezone.utc)
        meta = as_dict(tx.get("metadata"))
        raw_request = as_dict(meta.get("rawRequest"))
        return {
            "source_id": row.get("id"),
            "external_id": external_id,
            "username": username,
            "member_external_id": member.get("id"),
            "provider_name": game.get("provider") or "UNKNOWN",
            "game_name": game.get("name") or "UNKNOWN",
            "game_type": game.get("type") or "SLOTS",
            "created": created,
            "settled": self.normalize_end_datetime(tx, created),
            "bet_amount": tx.get("bet"),
            "valid_bet": tx.get("bet"),
            "payout_amount": tx.get("payout"),
            "jackpot_contribution": tx.get("jackpotContribution"),
            "jackpot_payout": tx.get("jackpotPayout"),
            "round_id": pick_first(tx.get("vendorRoundId"), raw_request.get("parent_bet_id"), raw_request.get("roundId")),
            "parlay": True,
        }

    def map_wallet(self, row: Dict[str, Any], kind: str) -> Optional[Dict[str, Any]]:
        data = as_dict(row.get("data"))
        if not data:
            return None
        member = as_dict(data.get("member"))
        username = clean_username(member.get("name"), remove_internal_spaces=False)
        ref = pick_first(data.get("reference"), data.get("referenceId"), data.get("reference_id"), data.get("transaction_id"), data.get("id"), row.get("id"))
        if not username or not ref:
            return None
        val = pick_first(data.get("domain"), data.get("site"), data.get("host"))
        domain = val if val == "android/o472" else config.DEFAULT_DOMAIN
        status = str(pick_first(data.get("status"), data.get("state"), data.get("result")) or "CONFIRMED").strip().lower()
        created = parse_iso_dt(pick_first(data.get("dateTimeCreated"), data.get("createdAt"), data.get("createdDatetime"), data.get("created_time"), data.get("timestamp"))) or datetime.now(timezone.utc)
        return {
            "kind": kind,
            "source_table": config.SOURCE_TABLES["deposits" if kind == "deposit" else "withdrawals"],
            "source_id": row.get("id"),
            "reference_id": str(ref),
            "username": username,
            "member_external_id": member.get("id"),
            "amount": pick_first(data.get("amount"), data.get("netAmount")),
            "status": status,
            "payment_gateway": str(pick_first(data.get("type"), data.get("paymentGateway"), data.get("gateway"), data.get("channel")) or ""),
            "domain": str(domain),
            "created": created,
            "confirmed": parse_iso_dt(pick_first(data.get("dateTimeConfirmed"), data.get("confirmedAt"), data.get("dateTimeLastUpdated"))),
        }
