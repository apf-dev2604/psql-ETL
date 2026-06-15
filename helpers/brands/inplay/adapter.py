from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from psycopg2.extras import RealDictCursor

from helpers.db import table_ref
from helpers.dates import parse_iso_dt
from helpers.players import (
    as_dict,
    clean_username,
    normalize_outlet_code,
    sanitize_email,
    safe_mobile_10,
    split_name,
    to_decimal_str,
)
from . import config


class Adapter:
    config = config

    @staticmethod
    def as_dict(value: Any) -> Dict[str, Any]:
        return as_dict(value)

    @staticmethod
    def source_date_expr(data_ref: str = "data") -> str:
        return ("immutable_json_timestamp(" f"COALESCE({data_ref}->>'dateTimeCreated', {data_ref}->>'createdDateTime')" ")")

    @staticmethod
    def source_created_value(data: Dict[str, Any]) -> Optional[str]:
        value = data.get("dateTimeCreated") or data.get("createdDateTime")
        return str(value).strip() if value else None

    def fetch_player_detail_map(self, src_conn, *args, **kwargs) -> Dict[str, Dict[str, str]]:
        detail_map: Dict[str, Dict[str, str]] = {}
        sql = f"""
        SELECT data->>'id' AS external_id,
               data->'verification'->>'address'        AS address_province,
               data->'verification'->>'sourceOfIncome' AS income_source,
               data->'verification'->>'natureOfWork'   AS industry
        FROM {table_ref(config.SOURCE_SCHEMA, config.SOURCE_TABLES['player_detail'])}
        WHERE data->'verification' IS NOT NULL
          AND data->'verification' != 'null'::jsonb
        """
        with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            for row in cur.fetchall():
                if row["external_id"]:
                    detail_map[row["external_id"]] = {
                        "address_province": row["address_province"] or config.DEFAULT_ADDRESS,
                        "income_source": row["income_source"] or config.DEFAULT_ADDRESS,
                        "industry": row["industry"] or config.DEFAULT_ADDRESS,
                    }
        src_conn.rollback()
        return detail_map

    def map_player(self, row: Dict[str, Any], detail_map: Dict[str, Dict[str, str]], **kwargs) -> Optional[Dict[str, Any]]:
        data = as_dict(row.get("data"))
        username = clean_username(data.get("name"))
        if not username:
            return None
        external_id = str(data.get("id") or "").strip() or None
        first, middle, last = split_name(data.get("realName"), fallback_unknown=True)
        detail = detail_map.get(external_id or "") or {}
        wallet = as_dict(data.get("wallet"))
        reg_dt = parse_iso_dt(data.get("dateTimeCreated")) or datetime.now(timezone.utc)
        return {
            "source_id": row.get("id"),
            "username": username,
            "external_id": external_id,
            "first_name": first,
            "middle_name": middle,
            "last_name": last,
            "mobile_number": safe_mobile_10(data.get("mobileNumber"), config.DEFAULT_MOBILE),
            "email": sanitize_email(data.get("emailAddress"), username),
            "registration_date": reg_dt,
            "registration_ip": data.get("ipAddress"),
            "is_verified": str(data.get("verificationStatus") or "").upper() in ("VERIFIED", "APPROVED"),
            "is_active": str(data.get("status") or "").upper() == "ACTIVE",
            "last_login": parse_iso_dt(data.get("dateTimeLastAndroidLogIn")) or parse_iso_dt(data.get("dateTimeLastActive")),
            "last_login_ip": data.get("ipAddress"),
            "outlet_code": normalize_outlet_code(data.get("branchCode")),
            "address_street": config.DEFAULT_ADDRESS,
            "address_barangay": config.DEFAULT_ADDRESS,
            "address_city": config.DEFAULT_ADDRESS,
            "address_province": detail.get("address_province") or config.DEFAULT_ADDRESS,
            "income_source": detail.get("income_source") or config.DEFAULT_ADDRESS,
            "industry": detail.get("industry") or config.DEFAULT_ADDRESS,
            "birthdate": parse_iso_dt(data.get("birthDay")),
            "wallet_balance": to_decimal_str(wallet.get("balance")),
            "wallet_balance_datetime": reg_dt,
        }

    def map_game_transaction(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        data = as_dict(row.get("data"))
        if not data:
            return None
        member = as_dict(data.get("member"))
        username = clean_username(member.get("name"))
        external_id = str(data.get("id") or row.get("id") or "").strip()
        if not username or not external_id:
            return None
        game = as_dict(data.get("game"))
        created = parse_iso_dt(data.get("dateTimeCreated")) or datetime.now(timezone.utc)
        return {
            "source_id": row.get("id"),
            "external_id": external_id,
            "username": username,
            "member_external_id": member.get("id"),
            "provider_name": game.get("provider") or "UNKNOWN",
            "game_name": game.get("name") or "UNKNOWN",
            "game_type": game.get("type") or "SLOTS",
            "created": created,
            "settled": parse_iso_dt(data.get("dateTimeSettled")) or created,
            "bet_amount": data.get("bet"),
            "valid_bet": data.get("bet"),
            "payout_amount": data.get("payout"),
            "jackpot_contribution": data.get("jackpotContribution"),
            "jackpot_payout": data.get("jackpotPayout"),
            "round_id": data.get("vendorRoundId"),
        }

    def map_wallet(self, row: Dict[str, Any], kind: str) -> Optional[Dict[str, Any]]:
        data = as_dict(row.get("data"))
        if not data:
            return None
        member = as_dict(data.get("member"))
        username = clean_username(member.get("name"))
        reference_id = str(data.get("id") or row.get("id") or "").strip()
        if not username or not reference_id:
            return None
        created = parse_iso_dt(data.get("dateTimeCreated") or data.get("createdDateTime")) or datetime.now(timezone.utc)
        return {
            "kind": kind,
            "source_table": config.SOURCE_TABLES["deposits" if kind == "deposit" else "withdrawals"],
            "source_id": row.get("id"),
            "reference_id": reference_id,
            "username": username,
            "member_external_id": member.get("id"),
            "amount": data.get("netAmount") or data.get("amount"),
            "status": str(data.get("status") or "").strip().lower(),
            "payment_gateway": str(data.get("type") or data.get("paymentGateway") or "N/A").strip() or "N/A",
            "domain": data.get("domain") or member.get("domain") or config.DEFAULT_DOMAIN,
            "created": created,
            "confirmed": parse_iso_dt(data.get("dateTimeConfirmed")) or created,
        }
