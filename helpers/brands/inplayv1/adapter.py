from datetime import datetime, timezone
from typing import Any, Dict, Optional

from psycopg2.extras import RealDictCursor

from helpers.db import table_ref
from helpers.dates import parse_iso_dt
from helpers.players import as_dict, clean_username, sanitize_email, safe_mobile_10, split_name, to_decimal_str, normalize_outlet_code
from . import config

SOURCE_DATE_KEYS = {
    "players": ("createddate",),
    "game_transactions": ("GameDate",),
    "deposits": ("transferDate",),
    "withdrawals": ("transferDate",),
}

class Adapter:
    config = config

    @staticmethod
    def as_dict(value: Any) -> Dict[str, Any]:
        return as_dict(value)

    @staticmethod
    def _first(data: Dict[str, Any], *keys: str) -> Any:
        for key in keys:
            value = data.get(key)
            if value is not None and str(value).strip() != "":
                return value
        return None

    @staticmethod
    def source_date_expr(data_ref: str = "data") -> str:
        return f"NULLIF({data_ref}->>'createddate','')::timestamptz"

    @staticmethod
    def source_date_expr_for_table(source_key: str, data_ref: str = "data") -> str:
        keys = SOURCE_DATE_KEYS[source_key]
        pieces = [f"NULLIF({data_ref}->>'{key}','')" for key in keys]
        raw = pieces[0] if len(pieces) == 1 else f"COALESCE({', '.join(pieces)})"
        return f"immutable_json_timestamp({raw})"

    @classmethod
    def source_created_value(cls, data: Dict[str, Any], source_key: str = "players") -> Optional[str]:
        value = cls._first(data or {}, *SOURCE_DATE_KEYS.get(source_key, ("createddate",)))
        return str(value).strip() if value is not None and str(value).strip() else None

    def fetch_player_detail_map(self, src_conn, from_dt=None, until_dt=None) -> Dict[str, Dict[str, str]]:
        detail_map: Dict[str, Dict[str, str]] = {}
        date_expr = self.source_date_expr_for_table("players", "data")
        conditions = ["data IS NOT NULL"]
        params = []
        if from_dt is not None:
            conditions.append(f"{date_expr} >= %s::timestamptz")
            params.append(from_dt)
        if until_dt is not None:
            conditions.append(f"{date_expr} < %s::timestamptz")
            params.append(until_dt)
        sql = f'''
        SELECT id, data->>'username' AS username, data
        FROM {table_ref(config.SOURCE_SCHEMA, config.SOURCE_TABLES['players'])}
        WHERE {' AND '.join(conditions)}
        '''
        with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                data = as_dict(row.get("data"))
                username = clean_username(row.get("username") or data.get("name"))
                if not username:
                    continue
                detail_map[username] = {
                    "address_province": data.get("permanent_address") or data.get("addressProvince") or config.DEFAULT_ADDRESS,
                    "wallet_balance": to_decimal_str(data.get("balance") or "0"),
                    "external_id": data.get("card_id") or data.get("cardId") or data.get("id") or data.get("externalId"),
                    "outlet_code": normalize_outlet_code(data.get("outlet_id") or data.get("outletCode")),
                    "contact_number": safe_mobile_10(data.get("contact_number") or data.get("contactNumber"), config.DEFAULT_MOBILE),
                    "birthdate": data.get("birthdate") or data.get("birthDay") or data.get("dateOfBirth") or data.get("birthDate"),
                    "income_source": data.get("incomeSource") or data.get("income_source") or "N/A",
                    "industry": data.get("industry") or data.get("natureOfWork") or "N/A",
                }
        src_conn.rollback()
        return detail_map

    def map_player(self, row: Dict[str, Any], detail_map: Dict[str, Dict[str, str]], **kwargs) -> Optional[Dict[str, Any]]:
        data = as_dict(row.get("data"))
        username = clean_username(self._first(data, "username", "name", "userName", "loginName", "userid", "userId"))
        if not username:
            return None
        detail = detail_map.get(username) or {}
        external_id = str(self._first(data, "card_id", "cardId", "memberId", "externalId") or detail.get("external_id") or "").strip() or None
        first = data.get("first_name")
        middle = data.get("middle_name") or ""
        last = data.get("last_name")
        if not first or not last:
            first, middle2, last2 = split_name(data.get("realName"), fallback_unknown=True)
            middle = middle or middle2
            last = last or last2
        reg_dt = parse_iso_dt(self._first(data, "createddate", "createdDate", "registrationDate")) or parse_iso_dt(self._first(data, "updatedate", "updatedDate")) or datetime.now(timezone.utc)
        suspended = data.get("suspended")
        is_active = (str(suspended) == "0") if suspended is not None else not str(data.get("closed") or "").strip().lower() in ("1", "true", "t", "yes", "y")
        email = sanitize_email(data.get("email"), username)
        birth_dt = parse_iso_dt(self._first(data, "birthdate", "birthDay", "dateOfBirth", "birthDate") or detail.get("birthdate"))
        return {
            "source_id": row.get("id"),
            "username": username,
            "external_id": external_id,
            "first_name": first or "Unknown",
            "middle_name": middle or "",
            "last_name": last or "Unknown",
            "mobile_number": safe_mobile_10(data.get("contact_number") or detail.get("contact_number"), config.DEFAULT_MOBILE),
            "email": email,
            "registration_date": reg_dt,
            "registration_ip": None,
            "is_verified": False,
            "is_active": is_active,
            "last_login": None,
            "last_login_ip": None,
            "outlet_code": normalize_outlet_code(data.get("outlet_id") or data.get("outletCode")) or detail.get("outlet_code"),
            "address_street": data.get("current_address") or data.get("addressStreet") or config.DEFAULT_ADDRESS,
            "address_barangay": config.DEFAULT_ADDRESS,
            "address_city": config.DEFAULT_ADDRESS,
            "address_province": detail.get("address_province") or data.get("permanent_address") or data.get("addressProvince") or config.DEFAULT_ADDRESS,
            "income_source": data.get("incomeSource") or data.get("income_source") or config.DEFAULT_ADDRESS,
            "industry": data.get("industry") or config.DEFAULT_ADDRESS,
            "birthdate": birth_dt.date() if hasattr(birth_dt, "date") else birth_dt,
            "wallet_balance": to_decimal_str(data.get("balance") or 0),
            "wallet_balance_datetime": reg_dt,
        }

    def map_game_transaction(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        data = as_dict(row.get("data"))
        if not data:
            return None
        member = as_dict(data.get("member"))
        username = clean_username(self._first(data, "PlayerAccount", "playerAccount", "username", "userName", "name", "loginName", "userid", "userId") or self._first(member, "name", "username", "userName"))
        external_id = str(self._first(data, "TransactionID", "transactionId", "externalId", "id") or row.get("id") or "").strip()
        if not username or not external_id:
            return None
        game = as_dict(data.get("game"))
        start_dt = parse_iso_dt(self._first(data, "GameDate", "gameDate", "gamedate", "dateTimeCreated", "createdDateTime")) or datetime.now(timezone.utc)
        end_dt = parse_iso_dt(self._first(data, "UpdateDateTime", "updatedAt", "dateTimeSettled", "settledDate", "GameDate")) or start_dt
        seed_over_raw = data.get("SEED_MONEY_JACKPOT_WON_OVER_1000") or data.get("seedMoneyJackpotOver1000")
        try:
            seed_over = 1 if float(seed_over_raw or 0) > 0 else 0
        except Exception:
            seed_over = 0
        return {
            "source_id": row.get("id"),
            "external_id": external_id,
            "username": username,
            "member_external_id": self._first(member, "id", "card_id", "cardId"),
            "provider_name": self._first(data, "GameProvider", "gameProvider", "provider", "Provider") or game.get("provider") or "UNKNOWN",
            "game_name": self._first(data, "GameName", "gameName", "name", "GameTitle") or game.get("name") or "UNKNOWN",
            "game_type": self._first(data, "GameType", "gameType", "type") or game.get("type") or "Slots",
            "created": start_dt,
            "settled": end_dt,
            "bet_amount": self._first(data, "TotalStakes", "bet", "betAmount", "stake"),
            "valid_bet": self._first(data, "ValidBet", "validBet", "TotalStakes", "bet", "betAmount", "stake"),
            "payout_amount": self._first(data, "TotalWins", "payout", "payoutAmount", "win"),
            "pc1": data.get("PC1"), "pc2": data.get("PC2"), "pc3": data.get("PC3"), "pc4": data.get("PC4"), "pc5": self._first(data, "PC5", "jackpotContribution"),
            "jw1": data.get("JW1"), "jw2": data.get("JW2"), "jw3": data.get("JW3"), "jw4": data.get("JW4"), "jw5": self._first(data, "JW5", "jackpotPayout"),
            "progression_contribution_paid": self._first(data, "PROGRESSIVE_CONTRIBUTION_PAID", "progressionContributionPaid"),
            "seed_money_won": self._first(data, "SEED_MONEY_WON", "seedMoneyWon"),
            "seed_money_jackpot_over_1000": seed_over,
            "table_room_id": self._first(data, "Outlet", "outlet", "tableRoomId"),
            "round_id": self._first(data, "SessionID", "sessionId", "vendorRoundId", "roundId"),
        }

    def map_wallet(self, row: Dict[str, Any], kind: str) -> Optional[Dict[str, Any]]:
        data = as_dict(row.get("data"))
        if not data:
            return None
        member = as_dict(data.get("member"))
        username = clean_username(self._first(data, "username", "userName", "name", "loginName", "userid", "userId", "PlayerAccount", "playerAccount") or self._first(member, "name", "username", "userName"))
        reference_id = str(self._first(data, "id", "referenceId", "transactionId") or row.get("id") or "").strip()
        if not username or not reference_id:
            return None
        created = parse_iso_dt(self._first(data, "transferDate", "transferdate", "TransferDate")) or datetime(1970, 1, 1, tzinfo=timezone.utc)
        try:
            amount = abs(float(self._first(data, "amount", "TotalAmount", "totalAmount") or 0))
        except Exception:
            amount = 0
        return {
            "kind": kind.lower(),
            "source_table": config.SOURCE_TABLES["deposits" if kind == "deposit" else "withdrawals"],
            "source_id": row.get("id"),
            "reference_id": reference_id,
            "username": username,
            "amount": amount,
            "status": "confirmed",
            "payment_gateway": self._first(data, "payment", "paymentMethod", "paymentGateway") or "N/A",
            "domain": config.DEFAULT_DOMAIN,
            "created": created,
            "confirmed": created,
        }
