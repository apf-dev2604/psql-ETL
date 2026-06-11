import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from psycopg2.extras import RealDictCursor

from helpers.dates import parse_iso_dt
from helpers.players import clean_username, safe_mobile_10, to_decimal_str
from helpers.db import table_ref
from . import config

EMAIL_RE = re.compile(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$", re.I)

class Adapter:
    config = config

    @staticmethod
    def as_dict(value: Any) -> Dict[str, Any]:
        return dict(value or {})

    @staticmethod
    def source_date_expr(data_ref: str = "data") -> str:
        return "now()"

    @staticmethod
    def source_created_value(data: Dict[str, Any], source_key: str = "players") -> Optional[str]:
        return None

    def fetch_player_detail_map(self, src_conn, *args, **kwargs) -> Dict[str, Dict[str, str]]:
        return {}

    @staticmethod
    def _safe_email(email: Any) -> str:
        e = str(email or "").strip().lower().rstrip(".,;:")
        if e and EMAIL_RE.match(e):
            return e
        return "unknown@example.com"

    @staticmethod
    def _dt(value: Any):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except Exception:
            return None

    @staticmethod
    def _append_flat_date_window(conditions: List[str], params: List[Any], column_expr: str, from_dt: Optional[str], until_dt: Optional[str]) -> None:
        # 1Play source columns are timestamp-like flat RDBMS columns. The shared
        # engine passes PHT business-window bounds as timestamptz text; convert
        # them back to Asia/Manila local timestamp for comparison against the
        # source timestamp columns.
        if from_dt is not None:
            conditions.append(f"{column_expr} >= (%s::timestamptz AT TIME ZONE 'Asia/Manila')")
            params.append(from_dt)
        if until_dt is not None:
            conditions.append(f"{column_expr} < (%s::timestamptz AT TIME ZONE 'Asia/Manila')")
            params.append(until_dt)

    def fetch_player_rows(self, src_conn, after_idx: int, batch_size: int, from_dt: Optional[str] = None, until_dt: Optional[str] = None) -> List[Dict[str, Any]]:
        conditions = ['pd."IDX" > %s']
        params: List[Any] = [after_idx]
        self._append_flat_date_window(conditions, params, 'COALESCE(pr."REGISTRATION_DATE", pd."START_DATE")', from_dt, until_dt)
        sql = f'''
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
          o."IW1P_CODE"                  AS "OUTLET_IW1P_CODE",
          o."OUTLET_CODE"                AS "OUTLET_CODE",
          o."SITE_NAME"                  AS "OUTLET_NAME",
          o."SITE_NAME"                  AS "SITE_NAME",
          o."DATE_CREATED"               AS "OUTLET_CREATED_AT"
        FROM {table_ref(config.SOURCE_SCHEMA, config.SOURCE_TABLES["players"])} pd
        LEFT JOIN {table_ref(config.SOURCE_SCHEMA, config.SOURCE_TABLES["player_registrations"])} pr
          ON pr."PLAYER_NAME" = pd."LOGIN_NAME"
        LEFT JOIN {table_ref(config.SOURCE_SCHEMA, config.SOURCE_TABLES["outlets"])} o
          ON o."IW1P_CODE" = LEFT(pd."LOGIN_NAME", 7)
        WHERE {' AND '.join(conditions)}
        ORDER BY pd."IDX"
        LIMIT %s
        '''
        params.append(batch_size)
        with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        src_conn.rollback()
        return rows

    def fetch_game_rows(self, src_conn, after_idx: int, batch_size: int, from_dt: Optional[str] = None, until_dt: Optional[str] = None) -> List[Dict[str, Any]]:
        conditions = ['"IDX" > %s']
        params: List[Any] = [after_idx]
        self._append_flat_date_window(conditions, params, '"GAME_DATE"', from_dt, until_dt)
        sql = f'''
        SELECT "IDX","GAME_PROVIDER","TRANSACTION_ID","SESSION_ID","GAME_DATE","OUTLET",
               "PLAYER_ACCOUNT","GAME_NAME","TOTAL_STAKES","TOTAL_WINS",
               "PC1","PC2","PC3","PC4","PC5","JW1","JW2","JW3","JW4","JW5",
               "UPDATE_DATE_TIME","PROGRESSIVE_CONTRIBUTION_PAID","SEED_MONEY_WON",
               "SEED_MONEY_JACKPOT_WON_OVER_1000"
        FROM {table_ref(config.SOURCE_SCHEMA, config.SOURCE_TABLES["game_transactions"])}
        WHERE {' AND '.join(conditions)}
        ORDER BY "IDX"
        LIMIT %s
        '''
        params.append(batch_size)
        with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        src_conn.rollback()
        return rows

    def fetch_wallet_rows(self, src_conn, after_idx: int, batch_size: int, from_dt: Optional[str] = None, until_dt: Optional[str] = None) -> List[Dict[str, Any]]:
        conditions = ['"IDX" > %s']
        params: List[Any] = [after_idx]
        self._append_flat_date_window(conditions, params, '"TRANSACTION_DATE"', from_dt, until_dt)
        sql = f'''
        SELECT "IDX","PLAYER_NAME","TRANSACTION_DATE","TRANSACTION_ID","TRANSACTION_TYPE",
               "AMOUNT","BONUS","FEE","BANK","ACCOUNT_NAME","ACCOUNT_NUMBER","IP"
        FROM {table_ref(config.SOURCE_SCHEMA, config.SOURCE_TABLES["deposits"])}
        WHERE {' AND '.join(conditions)}
        ORDER BY "IDX"
        LIMIT %s
        '''
        params.append(batch_size)
        with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        src_conn.rollback()
        return rows

    def ensure_outlet_from_player_row(self, tgt_conn, row: Dict[str, Any], dry_run: bool) -> None:
        outlet_code = str(row.get("OUTLET_CODE") or "").strip()
        if not outlet_code or dry_run:
            return
        outlet_name = str(row.get("OUTLET_NAME") or outlet_code).strip()
        operator = str(row.get("SITE_NAME") or "").strip()
        created_at = self._dt(row.get("OUTLET_CREATED_AT"))
        sql = f'''
        INSERT INTO {table_ref(config.TARGET_SCHEMA, config.OUTLET_TABLE)} (
            "outletCode", "outletName", "streetAddress", "barangayAddress",
            "cityAddress", "provinceAddress", "outletShare", "operator",
            "isActive", "lastUpdateDatetime", "createdAt", "updatedAt", "brand"
        ) VALUES (%s,%s,'','','','',0.00,%s,true,now(),COALESCE(%s, now()),now(),%s)
        ON CONFLICT ("outletCode") DO UPDATE SET
            "outletName" = EXCLUDED."outletName",
            "operator" = EXCLUDED."operator",
            "brand" = EXCLUDED."brand",
            "updatedAt" = now(),
            "lastUpdateDatetime" = now()
        '''
        with tgt_conn.cursor() as cur:
            cur.execute(sql, (outlet_code, outlet_name or outlet_code, operator, created_at, config.BRAND))

    def map_player(self, row: Dict[str, Any], detail_map: Dict[str, Dict[str, str]], **kwargs) -> Optional[Dict[str, Any]]:
        login = clean_username(row.get("LOGIN_NAME"), remove_internal_spaces=False)
        if not login:
            return None
        full_name = str(row.get("REG_FULL_NAME") or row.get("DETAILS_PLAYER_NAME") or login).strip()
        parts = [p for p in full_name.split() if p.strip()]
        if len(parts) == 0:
            first = middle = last = ""
        elif len(parts) == 1:
            first, middle, last = parts[0], "", ""
        elif len(parts) == 2:
            first, middle, last = parts[0], "", parts[1]
        else:
            first, middle, last = parts[0], " ".join(parts[1:-1]), parts[-1]
        if str(last).strip().upper() == "UNKNOWN":
            last = ""
        mobile_raw = row.get("DETAILS_MOBILE_NUMBER") or row.get("REG_PHONE_NUMBER")
        raw_email = row.get("DETAILS_EMAIL") or row.get("REG_EMAIL")
        reg_dt = self._dt(row.get("REGISTRATION_DATE")) or self._dt(row.get("DETAILS_START_DATE")) or datetime.now(timezone.utc)
        last_login = self._dt(row.get("DETAILS_LAST_LOGIN_DATE"))
        last_ip = str(row.get("DETAILS_LAST_IP") or "").strip().split("/")[0] or None
        ver = str(row.get("DETAILS_VERIFICATION_STATUS") or "").strip().upper()
        det_status = str(row.get("DETAILS_STATUS") or "").strip().upper()
        reg_status = str(row.get("REG_STATUS") or "").strip().upper()
        address_raw = str(row.get("DETAILS_ADDRESS") or row.get("DETAILS_PERMANENT_ADDRESS") or "").strip()
        dob = self._dt(row.get("DETAILS_DATE_OF_BIRTH"))
        return {
            "source_id": row.get("IDX"),
            "username": login,
            "external_id": login,
            "first_name": first,
            "middle_name": middle,
            "last_name": last,
            "mobile_number": safe_mobile_10(mobile_raw, config.DEFAULT_MOBILE),
            "email": self._safe_email(raw_email),
            "registration_date": reg_dt,
            "is_verified": ver in ("VERIFIED", "APPROVED"),
            "is_active": det_status == "ACTIVE" or reg_status == "ACTIVE",
            "last_login": last_login,
            "last_login_ip": last_ip,
            "outlet_code": str(row.get("OUTLET_CODE") or "").strip() or None,
            "address_street": "",
            "address_barangay": "",
            "address_city": "",
            "address_province": address_raw or config.DEFAULT_ADDRESS,
            "income_source": str(row.get("DETAILS_INCOME") or "").strip() or config.DEFAULT_ADDRESS,
            "industry": str(row.get("DETAILS_INDUSTRY") or "").strip() or config.DEFAULT_ADDRESS,
            "birthdate": dob.date() if dob else None,
            "wallet_balance": to_decimal_str(row.get("REG_BALANCE")),
            "wallet_balance_datetime": reg_dt,
        }

    def map_game_transaction(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        external_id = str(row.get("TRANSACTION_ID") or "").strip()
        login = clean_username(row.get("PLAYER_ACCOUNT"), remove_internal_spaces=False)
        if not external_id or not login:
            return None
        game_dt = self._dt(row.get("GAME_DATE")) or datetime.now(timezone.utc)
        over_raw = row.get("SEED_MONEY_JACKPOT_WON_OVER_1000")
        try:
            over = 1 if over_raw is not None and float(over_raw) > 0 else 0
        except Exception:
            over = 0
        return {
            "source_id": row.get("IDX"),
            "external_id": external_id,
            "username": login,
            "provider_name": str(row.get("GAME_PROVIDER") or "UNKNOWN"),
            "game_name": str(row.get("GAME_NAME") or "UNKNOWN"),
            "game_type": "Slots",
            "created": game_dt,
            "settled": game_dt,
            "bet_amount": row.get("TOTAL_STAKES"),
            "valid_bet": row.get("TOTAL_STAKES"),
            "payout_amount": row.get("TOTAL_WINS"),
            "pc1": row.get("PC1"), "pc2": row.get("PC2"), "pc3": row.get("PC3"), "pc4": row.get("PC4"), "pc5": row.get("PC5"),
            "jw1": row.get("JW1"), "jw2": row.get("JW2"), "jw3": row.get("JW3"), "jw4": row.get("JW4"), "jw5": row.get("JW5"),
            "progression_contribution_paid": row.get("PROGRESSIVE_CONTRIBUTION_PAID"),
            "seed_money_won": row.get("SEED_MONEY_WON"),
            "seed_money_jackpot_over_1000": over,
            "table_room_id": str(row.get("OUTLET")) if row.get("OUTLET") is not None else None,
            "round_id": str(row.get("SESSION_ID")) if row.get("SESSION_ID") is not None else None,
        }

    def map_wallet(self, row: Dict[str, Any], kind: str) -> Optional[Dict[str, Any]]:
        login = clean_username(row.get("PLAYER_NAME"), remove_internal_spaces=False)
        reference_id = str(row.get("TRANSACTION_ID") or "").strip() or None
        if not login or not reference_id:
            return None
        created = self._dt(row.get("TRANSACTION_DATE")) or datetime.now(timezone.utc)
        raw_type = str(row.get("TRANSACTION_TYPE") or "unknown").strip().lower()
        tx_type = re.sub(r"\s+", "_", raw_type) or "unknown"
        raw_bank = row.get("BANK")
        bank = "" if raw_bank is None else str(raw_bank).strip()
        return {
            "kind": tx_type,
            "source_table": config.SOURCE_TABLES["deposits"],
            "source_id": row.get("IDX"),
            "reference_id": reference_id,
            "username": login,
            "amount": row.get("AMOUNT"),
            "status": "confirmed",
            "payment_gateway": bank,
            "domain": config.DEFAULT_DOMAIN,
            "created": created,
            "confirmed": created,
        }
