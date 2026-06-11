"""Typed models used by the migration engine.

The brand adapters may still return normal dictionaries so existing mapping logic
stays familiar. The engine coerces those dictionaries into these dataclasses at
module boundaries. That gives us better readability/type-safety without changing
business behavior.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone, timedelta
from types import ModuleType
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class BrandConfig:
    """Runtime configuration for one brand.

    This is built from brands/<brand>/config.py. It keeps production settings
    typed and validated while allowing brand configs to remain simple Python
    files.
    """

    BRAND_KEY: str
    BRAND: str
    PLATFORM: str
    WALLET_PLATFORM: str
    DEFAULT_DOMAIN: str
    SOURCE_DB_NAME: str
    TARGET_DB_NAME: str
    SOURCE_SCHEMA: str
    TARGET_SCHEMA: str
    SOURCE_MODE: str
    SOURCE_TABLES: Dict[str, str]
    PLAYER_TABLE: str
    GAME_TRANSACTION_TABLE: str
    WALLET_TRANSACTION_TABLE: str
    GAME_PROVIDER_TABLE: str
    GAME_TYPE_TABLE: str
    GAME_LIST_TABLE: str
    OUTLET_TABLE: str
    CHECKPOINT_TABLE: str
    BUSINESS_TZ: timezone = field(default_factory=lambda: timezone(timedelta(hours=8)))
    BUSINESS_TZ_NAME: str = "Asia/Manila"
    BUSINESS_WINDOW_START_HOUR: int = 6
    DEFAULT_MOBILE: str = "0000000000"
    DEFAULT_EMAIL: str = ""
    DEFAULT_ADDRESS: str = "N/A"
    INSERT_PAGE_SIZE: int = 1000
    RECON_BATCH_SIZE: int = 10000
    RECON_DETAIL_LIMIT: int = 5000
    RECON_MAX_SCAN_ROWS: int = 0
    DQ_SAMPLE_LIMIT: int = 500

    @classmethod
    def from_module(cls, module: ModuleType) -> "BrandConfig":
        def get(name: str, default: Any = None) -> Any:
            return getattr(module, name, default)

        return cls(
            BRAND_KEY=get("BRAND_KEY"),
            BRAND=get("BRAND"),
            PLATFORM=get("PLATFORM"),
            WALLET_PLATFORM=get("WALLET_PLATFORM"),
            DEFAULT_DOMAIN=get("DEFAULT_DOMAIN", get("BRAND")),
            SOURCE_DB_NAME=get("SOURCE_DB_NAME"),
            TARGET_DB_NAME=get("TARGET_DB_NAME"),
            SOURCE_SCHEMA=get("SOURCE_SCHEMA", "public"),
            TARGET_SCHEMA=get("TARGET_SCHEMA", "kemet"),
            SOURCE_MODE=get("SOURCE_MODE", "table_batch"),
            SOURCE_TABLES=dict(get("SOURCE_TABLES", {})),
            PLAYER_TABLE=get("PLAYER_TABLE", "playerDetails_final"),
            GAME_TRANSACTION_TABLE=get("GAME_TRANSACTION_TABLE", "gameTransaction_final"),
            WALLET_TRANSACTION_TABLE=get("WALLET_TRANSACTION_TABLE", "walletTransaction_final"),
            GAME_PROVIDER_TABLE=get("GAME_PROVIDER_TABLE", "gameProvider_final"),
            GAME_TYPE_TABLE=get("GAME_TYPE_TABLE", "gameType_final"),
            GAME_LIST_TABLE=get("GAME_LIST_TABLE", "gameList_final"),
            OUTLET_TABLE=get("OUTLET_TABLE", "outletList_final"),
            CHECKPOINT_TABLE=get("CHECKPOINT_TABLE", "migrationCheckpoint_dev"),
            BUSINESS_TZ=get("BUSINESS_TZ", timezone(timedelta(hours=8))),
            BUSINESS_TZ_NAME=get("BUSINESS_TZ_NAME", "Asia/Manila"),
            BUSINESS_WINDOW_START_HOUR=int(get("BUSINESS_WINDOW_START_HOUR", 6)),
            DEFAULT_MOBILE=get("DEFAULT_MOBILE", "0000000000"),
            DEFAULT_EMAIL=get("DEFAULT_EMAIL", ""),
            DEFAULT_ADDRESS=get("DEFAULT_ADDRESS", "N/A"),
            INSERT_PAGE_SIZE=int(get("INSERT_PAGE_SIZE", 1000)),
            RECON_BATCH_SIZE=int(get("RECON_BATCH_SIZE", 10000)),
            RECON_DETAIL_LIMIT=int(get("RECON_DETAIL_LIMIT", 5000)),
            RECON_MAX_SCAN_ROWS=int(get("RECON_MAX_SCAN_ROWS", 0)),
            DQ_SAMPLE_LIMIT=int(get("DQ_SAMPLE_LIMIT", 500)),
        )

    def checkpoint_key(self, phase: str) -> str:
        return f"{self.BRAND}_{phase}"


@dataclass
class NormalizedPlayer:
    source_id: Any = None
    username: str = ""
    external_id: Optional[str] = None
    first_name: str = ""
    middle_name: str = ""
    last_name: str = ""
    mobile_number: str = ""
    mobile_verified: bool = False
    email: str = ""
    email_verified: bool = False
    registration_date: Optional[datetime] = None
    registration_ip: Any = None
    is_verified: bool = False
    is_active: bool = False
    last_login: Optional[datetime] = None
    last_login_ip: Any = None
    outlet_code: Any = None
    address_street: str = ""
    address_barangay: str = ""
    address_city: str = ""
    address_province: str = ""
    income_source: str = ""
    industry: str = ""
    birthdate: Any = None
    wallet_balance: Any = "0"
    wallet_balance_datetime: Optional[datetime] = None

    @classmethod
    def from_mapping(cls, data: Dict[str, Any]) -> "NormalizedPlayer":
        allowed = cls.__dataclass_fields__.keys()
        return cls(**{k: v for k, v in dict(data or {}).items() if k in allowed})

    def to_mapping(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class NormalizedGameTransaction:
    source_id: Any = None
    external_id: str = ""
    username: str = ""
    member_external_id: Any = None
    player_id: Any = None
    provider_name: str = "UNKNOWN"
    game_name: str = "UNKNOWN"
    game_type: str = "SLOTS"
    created: Optional[datetime] = None
    settled: Optional[datetime] = None
    bet_amount: Any = "0"
    valid_bet: Any = None
    payout_amount: Any = "0"
    jackpot_contribution: Any = "0"
    jackpot_payout: Any = "0"
    pc1: Any = "0"
    pc2: Any = "0"
    pc3: Any = "0"
    pc4: Any = "0"
    pc5: Any = None
    jw1: Any = "0"
    jw2: Any = "0"
    jw3: Any = "0"
    jw4: Any = "0"
    jw5: Any = None
    progression_contribution_paid: Any = "0"
    seed_money_won: Any = "0"
    seed_money_jackpot_over_1000: Any = 0
    table_room_id: Any = None
    round_id: Any = None
    parlay: bool = False
    bet_details: Any = None
    bet_timing: Any = None

    @classmethod
    def from_mapping(cls, data: Dict[str, Any]) -> "NormalizedGameTransaction":
        allowed = cls.__dataclass_fields__.keys()
        return cls(**{k: v for k, v in dict(data or {}).items() if k in allowed})

    def to_mapping(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class NormalizedWalletTransaction:
    kind: str = ""
    source_table: str = ""
    source_id: Any = None
    reference_id: str = ""
    username: str = ""
    member_external_id: Any = None
    player_id: Any = None
    amount: Any = "0"
    status: str = ""
    payment_gateway: str = ""
    domain: str = ""
    created: Optional[datetime] = None
    confirmed: Optional[datetime] = None

    @classmethod
    def from_mapping(cls, data: Dict[str, Any]) -> "NormalizedWalletTransaction":
        allowed = cls.__dataclass_fields__.keys()
        return cls(**{k: v for k, v in dict(data or {}).items() if k in allowed})

    def to_mapping(self) -> Dict[str, Any]:
        return asdict(self)
