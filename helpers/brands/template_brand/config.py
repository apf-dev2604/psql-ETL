from datetime import timezone, timedelta

BRAND_KEY = "brand_b"
BRAND = "BrandB"
PLATFORM = "Online"
WALLET_PLATFORM = "BrandB"
DEFAULT_DOMAIN = BRAND

SOURCE_DB_NAME = "iestdl"
TARGET_DB_NAME = "iestdl"
SOURCE_SCHEMA = "public"
TARGET_SCHEMA = "kemet"
SOURCE_MODE = "table_batch"  # or "member_driven"

SOURCE_TABLES = {
    "players": "PlayerRegistrationsBrandB",
    "player_detail": "PlayerDetailBrandB",
    "game_transactions": "GameTransactionBrandB",
    "deposits": "DepositsBrandB",
    "withdrawals": "WithdrawalsBrandB",
}

PLAYER_TABLE = "playerDetails_final"
GAME_TRANSACTION_TABLE = "gameTransaction_final"
WALLET_TRANSACTION_TABLE = "walletTransaction_final"
GAME_PROVIDER_TABLE = "gameProvider_final"
GAME_TYPE_TABLE = "gameType_final"
GAME_LIST_TABLE = "gameList_final"
OUTLET_TABLE = "outletList_final"
CHECKPOINT_TABLE = "migrationCheckpoint_dev"

BUSINESS_TZ = timezone(timedelta(hours=8))
BUSINESS_TZ_NAME = "Asia/Manila"
BUSINESS_WINDOW_START_HOUR = 6

DEFAULT_MOBILE = "0000000000"
DEFAULT_EMAIL = ""
DEFAULT_ADDRESS = "N/A"
INSERT_PAGE_SIZE = 1000


def checkpoint_key(phase: str) -> str:
    return f"{BRAND}_{phase}"

# Reconciliation/DQ controls
RECON_BATCH_SIZE = 10000
RECON_DETAIL_LIMIT = 5000
RECON_MAX_SCAN_ROWS = 0  # 0 = scan full date-window until detail limit reached
DQ_SAMPLE_LIMIT = 500
