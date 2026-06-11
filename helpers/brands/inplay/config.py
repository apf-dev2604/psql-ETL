import os
from datetime import timezone, timedelta

BRAND_KEY = "inplay"
BRAND = "Inplay"
PLATFORM = "Online"
WALLET_PLATFORM = "Inplay"
DEFAULT_DOMAIN = BRAND

SOURCE_DB_NAME = os.getenv("INPLAY_SOURCE_DB", "iestdl")
TARGET_DB_NAME = os.getenv("TARGET_DB", "iestdbrds")
SOURCE_SCHEMA = os.getenv("INPLAY_SOURCE_SCHEMA", "public")
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "migration_repair")

SOURCE_MODE = "table_batch"
SOURCE_TABLES = {
    "players": "PlayerRegistrationsInplayV2",
    "player_detail": "PlayerDetailInplayV2",
    "game_transactions": "GameTransactionInplayV2",
    "deposits": "DepositsInplayV2",
    "withdrawals": "WithdrawalsInplayV2",
}

PLAYER_TABLE = "playerDetails_final"
GAME_TRANSACTION_TABLE = "gameTransaction_final"
WALLET_TRANSACTION_TABLE = "walletTransaction_final"
GAME_PROVIDER_TABLE = "gameProvider_final"
GAME_TYPE_TABLE = "gameType_final"
GAME_LIST_TABLE = "gameList_final"
OUTLET_TABLE = "outletList_final"
CHECKPOINT_TABLE = "migrationCheckpoint"

BUSINESS_TZ = timezone(timedelta(hours=8))
BUSINESS_TZ_NAME = "Asia/Manila"
BUSINESS_WINDOW_START_HOUR = 6

DEFAULT_MOBILE = "0000000000"
DEFAULT_EMAIL = ""
DEFAULT_ADDRESS = "N/A"
INSERT_PAGE_SIZE = 100
RECON_BATCH_SIZE = 1000
RECON_DETAIL_LIMIT = 500
RECON_MAX_SCAN_ROWS = 0
DQ_SAMPLE_LIMIT = 100
