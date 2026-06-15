import os
from datetime import timezone, timedelta

BRAND_KEY = "instaplay"
BRAND = "Instaplay"
PLATFORM = "Online"
WALLET_PLATFORM = "Instaplay"
DEFAULT_DOMAIN = "instaplay.com.ph"

SOURCE_DB_NAME = os.getenv("INSTAPLAY_SOURCE_DB", os.getenv("SOURCE_DB", "iestdl"))
TARGET_DB_NAME = os.getenv("TARGET_DB", "iestdbrds")
SOURCE_SCHEMA = os.getenv("INSTAPLAY_SOURCE_SCHEMA", os.getenv("SOURCE_SCHEMA", "public"))
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "migration_repair")

SOURCE_MODE = "member_driven"
SOURCE_TABLES = {
    "players": "PlayerRegistrations88Play",
    "player_detail": "PlayerDetail88Play",
    "game_transactions": "GameTransaction88Play",
    "deposits": "Deposits88Play",
    "withdrawals": "Withdrawals88Play",
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
DEFAULT_MOBILE = ""
DEFAULT_EMAIL = ""
DEFAULT_ADDRESS = ""
INSERT_PAGE_SIZE = 100
RECON_BATCH_SIZE = 1000
RECON_DETAIL_LIMIT = 500
RECON_MAX_SCAN_ROWS = 0
DQ_SAMPLE_LIMIT = 100
