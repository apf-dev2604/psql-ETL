"""Shared constants for source-table keys, phase names, and default target-table names.

These constants keep string keys consistent across brand configs, adapters, and
shared migration helpers. Brand-specific values still live in brands/<brand>/config.py.
"""

SOURCE_PLAYERS = "players"
SOURCE_PLAYER_DETAIL = "player_detail"
SOURCE_GAME_TRANSACTIONS = "game_transactions"
SOURCE_DEPOSITS = "deposits"
SOURCE_WITHDRAWALS = "withdrawals"

PHASE_PLAYERS = "players"
PHASE_GAME_TRANSACTIONS = "game_transactions"
PHASE_DEPOSITS = "deposits"
PHASE_WITHDRAWALS = "withdrawals"

WALLET_DEPOSIT = "deposit"
WALLET_WITHDRAWAL = "withdrawal"

DEFAULT_TARGET_PLAYER_TABLE = "playerDetails_final"
DEFAULT_TARGET_GAME_TRANSACTION_TABLE = "gameTransaction_final"
DEFAULT_TARGET_WALLET_TRANSACTION_TABLE = "walletTransaction_final"
DEFAULT_TARGET_GAME_PROVIDER_TABLE = "gameProvider_final"
DEFAULT_TARGET_GAME_TYPE_TABLE = "gameType_final"
DEFAULT_TARGET_GAME_LIST_TABLE = "gameList_final"
DEFAULT_TARGET_OUTLET_TABLE = "outletList_final"
DEFAULT_CHECKPOINT_TABLE = "migrationCheckpoint_dev"

REQUIRED_SOURCE_TABLE_KEYS = (
    SOURCE_PLAYERS,
    SOURCE_GAME_TRANSACTIONS,
    SOURCE_DEPOSITS,
    SOURCE_WITHDRAWALS,
)
