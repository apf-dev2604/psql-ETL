"""Startup validation for configs, adapters, and required runtime settings."""

import os
from typing import Any, Iterable, Optional

from .constants import REQUIRED_SOURCE_TABLE_KEYS


REQUIRED_CONFIG_ATTRS = (
    "BRAND_KEY",
    "BRAND",
    "PLATFORM",
    "WALLET_PLATFORM",
    "SOURCE_DB_NAME",
    "TARGET_DB_NAME",
    "SOURCE_SCHEMA",
    "TARGET_SCHEMA",
    "SOURCE_MODE",
    "SOURCE_TABLES",
    "PLAYER_TABLE",
    "GAME_TRANSACTION_TABLE",
    "WALLET_TRANSACTION_TABLE",
    "CHECKPOINT_TABLE",
    "BUSINESS_WINDOW_START_HOUR",
)

REQUIRED_ADAPTER_METHODS = (
    "as_dict",
    "source_date_expr",
    "source_created_value",
    "fetch_player_detail_map",
    "map_player",
    "map_game_transaction",
    "map_wallet",
)


def _missing_attrs(obj: Any, names: Iterable[str]) -> list[str]:
    return [name for name in names if not hasattr(obj, name) or getattr(obj, name) in (None, "")]


def validate_brand_config(config: Any) -> None:
    missing = _missing_attrs(config, REQUIRED_CONFIG_ATTRS)
    if missing:
        raise RuntimeError("Brand config is missing required setting(s): " + ", ".join(missing))

    missing_table_keys = [key for key in REQUIRED_SOURCE_TABLE_KEYS if key not in config.SOURCE_TABLES]
    if missing_table_keys:
        raise RuntimeError(
            f"Brand config {config.BRAND_KEY!r} SOURCE_TABLES missing key(s): "
            + ", ".join(missing_table_keys)
        )

    if config.SOURCE_MODE not in ("table_batch", "member_driven", "flat_table_batch"):
        raise RuntimeError(
            f"Unsupported SOURCE_MODE={config.SOURCE_MODE!r}; expected 'table_batch', 'member_driven', or 'flat_table_batch'."
        )


def validate_adapter(adapter: Any, source_mode: str) -> None:
    missing = _missing_attrs(adapter, REQUIRED_ADAPTER_METHODS)
    if source_mode == "member_driven":
        for name in ("fetch_member_game_rows", "fetch_member_wallet_rows"):
            if not hasattr(adapter, name):
                missing.append(name)
    if source_mode == "flat_table_batch":
        for name in ("fetch_player_rows", "fetch_game_rows", "fetch_wallet_rows"):
            if not hasattr(adapter, name):
                missing.append(name)
    if missing:
        raise RuntimeError("Brand adapter is missing required method(s): " + ", ".join(missing))


def validate_db_env(prefix: Optional[str] = None) -> None:
    """Validate DB environment variables without printing secrets.

    Prefixed values are checked first, then generic RDS_* values. Passwords are
    required but never logged.
    """

    def env(name: str) -> Optional[str]:
        prefixed = os.getenv(f"{prefix}_{name}") if prefix else None
        return prefixed if prefixed is not None else os.getenv(name)

    required = ("RDS_HOST", "RDS_USER", "RDS_PASSWORD")
    missing = [name for name in required if not env(name)]
    if missing:
        label = f"{prefix}_" if prefix else ""
        raise RuntimeError(
            f"Missing required database environment variable(s) for {label or 'generic'}connection: "
            + ", ".join(missing)
        )
