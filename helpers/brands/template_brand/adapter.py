"""Template adapter for adding another brand.

Copy this folder to brands/<new_brand>, update config.py, and implement mappings.
"""
from typing import Any, Dict, Optional

from helpers.players import as_dict
from . import config


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

    def fetch_player_detail_map(self, src_conn):
        return {}

    def map_player(self, row, detail_map, **kwargs):
        raise NotImplementedError

    def map_game_transaction(self, row):
        raise NotImplementedError

    def map_wallet(self, row, kind):
        raise NotImplementedError
