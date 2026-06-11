from typing import Any, Iterable, Optional

from psycopg2.extras import RealDictCursor, execute_values

from .db import table_ref


class DBManager:
    """Small database helper used by the multi-brand engine.

    It intentionally stays thin: brand-specific SQL/mapping stays in each brand
    adapter, while shared target table references and execute helpers live here.
    """

    def __init__(self, conn, schema: Optional[str] = None):
        self.conn = conn
        self.schema = schema

    def table(self, table_name: str, schema: Optional[str] = None) -> str:
        return table_ref(schema if schema is not None else self.schema, table_name)

    def fetchall(self, sql: str, params: Optional[Iterable[Any]] = None, dict_rows: bool = True):
        cursor_factory = RealDictCursor if dict_rows else None
        with self.conn.cursor(cursor_factory=cursor_factory) as cur:
            cur.execute(sql, list(params or []))
            return cur.fetchall()

    def fetchone(self, sql: str, params: Optional[Iterable[Any]] = None, dict_rows: bool = False):
        cursor_factory = RealDictCursor if dict_rows else None
        with self.conn.cursor(cursor_factory=cursor_factory) as cur:
            cur.execute(sql, list(params or []))
            return cur.fetchone()

    def execute(self, sql: str, params: Optional[Iterable[Any]] = None) -> int:
        with self.conn.cursor() as cur:
            cur.execute(sql, list(params or []))
            return cur.rowcount

    def bulk_insert_values(self, sql: str, values, page_size: int = 100, fetch: bool = False):
        with self.conn.cursor() as cur:
            return execute_values(cur, sql, values, page_size=page_size, fetch=fetch)
