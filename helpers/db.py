import os
import socket
from typing import Optional

import psycopg2
from psycopg2.extras import register_uuid

from .validators import validate_db_env


def probe_host_port(host: str, port: int, timeout_sec: int = 5) -> None:
    print(f"Probing TCP {host}:{port} ...", flush=True)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout_sec)
    try:
        sock.connect((host, port))
        print("TCP reachable", flush=True)
    except Exception as exc:
        print(f"TCP probe failed: {exc}", flush=True)
    finally:
        try:
            sock.close()
        except Exception:
            pass


def env_name(prefix: Optional[str], key: str) -> str:
    return f"{prefix}_{key}" if prefix else key


def connect(dbname: str, env_prefix: Optional[str] = None):
    """Create a PostgreSQL connection using environment-based secrets.

    Passwords are never stored in config.py and are never printed. For separate
    source/target credentials, set SOURCE_RDS_* and TARGET_RDS_* values. If a
    prefixed value is absent, the generic RDS_* value is used.
    """

    validate_db_env(env_prefix)

    def get(name: str, default: str = "") -> str:
        prefixed = os.getenv(env_name(env_prefix, name)) if env_prefix else None
        return prefixed if prefixed is not None else os.getenv(name, default)

    # Host has no secret value, but it is still required by validate_db_env.
    host = get("RDS_HOST")
    user = get("RDS_USER")
    password = get("RDS_PASSWORD")
    port = int(get("RDS_PORT", "5432"))
    sslmode = get("RDS_SSLMODE", "require")

    probe_host_port(host, port, timeout_sec=5)
    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        port=port,
        dbname=dbname,
        connect_timeout=45,
        sslmode=sslmode,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    register_uuid(conn)
    conn.autocommit = False
    return conn


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def table_ref(schema: Optional[str], table: str) -> str:
    if schema:
        return f"{quote_ident(schema)}.{quote_ident(table)}"
    return quote_ident(table)
