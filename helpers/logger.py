"""Structured logging helper for migration runs."""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def setup_logger(path: str, level: int = logging.INFO) -> logging.Logger:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    logger = logging.getLogger("migration_engine")
    logger.setLevel(level)
    logger.handlers.clear()

    file_handler = logging.FileHandler(path, mode="a", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger


def event(message: str, *, level: int = logging.INFO, **fields: Any) -> None:
    payload: Optional[str] = None
    if fields:
        safe: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **fields,
        }
        try:
            payload = json.dumps(safe, ensure_ascii=False, default=str)
        except Exception:
            payload = str(safe)

    line = message if payload is None else f"{message} | {payload}"
    print(line, flush=True)
    logging.getLogger("migration_engine").log(level, line)
