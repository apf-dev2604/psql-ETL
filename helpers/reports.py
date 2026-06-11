import csv
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from .logger import setup_logger, event
from typing import Any, Dict, Iterable, List, Optional


def configure_logging(path: str) -> None:
    setup_logger(path)


def trace(message: str, level: int = 20, **fields: Any) -> None:
    event(message, level=level, **fields)


def short_text(value: Any, limit: int = 300) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text if len(text) <= limit else text[: max(0, limit - 3)] + "..."


def write_csv_row(path: str, fieldnames: List[str], row: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    exists = os.path.isfile(path)
    with open(path, "a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow(row)


@dataclass
class ReportPaths:
    log: str
    players: str
    game: str
    deposits: str
    withdrawals: str
    reconciliation: str
    data_quality: str


def make_report_paths(brand_key: str, date_from, date_to, timestamp: Optional[str] = None) -> ReportPaths:
    timestamp = timestamp or datetime.now().strftime("%Y%m%d%H%M%S")

    def part(value, fallback):
        return value.strftime("%Y%m%d") if value else fallback

    if date_from is None and date_to is None:
        suffix = f"full_rundate_{timestamp}"
        dq_suffix = suffix
    else:
        suffix = f"{part(date_from, '00000000')}_{part(date_to, '99999999')}-rundate_{timestamp}"
        dq_suffix = f"{part(date_from, '00000000')}-{part(date_to, '99999999')}-rundate_{timestamp}"

    base = brand_key.lower()
    return ReportPaths(
        log=f"logs/{base}_trace_{suffix}.log",
        players=f"reports/{base}_players_{suffix}.csv",
        game=f"reports/{base}_gameTransaction_{suffix}.csv",
        deposits=f"reports/{base}_deposits_{suffix}.csv",
        withdrawals=f"reports/{base}_withdrawals_{suffix}.csv",
        reconciliation=f"reports/{base}_reconciliation_{suffix}.csv",
        data_quality=f"reports/dataQuality_{base}_{dq_suffix}.csv",
    )


PHASE_FIELDS = [
    "issueType",
    "sourceTable",
    "sourceUsername",
    "sourceId",
    "referenceId",
    "targetId",
    "targetPlayerId",
    "targetUsername",
    "action",
    "reason",
    "error",
]


RECON_FIELDS = [
    "checkName",
    "recordType",
    "status",
    "sourceTable",
    "sourceId",
    "sourceUsername",
    "sourceExternalId",
    "targetTable",
    "targetId",
    "targetUsername",
    "targetExternalId",
    "referenceType",
    "referenceValue",
    "metric",
    "value",
    "reason",
    "notes",
    "timestamp",
]


def write_phase_report(path: str, **kwargs: Any) -> None:
    row = {name: short_text(kwargs.get(name), 300) for name in PHASE_FIELDS}
    write_csv_row(path, PHASE_FIELDS, row)


def write_reconciliation(path: str, **kwargs: Any) -> None:
    row = {name: short_text(kwargs.get(name), 500) for name in RECON_FIELDS}
    row["timestamp"] = datetime.now(timezone.utc).isoformat()
    write_csv_row(path, RECON_FIELDS, row)
