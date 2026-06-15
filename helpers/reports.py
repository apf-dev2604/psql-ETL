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
    summary: str


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
    report_dir = os.getenv("MIGRATION_REPORT_DIR", "reports")
    log_dir = os.getenv("MIGRATION_LOG_DIR", "logs")
    return ReportPaths(
        log=os.path.join(log_dir, f"{base}_trace_{suffix}.log"),
        players=os.path.join(report_dir, f"{base}_players_{suffix}.csv"),
        game=os.path.join(report_dir, f"{base}_gameTransaction_{suffix}.csv"),
        deposits=os.path.join(report_dir, f"{base}_deposits_{suffix}.csv"),
        withdrawals=os.path.join(report_dir, f"{base}_withdrawals_{suffix}.csv"),
        reconciliation=os.path.join(report_dir, f"{base}_reconciliation_{suffix}.csv"),
        data_quality=os.path.join(report_dir, f"dataQuality_{base}_{dq_suffix}.csv"),
        summary=os.path.join(report_dir, f"{base}_runSummary_{suffix}.csv"),
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


DQ_FIELDS = ["phase", "mismatchColCount", "date-from", "date-to", "columnList"]


def ensure_csv_header(path: str, fieldnames: List[str]) -> None:
    """Create a CSV file with only headers when it does not yet exist."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if os.path.isfile(path):
        return
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()


def initialize_report_files(paths: ReportPaths) -> None:
    """Create all standard run CSV files at startup, even when there are zero report rows."""
    ensure_csv_header(paths.players, PHASE_FIELDS)
    ensure_csv_header(paths.game, PHASE_FIELDS)
    ensure_csv_header(paths.deposits, PHASE_FIELDS)
    ensure_csv_header(paths.withdrawals, PHASE_FIELDS)
    ensure_csv_header(paths.reconciliation, RECON_FIELDS)
    ensure_csv_header(paths.data_quality, DQ_FIELDS)
    ensure_csv_header(paths.summary, SUMMARY_FIELDS)


SUMMARY_FIELDS = [
    "timestamp",
    "brand",
    "phase",
    "sourceRows",
    "mappedRows",
    "insertedRows",
    "duplicateRows",
    "skippedRows",
    "missingPlayerRows",
    "missingUsernameRows",
    "missingRequiredRows",
    "mappingErrorRows",
    "insertErrorRows",
    "dryRun",
    "dateFrom",
    "dateTo",
    "notes",
]


def write_summary_report(path: str, **kwargs: Any) -> None:
    row = {name: short_text(kwargs.get(name), 500) for name in SUMMARY_FIELDS}
    row["timestamp"] = datetime.now(timezone.utc).isoformat()
    write_csv_row(path, SUMMARY_FIELDS, row)
