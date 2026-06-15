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
    error_summary: str


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
        error_summary=os.path.join(report_dir, f"{base}_errorSummary_{suffix}.csv"),
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
    "sqlstate",
    "constraintName",
    "tableName",
    "columnName",
    "messageDetail",
    "messageHint",
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
    row = {}
    for name in PHASE_FIELDS:
        limit = 2000 if name in ("error", "reason") else 500
        row[name] = short_text(kwargs.get(name), limit)
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
    ensure_csv_header(paths.error_summary, ERROR_SUMMARY_FIELDS)


SUMMARY_FIELDS = [
    "timestamp",
    "brand",
    "phase",
    "sourceRows",
    "mappedRows",
    "insertAttemptRows",
    "insertedRows",
    "updatedRows",
    "duplicateSkippedRows",
    "skippedRows",
    "missingPlayerRows",
    "missingUsernameRows",
    "missingRequiredRows",
    "mappingErrorRows",
    "insertErrorRows",
    "totalErrorRows",
    "dataBatches",
    "dryRun",
    "dateFrom",
    "dateTo",
    "notes",
]


def write_summary_report(path: str, **kwargs: Any) -> None:
    row = {name: short_text(kwargs.get(name), 500) for name in SUMMARY_FIELDS}
    row["timestamp"] = datetime.now(timezone.utc).isoformat()
    write_csv_row(path, SUMMARY_FIELDS, row)


ERROR_SUMMARY_FIELDS = [
    "timestamp",
    "brand",
    "phase",
    "category",
    "rows",
    "issueType",
    "action",
    "sqlstate",
    "constraintName",
    "tableName",
    "columnName",
    "messageDetail",
    "reason",
    "error",
    "csvPath",
]

_UUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


def redact_ids(value: Any) -> str:
    """Keep final summaries readable and groupable without leaking thousands of UUID variants."""
    return _UUID_RE.sub("<uuid>", short_text(value, 1000))


def classify_report_issue(row: Dict[str, Any]) -> str:
    issue = str(row.get("issueType") or "").lower()
    action = str(row.get("action") or "").lower()
    reason = str(row.get("reason") or "").lower()
    detail = str(row.get("messageDetail") or "").lower()
    error = str(row.get("error") or "").lower()
    sqlstate = str(row.get("sqlstate") or "")
    combined = " ".join([issue, action, reason, detail, error])

    if "missing_username" in combined or "member.name" in combined or "missing username" in combined:
        return "missingUsername"
    if (
        "player_not_in_player_map" in combined
        or "no playerrecord" in combined
        or "player was not found" in combined
        or (sqlstate == "23503" and "playerid" in combined and "not present" in combined)
    ):
        return "missingTargetPlayer"
    if (
        "missing_required" in combined
        or "missing_required_field" in combined
        or "missing_external_id" in combined
        or "missing_reference_id" in combined
        or "missing_source_id" in combined
        or "missing or invalid" in combined
    ):
        return "missingRequired"
    if "mapping" in combined or "map error" in combined or "invalid_amount" in combined:
        return "mappingOrValidationError"
    if sqlstate.startswith("23"):
        return "databaseConstraint"
    if sqlstate:
        return "databaseError"
    if "insert" in action or "insert" in combined or "upsert" in combined:
        return "databaseInsertError"
    if "skipped" in action or "skipped" in combined:
        return "skippedOther"
    return "otherError"


def phase_name_from_path(path: str) -> str:
    base = os.path.basename(path or "").lower()
    if "player" in base and "gametransaction" not in base:
        return "players"
    if "gametransaction" in base:
        return "game_transactions"
    if "deposit" in base:
        return "deposits"
    if "withdrawal" in base:
        return "withdrawals"
    return "unknown"


def _count_summary_csv(summary_path: str) -> Dict[str, Dict[str, int]]:
    """Read the run summary CSV and pull high-level phase counts.

    This gives final summary coverage even for pre-insert skips whose row details
    were intentionally not verbose on screen.
    """
    out: Dict[str, Dict[str, int]] = {}
    if not summary_path or not os.path.isfile(summary_path):
        return out
    try:
        with open(summary_path, "r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                phase = row.get("phase") or "unknown"
                bucket = out.setdefault(phase, {})
                for key in (
                    "missingPlayerRows",
                    "missingUsernameRows",
                    "missingRequiredRows",
                    "mappingErrorRows",
                    "insertErrorRows",
                    "totalErrorRows",
                ):
                    try:
                        bucket[key] = bucket.get(key, 0) + int(row.get(key) or 0)
                    except Exception:
                        pass
    except Exception as exc:
        trace(f"[FINAL ERROR SUMMARY WARN] Could not read summary CSV {summary_path}: {exc}")
    return out


def _phase_report_rows(paths: ReportPaths) -> List[tuple[str, str]]:
    return [
        ("players", paths.players),
        ("game_transactions", paths.game),
        ("deposits", paths.deposits),
        ("withdrawals", paths.withdrawals),
    ]


def emit_final_error_summary(brand: str, paths: ReportPaths, top_n: int = 10) -> None:
    """Print and write an end-of-run descriptive error summary.

    Low-memory design: scan generated CSV files row-by-row at the end and keep
    only grouped counters. Exact row-level errors stay in phase CSVs.
    """
    from collections import Counter

    summary_counts = _count_summary_csv(paths.summary)
    detail_counts: Counter = Counter()
    category_counts: Counter = Counter()
    phase_counts: Counter = Counter()

    for phase, csv_path in _phase_report_rows(paths):
        if not csv_path or not os.path.isfile(csv_path):
            continue
        try:
            with open(csv_path, "r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    issue_type = row.get("issueType") or ""
                    action = row.get("action") or ""
                    sqlstate = row.get("sqlstate") or ""
                    # Ignore empty/header-only files and duplicate info rows in final error summary.
                    if not any(row.get(k) for k in ("issueType", "action", "reason", "error", "sqlstate")):
                        continue
                    if "duplicate" in issue_type.lower() or "duplicate" in action.lower():
                        continue
                    category = classify_report_issue(row)
                    category_counts[category] += 1
                    phase_counts[phase] += 1
                    key = (
                        phase,
                        category,
                        issue_type or "unknown_issue",
                        action or "unknown_action",
                        sqlstate or "no_sqlstate",
                        row.get("constraintName") or "no_constraint",
                        row.get("tableName") or "no_table",
                        row.get("columnName") or "no_column",
                        redact_ids(row.get("messageDetail") or row.get("reason") or row.get("error") or ""),
                        csv_path,
                    )
                    detail_counts[key] += 1
        except Exception as exc:
            trace(f"[FINAL ERROR SUMMARY WARN] Could not read phase CSV {csv_path}: {exc}")

    # Add high-level phase counts from the run summary. Do not double-count rows
    # already represented by phase CSV details for insert errors, but do surface
    # missing/mapping categories clearly even when row-level CSV is sparse.
    rollup_lines = []
    total_error_rows = 0
    for phase, counts in summary_counts.items():
        total = int(counts.get("totalErrorRows") or 0)
        if total:
            total_error_rows += total
            rollup_lines.append((phase, counts))

    ensure_csv_header(paths.error_summary, ERROR_SUMMARY_FIELDS)
    # Rewrite aggregate error summary fresh for this run, preserving header.
    try:
        with open(paths.error_summary, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=ERROR_SUMMARY_FIELDS, extrasaction="ignore")
            writer.writeheader()
            for (phase, category, issue_type, action, sqlstate, constraint, table, column, detail, csv_path), rows in detail_counts.most_common():
                writer.writerow({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "brand": brand,
                    "phase": phase,
                    "category": category,
                    "rows": rows,
                    "issueType": issue_type,
                    "action": action,
                    "sqlstate": sqlstate,
                    "constraintName": constraint,
                    "tableName": table,
                    "columnName": column,
                    "messageDetail": detail,
                    "reason": detail,
                    "error": detail,
                    "csvPath": csv_path,
                })
    except Exception as exc:
        trace(f"[FINAL ERROR SUMMARY WARN] Could not write error summary CSV {paths.error_summary}: {exc}")

    lines = [
        f"[FINAL ERROR SUMMARY][{brand}]",
        f"  totalErrorRows       : {total_error_rows}",
        f"  errorSummaryCsv      : {paths.error_summary}",
        "  byPhase:",
    ]
    if rollup_lines:
        for phase, counts in rollup_lines:
            lines.extend([
                f"    {phase}:",
                f"      totalErrorRows      : {int(counts.get('totalErrorRows') or 0)}",
                f"      missingTargetPlayer : {int(counts.get('missingPlayerRows') or 0)}",
                f"      missingUsername     : {int(counts.get('missingUsernameRows') or 0)}",
                f"      missingRequired     : {int(counts.get('missingRequiredRows') or 0)}",
                f"      mappingErrors       : {int(counts.get('mappingErrorRows') or 0)}",
                f"      insertErrors        : {int(counts.get('insertErrorRows') or 0)}",
            ])
    else:
        lines.append("    none")

    lines.append("  byCategoryFromCsv:")
    if category_counts:
        for category, count in category_counts.most_common():
            lines.append(f"    {category:<24}: {count}")
    else:
        lines.append("    none")

    lines.append("  topDetailedErrors:")
    if detail_counts:
        for idx, ((phase, category, issue_type, action, sqlstate, constraint, table, column, detail, csv_path), rows) in enumerate(detail_counts.most_common(top_n), start=1):
            lines.extend([
                f"    {idx}.",
                f"      rows       : {rows}",
                f"      phase      : {phase}",
                f"      category   : {category}",
                f"      issueType  : {issue_type}",
                f"      action     : {action}",
                f"      sqlstate   : {sqlstate}",
                f"      constraint : {constraint}",
                f"      table      : {table}",
                f"      column     : {column}",
                f"      detail     : {detail}",
                f"      csv        : {csv_path}",
            ])
    else:
        lines.append("    none")

    trace("\n".join(lines))
