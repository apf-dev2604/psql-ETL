import os
import json
import re
import uuid
import argparse
import socket
import time
import csv
import logging
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from utilities.mailer import send_migration_reports

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values, register_uuid

# ----------------------------
# Globals and Dynamic File Identifiers
# ----------------------------
BRAND = "Inplay"  # playerDetails.brandName + gameTransaction.brand
PLATFORM = "Online"  # gameTransaction.platform
WALLET_PLATFORM = "Inplay"  # walletTransaction.platform

# Shared timestamp string used to synchronize runtime outputs
TIMESTAMP_STR = datetime.now().strftime("%Y%m%d%H%M%S")

# Dynamic Filepath Definitions
LOG_FILE_PATH = f"logs/migration_trace_{TIMESTAMP_STR}.log"
CSV_GAMETX_PATH = f"reports/gameTransaction_{TIMESTAMP_STR}.csv"
CSV_DEPOSITS_PATH = f"reports/deposits_{TIMESTAMP_STR}.csv"
CSV_WITHDRAWALS_PATH = f"reports/withdrawals_{TIMESTAMP_STR}.csv"
CSV_PLAYERS_PATH = f"reports/players_{TIMESTAMP_STR}.csv"
CSV_RECONCILIATION_PATH = f"reports/reconciliation_{TIMESTAMP_STR}.csv"
CSV_DATA_QUALITY_PATH = f"reports/data_quality_{TIMESTAMP_STR}.csv"

# Zip report attachments when the CSV payload reaches the mail-safe threshold.
REPORT_ZIP_THRESHOLD_BYTES = 17 * 1024 * 1024

# Source table names for the V1_dev migration pipeline.
# Only PLAYER_REGISTRATION_SOURCE_TABLE is allowed to create playerDetails_final rows.
PLAYER_REGISTRATION_SOURCE_TABLE = "PlayerRegistrationsInplayV1_dev"
GAME_TRANSACTION_SOURCE_TABLE = "GameTransactionInplayV1_dev"
DEPOSITS_SOURCE_TABLE = "DepositsInplayV1_dev"
WITHDRAWALS_SOURCE_TABLE = "WithdrawalsInplayV1_dev"

# Phase-specific source date fields used by --date-from/--date-to and checkpoints.
# These are JSONB keys inside the source table `data` column.
SOURCE_DATE_KEYS_BY_TABLE = {
    PLAYER_REGISTRATION_SOURCE_TABLE: ("createddate",),
    GAME_TRANSACTION_SOURCE_TABLE: ("GameDate",),
    DEPOSITS_SOURCE_TABLE: ("transferDate",),
    WITHDRAWALS_SOURCE_TABLE: ("transferDate",),
}

# Set to False only if the exact source SELECT output becomes too verbose.
PRINT_SOURCE_SQL = True

# Source SELECTs default to table-only names so the emitted SQL can be copied
# directly into psql. REVERT NOTE: set SOURCE_SCHEMA=kemet in the environment
# if your source tables require the previous schema-qualified form.
SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "").strip()

# Keeps the last source SELECT per phase/table so it can be printed once at the
# end of each phase instead of spamming the screen inside batch loops.
SOURCE_QUERY_AUDIT: Dict[str, Dict[str, Any]] = {}

# Runtime counters for player report CSV rows. These distinguish true skips/failures
# from informational/corrective rows such as email normalization.
PLAYER_REPORT_COUNTS: Dict[str, int] = {}
PLAYER_REPORT_ISSUE_COUNTS: Dict[str, int] = {}

# Runtime counters for non-player phases. These are separate from true skips.
# Duplicates here are successful/no-op conflict outcomes such as
# gameTransaction.externalId or walletTransaction(platform, referenceId) already existing.
PHASE_REPORT_COUNTS: Dict[str, Dict[str, int]] = {}
PHASE_REPORT_ISSUE_COUNTS: Dict[str, Dict[str, int]] = {}

# Reconciliation summaries are populated by run_player_reconciliation_checks()
# and used in both the trace log and email body.
RECONCILIATION_SUMMARY_LINES: List[str] = []

# DATA QUALITY CHECKER NOTE 2026-05-21:
# These summaries are populated by run_post_migration_data_quality_checks().
# This is intentionally separate from reconciliation so column-level mismatches
# can be reported without changing migration/write behavior.
DATA_QUALITY_SUMMARY_LINES: List[str] = []

def trace_print(message: str, level: int = logging.INFO) -> None:
    """Print a run message and mirror it to the migration trace log."""
    print(message, flush=True)
    try:
        logging.log(level, message)
    except Exception:
        pass

def _counter_inc(counter: Dict[str, int], key: Optional[str], amount: int = 1) -> None:
    k = str(key or "unknown")
    counter[k] = int(counter.get(k) or 0) + amount

def player_report_total() -> int:
    return sum(int(v or 0) for v in PLAYER_REPORT_COUNTS.values())

def player_report_count(action: str) -> int:
    return int(PLAYER_REPORT_COUNTS.get(action) or 0)

def player_report_issue_count(issue_type: str) -> int:
    return int(PLAYER_REPORT_ISSUE_COUNTS.get(issue_type) or 0)

def player_report_fixed_total() -> int:
    """Rows corrected in-flight and still allowed to continue; not true skips."""
    return player_report_count("email_normalized") + player_report_count("email_corrected")

def player_report_duplicate_total() -> int:
    """Rows that hit ON CONFLICT and were updated instead of inserted; not true skips."""
    return player_report_count("duplicate_key_upserted")

def player_report_failure_total() -> int:
    """Rows reported because they failed or were skipped, excluding in-flight fixes and duplicate updates."""
    return max(0, player_report_total() - player_report_fixed_total() - player_report_duplicate_total())

def player_report_counts_text() -> str:
    if not PLAYER_REPORT_COUNTS:
        return "none"
    return ", ".join(f"{k}={PLAYER_REPORT_COUNTS[k]}" for k in sorted(PLAYER_REPORT_COUNTS))

def player_report_issue_counts_text() -> str:
    if not PLAYER_REPORT_ISSUE_COUNTS:
        return "none"
    return ", ".join(f"{k}={PLAYER_REPORT_ISSUE_COUNTS[k]}" for k in sorted(PLAYER_REPORT_ISSUE_COUNTS))

def phase_report_count(phase: str, action: str) -> int:
    return int((PHASE_REPORT_COUNTS.get(phase) or {}).get(action) or 0)

def phase_report_total(phase: str) -> int:
    return sum(int(v or 0) for v in (PHASE_REPORT_COUNTS.get(phase) or {}).values())

def phase_duplicate_total(phase: str) -> int:
    return phase_report_count(phase, "duplicate_key_ignored")

def phase_report_counts_text(phase: str) -> str:
    counts = PHASE_REPORT_COUNTS.get(phase) or {}
    if not counts:
        return "none"
    return ", ".join(f"{k}={counts[k]}" for k in sorted(counts))

def phase_report_issue_counts_text(phase: str) -> str:
    counts = PHASE_REPORT_ISSUE_COUNTS.get(phase) or {}
    if not counts:
        return "none"
    return ", ".join(f"{k}={counts[k]}" for k in sorted(counts))

def all_phase_duplicate_total() -> int:
    phases = ("gameTransaction", "walletTransaction.deposit", "walletTransaction.withdrawal")
    return sum(phase_duplicate_total(phase) for phase in phases)

def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'

def source_table_ref(table: str) -> str:
    if SOURCE_SCHEMA:
        return f"{quote_ident(SOURCE_SCHEMA)}.{quote_ident(table)}"
    return quote_ident(table)

# ----------------------------
# Portable Production CSV Writer
# ----------------------------
def write_skipped_to_csv(filepath: str, fieldnames: List[str], row_data: Dict[str, Any]) -> None:
    """
    Robust, production-grade portable CSV writer utility.
    Ensures safe multi-record ingestion across all pipeline transaction functions.
    """
    try:
        dir_name = os.path.dirname(filepath)
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)

        file_exists = os.path.isfile(filepath)
        with open(filepath, mode="a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row_data)
    except Exception as e:
        print(f"[ERROR] Dynamically tracking skipped account data to CSV failed: {e}", flush=True)




RECONCILIATION_FIELDNAMES = [
    # Compact investigation columns first. These are the fields intended for
    # day-to-day manual checking and source-to-target tracing.
    "checkName", "recordType", "status",
    "sourceId", "sourceUsername", "sourceCardId",
    "targetId", "playerId", "targetUsername", "targetExternalId",
    "sourceReferenceType", "sourceReferenceValue",
    "targetReferenceType", "targetReferenceValue",
    "metric", "value", "reason", "notes",

    # Extended trace columns kept for duplicate/case/normalization analysis.
    "sourceTable", "targetTable",
    "sourceUsernameNormalized", "targetUsernameNormalized",
    "sourceDuplicateCount", "targetDuplicateCount",
    "timestamp",
]


def write_reconciliation_row(check_name: str, metric: str, value: Any, notes: str = "") -> None:
    """Append one reconciliation metric row to the reconciliation CSV report."""
    write_skipped_to_csv(
        filepath=CSV_RECONCILIATION_PATH,
        fieldnames=RECONCILIATION_FIELDNAMES,
        row_data={
            "checkName": check_name,
            "recordType": "summary",
            "status": "summary",
            "sourceId": "",
            "sourceUsername": "",
            "sourceCardId": "",
            "targetId": "",
            "playerId": "",
            "targetUsername": "",
            "targetExternalId": "",
            "sourceReferenceType": "",
            "sourceReferenceValue": "",
            "targetReferenceType": "",
            "targetReferenceValue": "",
            "metric": metric,
            "value": value if value is not None else "",
            "reason": "",
            "notes": notes or "",
            "sourceTable": "",
            "targetTable": "",
            "sourceUsernameNormalized": "",
            "targetUsernameNormalized": "",
            "sourceDuplicateCount": "",
            "targetDuplicateCount": "",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )


def write_reconciliation_trace_row(
        check_name: str,
        status: str,
        source_table: str = "",
        source_id: Any = "",
        source_username: Any = "",
        source_username_normalized: Any = "",
        source_card_id: Any = "",
        source_duplicate_count: Any = "",
        source_reference_type: Any = "",
        source_reference_value: Any = "",
        target_table: str = "",
        target_id: Any = "",
        target_username: Any = "",
        target_username_normalized: Any = "",
        target_external_id: Any = "",
        target_duplicate_count: Any = "",
        target_reference_type: Any = "",
        target_reference_value: Any = "",
        reason: str = "",
        notes: str = "",
        metric: str = "detail",
        value: Any = "",
) -> None:
    """Append one traceable reconciliation detail row with source/target IDs and usernames."""
    write_skipped_to_csv(
        filepath=CSV_RECONCILIATION_PATH,
        fieldnames=RECONCILIATION_FIELDNAMES,
        row_data={
            "checkName": check_name,
            "recordType": "detail",
            "status": status or "detail",
            "sourceId": source_id or "",
            "sourceUsername": source_username or "",
            "sourceCardId": source_card_id or "",
            "targetId": target_id or "",
            "playerId": target_id or "",
            "targetUsername": target_username or "",
            "targetExternalId": target_external_id or "",
            "sourceReferenceType": source_reference_type or "",
            "sourceReferenceValue": source_reference_value or "",
            "targetReferenceType": target_reference_type or "",
            "targetReferenceValue": target_reference_value or "",
            "metric": metric,
            "value": value if value is not None else "",
            "reason": reason or "",
            "notes": notes or "",
            "sourceTable": source_table or "",
            "targetTable": target_table or "",
            "sourceUsernameNormalized": source_username_normalized or "",
            "targetUsernameNormalized": target_username_normalized or "",
            "sourceDuplicateCount": source_duplicate_count or "",
            "targetDuplicateCount": target_duplicate_count or "",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )


def add_reconciliation_summary(line: str) -> None:
    """Store and trace one human-readable reconciliation summary line."""
    RECONCILIATION_SUMMARY_LINES.append(line)
    trace_print(line)


def reconciliation_email_summary() -> str:
    """Return a compact reconciliation section for the notification email."""
    if not RECONCILIATION_SUMMARY_LINES:
        return "Reconciliation Summary:\n- Reconciliation checks were not executed for this run.\n"
    return "Reconciliation Summary:\n" + "\n".join(f"- {line}" for line in RECONCILIATION_SUMMARY_LINES) + "\n"


def write_data_quality_row(
        table_name: str,
        phase: str,
        source_id: Any,
        business_key: Any,
        target_id: Any,
        column_name: str,
        source_value: Any,
        target_value: Any,
        issue_type: str,
        notes: str = "",
) -> None:
    """Append one column-level data-quality mismatch row."""
    write_skipped_to_csv(
        filepath=CSV_DATA_QUALITY_PATH,
        fieldnames=[
            "tableName", "phase", "sourceId", "businessKey", "targetId",
            "columnName", "sourceValue", "targetValue", "issueType",
            "notes", "timestamp",
        ],
        row_data={
            "tableName": table_name,
            "phase": phase,
            "sourceId": source_id or "",
            "businessKey": business_key or "",
            "targetId": target_id or "",
            "columnName": column_name or "",
            "sourceValue": "" if source_value is None else str(source_value),
            "targetValue": "" if target_value is None else str(target_value),
            "issueType": issue_type or "mismatch",
            "notes": notes or "",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )


def add_data_quality_summary(line: str) -> None:
    """Store and trace one human-readable data-quality summary line."""
    DATA_QUALITY_SUMMARY_LINES.append(line)
    trace_print(line)


def data_quality_email_summary() -> str:
    """Return a compact data-quality section for the notification email."""
    if not DATA_QUALITY_SUMMARY_LINES:
        return "Data Quality Summary:\n- Data quality checks were not executed for this run.\n"
    return "Data Quality Summary:\n" + "\n".join(f"- {line}" for line in DATA_QUALITY_SUMMARY_LINES) + "\n"

def _csv_safe_json(value: Any) -> str:
    """Serialize source payload snippets safely for CSV diagnostics."""
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return str(value)


def record_player_skip(
        source_id: Any,
        username: Optional[str],
        reason: str,
        dry_run: bool,
        source_table: str = PLAYER_REGISTRATION_SOURCE_TABLE,
        action: str = "skipped",
        error: Optional[Any] = None,
        data: Optional[Dict[str, Any]] = None,
        issue_type: Optional[str] = None,
        raw_email: Optional[Any] = None,
        sanitized_email: Optional[Any] = None,
) -> None:
    """Print, trace, and export player registration skips, corrections, and failures."""
    event_label = "[REPORT][playerRegistration]" if action in ("email_normalized", "email_corrected", "duplicate_key_upserted") else "[SKIP][playerRegistrat
ion]"
    parts = [
        event_label,
        f"sourceTable={source_table}",
        f"sourceId={source_id or 'N/A'}",
    ]
    if username:
        parts.append(f"username={username}")
    parts.append(f"action={action}")
    if issue_type:
        parts.append(f"issueType={issue_type}")
    parts.append(f"reason={reason}")
    if raw_email is not None:
        parts.append(f"rawEmail={raw_email}")
    if sanitized_email is not None:
        parts.append(f"sanitizedEmail={sanitized_email}")
    if error is not None:
        parts.append(f"error={error}")
    msg = " ".join(parts)
    print(msg, flush=True)
    logging.info(msg)

    _counter_inc(PLAYER_REPORT_COUNTS, action)
    _counter_inc(PLAYER_REPORT_ISSUE_COUNTS, issue_type or action)

    row = {
        "sourceTable": source_table,
        "sourceId": source_id or "",
        "username": username or "",
        "action": action,
        "issueType": issue_type or action,
        "reason": reason,
        "rawEmail": str(raw_email or ""),
        "sanitizedEmail": str(sanitized_email or ""),
        "error": str(error or ""),
        "dryRun": str(bool(dry_run)),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sourcePayload": _csv_safe_json(data or {}),
    }
    write_skipped_to_csv(
        filepath=CSV_PLAYERS_PATH,
        fieldnames=[
            "sourceTable", "sourceId", "username", "action", "issueType", "reason",
            "rawEmail", "sanitizedEmail", "error", "dryRun", "timestamp", "sourcePayload"
        ],
        row_data=row,
    )
    csv_msg = f"[CSV][playerRegistration] wrote report row path={CSV_PLAYERS_PATH} sourceId={source_id or 'N/A'} action={action}"
    print(csv_msg, flush=True)
    logging.info(csv_msg)


def emit_player_phase_summary(
        source_processed: int,
        inserted_or_updated: int,
        skipped: int,
        prefix: str = "playerRegistration",
) -> None:
    """Emit player counts to screen and trace log with report CSV counts separated from skips."""
    report_total = player_report_total()
    fixed_rows = player_report_fixed_total()
    duplicate_rows = player_report_duplicate_total()
    failure_rows = player_report_failure_total()
    msg = (
        f"Completed {prefix} extraction. "
        f"sourceProcessed={source_processed} inserted_or_updated={inserted_or_updated} skipped={skipped} "
        f"playerReportCsvRows={report_total} fixed={fixed_rows} total_fixed={fixed_rows} "
        f"duplicates={duplicate_rows} total_duplicates={duplicate_rows} "
        f"playerReportEmailCorrections={fixed_rows} playerReportDuplicateRows={duplicate_rows} playerReportFailureRows={failure_rows} "
        f"playerReportCsvPath={CSV_PLAYERS_PATH} "
        f"playerReportActions=[{player_report_counts_text()}] "
        f"playerReportIssues=[{player_report_issue_counts_text()}]"
    )
    trace_print(msg)


def package_reports_if_needed(file_paths: List[str], threshold_bytes: int = REPORT_ZIP_THRESHOLD_BYTES) -> List[str]:
    """Zip all existing report files when their combined size reaches the configured threshold."""
    existing_paths = [path for path in file_paths if path and os.path.isfile(path)]
    if not existing_paths:
        print("[REPORT PACKAGING] No report files found for attachment.", flush=True)
        logging.info("[REPORT PACKAGING] No report files found for attachment.")
        return file_paths

    total_size = sum(os.path.getsize(path) for path in existing_paths)
    if total_size < threshold_bytes:
        print(
            f"[REPORT PACKAGING] CSV reports total={total_size} bytes below threshold={threshold_bytes}; sending as individual files.",
            flush=True,
        )
        logging.info(
            "[REPORT PACKAGING] CSV reports total=%s below threshold=%s; sending individual files.",
            total_size,
            threshold_bytes,
        )
        return file_paths

    zip_path = f"reports/migration_reports_{TIMESTAMP_STR}.zip"
    zip_dir = os.path.dirname(zip_path)
    if zip_dir and not os.path.exists(zip_dir):
        os.makedirs(zip_dir, exist_ok=True)

    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in existing_paths:
            zf.write(path, arcname=os.path.basename(path))

    zip_size = os.path.getsize(zip_path)
    msg = (
        f"[REPORT PACKAGING] CSV reports total={total_size} bytes reached threshold={threshold_bytes}; "
        f"created zip={zip_path} zipBytes={zip_size}"
    )
    print(msg, flush=True)
    logging.info(msg)
    return [zip_path]

# ----------------------------
# Helpers
# ----------------------------
def probe_host_port(host: str, port: int, timeout_sec: int = 5) -> None:
    print(f"Probing TCP {host}:{port} ...", flush=True)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout_sec)
    try:
        s.connect((host, port))
        print("TCP reachable", flush=True)
    except Exception as e:
        print(f"TCP probe failed: {e}", flush=True)
    finally:
        try:
            s.close()
        except Exception:
            pass

def connect(dbname: str):
    host = os.getenv("RDS_HOST", "iest-db-postgresql.cvmg4ca8uhd2.ap-southeast-1.rds.amazonaws.com")
    user = os.getenv("RDS_USER", "inplay_mg8")
    password = os.getenv("RDS_PASSWORD", "hHnasl-ai#1-09Mjn-122356")
    port = int(os.getenv("RDS_PORT", "5432"))

    probe_host_port(host, port, timeout_sec=5)

    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        port=port,
        dbname=dbname,
        connect_timeout=45,
        sslmode=os.getenv("RDS_SSLMODE", "require"),
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    register_uuid(conn)
    conn.autocommit = False
    return conn


def as_dict(x: Any) -> Dict[str, Any]:
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    if isinstance(x, str) and x.strip():
        try:
            return json.loads(x)
        except Exception:
            return {}
    return {}


def digits_only(s: str) -> str:
    return "".join(c for c in (s or "") if c.isdigit())


def safe_mobile_10(mobile: Any) -> str:
    d = digits_only(str(mobile or ""))
    if len(d) >= 10:
        return d[-10:]
    return "0000000000"


EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)


def _safe_email_local_part(value: Any) -> str:
    """Return an RFC-ish local part safe enough for target email CHECK constraints."""
    raw = str(value or "").strip().lower()
    local = re.sub(r"[^a-z0-9._%+-]+", "_", raw)
    local = re.sub(r"_+", "_", local).strip("._%+-")
    return local or "unknown"


def sanitize_email(email: Any, username: str) -> str:
    e = str(email or "").strip().rstrip(".")
    if e and e.lower() != "null" and EMAIL_RE.match(e):
        return e
    return f"{_safe_email_local_part(username)}@unknown.local"


def classify_player_upsert_error(error: Any) -> str:
    text = str(error or "")
    lower_text = text.lower()
    if "chk_player_email_format" in lower_text or "email" in lower_text:
        return "Invalid email format rejected by playerDetails_final constraint"
    if "not-null" in lower_text or "null value" in lower_text:
        return "Required player field rejected by playerDetails_final NOT NULL constraint"
    if "check constraint" in lower_text:
        return "Player row rejected by playerDetails_final CHECK constraint"
    if "duplicate" in lower_text or "unique" in lower_text:
        return "Player row rejected by playerDetails_final unique constraint"
    return "Player upsert failed"


def to_decimal_str(x: Any) -> str:
    if x is None:
        return "0"
    if isinstance(x, (int, float)):
        return str(x)
    s = str(x).strip()
    return s if s else "0"


def parse_iso_dt(s: Any) -> Optional[datetime]:
    if not s:
        return None
    if isinstance(s, datetime):
        return s if s.tzinfo else s.replace(tzinfo=timezone.utc)
    txt = str(s).strip()
    if not txt:
        return None
    txt = txt.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(txt)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def latest_dt(*fields: Any) -> Optional[datetime]:
    candidates = [parse_iso_dt(f) for f in fields]
    valid = [dt for dt in candidates if dt is not None]
    return max(valid) if valid else None


def normalize_game_type(raw: Optional[str]) -> str:
    if not raw:
        return "Slots"
    r = raw.strip().upper()
    if r in ("SLOTS", "SLOT"):
        return "Slots"
    if r in ("LIVE", "LIVE_CASINO", "CASINO"):
        return "Live"
    if r in ("SPORTS", "SPORT"):
        return "Sports"
    return raw.strip().title()


def normalize_wallet_status(raw: Any) -> str:
    return (str(raw or "")).strip().lower()


# ----------------------------
# Checkpointing
# ----------------------------
def ck_key(phase: str) -> str:
    return f"{WALLET_PLATFORM}_{phase}"


def checkpoint_get(tgt_conn, phase: str) -> Optional[str]:
    key = ck_key(phase)
    with tgt_conn.cursor() as cur:
        cur.execute('SELECT "lastSourceId" FROM kemet."migrationCheckpoint_dev" WHERE platform=%s', (key,))
        row = cur.fetchone()
        return row[0] if row else None


def checkpoint_sort_key(raw: Optional[str]) -> Tuple[datetime, str]:
    """Return a comparable checkpoint key from the stored date|sourceId pointer.

    The migration stores lastSourceId as "<sourceDateIso>|<sourceRowId>".
    Checkpoint writes must be monotonic so bounded date-window runs do not
    rewind an already newer checkpoint.
    """
    dt_raw, id_raw = parse_inplayv2_checkpoint(raw)
    dt = parse_iso_dt(dt_raw) or datetime.min.replace(tzinfo=timezone.utc)
    return (dt, str(id_raw or ""))


def checkpoint_is_greater(candidate: Optional[str], current: Optional[str]) -> bool:
    """True only when candidate is strictly newer than current."""
    if not candidate:
        return False
    if not current:
        return True
    return checkpoint_sort_key(candidate) > checkpoint_sort_key(current)


def checkpoint_set(tgt_conn, phase: str, last_source_id: str, dry_run: bool) -> None:
    if dry_run:
        return

    key = ck_key(phase)
    candidate = str(last_source_id or "")
    with tgt_conn.cursor() as cur:
        cur.execute('SELECT "lastSourceId" FROM kemet."migrationCheckpoint_dev" WHERE platform=%s', (key,))
        row = cur.fetchone()
        current = row[0] if row else None

        if current is not None and not checkpoint_is_greater(candidate, current):
            msg = (
                f"[CHECKPOINT SKIP][{phase}] existing checkpoint is newer or equal; "
                f"current={current} candidate={candidate}"
            )
            print(msg, flush=True)
            logging.info(msg)
            return

        cur.execute(
            """
            INSERT INTO kemet."migrationCheckpoint_dev" (platform, "lastSourceId", "updatedAt")
            VALUES (%s, %s, now())
            ON CONFLICT (platform) DO UPDATE SET
              "lastSourceId" = EXCLUDED."lastSourceId",
              "updatedAt" = now()
            """,
            (key, candidate),
        )
        msg = f"[CHECKPOINT UPDATE][{phase}] platform={key} lastSourceId={candidate}"
        print(msg, flush=True)
        logging.info(msg)


# ----------------------------
# Target: ensure unique index for wallet de-dupe
# ----------------------------
def ensure_wallet_dedupe_index(tgt_conn, dry_run: bool) -> None:
    if dry_run:
        return
    with tgt_conn.cursor() as cur:
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_wallet_inplayv2_reference
            ON kemet."walletTransaction_final" ("platform", "referenceId")
            WHERE "platform" = %s AND "referenceId" IS NOT NULL
            """,
            (WALLET_PLATFORM,),
        )


# ----------------------------
# Target: dimension upserts (cached)
# ----------------------------
def get_or_create_game_provider(
        tgt_conn,
        provider_name: str,
        cache: Dict[str, uuid.UUID],
        dry_run: bool
) -> uuid.UUID:
    name = (provider_name or "UNKNOWN").strip().upper() or "UNKNOWN"
    if name in cache:
        return cache[name]

    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute('SELECT id FROM kemet."gameProvider_final" WHERE "gameProvider"=%s', (name,))
            row = cur.fetchone()
            gid = row[0] if row else uuid.uuid4()
            cache[name] = gid
            return gid

    sql = """
    INSERT INTO kemet."gameProvider_final" ("gameProvider","isActive","createdAt","updatedAt")
    VALUES (%s, true, now(), now())
    ON CONFLICT ("gameProvider") DO UPDATE SET "updatedAt"=now()
    RETURNING id
    """
    with tgt_conn.cursor() as cur:
        cur.execute(sql, (name,))
        gid = cur.fetchone()[0]
        cache[name] = gid
        return gid


def get_or_create_game_type(
        tgt_conn,
        game_type: str,
        cache: Dict[str, uuid.UUID],
        dry_run: bool
) -> uuid.UUID:
    gt = normalize_game_type(game_type)
    if gt in cache:
        return cache[gt]

    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute('SELECT id FROM kemet."gameType_final" WHERE "gameType"=%s', (gt,))
            row = cur.fetchone()
            gid = row[0] if row else uuid.uuid4()
            cache[gt] = gid
            return gid

    sql = """
    INSERT INTO kemet."gameType_final" ("gameType","isActive","createdAt","updatedAt")
    VALUES (%s, true, now(), now())
    ON CONFLICT ("gameType") DO UPDATE SET "updatedAt"=now()
    RETURNING id
    """
    with tgt_conn.cursor() as cur:
        cur.execute(sql, (gt,))
        gid = cur.fetchone()[0]
        cache[gt] = gid
        return gid


def get_or_create_game_list(
        tgt_conn,
        game_name: str,
        provider_id: uuid.UUID,
        game_type_id: uuid.UUID,
        cache: Dict[Tuple[uuid.UUID, str], uuid.UUID],
        dry_run: bool
) -> uuid.UUID:
    gname = (game_name or "UNKNOWN").strip() or "UNKNOWN"
    key = (provider_id, gname)
    if key in cache:
        return cache[key]

    if dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute(
                'SELECT id FROM kemet."gameList_final" WHERE "gameProviderId"=%s AND "gameName"=%s',
                (provider_id, gname),
            )
            row = cur.fetchone()
            gid = row[0] if row else uuid.uuid4()
            cache[key] = gid
            return gid

    sql = """
    INSERT INTO kemet."gameList_final" (
        "gameTypeId","gameProviderId","gameName",
        "isProgressive","isActive","createdAt","updatedAt",
        "brandName"
    )
    VALUES (%s,%s,%s,false,true,now(),now(),%s)
    ON CONFLICT ("gameProviderId","gameName") DO UPDATE SET
        "gameTypeId"=EXCLUDED."gameTypeId",
        "brandName"=EXCLUDED."brandName",
        "updatedAt"=now()
    RETURNING id
    """
    with tgt_conn.cursor() as cur:
        cur.execute(sql, (game_type_id, provider_id, gname, WALLET_PLATFORM))
        gid = cur.fetchone()[0]
        cache[key] = gid
        return gid

def _player_upsert_row_from_member(
        member: Dict[str, Any],
        detail_map: Optional[Dict[str, Dict[str, str]]] = None,
        source="Unknown"):
    # ) -> Optional[Tuple[Any, ...]]:
    """
    Extracts data from member JSON and returns the 29-value tuple
    required for the 33-column SQL template (including remarks).
    """
    data = member.get("data") if isinstance(member.get("data"), dict) else member
    username = str(
        data.get("username") or
        data.get("userName") or
        member.get("username") or
        member.get("userName") or
        ""
    ).strip()

    income_source = (data.get("incomeSource") or data.get("income_source") or "Other").strip()

    if not username:
        rid = member.get("id") or "unknown"
        print(f"    [SKIP][{source}]: Row ID: {rid} skipped. Reason: No username found.")
        return None

    member_id = str(member.get("card_id") or "").strip() or None

    # --- UPDATED NAME PARSING (Checks for pre-split orphan names first) ---
    first = member.get("firstName") or "Unknown"
    middle = ""
    last = member.get("lastName") or "Unknown"

    # If not an orphan (no pre-split names), parse the real_name field
    if first == "Unknown" and last == "Unknown":
        real_name = (member.get("realName") or "").strip()
        if real_name:
            parts = real_name.split()
            if len(parts) == 1:
                first = parts[0]
            elif len(parts) == 2:
                first, last = parts[0], parts[1]
            else:
                first, middle, last = parts[0], " ".join(parts[1:-1]), parts[-1]
    mobile_10 = safe_mobile_10(member.get("contact_number"))
    raw_email = member.get("email")
    email = sanitize_email(raw_email, username)
    raw_email_text = str(raw_email or "").strip().rstrip(".")
    if (not raw_email_text) or raw_email_text.lower() == "null" or raw_email_text != email:
        reason = "Missing email; generated safe fallback email" if (not raw_email_text or raw_email_text.lower() == "null") else "Invalidemail format; gener
ated safe fallback email"
        record_player_skip(
            source_id=row_source_id,
            username=username,
            reason=reason,
            dry_run=dry_run,
            source_table=source_table,
            action="email_corrected",
            error=f"rawEmail={raw_email_text!r}; finalEmail={email!r}",
            data=member,
        )

    # --- UPDATED DATE LOGIC (Uses registrationDate from orphan logic if present) ---
    raw_reg_date = member.get("registrationDate") or member.get("createddate")
    reg_dt = parse_iso_dt(raw_reg_date) or datetime.now(timezone.utc)

    # Status Handling: suspended '0' means isActive = True
    is_active = str(member.get("suspended")) == "0"
    outlet_code = str(member.get("outlet_id")).strip() or None

    # Use 1900-01-01 if birthdate is missing
    birthdate = parse_iso_dt(member.get("birthdate")) or datetime(1900, 1, 1).date()

    _detail = (detail_map or {}).get(member_id or "") or {}
    address_province = _detail.get("permanent_address") or member.get("permanent_address") or "N/A"
    industry = (data.get("industry") or "Other").strip()
    street = (data.get("addressStreet") or "N/A").strip()
    wallet_balance = float(member.get("balance") or 0)
    now = datetime.now(timezone.utc)

    # Capture remarks for the 29th tuple item
    remarks = member.get("remarks") or f"Migrated from {source}"

    # RETURN EXACTLY 29 ITEMS (To match the 33-column SQL template)
    return (
        username,           # 1. %s -> userName
        first,              # 2. %s -> firstName
        middle,             # 3. %s -> middleName
        last,               # 4. %s -> lastName
        mobile_10,          # 5. %s -> mobileNumber
        False,              # 6. %s -> mobileNumberVerified
        email,              # 7. %s -> emailAddress
        False,              # 8. %s -> emailVerified
        reg_dt,             # 9. %s -> registrationDate
        # (Template Pos 10: registrationIp is NULL)
        # (Template Pos 11: registrationReferrer is NULL)
        WALLET_PLATFORM,              # 10. %s -> brandName
        False,              # 11. %s -> isVerified
        False,              # 12. %s -> isBlocked
        # (Template Pos 15: blockedDatetime is NULL)
        is_active,          # 13. %s -> isActive
        None,               # 14. %s -> lastLogin
        None,               # 15. %s -> lastLoginIp
        outlet_code,        # 16. %s -> outletCode
        # (Template Pos 20: affiliateCode is NULL)
        street,              # 17. %s -> addressStreet
        "N/A",              # 18. %s -> addressBarangay
        "N/A",              # 19. %s -> addressCity
        address_province,   # 20. %s -> addressProvince
        income_source,      # 21. %s -> incomeSource
        industry,               # 22. %s -> industry
        member_id,          # 23. %s -> externalId
        birthdate,          # 24. %s -> birthdate
        wallet_balance,     # 25. %s -> walletBalance
        now,                # 26. %s -> walletBalanceDatetime
        now,                # 27. %s -> createdAt
        now,                # 28. %s -> updatedAt
        remarks             # 29. %s -> remarks
    )


def _first_present(data: Dict[str, Any], *keys: str) -> Any:
    """Return the first non-empty value found in data for the provided keys."""
    for key in keys:
        value = data.get(key)
        if value is not None and str(value).strip() != "":
            return value
    return None


def _nested_dict(data: Dict[str, Any], key: str) -> Dict[str, Any]:
    return as_dict(data.get(key))


def extract_username(member: Dict[str, Any]) -> str:
    """Extract a stable username from V1 registration/member JSON."""
    data = member.get("data") if isinstance(member.get("data"), dict) else member
    return str(
        _first_present(
            data,
            "name",
            "username",
            "userName",
            "loginName",
            "userid",
            "userId",
        ) or ""
    ).strip()




def extract_external_id(member: Dict[str, Any], details: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Extract playerDetails_final.externalId from source registration JSON.

    Primary rule for PlayerRegistrationsInplayV1_dev: top-level card_id maps to
    playerDetails_final.externalId. Fallbacks exist only for legacy/variant payloads.
    """
    details = details or {}
    external_id = str(
        _first_present(member, "card_id", "cardId", "memberId", "externalId")
        or details.get("externalId")
        or details.get("external_id")
        or ""
    ).strip()
    return external_id or None

def source_dt_value(data: Dict[str, Any], table: Optional[str] = None) -> Optional[str]:
    """Return the source timestamp used for checkpointing.

    When table is provided, use only the requested V1_dev phase-specific date key:
    player registrations=createddate, game transactions=GameDate, wallet=transferDate.
    """
    data = data or {}
    if table:
        keys = SOURCE_DATE_KEYS_BY_TABLE.get(table, ())
    else:
        keys = (
            "createddate",
            "GameDate",
            "transferDate",
            "transferdate",
            "dateTimeCreated",
            "createdDateTime",
            "createdDate",
            "registrationDate",
        )
    value = _first_present(data, *keys)
    return str(value).strip() if value is not None and str(value).strip() else None


def _bool_from_source(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "t", "yes", "y", "verified", "approved", "active")

def upsert_player_from_member(
    tgt_conn,
    member: Dict[str, Any],
    brand,
    dry_run: bool = False,
    detail_map: Optional[Dict[str, Dict[str, str]]] = None,
    source_id: Optional[Any] = None,
    source_table: str = PLAYER_REGISTRATION_SOURCE_TABLE,
) -> uuid.UUID:
    """
    Upserts a player and returns the UUID.
    Fixed to actually execute the SQL and return the ID.
    """
    # 1. Universal Identity Hunter
    username = str(
        member.get("username") or
        member.get("name") or
        member.get("loginName") or
        member.get("userName") or
        member.get("userid") or ""
    ).strip()

    row_source_id = source_id or member.get("id") or member.get("card_id") or member.get("cardId") or "unknown"

    if not username:
        record_player_skip(
            source_id=row_source_id,
            username=None,
            reason="Missing username / identity keys",
            dry_run=dry_run,
            source_table=source_table,
            action="skipped_missing_username",
            data=member,
        )
        return None

    # 2. Get supplemental KYC/data lookups
    details = (detail_map or {}).get(username) or {}

    # Source JSON shape confirmed for PlayerRegistrationsInplayV1_dev:
    # top-level card_id is the player card number and must map to playerDetails_final.externalId.
    # Use a single extractor so this mapping cannot silently regress again.
    member_id = extract_external_id(member, details)
    if not member_id:
        record_player_skip(
            source_id=row_source_id,
            username=username,
            reason="Source player row has no card_id/cardId/memberId/externalId; target externalId will remain NULL unless already populated",
            dry_run=dry_run,
            source_table=source_table,
            action="missing_external_id",
            issue_type="missing_card_id",
            data=member,
        )

    # 3. Name Parsing Logic
    # IMPORTANT: do not change first/middle/last logic unless business rules change.
    first = member.get("first_name")
    middle = member.get("middle_name") or ""
    last = member.get("last_name")

    if not first or not last:
        real_name = (member.get("realName") or "").strip()
        if real_name:
            parts = real_name.split()
            if len(parts) == 1:
                first, last = parts[0], "Unknown"
            elif len(parts) == 2:
                first, last = parts[0], parts[1]
            else:
                first, middle, last = parts[0], " ".join(parts[1:-1]), parts[-1]
        else:
            first = first or "Unknown"
            last = last or "Unknown"

    # 4. Data Sanitization
    mobile_10 = safe_mobile_10(member.get("contact_number"))
    raw_email = member.get("email")
    email = sanitize_email(raw_email, username)
    raw_email_txt = str(raw_email or "").strip().rstrip(".")
    if (not raw_email_txt) or raw_email_txt.lower() == "null" or raw_email_txt != email:
        email_issue_type = "missing_email_auto_generated" if (not raw_email_txt or raw_email_txt.lower() == "null") else "invalid_email_auto_corrected"
        record_player_skip(
            source_id=row_source_id,
            username=username,
            reason="Player email was missing or invalid and was normalized before upsert",
            dry_run=dry_run,
            source_table=source_table,
            action="email_normalized",
            issue_type=email_issue_type,
            raw_email=raw_email,
            sanitized_email=email,
            data=member,
        )
    outlet_code = str(_first_present(member, "outlet_id", "outletCode") or details.get("outletCode") or "").strip() or None
    birthdate = parse_iso_dt(_first_present(member, "birthdate", "birthDay", "dateOfBirth", "birthDate") or details.get("birthdate")) or "1900-01-01"
    reg_dt = (
        parse_iso_dt(_first_present(member, "createddate", "createdDate", "registrationDate"))
        or parse_iso_dt(_first_present(member, "updatedate", "updatedDate"))
        or datetime.now(timezone.utc)
    )
    is_active = str(member.get("suspended")) == "0" if member.get("suspended") is not None else not _bool_from_source(member.get("closed")
, default=False)
    address_street = str(_first_present(member, "current_address", "addressStreet", "street") or "N/A").strip() or "N/A"
    address_province = str(
        details.get("permanent_address")
        or details.get("addressProvince")
        or _first_present(member, "permanent_address", "addressProvince", "province")
        or "N/A"
    ).strip() or "N/A"
    try:
        wallet_balance = float(member.get("balance") or 0)
    except Exception:
        wallet_balance = 0.0

    if dry_run:
        return uuid.uuid4()

    sql = """
    INSERT INTO kemet."playerDetails_final" (
        "userName", "firstName", "middleName", "lastName", "mobileNumber",
        "emailAddress", "registrationDate", "brandName", "isVerified", "isActive",
        "lastLogin", "lastLoginIp", "outletCode", "addressStreet", "addressBarangay",
        "addressCity", "addressProvince", "externalId", "birthdate", "walletBalance",
        "incomeSource", "industry"  -- Adding these to satisfy NOT NULL constraints
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s
    )
    ON CONFLICT ("userName") DO UPDATE SET
        "mobileNumber" = EXCLUDED."mobileNumber",
        "emailAddress" = EXCLUDED."emailAddress",
        "registrationDate" = COALESCE(EXCLUDED."registrationDate", kemet."playerDetails_final"."registrationDate"),
        "brandName" = EXCLUDED."brandName",
        "isActive" = EXCLUDED."isActive",
        "outletCode" = COALESCE(EXCLUDED."outletCode", kemet."playerDetails_final"."outletCode"),
        "addressStreet" = COALESCE(NULLIF(EXCLUDED."addressStreet", 'N/A'), kemet."playerDetails_final"."addressStreet", EXCLUDED."addressStreet"),
        "addressProvince" = COALESCE(NULLIF(EXCLUDED."addressProvince", 'N/A'), kemet."playerDetails_final"."addressProvince", EXCLUDED."addressProvince"),
        "externalId" = COALESCE(kemet."playerDetails_final"."externalId", EXCLUDED."externalId"),
        "birthdate" = COALESCE(EXCLUDED."birthdate", kemet."playerDetails_final"."birthdate"),
        "walletBalance" = EXCLUDED."walletBalance",
        "updatedAt" = now()
    RETURNING id, (xmax = 0) AS inserted
    """

    # Use 'N/A' as a safe default for required string columns
    params = (
        username, first, middle, last, mobile_10,
        email, reg_dt, brand, False, is_active,
        None, None, outlet_code, address_street, "N/A",
        "N/A", address_province, member_id, birthdate, wallet_balance,
        "N/A", "N/A"  # Explicitly providing values for incomeSource and industry
    )

    with tgt_conn.cursor() as cur:
        try:
            cur.execute(sql, params)
            row = cur.fetchone()
            if row:
                player_id = row[0]
                inserted_flag = bool(row[1]) if len(row) > 1 else True
                if not inserted_flag:
                    record_player_skip(
                        source_id=row_source_id,
                        username=username,
                        reason="Duplicate username encountered; existing playerDetails_final row updated by ON CONFLICT",
                        dry_run=dry_run,
                        source_table=source_table,
                        action="duplicate_key_upserted",
                        issue_type="duplicate_username_updated",
                        raw_email=raw_email,
                        sanitized_email=email,
                        data=member,
                    )
                return player_id
            else:
                cur.execute('SELECT id FROM kemet."playerDetails_final" WHERE "userName" = %s', (username,))
                fallback_row = cur.fetchone()
                if fallback_row:
                    record_player_skip(
                        source_id=row_source_id,
                        username=username,
                        reason="Duplicate username encountered; existing playerDetails_final row found after upsert fallback",
                        dry_run=dry_run,
                        source_table=source_table,
                        action="duplicate_key_upserted",
                        issue_type="duplicate_username_updated",
                        raw_email=raw_email,
                        sanitized_email=email,
                        data=member,
                    )
                    return fallback_row[0]
                return None
        except Exception as e:
            # IMPORTANT: Since one error kills the transaction,
            # we rollback so the NEXT attempt in the loop can succeed.
            tgt_conn.rollback()
            reason = classify_player_upsert_error(e)
            issue_type = "invalid_email_constraint" if "email" in reason.lower() else "player_upsert_constraint_failure"
            record_player_skip(
                source_id=row_source_id,
                username=username,
                reason=reason,
                dry_run=dry_run,
                source_table=source_table,
                action="upsert_failed",
                issue_type=issue_type,
                raw_email=raw_email,
                sanitized_email=email,
                error=e,
                data=member,
            )
            return None

def build_player_map(tgt_conn) -> Dict[str, uuid.UUID]:
    m: Dict[str, uuid.UUID] = {}
    with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, "userName"
            FROM kemet."playerDetails_final"
            WHERE "brandName"=%s
            """,
            (WALLET_PLATFORM,),
        )
        for r in cur.fetchall():
            m[str(r["userName"])] = r["id"]
    return m


def ensure_outlet_code_enrolled(tgt_conn, outlet_code: str, dry_run: bool) -> None:
    if dry_run:
        return
    sql = """
    INSERT INTO kemet."outletList_final" (
        "outletCode", "outletName",
        "streetAddress", "barangayAddress", "cityAddress", "provinceAddress",
        "outletShare", "operator", "isActive", "brand",
        "createdAt", "updatedAt", "lastUpdateDatetime"
    ) VALUES (
        %s, %s, '', '', '', '',
        0.00, 'Inplay', true, %s,
        now(), now(), now()
    )
    ON CONFLICT ("outletCode") DO NOTHING
    """
    with tgt_conn.cursor() as cur:
        cur.execute(sql, (outlet_code, outlet_code, WALLET_PLATFORM))
    tgt_conn.commit()


# ----------------------------
# Source fetchers
# ----------------------------
def parse_inplayv2_checkpoint(raw: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not raw:
        return (None, None)
    if "|" in raw:
        dt_str, id_str = raw.split("|", 1)
        return (dt_str or None, id_str or None)
    return (None, None)


def format_inplayv2_checkpoint(dt_iso: Optional[str], raw_id: str) -> str:
    return f"{dt_iso or ''}|{raw_id}"


def _source_date_expr_for_table(table: str) -> str:
    """
    Build a JSONB timestamp expression for the table-specific source date field.

    Required V1_dev source date keys for --date-from / --date-to:
      - PlayerRegistrationsInplayV1_dev: "data"->>'createddate'
      - GameTransactionInplayV1_dev: "data"->>'GameDate'
      - DepositsInplayV1_dev / WithdrawalsInplayV1_dev: "data"->>'transferDate'
    """
    keys = SOURCE_DATE_KEYS_BY_TABLE.get(table)
    if not keys:
        raise ValueError(f"No source date key mapping configured for table: {table}")
    pieces = [f"NULLIF({quote_ident('data')}->>'{key}','')" for key in keys]
    raw_expr = pieces[0] if len(pieces) == 1 else f"COALESCE({', '.join(pieces)})"
    return f"({raw_expr})::timestamptz"


def print_source_query(cur, label: str, query: str, params: Iterable[Any]) -> None:
    """Record the exact source SQL; phase summaries print it once per phase."""
    if not PRINT_SOURCE_SQL:
        return
    params_list = list(params)
    try:
        exact_query = cur.mogrify(query, params_list).decode("utf-8")
    except Exception as e:
        exact_query = f"{query.strip()}\n-- PARAMS: {params_list!r}\n-- mogrify failed: {e}"

    # Screen requirement: show a psql-copyable SELECT with table-only names,
    # even if SOURCE_SCHEMA is used for actual execution.
    # REVERT NOTE: remove this replacement if you need schema-qualified screen traces.
    screen_query = exact_query
    if SOURCE_SCHEMA:
        screen_query = screen_query.replace(f"{quote_ident(SOURCE_SCHEMA)}.", "")

    audit = SOURCE_QUERY_AUDIT.setdefault(label, {"executions": 0})
    audit["executions"] = int(audit.get("executions") or 0) + 1
    audit["exact_query"] = screen_query.strip()
    audit["params"] = params_list
    logging.info("[SOURCE QUERY][%s] exact_query=%s", label, " ".join(screen_query.split()))


def note_source_query_result(label: str, rows: int) -> None:
    if not PRINT_SOURCE_SQL:
        return
    audit = SOURCE_QUERY_AUDIT.setdefault(label, {"executions": 0})
    audit["last_rows"] = rows
    audit["total_rows"] = int(audit.get("total_rows") or 0) + rows


def print_source_query_summary(label: str, phase_label: Optional[str] = None) -> None:
    """Print the last exact source SELECT for a phase/table once, after the phase loop."""
    if not PRINT_SOURCE_SQL:
        return
    audit = SOURCE_QUERY_AUDIT.get(label)
    if not audit or not audit.get("exact_query"):
        return
    shown_label = phase_label or label
    msg = (
        f"\n[SOURCE QUERY SUMMARY][{shown_label}] "
        f"executions={audit.get('executions', 0)} "
        f"lastRows={audit.get('last_rows', 'N/A')} "
        f"totalRowsFetched={audit.get('total_rows', 'N/A')}\n"
        f"{audit['exact_query']}\n"
    )
    print(msg, flush=True)
    logging.info("[SOURCE QUERY SUMMARY][%s] executions=%s lastRows=%s totalRowsFetched=%s",
                 shown_label, audit.get('executions', 0), audit.get('last_rows', 'N/A'), audit.get('total_rows', 'N/A'))


def print_skip_message(phase: str, source_id: Any, reason: str, username: Optional[str] = None, **extra: Any) -> None:
    """Print a consistent live skip reason for game and wallet transaction phases."""
    phase_lc = phase.lower()
    if "gametransaction" in phase_lc:
        source_table = GAME_TRANSACTION_SOURCE_TABLE
    elif "deposit" in phase_lc:
        source_table = DEPOSITS_SOURCE_TABLE
    elif "withdrawal" in phase_lc:
        source_table = WITHDRAWALS_SOURCE_TABLE
    else:
        source_table = extra.pop("sourceTable", "N/A")

    parts = [
        f"[SKIP][{phase}]",
        f"sourceTable={source_table}",
        f"sourceId={source_id or 'N/A'}",
    ]
    if username:
        parts.append(f"username={username}")
    parts.append(f"reason={reason}")
    for key, value in extra.items():
        if value is not None and value != "":
            parts.append(f"{key}={value}")
    msg = " ".join(parts)
    print(msg, flush=True)
    logging.info(msg)


def record_game_report(
        source_id: Any,
        external_id: Any,
        username: Optional[str],
        reason: str,
        dry_run: bool,
        action: str = "skipped",
        issue_type: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
) -> None:
    """Print/log/export gameTransaction skip, duplicate, and report rows."""
    phase = "gameTransaction"
    event_label = "[REPORT][gameTransaction]" if action == "duplicate_key_ignored" else "[SKIP][gameTransaction]"
    msg = (
        f"{event_label} sourceTable={GAME_TRANSACTION_SOURCE_TABLE} sourceId={source_id or 'N/A'} "
        f"externalId={external_id or ''} username={username or ''} action={action} "
        f"issueType={issue_type or action} reason={reason}"
    )
    trace_print(msg)
    _counter_inc(PHASE_REPORT_COUNTS.setdefault(phase, {}), action)
    _counter_inc(PHASE_REPORT_ISSUE_COUNTS.setdefault(phase, {}), issue_type or action)
    write_skipped_to_csv(
        filepath=CSV_GAMETX_PATH,
        fieldnames=["sourceTable", "sourceId", "externalId", "username", "action", "issueType", "reason", "dryRun", "timestamp", "sourcePayload"],
        row_data={
            "sourceTable": GAME_TRANSACTION_SOURCE_TABLE,
            "sourceId": source_id or "",
            "externalId": external_id or "",
            "username": username or "",
            "action": action,
            "issueType": issue_type or action,
            "reason": reason,
            "dryRun": str(bool(dry_run)),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sourcePayload": _csv_safe_json(data or {}),
        },
    )
    trace_print(f"[CSV][gameTransaction] wrote report row path={CSV_GAMETX_PATH} sourceId={source_id or 'N/A'} action={action}")


def record_game_skip(source_id: Any, external_id: Any, username: Optional[str], reason: str, dry_run: bool, data: Optional[Dict[str, Any]]
 = None) -> None:
    """Print/log/export a gameTransaction skip reason."""
    record_game_report(
        source_id=source_id,
        external_id=external_id,
        username=username,
        reason=reason,
        dry_run=dry_run,
        action="skipped",
        issue_type="skipped",
        data=data,
    )

def record_wallet_report(
        kind: str,
        source_id: Any,
        username: Optional[str],
        reason: str,
        dry_run: bool,
        reference_id: Optional[Any] = None,
        action: str = "skipped",
        issue_type: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
) -> None:
    """Print/log/export walletTransaction skip, duplicate, and report rows."""
    phase = f"walletTransaction.{kind}"
    source_table = DEPOSITS_SOURCE_TABLE if kind == "deposit" else WITHDRAWALS_SOURCE_TABLE
    event_label = f"[REPORT][walletTransaction.{kind}]" if action == "duplicate_key_ignored" else f"[SKIP][walletTransaction.{kind}]"
    msg = (
        f"{event_label} sourceTable={source_table} sourceId={source_id or 'N/A'} "
        f"referenceId={reference_id or ''} username={username or ''} transactionType={kind} action={action} "
        f"issueType={issue_type or action} reason={reason}"
    )
    trace_print(msg)
    _counter_inc(PHASE_REPORT_COUNTS.setdefault(phase, {}), action)
    _counter_inc(PHASE_REPORT_ISSUE_COUNTS.setdefault(phase, {}), issue_type or action)
    target_csv = CSV_DEPOSITS_PATH if kind == "deposit" else CSV_WITHDRAWALS_PATH
    write_skipped_to_csv(
        filepath=target_csv,
        fieldnames=["sourceTable", "sourceId", "referenceId", "username", "transactionType", "action", "issueType", "reason", "dryRun", "timestamp", "source
Payload"],
        row_data={
            "sourceTable": source_table,
            "sourceId": source_id or "",
            "referenceId": reference_id or "",
            "username": username or "",
            "transactionType": kind,
            "action": action,
            "issueType": issue_type or action,
            "reason": reason,
            "dryRun": str(bool(dry_run)),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sourcePayload": _csv_safe_json(data or {}),
        },
    )
    trace_print(f"[CSV][walletTransaction.{kind}] wrote report row path={target_csv} sourceId={source_id or 'N/A'} action={action}")


def record_wallet_skip(kind: str, source_id: Any, username: Optional[str], reason: str, dry_run: bool, data: Optional[Dict[str, Any]] = None, reference_id:
Optional[Any] = None) -> None:
    """Print/log/export a walletTransaction skip reason."""
    record_wallet_report(
        kind=kind,
        source_id=source_id,
        username=username,
        reason=reason,
        dry_run=dry_run,
        reference_id=reference_id,
        action="skipped",
        issue_type="skipped",
        data=data,
    )

def fetch_json_table_batch(
        src_conn,
        table: str,
        after_dt: Optional[str],
        after_id: Optional[str],
        limit: int,
        from_dt: Optional[str] = None,
        until_dt: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch a deterministic batch from a JSONB source table.

    --date-from/--date-to filters are applied using the requested source date
    field for each phase: createddate for registrations, GameDate for games,
    and transferDate for deposits/withdrawals.
    """
    allowed_tables = {
        PLAYER_REGISTRATION_SOURCE_TABLE,
        GAME_TRANSACTION_SOURCE_TABLE,
        DEPOSITS_SOURCE_TABLE,
        WITHDRAWALS_SOURCE_TABLE,
    }
    if table not in allowed_tables:
        raise ValueError(f"Unsupported source table: {table}")

    date_col = _source_date_expr_for_table(table)
    anchor_id = after_id or ""

    conditions = [f"{quote_ident('data')} IS NOT NULL"]
    params: List[Any] = []

    if after_dt is not None:
        conditions.append(f"({date_col}, {quote_ident('id')}) > (%s::timestamptz, %s)")
        params.extend([after_dt, anchor_id])
    elif anchor_id:
        conditions.append(f"{quote_ident('id')} > %s")
        params.append(anchor_id)

    # Optional source WHERE clause controlled by --date-from / --date-to.
    if from_dt is not None:
        conditions.append(f"{date_col} >= %s::timestamptz")
        params.append(from_dt)

    if until_dt is not None:
        conditions.append(f"{date_col} <= %s::timestamptz")
        params.append(until_dt)

    params.append(limit)
    where_clause = " AND ".join(conditions)
    label = table
    query = f"""
            SELECT {quote_ident('id')}, {quote_ident('data')}
            FROM {source_table_ref(table)}
            WHERE {where_clause}
            ORDER BY {date_col} ASC NULLS LAST, {quote_ident('id')} ASC
            LIMIT %s
            """

    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        print_source_query(cur, label, query, params)
        cur.execute(query, params)
        rows = cur.fetchall()
        note_source_query_result(label, len(rows))
    src_conn.rollback()
    return rows

def fetch_player_detail_map(src_conn, date_from: str, date_to: str) -> Dict[str, Dict]:
    """
    Builds a lookup map from the source registration data to fill
    KYC details in the target playerDetails table.
    """
    print(f"\n>>> [FUNCTION START]: fetch_player_detail_map")
    print(f">>> [SQL START]: Selecting from kemet.PlayerRegistrationsInplayV1_dev")

    detail_map: Dict[str, Dict[str, str]] = {}
    # Queries the 3-column source table (id, data, created_date)
    # querying prod
    #query = """
    #    SELECT data->>'username' as uname, data
    #    FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
    #    WHERE data IS NOT NULL
    #"""
    query = f"""
          SELECT data->>'username' as uname, data
         FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
          WHERE data->>'createddate' >= %s
          AND data->>'createddate' >= %s
          AND data IS NOT NULL
    """

    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (date_from, date_to))
        #cur.execute(query)
        rows = cur.fetchall()

        for r in rows:
            uname = r.get('uname')
            data = as_dict(r.get('data'))

            if not uname or not data:
                # Screen message for skipped records only
                print(f"    [SKIP]: Record ID {r.get('id')} skipped. Reason: Missing username or empty data.")
                continue


            detail_map[uname] = {
                "addressProvince": data.get("permanent_address"),
                "walletBalance": to_decimal_str(data.get("balance") or "0"),
                "externalId" : data.get("card_id"),
                "outletCode" : data.get("outlet_id"),
                "contactNumber" : safe_mobile_10(data.get("contact_number")),
                "birthdate": data.get("birthdate")
            }

    print(f"<<< [SQL END]: Successfully mapped {len(detail_map)} verification records.")
    print(f">>> [FUNCTION END]: fetch_player_detail_map")
    return detail_map


# ----------------------------
# Target inserts: gameTransaction
# ----------------------------
def _bool_from_numeric_flag(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    try:
        return float(str(value).strip() or 0) > 0
    except Exception:
        return str(value).strip().lower() in ("true", "t", "yes", "y")


def insert_game_tx_batch(
        tgt_conn,
        rows: List[Dict[str, Any]],
        player_map: Dict[str, uuid.UUID],
        provider_cache: Dict[str, uuid.UUID],
        gametype_cache: Dict[str, uuid.UUID],
        gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID],
        dry_run: bool
) -> Tuple[int, int]:
    """
    Insert gameTransaction rows only for players that already exist in player_map.

    Business rule: game transactions never create playerDetails_final rows. Missing
    players are displayed/logged as skipped in live and --dry-run modes.
    """
    values: List[Tuple[Any, ...]] = []
    skipped_rows = 0

    for r in rows:
        source_id = str(r.get("id") or r.get("src_id") or "").strip()
        data = as_dict(r.get("data"))
        if not source_id:
            skipped_rows += 1
            record_game_skip(None, None, None, "Missing source row id", dry_run)
            continue
        if not data:
            skipped_rows += 1
            record_game_skip(source_id, None, None, "Missing or invalid source JSON data", dry_run)
            continue

        external_id = str(
            _first_present(data, "TransactionID", "transactionId", "id", "externalId")
            or source_id
            or ""
        ).strip()
        if not external_id:
            skipped_rows += 1
            record_game_skip(source_id, None, None, "Missing game transaction externalId/TransactionID", dry_run)
            continue

        member = as_dict(data.get("member"))
        username = str(
            _first_present(data, "PlayerAccount", "username", "userName", "name", "loginName", "userid", "userId")
            or extract_username(member)
            or ""
        ).strip()
        if not username:
            skipped_rows += 1
            record_game_skip(source_id, external_id, None, "Missing username/PlayerAccount in game transaction source payload", dry_run)
            continue

        player_id = player_map.get(username)
        if not player_id:
            skipped_rows += 1
            record_game_skip(
                source_id,
                external_id,
                username,
                "Unable to process no playerRecord; username not found in player_map/playerDetails_final after player-registration phase.No playerDetails_fi
nal ghost/shadow row was created.",
                dry_run,
            )
            continue

        game = as_dict(data.get("game"))
        provider_name = str(
            _first_present(data, "GameProvider", "gameProvider", "provider")
            or game.get("provider")
            or "UNKNOWN"
        ).strip() or "UNKNOWN"
        game_name = str(
            _first_present(data, "GameName", "gameName", "name")
            or game.get("name")
            or "UNKNOWN"
        ).strip() or "UNKNOWN"
        game_type_raw = str(
            _first_present(data, "GameType", "gameType", "type")
            or game.get("type")
            or "Slots"
        ).strip() or "Slots"

        provider_id = get_or_create_game_provider(tgt_conn, provider_name, provider_cache, dry_run)
        game_type_id = get_or_create_game_type(tgt_conn, game_type_raw, gametype_cache, dry_run)
        game_id = get_or_create_game_list(tgt_conn, game_name, provider_id, game_type_id, gamelist_cache, dry_run)

        start_dt = parse_iso_dt(_first_present(data, "GameDate", "gameDate", "gamedate")) or datetime.now(timezone.utc)
        end_dt = parse_iso_dt(_first_present(data, "UpdateDateTime", "updateDateTime", "dateTimeSettled", "settledDate")) or start_dt

        bet_amount = to_decimal_str(_first_present(data, "TotalStakes", "totalStakes", "bet", "betAmount"))
        payout_amount = to_decimal_str(_first_present(data, "TotalWins", "totalWins", "payout", "payoutAmount"))

        pc1 = to_decimal_str(data.get("PC1"))
        pc2 = to_decimal_str(data.get("PC2"))
        pc3 = to_decimal_str(data.get("PC3"))
        pc4 = to_decimal_str(data.get("PC4"))
        pc5 = to_decimal_str(data.get("PC5") or data.get("jackpotContribution"))

        jw1 = to_decimal_str(data.get("JW1"))
        jw2 = to_decimal_str(data.get("JW2"))
        jw3 = to_decimal_str(data.get("JW3"))
        jw4 = to_decimal_str(data.get("JW4"))
        jw5 = to_decimal_str(data.get("JW5") or data.get("jackpotPayout"))

        progression_paid = to_decimal_str(data.get("PROGRESSIVE_CONTRIBUTION_PAID"))
        seed_won = to_decimal_str(data.get("SEED_MONEY_WON"))
        seed_over_1000 = _bool_from_numeric_flag(data.get("SEED_MONEY_JACKPOT_WON_OVER_1000"))

        outlet = str(_first_present(data, "Outlet", "outlet", "tableRoomId") or "").strip() or None
        round_id = str(_first_present(data, "SessionID", "sessionId", "vendorRoundId", "roundId") or "").strip() or None

        values.append((
            start_dt, provider_id, game_id, game_type_id, player_id,
            username, outlet, "0", bet_amount, bet_amount, payout_amount,
            pc1, pc2, pc3, pc4, pc5,
            jw1, jw2, jw3, jw4, jw5,
            progression_paid, seed_won, seed_over_1000,
            end_dt, external_id, False, None, None,
            BRAND, PLATFORM, round_id,
        ))

    if skipped_rows:
        print(f"[SUMMARY][gameTransaction] insertable={len(values)} skipped={skipped_rows}", flush=True)

    if dry_run:
        print(f"[DRY-RUN][gameTransaction] Would insert {len(values)} rows; skipped={skipped_rows}", flush=True)
        return (len(values), skipped_rows)

    if not values:
        return (0, skipped_rows)

    sql = """
    INSERT INTO kemet."gameTransaction_final" (
        "startDateTime", "providerId", "gameId", "gameTypeId", "playerId",
        "playerUserName", "tableRoomId", "sideBetAmount", "betAmount", "validBet",
        "payoutAmount", "PC1","PC2","PC3","PC4","PC5",
        "JW1","JW2","JW3","JW4","JW5",
        "progressionContributionPaid", "seedMoneyWon", "seedMoneyJackpotOver1000",
        "endDateTime", "externalId", "parlay", "betDetails", "betTiming",
        "brand", "platform", "roundId"
    )
    VALUES %s
    ON CONFLICT ("externalId") DO NOTHING
    """

    with tgt_conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
        inserted_rows = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else len(values)

    print(
        f"[LIVE][gameTransaction] inserted={inserted_rows} attempted={len(values)} skipped={skipped_rows}",
        flush=True,
    )
    return (inserted_rows, skipped_rows)




# ----------------------------
# Target inserts: walletTransaction
# ----------------------------
def wallet_row_to_values(
        tgt_conn,
        kind: str,
        src_id: str,
        data: Dict[str, Any],
        player_map: Dict[str, uuid.UUID],
        dry_run: bool
) -> Optional[Tuple[Any, ...]]:
    """
    Transform wallet records into target values.

    Wallet transactions never create playerDetails_final records. If a player is
    missing from player_map, the row is skipped and the live console shows why.
    """
    member = as_dict(data.get("member"))
    username = str(
        _first_present(data, "PlayerAccount", "playerAccount", "username", "userName", "name", "loginName", "userid", "userId")
        or extract_username(member)
        or ""
    ).strip()

    if not username:
        record_wallet_skip(kind, src_id, None, "Missing username in wallet source payload", dry_run)
        return None

    player_id = player_map.get(username)
    if not player_id:
        record_wallet_skip(kind, src_id, username, "Unable to process no playerRecord; username not found in player_map/playerDetails_final after player-reg
istration phase. No playerDetails_final ghost/shadow row was created.", dry_run)
        return None

    ref_id = str(_first_present(data, "id", "TransactionID", "transactionId", "referenceId") or src_id or "").strip()
    if not ref_id:
        record_wallet_skip(kind, src_id, username, "Missing wallet referenceId/id", dry_run)
        return None

    amount_raw = _first_present(data, "amount", "Amount", "TotalAmount", "totalAmount", "transferAmount")
    try:
        amount = abs(float(amount_raw or 0))
    except Exception:
        record_wallet_skip(kind, src_id, username, f"Invalid amount value: {amount_raw!r}", dry_run)
        return None

    raw_date = _first_present(data, "transferDate", "transferdate", "TransferDate")
    t_date = parse_iso_dt(raw_date)
    if t_date is None:
        t_date = datetime(1970, 1, 1, tzinfo=timezone.utc)
        print(
            f"[WARN][walletTransaction.{kind}] sourceId={src_id} username={username} "
            "missing transferDate; defaulting createdDatetime to 1970-01-01",
            flush=True,
        )

    domain = "www.inplay.com.ph"
    payment_gateway = str(_first_present(data, "payment", "paymentMethod", "paymentGateway") or "N/A")

    return (
        kind.lower(),           # transactionType
        WALLET_PLATFORM,        # platform
        player_id,              # playerId
        payment_gateway,        # paymentGateway
        domain,                 # domain
        amount,                 # amount
        "confirmed",            # status
        None,                   # bettingPhase
        t_date,                 # createdDatetime
        t_date,                 # confirmedDatetime
        None,                   # cancelledDatetime
        None,                   # failedDatetime
        ref_id,                 # referenceId
        t_date,                 # updatedAt
    )

def insert_wallet_batch(
        tgt_conn,
        rows: List[Dict[str, Any]],
        kind: str,
        player_map: Dict[str, uuid.UUID],
        dry_run: bool,
) -> Tuple[int, int]:
    """
    Insert walletTransaction rows only for players that already exist in player_map.

    Business rule: deposits/withdrawals never create playerDetails_final rows. All
    skipped rows are displayed in live and --dry-run modes and retained through
    the existing CSV writer in live mode.
    """
    values: List[Tuple[Any, ...]] = []
    skipped_rows = 0

    for r in rows:
        src_id = str(r.get("id") or "").strip()
        data = as_dict(r.get("data"))
        if not src_id:
            skipped_rows += 1
            record_wallet_skip(kind, None, None, "Missing source row id", dry_run)
            continue
        if not data:
            skipped_rows += 1
            record_wallet_skip(kind, src_id, None, "Missing or invalid source JSON data", dry_run)
            continue

        v = wallet_row_to_values(tgt_conn, kind, src_id, data, player_map, dry_run=dry_run)
        if v:
            values.append(v)
        else:
            skipped_rows += 1

    if skipped_rows:
        print(f"[SUMMARY][walletTransaction.{kind}] insertable={len(values)} skipped={skipped_rows}", flush=True)

    if dry_run:
        print(f"[DRY-RUN][walletTransaction.{kind}] Would insert {len(values)} rows; skipped={skipped_rows}", flush=True)
        return (len(values), skipped_rows)

    if not values:
        return (0, skipped_rows)

    sql = f"""
    INSERT INTO kemet."walletTransaction_final" (
        "transactionType",
        "platform",
        "playerId",
        "paymentGateway",
        "domain",
        "amount",
        "status",
        "bettingPhase",
        "createdDatetime",
        "confirmedDatetime",
        "cancelledDatetime",
        "failedDatetime",
        "referenceId",
        "updatedAt"
    )
    VALUES %s
    ON CONFLICT ("platform", "referenceId")
    WHERE ("platform" = '{WALLET_PLATFORM}' AND "referenceId" IS NOT NULL) DO NOTHING
    """
    with tgt_conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=1000)
        inserted_rows = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else len(values)

    print(
        f"[LIVE][walletTransaction.{kind}] inserted={inserted_rows} attempted={len(values)} skipped={skipped_rows}",
        flush=True,
    )
    return (inserted_rows, skipped_rows)


# ----------------------------
# Delete-first
# ----------------------------
def _parse_date_arg(s: str) -> datetime:
    try:
        dt = datetime.fromisoformat(s.strip())
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        raise argparse.ArgumentTypeError(f"Not a valid ISO datetime: '{s}'")


def delete_target_data(tgt_conn, dry_run: bool, keep_from: Optional[datetime], keep_to: Optional[datetime]) -> None:
    has_range = (keep_from is not None) or (keep_to is not None)

    def _exclusion(col: str, params: List[Any]) -> str:
        if not has_range:
            return ""
        parts: List[str] = []
        if keep_from is not None:
            parts.append(f'"{col}" < %s')
            params.append(keep_from)
        if keep_to is not None:
            parts.append(f'"{col}" > %s')
            params.append(keep_to)
        return " AND (" + " OR ".join(parts) + ")"

    if dry_run:
        msg = "[DRY-RUN] would delete InPlayV2 target rows (gameTransaction, walletTransaction, playerDetails, checkpoints)"
        if has_range:
            msg = f"[DRY-RUN] would delete Inplay data (keeping records between {keep_from} and {keep_to})"
        print(msg)
        return

    with tgt_conn.cursor() as cur:
        gt_params: List[Any] = [BRAND]
        cur.execute(
            f'DELETE FROM kemet."gameTransaction_final" WHERE "brand"=%s{_exclusion("startDateTime", gt_params)}',
            gt_params,
        )
        gt = cur.rowcount

        wt_params: List[Any] = [WALLET_PLATFORM]
        cur.execute(
            f'DELETE FROM kemet."walletTransaction_final" WHERE "platform"=%s{_exclusion("createdDatetime", wt_params)}',
            wt_params,
        )
        wt = cur.rowcount

        pd_params: List[Any] = [BRAND]
        cur.execute(
            f'DELETE FROM kemet."playerDetails_final" WHERE "brandName"=%s{_exclusion("registrationDate", pd_params)}',
            pd_params,
        )
        pl = cur.rowcount

        if not has_range:
            cur.execute('DELETE FROM kemet."migrationCheckpoint_dev" WHERE platform LIKE %s', (f"{BRAND}_%",))
            ck = cur.rowcount
        else:
            ck = 0

    tgt_conn.commit()
    print(f"Deleted: gameTransaction={gt}, walletTransaction={wt}, playerDetails={pl}, checkpoints={ck}", flush=True)


# ----------------------------
# Repair existing data (fix already-migrated records)
# ----------------------------
OLD_BRAND = "InPlayV2"


def repair_existing_data(
        src_conn,
        tgt_conn,
        dry_run: bool,
        batch_size: int,
        commit_every: int,
) -> None:
    """
    Fix records already inserted under the old BRAND='InPlayV2' mapping.
    """
    print("\n[Repair] Applying static data schema adjustments ...", flush=True)
    if dry_run:
        print("[DRY-RUN] Would execute schema fix adjustments and rename platform/brand identifiers.", flush=True)
        return

    with tgt_conn.cursor() as cur:
        cur.execute(
            'UPDATE kemet."gameTransaction_final" SET "brand" = %s WHERE "brand" = %s',
            (BRAND, OLD_BRAND),
        )
        gt_brand = cur.rowcount
        print(f"  gameTransaction brand renamed: {gt_brand} rows", flush=True)

        cur.execute(
            """
            UPDATE kemet."gameTransaction_final"
            SET "PC5" = "PC1", "PC1" = '0'
            WHERE "brand" = %s AND "PC1" <> '0'
            """,
            (BRAND,),
        )
        gt_pc = cur.rowcount
        print(f"  gameTransaction PC1/PC5 swap: {gt_pc} rows", flush=True)

        cur.execute(
            """
            UPDATE kemet."gameTransaction_final"
            SET "JW5" = "seedMoneyWon", "seedMoneyWon" = '0'
            WHERE "brand" = %s AND "seedMoneyWon" <> '0'
            """,
            (BRAND,),
        )
        gt_jw = cur.rowcount
        print(f"  gameTransaction JW5/seedMoneyWon swap: {gt_jw} rows", flush=True)

        cur.execute(
            'UPDATE kemet."gameTransaction_final" SET "tableRoomId" = NULL WHERE "brand" = %s AND "tableRoomId" IS NOT NULL',
            (BRAND,),
        )
        gt_tr = cur.rowcount
        print(f"  gameTransaction tableRoomId cleared: {gt_tr} rows", flush=True)

        cur.execute(
            'UPDATE kemet."walletTransaction_final" SET "platform" = %s WHERE "platform" = %s',
            (WALLET_PLATFORM, OLD_BRAND),
        )
        wt_plat = cur.rowcount
        print(f"  walletTransaction platform renamed: {wt_plat} rows", flush=True)

        cur.execute(
            'UPDATE kemet."playerDetails_final" SET "brandName" = %s WHERE "brandName" = %s',
            (BRAND, OLD_BRAND),
        )
        pd_brand = cur.rowcount
        print(f"  playerDetails brandName renamed: {pd_brand} rows", flush=True)

        cur.execute(
            """
            UPDATE kemet."migrationCheckpoint_dev"
            SET platform = REPLACE(platform, %s || '_', %s || '_')
            WHERE platform LIKE %s
            """,
            (OLD_BRAND, BRAND, f"{OLD_BRAND}_%"),
        )
        ck_renamed = cur.rowcount
        print(f"  migrationCheckpoint keys renamed: {ck_renamed} rows", flush=True)

        cur.execute("DROP INDEX IF EXISTS ux_wallet_inplayv2_reference")
        print("  Dropped old wallet dedupe index", flush=True)

    tgt_conn.commit()
    print("SQL fixes committed.", flush=True)

    ensure_wallet_dedupe_index(tgt_conn, dry_run=False)
    tgt_conn.commit()
    print(f"  Recreated wallet dedupe index for platform='{WALLET_PLATFORM}'", flush=True)

    # Re-upsert players from source
    print("\n[Repair] Step 2: Refreshing registration details from PlayerRegistrationsInplayV1_dev ...", flush=True)
    processed_p = 0
    last_dt, last_id = None, None
    while True:
        rows = fetch_json_table_batch(src_conn, PLAYER_REGISTRATION_SOURCE_TABLE, last_dt, last_id, batch_size)
        if not rows:
            break
        for r in rows:
            data = as_dict(r.get("data"))
            if data and not dry_run:
                try:
                    upsert_player_from_member(tgt_conn, data, BRAND, dry_run=False, source_id=str(r.get("id") or "N/A"))
                except Exception as e:
                    record_player_skip(
                        source_id=str(r.get("id") or "N/A"),
                        username=extract_username(data) if data else None,
                        reason="Repair player upsert failed",
                        dry_run=dry_run,
                        source_table=PLAYER_REGISTRATION_SOURCE_TABLE,
                        action="repair_upsert_failed",
                        error=e,
                        data=data,
                    )
            processed_p += 1

        last_row_data = as_dict(rows[-1].get("data"))
        last_id = str(rows[-1]["id"])
        last_dt = source_dt_value(last_row_data, PLAYER_REGISTRATION_SOURCE_TABLE)
        if not dry_run and (processed_p % commit_every) < batch_size:
            tgt_conn.commit()
        print(f"  Progress players: {processed_p}", flush=True)
    if not dry_run:
        tgt_conn.commit()

    detail_map = fetch_player_detail_map(src_conn, None, None)
    print("\n[Repair] Back-filling KYC fields (addressProvince/incomeSource/industry) by externalId...", flush=True)
    if dry_run:
        print(f"[DRY-RUN] Would update KYC fields for {len(detail_map)} players", flush=True)
    else:
        kyc_updated = 0
        with tgt_conn.cursor() as cur:
            for external_id, detail in detail_map.items():
                cur.execute(
                    """
                    UPDATE kemet."playerDetails_final"
                    SET "addressProvince" = %s, "incomeSource" = %s, "industry" = %s, "updatedAt" = now()
                    WHERE "externalId" = %s AND "brandName" = %s AND (
                        "addressProvince" = 'N/A' OR "addressProvince" IS NULL OR
                        "incomeSource" = 'N/A' OR "incomeSource" IS NULL OR
                        "industry" = 'N/A' OR "industry" IS NULL
                    )
                    """,
                    (
                        detail["address_province"],
                        detail["income_source"],
                        detail["industry"],
                        external_id,
                        BRAND,
                    ),
                )
                kyc_updated += cur.rowcount
        tgt_conn.commit()
        print(f"[Repair] KYC back-fill done. updated={kyc_updated} players", flush=True)

    # Re-insert wallet transactions
    print("\n[Repair] Deleting existing wallet records for re-insertion...", flush=True)
    if not dry_run:
        with tgt_conn.cursor() as cur:
            cur.execute('DELETE FROM kemet."walletTransaction_final" WHERE "platform" = %s', (WALLET_PLATFORM,))
        wt_deleted = cur.rowcount
        tgt_conn.commit()
        print(f"  Deleted {wt_deleted} walletTransaction rows", flush=True)
    else:
        print("[DRY-RUN] Would delete walletTransaction rows and re-insert", flush=True)

    player_map = build_player_map(tgt_conn) if not dry_run else {}
    for kind, table in [("deposit", DEPOSITS_SOURCE_TABLE), ("withdrawal", WITHDRAWALS_SOURCE_TABLE)]:
        print(f"[Repair] Re-inserting {kind}s from {table} ...", flush=True)
        processed_w = 0
        last_dt, last_id = None, None
        while True:
            rows = fetch_json_table_batch(src_conn, table, last_dt, last_id, batch_size)
            if not rows:
                break
            insert_wallet_batch(tgt_conn, rows, kind, player_map, dry_run=dry_run)
            processed_w += len(rows)
            last_row_data = as_dict(rows[-1].get("data"))
            last_id = str(rows[-1]["id"])
            last_dt = source_dt_value(last_row_data, table)
            if not dry_run and (processed_w % commit_every) < batch_size:
                tgt_conn.commit()
        if not dry_run:
            tgt_conn.commit()
        print(f"  Re-inserted {kind}: {processed_w} records", flush=True)


def repair_wallet_status(
        src_conn,
        tgt_conn,
        dry_run: bool,
        batch_size: int,
        commit_every: int,
) -> None:
    print("\n[Repair-Status] Syncing walletTransaction status fields with source tables ...", flush=True)
    for kind, table in [("deposit", DEPOSITS_SOURCE_TABLE), ("withdrawal", WITHDRAWALS_SOURCE_TABLE)]:
        processed = 0
        updated = 0
        last_dt, last_id = None, None
        while True:
            rows = fetch_json_table_batch(src_conn, table, last_dt, last_id, batch_size)
            if not rows:
                break
            for r in rows:
                src_id = str(r["id"])
                data = as_dict(r.get("data"))
                raw_status = normalize_wallet_status(data.get("status"))
                ref = data.get("id") or src_id
                reference_id = str(ref).strip() if ref is not None else f"{kind}:{src_id}"

                confirmed = None
                cancelled = None
                failed = None
                if raw_status == "confirmed":
                    confirmed_dt = parse_iso_dt(data.get("dateTimeConfirmed")) or parse_iso_dt(
                        data.get("dateTimeCreated") or data.get("createdDateTime"))
                    confirmed = confirmed_dt
                elif raw_status == "cancelled":
                    created_dt = parse_iso_dt(
                        data.get("dateTimeCreated") or data.get("createdDateTime")) or datetime.now(timezone.utc)
                    cancelled = created_dt
                elif raw_status == "failed":
                    created_dt = parse_iso_dt(
                        data.get("dateTimeCreated") or data.get("createdDateTime")) or datetime.now(timezone.utc)
                    failed = created_dt

                if not dry_run:
                    with tgt_conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE kemet."walletTransaction_final"
                            SET status = %s,
                                "confirmedDatetime" = %s,
                                "cancelledDatetime" = %s,
                                "failedDatetime" = %s,
                                "updatedAt" = now()
                            WHERE platform = %s AND "referenceId" = %s AND "transactionType" = %s AND status <> %s
                            """,
                            (raw_status, confirmed, cancelled, failed, WALLET_PLATFORM, reference_id, kind, raw_status),
                        )
                        updated += cur.rowcount
                processed += 1

            last_row_data = as_dict(rows[-1].get("data"))
            last_id = str(rows[-1]["id"])
            last_dt = source_dt_value(last_row_data, table)

            if not dry_run and (processed % commit_every) < batch_size:
                tgt_conn.commit()
            print(f"  Progress {kind}s: processed={processed} updated={updated}", flush=True)

        if not dry_run:
            tgt_conn.commit()
        print(f"[repair-status] {kind}s done. processed={processed} updated={updated}", flush=True)

    if dry_run:
        tgt_conn.rollback()
        print("[DRY-RUN] rolled back all writes.", flush=True)


# ----------------------------
# Migrate single user
# ----------------------------

# ---------------------------------------------------------------------------
# Reconciliation checks (easy trace marker)
# ---------------------------------------------------------------------------
def _source_registration_date_filter_sql(date_from: Optional[str], date_to: Optional[str], params: List[Any]) -> str:
    """Build the source date-window SQL for reconciliation checks.

    This mirrors --date-from/--date-to using PlayerRegistrations.createddate.
    Keeping this in one function prevents reconciliation scope from drifting away
    from the migration source-read scope.
    """
    date_col = _source_date_expr_for_table(PLAYER_REGISTRATION_SOURCE_TABLE)
    clauses: List[str] = []
    if date_from is not None:
        clauses.append(f"{date_col} >= %s::timestamptz")
        params.append(date_from)
    if date_to is not None:
        clauses.append(f"{date_col} <= %s::timestamptz")
        params.append(date_to)
    return (" AND " + " AND ".join(clauses)) if clauses else ""


def run_player_reconciliation_checks(
        src_conn,
        tgt_conn,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        dry_run: bool = False,
) -> None:
    """Run player reconciliation checks and write results to CSV, trace, and email summary.

    Checks included:
      1. Source-vs-target audit buckets.
      2. True duplicate summary using SUM(count - 1).
      3. Source usernames missing from target.
      4. Post-phase externalId/card_id validation.
      5. Missing/blank username rows.
      6. Brand mismatch distribution.

    This is intentionally post-player-phase so it can catch cases where source
    card_id exists but target externalId stayed NULL after insert/upsert.
    """
    trace_print("[RECONCILIATION] Starting playerDetails reconciliation checks.")

    source_filter_params: List[Any] = []
    source_filter_sql = _source_registration_date_filter_sql(date_from, date_to, source_filter_params)

    q1 = f"""
        WITH src AS (
            SELECT
                "id" AS source_id,
                NULLIF(TRIM("data"->>'username'), '') AS username,
                "data"
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE "data" IS NOT NULL{source_filter_sql}
        ),
        src_ranked AS (
            SELECT
                source_id,
                username,
                data,
                COUNT(*) OVER (PARTITION BY username) AS source_username_count,
                ROW_NUMBER() OVER (PARTITION BY username ORDER BY source_id) AS source_username_rownum
            FROM src
        ),
        tgt AS (
            SELECT id AS target_id, "userName" AS username, "brandName"
            FROM kemet."playerDetails_final"
            WHERE "brandName" = %s
        )
        SELECT
            CASE
                WHEN s.username IS NULL THEN 'source_missing_username'
                WHEN s.source_username_count > 1 AND t.target_id IS NOT NULL AND s.source_username_rownum = 1 THEN 'duplicate_username_can
onical_target_exists'
                WHEN s.source_username_count > 1 AND t.target_id IS NOT NULL AND s.source_username_rownum > 1 THEN 'duplicate_extra_row_ta
rget_exists'
                WHEN s.source_username_count > 1 AND t.target_id IS NULL THEN 'duplicate_username_target_missing'
                WHEN s.source_username_count = 1 AND t.target_id IS NULL THEN 'unique_username_target_missing'
                WHEN s.source_username_count = 1 AND t.target_id IS NOT NULL THEN 'unique_username_target_exists'
                ELSE 'unknown'
            END AS audit_status,
            COUNT(*) AS source_rows
        FROM src_ranked s
        LEFT JOIN tgt t ON t.username = s.username
        GROUP BY audit_status
        ORDER BY audit_status
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q1, source_filter_params + [BRAND])
        rows = cur.fetchall()
    for row in rows:
        write_reconciliation_row("1_source_vs_target_audit", row["audit_status"], row["source_rows"], "Player source rows classified against target playerDe
tails_final")
    add_reconciliation_summary("Player reconciliation audit buckets written to CSV.")

    q2 = f"""
        WITH dupes AS (
            SELECT
                NULLIF(TRIM("data"->>'username'), '') AS username,
                COUNT(*) AS cnt
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE "data" IS NOT NULL
              AND NULLIF(TRIM("data"->>'username'), '') IS NOT NULL{source_filter_sql}
            GROUP BY NULLIF(TRIM("data"->>'username'), '')
            HAVING COUNT(*) > 1
        )
        SELECT
            COUNT(*) AS duplicate_username_groups,
            COALESCE(SUM(cnt), 0) AS source_rows_in_duplicate_groups,
            COALESCE(SUM(cnt - 1), 0) AS duplicate_extra_rows
        FROM dupes
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q2, source_filter_params)
        dup_summary = cur.fetchone() or {}
    for metric in ("duplicate_username_groups", "source_rows_in_duplicate_groups", "duplicate_extra_rows"):
        write_reconciliation_row("2_duplicate_summary", metric, dup_summary.get(metric), "duplicate_extra_rows is SUM(count - 1), the value to compare again
st source-target row gap")
    add_reconciliation_summary(
        f"Duplicate summary: groups={dup_summary.get('duplicate_username_groups', 0)}, "
        f"extra_rows={dup_summary.get('duplicate_extra_rows', 0)}."
    )

    q3 = f"""
        WITH src AS (
            SELECT
                "id" AS source_id,
                NULLIF(TRIM("data"->>'username'), '') AS username
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE "data" IS NOT NULL
              AND NULLIF(TRIM("data"->>'username'), '') IS NOT NULL{source_filter_sql}
        ),
        src_counts AS (
            SELECT source_id, username, COUNT(*) OVER (PARTITION BY username) AS source_username_count
            FROM src
        ),
        tgt AS (
            SELECT "userName" AS username
            FROM kemet."playerDetails_final"
            WHERE "brandName" = %s
        )
        SELECT
            COUNT(DISTINCT s.username) AS missing_username_count,
            COUNT(*) AS missing_source_rows
        FROM src_counts s
        LEFT JOIN tgt t ON t.username = s.username
        WHERE t.username IS NULL
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q3, source_filter_params + [BRAND])
        missing_summary = cur.fetchone() or {}
    for metric in ("missing_username_count", "missing_source_rows"):
        write_reconciliation_row("3_missing_source_usernames_from_target", metric, missing_summary.get(metric), "Source usernames not found in target player
Details_final for brand")
    add_reconciliation_summary(
        f"Missing target usernames: unique_usernames={missing_summary.get('missing_username_count', 0)}, "
        f"source_rows={missing_summary.get('missing_source_rows', 0)}."
    )

    q4 = f"""
        WITH missing_target AS (
            SELECT "userName" AS username
            FROM kemet."playerDetails_final"
            WHERE "brandName" = %s
              AND ("externalId" IS NULL OR TRIM("externalId"::text) = '')
        ),
        src AS (
            SELECT
                NULLIF(TRIM("data"->>'username'), '') AS username,
                NULLIF(TRIM("data"->>'card_id'), '') AS card_id
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE "data" IS NOT NULL{source_filter_sql}
        )
        SELECT
            CASE
                WHEN NULLIF(TRIM(src.card_id), '') IS NOT NULL THEN 'source_has_card_id_but_target_missing_externalId'
                ELSE 'source_missing_card_id'
            END AS issue_type,
            COUNT(DISTINCT mt.username) AS username_count
        FROM missing_target mt
        LEFT JOIN src ON src.username = mt.username
        GROUP BY issue_type
        ORDER BY issue_type
    """
    with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q4, [BRAND] + source_filter_params)
        ext_rows = cur.fetchall()
    ext_issue_total = 0
    source_has_card_but_missing = 0
    for row in ext_rows:
        count_val = int(row["username_count"] or 0)
        ext_issue_total += count_val
        if row["issue_type"] == "source_has_card_id_but_target_missing_externalId":
            source_has_card_but_missing = count_val
        write_reconciliation_row("4_external_id_post_phase_check", row["issue_type"], count_val, "Post-player-phase externalId/card_id validation")
    write_reconciliation_row(
        "4_external_id_post_phase_check",
        "target_missing_externalId",
        ext_issue_total,
        "Total target playerDetails_final rows with blank externalId; detail rows are written under 4_external_id_post_phase_detail."
    )
    add_reconciliation_summary(
        f"ExternalId post-check: target_missing_externalId={ext_issue_total}, "
        f"source_has_card_id_but_target_missing_externalId={source_has_card_but_missing}."
    )

    q5 = f"""
        SELECT COUNT(*) AS source_rows_missing_username
        FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
        WHERE ("data" IS NULL OR NULLIF(TRIM("data"->>'username'), '') IS NULL){source_filter_sql}
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q5, source_filter_params)
        missing_user_row = cur.fetchone() or {}
    write_reconciliation_row("5_missing_username_rows", "source_rows_missing_username", missing_user_row.get("source_rows_missing_username"), "Rows that can
not be migrated/matched by username")
    add_reconciliation_summary(f"Missing/blank username rows={missing_user_row.get('source_rows_missing_username', 0)}.")

    q6 = f"""
        WITH src AS (
            SELECT DISTINCT NULLIF(TRIM("data"->>'username'), '') AS username
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE "data" IS NOT NULL
              AND NULLIF(TRIM("data"->>'username'), '') IS NOT NULL{source_filter_sql}
        )
        SELECT b."brandName" AS brand_name, COUNT(*) AS matched_usernames
        FROM src s
        JOIN kemet."playerDetails_final" b ON b."userName" = s.username
        GROUP BY b."brandName"
        ORDER BY matched_usernames DESC
    """
    with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q6, source_filter_params)
        brand_rows = cur.fetchall()
    for row in brand_rows:
        write_reconciliation_row("6_brand_mismatch_distribution", row["brand_name"], row["matched_usernames"], "Shows where source usernames matched target
by brandName")
    add_reconciliation_summary(f"Brand distribution rows written to reconciliation CSV: {CSV_RECONCILIATION_PATH}.")
    trace_print(f"[RECONCILIATION] Completed playerDetails checks. csv={CSV_RECONCILIATION_PATH}")

def migrate_single_user(
        src_conn,
        tgt_conn,
        username: str,
        dry_run: bool,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
) -> None:
    SOURCE_QUERY_TRACE.clear()
    username = username.strip()
    print(f"\n[Single User] Migrating targeted username: '{username}'", flush=True)

    from_dt_iso: Optional[str] = date_from.isoformat() if date_from is not None else None
    until_dt_iso: Optional[str] = date_to.isoformat() if date_to is not None else None
    if from_dt_iso or until_dt_iso:
        print(
            "[DATE WINDOW][single-user] Source reads use "
            "PlayerRegistrations.createddate, GameTransaction.GameDate, and "
            "Deposits/Withdrawals.transferDate.",
            flush=True,
        )

    def _append_date_filters(table: str, conditions: List[str], params: List[Any]) -> None:
        date_col = _source_date_expr_for_table(table)
        if from_dt_iso is not None:
            conditions.append(f"{date_col} >= %s::timestamptz")
            params.append(from_dt_iso)
        if until_dt_iso is not None:
            conditions.append(f"{date_col} <= %s::timestamptz")
            params.append(until_dt_iso)

    detail_map = fetch_player_detail_map(src_conn, from_dt_iso, until_dt_iso)
    data_col = quote_ident('data')
    id_col = quote_ident('id')

    # Fetch the canonical registration row. This is the only single-user step
    # allowed to create a missing playerDetails_final record.
    registration_label = f"single-user:{PLAYER_REGISTRATION_SOURCE_TABLE}"
    registration_params: List[Any] = [username]
    registration_conditions = [
        f"{data_col} IS NOT NULL",
        f"COALESCE({data_col}->>'name', {data_col}->>'username', {data_col}->>'userName', {data_col}->>'loginName', {data_col}->>'userid') = %s",
    ]
    _append_date_filters(PLAYER_REGISTRATION_SOURCE_TABLE, registration_conditions, registration_params)
    registration_sql = f"""
            SELECT {id_col}, {data_col}
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE {' AND '.join(registration_conditions)}
            ORDER BY {_source_date_expr_for_table(PLAYER_REGISTRATION_SOURCE_TABLE)} ASC NULLS LAST, {id_col} ASC
            LIMIT 1
            """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        print_source_query(cur, registration_label, registration_sql, registration_params)
        cur.execute(registration_sql, registration_params)
        reg_row = cur.fetchone()
        note_source_query_result(registration_label, 1 if reg_row else 0)

    player_map: Dict[str, uuid.UUID] = build_player_map(tgt_conn)
    player_upserted = 0

    if reg_row:
        data = as_dict(reg_row["data"])
        pid = upsert_player_from_member(tgt_conn, data, BRAND, dry_run=dry_run, detail_map=detail_map, source_id=str(reg_row.get("id") or
"N/A"))
        mapped_username = extract_username(data) or username
        if pid:
            player_map[mapped_username] = pid
            player_upserted = 1
        if dry_run:
            print(f"[DRY-RUN] Would upsert registration data for player username={mapped_username}")
        else:
            print(f"Upserted player entry. id={pid}")
    else:
        record_player_skip(
            source_id="N/A",
            username=username,
            reason=f"No canonical registration row found inside {PLAYER_REGISTRATION_SOURCE_TABLE}",
            dry_run=dry_run,
            source_table=PLAYER_REGISTRATION_SOURCE_TABLE,
            action="skipped",
        )
    print_source_query_summary(registration_label, "single-user playerRegistration phase")

    # Dimension caches for tracking.
    provider_cache: Dict[str, uuid.UUID] = {}
    gametype_cache: Dict[str, uuid.UUID] = {}
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID] = {}

    member_name_expr = (
        f"COALESCE({data_col}->>'PlayerAccount', {data_col}->'member'->>'name', {data_col}->'member'->>'username', "
        f"{data_col}->'member'->>'userName', {data_col}->'member'->>'loginName', "
        f"{data_col}->'member'->>'userid', {data_col}->>'username', {data_col}->>'userName', {data_col}->>'name')"
    )

    game_label = f"single-user:{GAME_TRANSACTION_SOURCE_TABLE}"
    game_params: List[Any] = [username]
    game_conditions = [f"{data_col} IS NOT NULL", f"{member_name_expr} = %s"]
    _append_date_filters(GAME_TRANSACTION_SOURCE_TABLE, game_conditions, game_params)
    game_sql = f"""
            SELECT {id_col}, {data_col}
            FROM {source_table_ref(GAME_TRANSACTION_SOURCE_TABLE)}
            WHERE {' AND '.join(game_conditions)}
            ORDER BY {_source_date_expr_for_table(GAME_TRANSACTION_SOURCE_TABLE)} ASC NULLS LAST, {id_col} ASC
            """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        print_source_query(cur, game_label, game_sql, game_params)
        cur.execute(game_sql, game_params)
        tx_rows = cur.fetchall()
        note_source_query_result(game_label, len(tx_rows))

    attempted_gt, skipped_gt = insert_game_tx_batch(
        tgt_conn, tx_rows, player_map, provider_cache, gametype_cache, gamelist_cache, dry_run=dry_run
    )
    print(f"gameTransaction: attempted_insert={attempted_gt}, skipped={skipped_gt}")
    print_source_query_summary(game_label, "single-user gameTransaction phase")

    deposit_label = f"single-user:{DEPOSITS_SOURCE_TABLE}"
    deposit_params: List[Any] = [username]
    deposit_conditions = [f"{data_col} IS NOT NULL", f"{member_name_expr} = %s"]
    _append_date_filters(DEPOSITS_SOURCE_TABLE, deposit_conditions, deposit_params)
    deposit_sql = f"""
            SELECT {id_col}, {data_col}
            FROM {source_table_ref(DEPOSITS_SOURCE_TABLE)}
            WHERE {' AND '.join(deposit_conditions)}
            ORDER BY {_source_date_expr_for_table(DEPOSITS_SOURCE_TABLE)} ASC NULLS LAST, {id_col} ASC
            """

    withdrawal_label = f"single-user:{WITHDRAWALS_SOURCE_TABLE}"
    withdrawal_params: List[Any] = [username]
    withdrawal_conditions = [f"{data_col} IS NOT NULL", f"{member_name_expr} = %s"]
    _append_date_filters(WITHDRAWALS_SOURCE_TABLE, withdrawal_conditions, withdrawal_params)
    withdrawal_sql = f"""
            SELECT {id_col}, {data_col}
            FROM {source_table_ref(WITHDRAWALS_SOURCE_TABLE)}
            WHERE {' AND '.join(withdrawal_conditions)}
            ORDER BY {_source_date_expr_for_table(WITHDRAWALS_SOURCE_TABLE)} ASC NULLS LAST, {id_col} ASC
            """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        print_source_query(cur, deposit_label, deposit_sql, deposit_params)
        cur.execute(deposit_sql, deposit_params)
        dep_rows = cur.fetchall()
        note_source_query_result(deposit_label, len(dep_rows))

        print_source_query(cur, withdrawal_label, withdrawal_sql, withdrawal_params)
        cur.execute(withdrawal_sql, withdrawal_params)
        wd_rows = cur.fetchall()
        note_source_query_result(withdrawal_label, len(wd_rows))

    attempted_dep, skipped_dep = insert_wallet_batch(tgt_conn, dep_rows, "deposit", player_map, dry_run=dry_run)
    print(f"walletTransaction.deposit: attempted_insert={attempted_dep}, skipped={skipped_dep}")
    print_source_query_summary(deposit_label, "single-user deposits phase")

    attempted_wd, skipped_wd = insert_wallet_batch(tgt_conn, wd_rows, "withdrawal", player_map, dry_run=dry_run)
    print(f"walletTransaction.withdrawal: attempted_insert={attempted_wd}, skipped={skipped_wd}")
    print_source_query_summary(withdrawal_label, "single-user withdrawals phase")

    print(
        "\n[FINAL SUMMARY][single-user] "
        f"playerDetails_final upserted={player_upserted}; "
        f"gameTransaction_final attempted_insert={attempted_gt}, skipped={skipped_gt}; "
        f"walletTransaction_final deposits attempted_insert={attempted_dep}, skipped={skipped_dep}; "
        f"withdrawals attempted_insert={attempted_wd}, skipped={skipped_wd}",
        flush=True,
    )

    if dry_run:
        tgt_conn.rollback()
        print("[DRY-RUN] rolled back single user writes.")
    else:
        tgt_conn.commit()
        print("Committed successfully.")


# ----------------------------
# Migrate-all (phased, checkpointed)
# ----------------------------
def migrate_all(
        src_conn,
        tgt_conn,
        dry_run: bool,
        batch_size: int,
        commit_every: int,
        resume: bool,
        start_after_id: Optional[str],
        max_rows_total: Optional[int],
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
) -> int:
    ensure_wallet_dedupe_index(tgt_conn, dry_run=dry_run)

    from_dt_iso: Optional[str] = date_from.isoformat() if date_from is not None else None
    until_dt_iso: Optional[str] = date_to.isoformat() if date_to is not None else None
    date_window_requested = (from_dt_iso is not None) or (until_dt_iso is not None)

    if date_window_requested:
        print(
            "[DATE WINDOW] --date-from/--date-to enabled. Source reads use "
            "PlayerRegistrations.createddate, GameTransaction.GameDate, and "
            "Deposits/Withdrawals.transferDate. Checkpoints are ignored for source reads in this run.",
            flush=True,
        )

    def _initial_cursor(phase_name: str) -> Tuple[Optional[str], Optional[str]]:
        if date_window_requested:
            return (None, start_after_id)
        if resume:
            cp = checkpoint_get(tgt_conn, phase_name)
            if cp:
                print(f"Resuming phase '{phase_name}' from checkpoint pointer: {cp}", flush=True)
                return parse_inplayv2_checkpoint(cp)
        return (None, start_after_id)

    player_map = build_player_map(tgt_conn)
    print(f"Initial target player mapping lookup loaded: {len(player_map)} items mapped", flush=True)

    provider_cache: Dict[str, uuid.UUID] = {}
    gametype_cache: Dict[str, uuid.UUID] = {}
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID] = {}
    detail_map = fetch_player_detail_map(src_conn, from_dt_iso, until_dt_iso)

    # --------------------
    # Phase 1: Players (registrations)
    # --------------------
    SOURCE_QUERY_AUDIT.pop(PLAYER_REGISTRATION_SOURCE_TABLE, None)
    phase = "player"
    after_dt, after_id = _initial_cursor(phase)
    processed = 0
    player_upserted = 0
    player_inserted_new = 0
    player_updated_existing = 0
    player_skipped = 0
    last_dt, last_id = after_dt, after_id or ""
    while True:
        if max_rows_total is not None and processed >= max_rows_total:
            break
        fetch_limit = batch_size
        if max_rows_total is not None:
            fetch_limit = min(fetch_limit, max_rows_total - processed)

        rows = fetch_json_table_batch(
            src_conn, PLAYER_REGISTRATION_SOURCE_TABLE, last_dt, last_id or None, fetch_limit,
            from_dt=from_dt_iso, until_dt=until_dt_iso,
        )
        if not rows:
            break

        for r in rows:
            rid = str(r["id"])
            data = as_dict(r.get("data"))
            if data:
                try:
                    # FINAL BUSINESS RULE: only this player-registration phase may
                    # insert/upsert playerDetails_final records. Revert by moving
                    # this call back into transaction phases only if ghost players
                    # are intentionally allowed again.
                    username = extract_username(data)
                    was_existing_player = bool(username and username in player_map)
                    pid = upsert_player_from_member(tgt_conn, data, BRAND, dry_run=dry_run, detail_map=detail_map, source_id=str(r.get("id") or "N/A"))
                    if pid and username:
                        player_map[username] = pid
                        player_upserted += 1
                        if was_existing_player:
                            player_updated_existing += 1
                        else:
                            player_inserted_new += 1
                    else:
                        player_skipped += 1
                except Exception as e:
                    player_skipped += 1
                    record_player_skip(
                        source_id=rid,
                        username=username if 'username' in locals() else None,
                        reason="Player upsert failed",
                        dry_run=dry_run,
                        source_table=PLAYER_REGISTRATION_SOURCE_TABLE,
                        action="upsert_failed",
                        error=e,
                        data=data,
                    )
                    tgt_conn.rollback()
            else:
                player_skipped += 1
                record_player_skip(
                    source_id=rid,
                    username=None,
                    reason="Missing or invalid source JSON data",
                    dry_run=dry_run,
                    source_table=PLAYER_REGISTRATION_SOURCE_TABLE,
                    action="skipped",
                    data=data,
                )

            processed += 1
            last_id = rid
            last_dt = source_dt_value(data, PLAYER_REGISTRATION_SOURCE_TABLE)

            if (not dry_run) and (processed % commit_every == 0):
                checkpoint_set(tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run)
                tgt_conn.commit()

        print(f"Progress players: processed={processed} new={player_inserted_new} updated={player_updated_existing} upserted={player_upserted} skipped={play
er_skipped} lastId={last_id}", flush=True)

    if not dry_run:
        if processed > 0:
            checkpoint_set(tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run)
        tgt_conn.commit()
    emit_player_phase_summary(processed, player_upserted, player_skipped)
    print_source_query_summary(PLAYER_REGISTRATION_SOURCE_TABLE, "playerRegistration phase")

    # Re-verify tracking maps to align across ongoing transactional sequences.
    # In dry-run mode, keep the in-memory IDs created above so transaction phases
    # simulate the same mapping that a real run would have after the player phase.
    if not dry_run:
        player_map = build_player_map(tgt_conn)

    # --------------------
    # Phase 2: gameTransaction
    # --------------------
    SOURCE_QUERY_AUDIT.pop(GAME_TRANSACTION_SOURCE_TABLE, None)
    phase = "gameTx"
    after_dt, after_id = _initial_cursor(phase)
    processed_gt = 0
    inserted_gt_total = 0
    skipped_gt_total = 0
    last_dt, last_id = after_dt, after_id or ""
    while True:
        if max_rows_total is not None and processed_gt >= max_rows_total:
            break
        fetch_limit = batch_size
        if max_rows_total is not None:
            fetch_limit = min(fetch_limit, max_rows_total - processed_gt)

        rows = fetch_json_table_batch(
            src_conn, GAME_TRANSACTION_SOURCE_TABLE, last_dt, last_id or None, fetch_limit,
            from_dt=from_dt_iso, until_dt=until_dt_iso,
        )
        if not rows:
            break

        inserted, skipped = insert_game_tx_batch(
            tgt_conn, rows, player_map, provider_cache, gametype_cache, gamelist_cache, dry_run=dry_run
        )
        processed_gt += len(rows)
        inserted_gt_total += inserted
        skipped_gt_total += skipped

        last_row_data = as_dict(rows[-1].get("data"))
        last_id = str(rows[-1]["id"])
        last_dt = source_dt_value(last_row_data, GAME_TRANSACTION_SOURCE_TABLE)

        if not dry_run:
            checkpoint_set(tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run)
            if (processed_gt % commit_every) < batch_size:
                tgt_conn.commit()

        print(
            f"Progress gameTx: processed={processed_gt} attempted_insert={inserted_gt_total} "
            f"skipped={skipped_gt_total} lastId={last_id}",
            flush=True,
        )

    if not dry_run:
        tgt_conn.commit()
    print(
        f"Completed gameTx phase. processed={processed_gt} attempted_insert={inserted_gt_total} skipped={skipped_gt_total}",
        flush=True,
    )
    print_source_query_summary(GAME_TRANSACTION_SOURCE_TABLE, "gameTransaction phase")

    # --------------------
    # Phase 3a: Deposits
    # --------------------
    SOURCE_QUERY_AUDIT.pop(DEPOSITS_SOURCE_TABLE, None)
    phase = "deposits"
    after_dt, after_id = _initial_cursor(phase)
    processed_dep = 0
    inserted_dep_total = 0
    skipped_dep_total = 0
    last_dt, last_id = after_dt, after_id or ""
    while True:
        if max_rows_total is not None and processed_dep >= max_rows_total:
            break
        fetch_limit = batch_size
        if max_rows_total is not None:
            fetch_limit = min(fetch_limit, max_rows_total - processed_dep)

        rows = fetch_json_table_batch(
            src_conn, DEPOSITS_SOURCE_TABLE, last_dt, last_id or None, fetch_limit,
            from_dt=from_dt_iso, until_dt=until_dt_iso,
        )
        if not rows:
            break

        inserted, skipped = insert_wallet_batch(tgt_conn, rows, "deposit", player_map, dry_run=dry_run)
        processed_dep += len(rows)
        inserted_dep_total += inserted
        skipped_dep_total += skipped

        last_row_data = as_dict(rows[-1].get("data"))
        last_id = str(rows[-1]["id"])
        last_dt = source_dt_value(last_row_data, DEPOSITS_SOURCE_TABLE)

        if not dry_run:
            checkpoint_set(tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run)
            if (processed_dep % commit_every) < batch_size:
                tgt_conn.commit()

        print(
            f"Progress deposits: processed={processed_dep} attempted_insert={inserted_dep_total} "
            f"skipped={skipped_dep_total} lastId={last_id}",
            flush=True,
        )

    if not dry_run:
        tgt_conn.commit()
    print(
        f"Completed deposits phase. processed={processed_dep} attempted_insert={inserted_dep_total} skipped={skipped_dep_total}",
        flush=True,
    )
    print_source_query_summary(DEPOSITS_SOURCE_TABLE, "deposits phase")

    # --------------------
    # Phase 3b: Withdrawals
    # --------------------
    SOURCE_QUERY_AUDIT.pop(WITHDRAWALS_SOURCE_TABLE, None)
    phase = "withdrawals"
    after_dt, after_id = _initial_cursor(phase)
    processed_wd = 0
    inserted_wd_total = 0
    skipped_wd_total = 0
    last_dt, last_id = after_dt, after_id or ""
    while True:
        if max_rows_total is not None and processed_wd >= max_rows_total:
            break
        fetch_limit = batch_size
        if max_rows_total is not None:
            fetch_limit = min(fetch_limit, max_rows_total - processed_wd)

        rows = fetch_json_table_batch(
            src_conn, WITHDRAWALS_SOURCE_TABLE, last_dt, last_id or None, fetch_limit,
            from_dt=from_dt_iso, until_dt=until_dt_iso,
        )
        if not rows:
            break

        inserted, skipped = insert_wallet_batch(tgt_conn, rows, "withdrawal", player_map, dry_run=dry_run)
        processed_wd += len(rows)
        inserted_wd_total += inserted
        skipped_wd_total += skipped

        last_row_data = as_dict(rows[-1].get("data"))
        last_id = str(rows[-1]["id"])
        last_dt = source_dt_value(last_row_data, WITHDRAWALS_SOURCE_TABLE)

        if not dry_run:
            checkpoint_set(tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run)
            if (processed_wd % commit_every) < batch_size:
                tgt_conn.commit()

        print(
            f"Progress withdrawals: processed={processed_wd} attempted_insert={inserted_wd_total} "
            f"skipped={skipped_wd_total} lastId={last_id}",
            flush=True,
        )

    if not dry_run:
        tgt_conn.commit()
    print(
        f"Completed withdrawals phase. processed={processed_wd} attempted_insert={inserted_wd_total} skipped={skipped_wd_total}",
        flush=True,
    )
    print_source_query_summary(WITHDRAWALS_SOURCE_TABLE, "withdrawals phase")

    total_inserted = player_inserted_new + inserted_gt_total + inserted_dep_total + inserted_wd_total
    total_skipped = player_skipped + skipped_gt_total + skipped_dep_total + skipped_wd_total
    print(
        "\n[FINAL SUMMARY][migrate-all]\n"
        f"  playerDetails_final new_inserted_from_registration={player_inserted_new} existing_updated_from_registration={player_updated_existing} skipped={p
layer_skipped}\n"
        f"  gameTransaction_final inserted={inserted_gt_total} skipped={skipped_gt_total}\n"
        f"  walletTransaction_final deposits_inserted={inserted_dep_total} deposits_skipped={skipped_dep_total}\n"
        f"  walletTransaction_final withdrawals_inserted={inserted_wd_total} withdrawals_skipped={skipped_wd_total}\n"
        f"  total_inserted={total_inserted} total_skipped={total_skipped}",
        flush=True,
    )

    return processed + processed_gt + processed_dep + processed_wd



# ============================================================================
# Final V1_dev safety overrides requested 2026-05-20
# - Only PlayerRegistrationsInplayV1_dev may create/update playerDetails_final.
# - gameTransaction/walletTransaction never create ghost/shadow players.
# - Source SELECTs are recorded and printed at phase end, not inside row loops.
# - Date filters use createddate/GameDate/transferDate per source phase.
# REVERT NOTE: remove this override block to return to the prior r9 behavior.
# ============================================================================
SOURCE_QUERY_TRACE: Dict[str, Dict[str, Any]] = {}


def _source_date_expr_for_table(table: str) -> str:
    """Build the phase-specific source date expression with quoted source columns."""
    keys = SOURCE_DATE_KEYS_BY_TABLE.get(table)
    if not keys:
        raise ValueError(f"No source date key mapping configured for table: {table}")
    pieces = [f"NULLIF(\"data\"->>'{key}','')" for key in keys]
    raw_expr = pieces[0] if len(pieces) == 1 else f"COALESCE({', '.join(pieces)})"
    return f"({raw_expr})::timestamptz"


def _source_query_for_psql_copy(exact_query: str) -> str:
    """Display source SELECT with table name only and quoted source columns."""
    q = exact_query.strip()
    q = re.sub(r'\bFROM\s+kemet\."', 'FROM "', q, flags=re.I)
    q = re.sub(r'\bJOIN\s+kemet\."', 'JOIN "', q, flags=re.I)
    return q


def print_source_query(cur, label: str, query: str, params: Iterable[Any]) -> None:
    """Record source SQL. Screen output is emitted once at the end of each phase."""
    if not PRINT_SOURCE_SQL:
        return
    params_list = list(params)
    try:
        exact_query = cur.mogrify(query, params_list).decode("utf-8")
    except Exception as e:
        exact_query = f"{query.strip()}\n-- PARAMS: {params_list!r}\n-- mogrify failed: {e}"
    display_query = _source_query_for_psql_copy(exact_query)
    entry = SOURCE_QUERY_TRACE.setdefault(label, {"count": 0, "first": None, "last": None})
    entry["count"] += 1
    if entry["first"] is None:
        entry["first"] = display_query
    entry["last"] = display_query
    logging.info("[SOURCE QUERY][%s] %s", label, " ".join(display_query.split()))


def print_source_query_summary(phase: str, labels: Optional[List[str]] = None, clear: bool = True) -> None:
    """Print source SELECT trace once at phase end."""
    if not PRINT_SOURCE_SQL:
        return
    selected = labels or sorted(SOURCE_QUERY_TRACE.keys())
    printed_any = False
    print(f"\n[SOURCE QUERY SUMMARY][{phase}]", flush=True)
    for label in selected:
        entry = SOURCE_QUERY_TRACE.get(label)
        if not entry:
            continue
        printed_any = True
        count = int(entry.get("count") or 0)
        first_q = entry.get("first") or ""
        last_q = entry.get("last") or ""
        print(f"[SOURCE QUERY SUMMARY][{phase}][{label}] executed_selects={count}", flush=True)
        if count <= 1 or first_q == last_q:
            print(f"exact_query_for_psql:\n{last_q}\n", flush=True)
        else:
            print(f"first_exact_query_for_psql:\n{first_q}\n", flush=True)
            print(f"last_exact_query_for_psql:\n{last_q}\n", flush=True)
        if clear:
            SOURCE_QUERY_TRACE.pop(label, None)
    if not printed_any:
        print("No source SELECT was executed for this phase.", flush=True)


def fetch_json_table_batch(
        src_conn,
        table: str,
        after_dt: Optional[str],
        after_id: Optional[str],
        limit: int,
        from_dt: Optional[str] = None,
        until_dt: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch a deterministic source batch using quoted source columns."""
    allowed_tables = {
        PLAYER_REGISTRATION_SOURCE_TABLE,
        GAME_TRANSACTION_SOURCE_TABLE,
        DEPOSITS_SOURCE_TABLE,
        WITHDRAWALS_SOURCE_TABLE,
    }
    if table not in allowed_tables:
        raise ValueError(f"Unsupported source table: {table}")

    date_col = _source_date_expr_for_table(table)
    anchor_id = after_id or ""
    conditions = ['"data" IS NOT NULL']
    params: List[Any] = []

    if after_dt is not None:
        conditions.append(f"({date_col}, \"id\") > (%s::timestamptz, %s)")
        params.extend([after_dt, anchor_id])
    elif anchor_id:
        conditions.append('"id" > %s')
        params.append(anchor_id)

    if from_dt is not None:
        conditions.append(f"{date_col} >= %s::timestamptz")
        params.append(from_dt)
    if until_dt is not None:
        conditions.append(f"{date_col} <= %s::timestamptz")
        params.append(until_dt)

    params.append(limit)
    where_clause = " AND ".join(conditions)
    query = f"""
            SELECT "id", "data"
            FROM {source_table_ref(table)}
            WHERE {where_clause}
            ORDER BY {date_col} ASC NULLS LAST, "id" ASC
            LIMIT %s
            """

    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        print_source_query(cur, f"{table} batch", query, params)
        cur.execute(query, params)
        rows = cur.fetchall()
        print(f"[SOURCE QUERY RESULT][{table}] rows={len(rows)} limit={limit}", flush=True)
    src_conn.rollback()
    return rows


def fetch_player_detail_map(
        src_conn,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
) -> Dict[str, Dict]:
    """
    Builds a lookup map from source registration data for player KYC/details.

    IMPORTANT: this function intentionally uses positional date_from/date_to
    arguments for the --date-from/--date-to date-window flow. Do not call this
    with from_dt/until_dt keyword arguments.
    """
    print("\n>>> [FUNCTION START]: fetch_player_detail_map", flush=True)
    print(">>> [SQL START]: Selecting from kemet.PlayerRegistrationsInplayV1_dev", flush=True)

    detail_map: Dict[str, Dict] = {}
    date_col = _source_date_expr_for_table(PLAYER_REGISTRATION_SOURCE_TABLE)
    conditions = ['"data" IS NOT NULL']
    params: List[Any] = []

    if date_from is not None:
        conditions.append(f"{date_col} >= %s::timestamptz")
        params.append(date_from)
    if date_to is not None:
        conditions.append(f"{date_col} <= %s::timestamptz")
        params.append(date_to)

    query = f"""
            SELECT "id", "data"->>'username' as uname, "data"
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE {' AND '.join(conditions)}
            """

    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        print_source_query(cur, f"{PLAYER_REGISTRATION_SOURCE_TABLE} detail-map", query, params)
        cur.execute(query, params)
        rows = cur.fetchall()
        for r in rows:
            uname = r.get("uname")
            data = as_dict(r.get("data"))

            if not uname or not data:
                record_player_skip(
                    source_id=r.get("id"),
                    username=uname,
                    reason="Missing username or empty data while building player detail map",
                    dry_run=False,
                    source_table=PLAYER_REGISTRATION_SOURCE_TABLE,
                    action="detail_map_skipped",
                    data=data,
                )
                continue

            detail_map[str(uname)] = {
                "addressProvince": data.get("permanent_address") or data.get("addressProvince") or "N/A",
                "address_province": data.get("permanent_address") or data.get("addressProvince") or "N/A",
                "walletBalance": to_decimal_str(data.get("balance") or "0"),
                "externalId": data.get("card_id") or data.get("cardId") or data.get("id") or data.get("externalId"),
                "outletCode": data.get("outlet_id") or data.get("outletCode"),
                "contactNumber": safe_mobile_10(data.get("contact_number") or data.get("contactNumber")),
                "birthdate": data.get("birthdate") or data.get("birthDay") or data.get("dateOfBirth") or data.get("birthDate"),
                "income_source": data.get("incomeSource") or data.get("income_source") or "Other",
                "industry": data.get("industry") or data.get("natureOfWork") or "Other",
            }

    src_conn.rollback()
    print(f"<<< [SQL END]: Successfully mapped {len(detail_map)} verification records.", flush=True)
    print(">>> [FUNCTION END]: fetch_player_detail_map", flush=True)
    return detail_map

def _skip_reason_missing_player(username: str, registered_usernames: Optional[set]) -> str:
    if registered_usernames is not None and username not in registered_usernames:
        return (
            f"Unable to process no playerRecord; username not found in player_map/playerDetails_final "
            f"and not present in source {PLAYER_REGISTRATION_SOURCE_TABLE}"
        )
    return "Unable to process no playerRecord; username not found in player_map/playerDetails_final after player-registration phase"


def insert_game_tx_batch(
    tgt_conn,
    rows: List[Dict[str, Any]],
    player_map: Dict[str, uuid.UUID],
    provider_cache: Dict[str, uuid.UUID],
    gametype_cache: Dict[str, uuid.UUID],
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID],
    dry_run: bool,
    registered_usernames: Optional[set] = None,
) -> Tuple[int, int]:
    """Insert game transactions only for players already in player_map.

    Duplicate externalId conflicts are not treated as skipped rows. They are
    reported as duplicate_key_ignored in gameTransaction CSV and trace logs.
    """
    values: List[Tuple[Any, ...]] = []
    value_meta: List[Dict[str, Any]] = []
    skipped_rows = 0

    for r in rows:
        source_id = str(r.get("id") or r.get("src_id") or "").strip()
        data = as_dict(r.get("data"))
        if not source_id:
            skipped_rows += 1
            record_game_skip(None, None, None, "Missing source row id", dry_run, data=data)
            continue
        if not data:
            skipped_rows += 1
            record_game_skip(source_id, None, None, "Missing or invalid source JSON data", dry_run, data=data)
            continue

        member = as_dict(data.get("member"))
        game = as_dict(data.get("game"))
        external_id = str(_first_present(data, "TransactionID", "transactionId", "externalId", "id") or source_id).strip()
        if not external_id:
            skipped_rows += 1
            record_game_skip(source_id, None, None, "Missing game transaction externalId/id", dry_run, data=data)
            continue

        username = str(
            _first_present(data, "PlayerAccount", "playerAccount", "username", "userName", "name", "loginName", "userid", "userId")
            or extract_username(member)
            or ""
        ).strip()
        if not username:
            skipped_rows += 1
            record_game_skip(source_id, external_id, None, "Missing username in game transaction source payload", dry_run, data=data)
            continue

        player_id = player_map.get(username)
        if not player_id:
            skipped_rows += 1
            record_game_skip(source_id, external_id, username, _skip_reason_missing_player(username, registered_usernames), dry_run, data=
data)
            continue

        provider_name = str(_first_present(data, "GameProvider", "gameProvider", "provider", "Provider") or game.get("provider") or "UNKNOWN").strip() or "U
NKNOWN"
        game_name = str(_first_present(data, "GameName", "gameName", "name", "GameTitle") or game.get("name") or "UNKNOWN").strip() or "UNKNOWN"
        game_type_raw = str(_first_present(data, "GameType", "gameType", "type") or game.get("type") or "Slots")

        provider_id = get_or_create_game_provider(tgt_conn, provider_name, provider_cache, dry_run)
        game_type_id = get_or_create_game_type(tgt_conn, game_type_raw, gametype_cache, dry_run)
        game_id = get_or_create_game_list(tgt_conn, game_name, provider_id, game_type_id, gamelist_cache, dry_run)

        start_dt = parse_iso_dt(_first_present(data, "GameDate", "gameDate", "gamedate", "dateTimeCreated", "createdDateTime")) or datetime.now(timezone.utc
)
        end_dt = parse_iso_dt(_first_present(data, "UpdateDateTime", "updatedAt", "dateTimeSettled", "settledDate", "GameDate")) or start_dt
        bet_amount = to_decimal_str(_first_present(data, "TotalStakes", "bet", "betAmount", "stake"))
        payout_amount = to_decimal_str(_first_present(data, "TotalWins", "payout", "payoutAmount", "win"))
        valid_bet = to_decimal_str(_first_present(data, "ValidBet", "validBet", "TotalStakes", "bet", "betAmount", "stake"))
        pc1, pc2, pc3, pc4 = (to_decimal_str(data.get(k)) for k in ("PC1", "PC2", "PC3", "PC4"))
        pc5 = to_decimal_str(_first_present(data, "PC5", "jackpotContribution"))
        jw1, jw2, jw3, jw4 = (to_decimal_str(data.get(k)) for k in ("JW1", "JW2", "JW3", "JW4"))
        jw5 = to_decimal_str(_first_present(data, "JW5", "jackpotPayout"))
        progression_paid = to_decimal_str(_first_present(data, "PROGRESSIVE_CONTRIBUTION_PAID", "progressionContributionPaid"))
        seed_won = to_decimal_str(_first_present(data, "SEED_MONEY_WON", "seedMoneyWon"))
        outlet = str(_first_present(data, "Outlet", "outlet", "tableRoomId") or "").strip() or None
        round_id = str(_first_present(data, "SessionID", "sessionId", "vendorRoundId", "roundId") or "").strip() or None

        seed_over_raw = data.get("SEED_MONEY_JACKPOT_WON_OVER_1000") or data.get("seedMoneyJackpotOver1000")
        try:
            seed_over_1000_bool = bool(int(float(seed_over_raw or 0)))
        except Exception:
            seed_over_1000_bool = False
        seed_over_1000_int = 1 if seed_over_1000_bool else 0

        values.append((
            start_dt, provider_id, game_id, game_type_id, player_id,
            username, outlet, "0", bet_amount, bet_amount, payout_amount,
            pc1, pc2, pc3, pc4, pc5,
            jw1, jw2, jw3, jw4, jw5,
            progression_paid, seed_won, seed_over_1000_int,
            end_dt, external_id, False, None, None,
            BRAND, PLATFORM, round_id,
        ))
        value_meta.append({
            "sourceId": source_id,
            "externalId": external_id,
            "username": username,
            "data": data,
        })

    if skipped_rows:
        trace_print(f"[SUMMARY][gameTransaction] insertable={len(values)} skipped={skipped_rows}")
    if dry_run:
        trace_print(f"[DRY-RUN] Would insert {len(values)} gameTransaction_final rows. skipped={skipped_rows}")
        return (len(values), skipped_rows)
    if not values:
        return (0, skipped_rows)

    sql = """
    INSERT INTO kemet."gameTransaction_final" (
        "startDateTime", "providerId", "gameId", "gameTypeId", "playerId",
        "playerUserName", "tableRoomId", "sideBetAmount", "betAmount", "validBet",
        "payoutAmount", "PC1","PC2","PC3","PC4","PC5",
        "JW1","JW2","JW3","JW4","JW5",
        "progressionContributionPaid", "seedMoneyWon", "seedMoneyJackpotOver1000",
        "endDateTime", "externalId", "parlay", "betDetails", "betTiming",
        "brand", "platform", "roundId"
    )
    VALUES %s
    ON CONFLICT ("externalId") DO NOTHING
    RETURNING "externalId"
    """
    with tgt_conn.cursor() as cur:
        inserted_rows = execute_values(cur, sql, values, page_size=500, fetch=True)

    inserted_counts: Dict[str, int] = {}
    for row in inserted_rows or []:
        inserted_ref = str(row[0])
        inserted_counts[inserted_ref] = inserted_counts.get(inserted_ref, 0) + 1

    duplicate_count = 0
    for meta in value_meta:
        ext = str(meta["externalId"])
        if inserted_counts.get(ext, 0) > 0:
            inserted_counts[ext] -= 1
            continue
        duplicate_count += 1
        record_game_report(
            source_id=meta.get("sourceId"),
            external_id=ext,
            username=meta.get("username"),
            reason="Duplicate gameTransaction externalId encountered; target row already existed and ON CONFLICT DO NOTHING ignored this source row",
            dry_run=dry_run,
            action="duplicate_key_ignored",
            issue_type="duplicate_external_id_ignored",
            data=meta.get("data") or {},
        )

    inserted_count = len(inserted_rows or [])
    trace_print(f"[LIVE] Inserted {inserted_count} gameTransaction_final rows. duplicates={duplicate_count} skipped={skipped_rows}")
    return (inserted_count, skipped_rows)

def insert_game_transactions_batch(
    tgt_conn,
    rows: List[Dict[str, Any]],
    player_map: Dict[str, uuid.UUID],
    provider_cache: Dict[str, uuid.UUID],
    gametype_cache: Dict[str, uuid.UUID],
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID],
    dry_run: bool,
    registered_usernames: Optional[set] = None,
) -> Tuple[int, int]:
    return insert_game_tx_batch(
        tgt_conn, rows, player_map, provider_cache, gametype_cache, gamelist_cache,
        dry_run=dry_run, registered_usernames=registered_usernames,
    )


def wallet_row_to_values(
        tgt_conn,
        kind: str,
        src_id: str,
        data: Dict[str, Any],
        player_map: Dict[str, uuid.UUID],
        dry_run: bool,
        registered_usernames: Optional[set] = None,
) -> Optional[Tuple[Any, ...]]:
    member = as_dict(data.get("member"))
    username = str(
        _first_present(data, "username", "userName", "name", "loginName", "userid", "userId", "PlayerAccount", "playerAccount")
        or extract_username(member)
        or ""
    ).strip()
    if not username:
        record_wallet_skip(kind, src_id, None, "Missing username in wallet source payload", dry_run)
        return None
    player_id = player_map.get(username)
    if not player_id:
        record_wallet_skip(kind, src_id, username, _skip_reason_missing_player(username, registered_usernames), dry_run)
        return None
    ref_id = str(_first_present(data, "id", "referenceId", "transactionId") or src_id or "").strip()
    if not ref_id:
        record_wallet_skip(kind, src_id, username, "Missing wallet referenceId/id", dry_run)
        return None
    amount_raw = _first_present(data, "amount", "TotalAmount", "totalAmount")
    try:
        amount = abs(float(amount_raw or 0))
    except Exception:
        record_wallet_skip(kind, src_id, username, f"Invalid amount value: {amount_raw!r}", dry_run)
        return None
    raw_date = _first_present(data, "transferDate", "transferdate", "TransferDate")
    t_date = parse_iso_dt(raw_date)
    if t_date is None:
        t_date = datetime(1970, 1, 1, tzinfo=timezone.utc)
        print(f"[WARN][walletTransaction.{kind}] sourceId={src_id} username={username} missing transferDate; defaulting createdDatetime to 1970-01-01", flus
h=True)
    payment_gateway = str(_first_present(data, "payment", "paymentMethod", "paymentGateway") or "N/A")
    return (
        kind.lower(), WALLET_PLATFORM, player_id, payment_gateway, "www.inplay.com.ph",
        amount, "confirmed", None, t_date, t_date, None, None, ref_id, t_date,
    )


def insert_wallet_batch(
    tgt_conn,
    rows: List[Dict[str, Any]],
    kind: str,
    player_map: Dict[str, uuid.UUID],
    dry_run: bool,
    registered_usernames: Optional[set] = None,
) -> Tuple[int, int]:
    """Insert wallet transactions only when the player already exists in player_map.

    Duplicate (platform, referenceId) conflicts are not treated as skipped rows.
    They are reported as duplicate_key_ignored in deposit/withdrawal CSV and trace logs.
    """
    values: List[Tuple[Any, ...]] = []
    value_meta: List[Dict[str, Any]] = []
    skipped_rows = 0
    for r in rows:
        src_id = str(r.get("id") or "").strip()
        data = as_dict(r.get("data"))
        if not src_id:
            skipped_rows += 1
            record_wallet_skip(kind, None, None, "Missing source row id", dry_run, data=data)
            continue
        if not data:
            skipped_rows += 1
            record_wallet_skip(kind, src_id, None, "Missing or invalid source JSON data", dry_run, data=data)
            continue
        v = wallet_row_to_values(tgt_conn, kind, src_id, data, player_map, dry_run=dry_run, registered_usernames=registered_usernames)
        if v:
            values.append(v)
            value_meta.append({
                "sourceId": src_id,
                "referenceId": v[12],
                "username": str(
                    _first_present(data, "username", "userName", "name", "loginName", "userid", "userId", "PlayerAccount", "playerAccount"
)
                    or extract_username(as_dict(data.get("member")))
                    or ""
                ).strip(),
                "data": data,
            })
        else:
            skipped_rows += 1

    if skipped_rows:
        trace_print(f"[SUMMARY][walletTransaction.{kind}] insertable={len(values)} skipped={skipped_rows}")
    if dry_run:
        trace_print(f"[DRY-RUN] Would insert {len(values)} walletTransaction_final rows for {kind}. skipped={skipped_rows}")
        return (len(values), skipped_rows)
    if not values:
        return (0, skipped_rows)

    sql = f"""
    INSERT INTO kemet."walletTransaction_final" (
        "transactionType", "platform", "playerId", "paymentGateway", "domain",
        "amount", "status", "bettingPhase", "createdDatetime", "confirmedDatetime",
        "cancelledDatetime", "failedDatetime", "referenceId", "updatedAt"
    )
    VALUES %s
    ON CONFLICT ("platform", "referenceId")
    WHERE ("platform" = '{WALLET_PLATFORM}' AND "referenceId" IS NOT NULL) DO NOTHING
    RETURNING "referenceId"
    """
    with tgt_conn.cursor() as cur:
        inserted_rows = execute_values(cur, sql, values, page_size=1000, fetch=True)

    inserted_counts: Dict[str, int] = {}
    for row in inserted_rows or []:
        inserted_ref = str(row[0])
        inserted_counts[inserted_ref] = inserted_counts.get(inserted_ref, 0) + 1

    duplicate_count = 0
    for meta in value_meta:
        ref = str(meta["referenceId"])
        if inserted_counts.get(ref, 0) > 0:
            inserted_counts[ref] -= 1
            continue
        duplicate_count += 1
        record_wallet_report(
            kind=kind,
            source_id=meta.get("sourceId"),
            username=meta.get("username"),
            reference_id=ref,
            reason="Duplicate walletTransaction platform/referenceId encountered; target row already existed and ON CONFLICT DO NOTHING ignored this source
row",
            dry_run=dry_run,
            action="duplicate_key_ignored",
            issue_type="duplicate_reference_id_ignored",
            data=meta.get("data") or {},
        )

    inserted_count = len(inserted_rows or [])
    trace_print(f"[LIVE] Inserted {inserted_count} walletTransaction_final rows for {kind}. duplicates={duplicate_count} skipped={skipped_rows}")
    return (inserted_count, skipped_rows)


# ---------------------------------------------------------------------------
# Reconciliation checks (easy trace marker)
# ---------------------------------------------------------------------------
def _source_registration_date_filter_sql(date_from: Optional[str], date_to: Optional[str], params: List[Any]) -> str:
    """Build the source date-window SQL for reconciliation checks.

    This mirrors --date-from/--date-to using PlayerRegistrations.createddate.
    Keeping this in one function prevents reconciliation scope from drifting away
    from the migration source-read scope.
    """
    date_col = _source_date_expr_for_table(PLAYER_REGISTRATION_SOURCE_TABLE)
    clauses: List[str] = []
    if date_from is not None:
        clauses.append(f"{date_col} >= %s::timestamptz")
        params.append(date_from)
    if date_to is not None:
        clauses.append(f"{date_col} <= %s::timestamptz")
        params.append(date_to)
    return (" AND " + " AND ".join(clauses)) if clauses else ""



def _emit_player_reconciliation_trace_details(
        src_conn,
        tgt_conn,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
) -> Dict[str, int]:
    """Write traceable reconciliation detail rows without changing migration behavior."""
    counts: Dict[str, int] = {
        "source_vs_target_detail": 0,
        "missing_target_detail": 0,
        "source_duplicate_username_detail": 0,
        "source_duplicate_normalized_username_detail": 0,
        "target_duplicate_normalized_username_detail": 0,
        "external_id_missing_detail": 0,
        "missing_username_detail": 0,
    }
    filter_params: List[Any] = []
    filter_sql = _source_registration_date_filter_sql(date_from, date_to, filter_params)
    source_username_expr = "NULLIF(TRIM(COALESCE(\"data\"->>'username', \"data\"->>'name', \"data\"->>'userName', \"data\"->>'loginName', \"data\"->>'userid
', \"data\"->>'userId')), '')"

    q_detail = f"""
        WITH src AS (
            SELECT
                "id" AS source_id,
                {source_username_expr} AS source_username,
                LOWER(TRIM({source_username_expr})) AS source_username_norm,
                NULLIF(TRIM("data"->>'card_id'), '') AS source_card_id
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE "data" IS NOT NULL{filter_sql}
        ), src_ranked AS (
            SELECT
                source_id,
                source_username,
                source_username_norm,
                source_card_id,
                COUNT(*) OVER (PARTITION BY source_username) AS source_exact_count,
                COUNT(*) OVER (PARTITION BY source_username_norm) AS source_norm_count
            FROM src
        ), tgt AS (
            SELECT
                id AS target_id,
                "userName" AS target_username,
                LOWER(TRIM("userName")) AS target_username_norm,
                "externalId" AS target_external_id,
                "brandName",
                COUNT(*) OVER (PARTITION BY "userName") AS target_exact_count,
                COUNT(*) OVER (PARTITION BY LOWER(TRIM("userName"))) AS target_norm_count
            FROM kemet."playerDetails_final"
            WHERE "brandName" = %s
        )
        SELECT
            CASE
                WHEN s.source_username IS NULL THEN 'source_missing_username'
                WHEN t.target_id IS NULL THEN 'not_inserted_or_missing_target'
                WHEN s.source_exact_count > 1 THEN 'duplicate_source_username_exact'
                WHEN s.source_norm_count > 1 THEN 'duplicate_source_username_normalized'
                WHEN t.target_norm_count > 1 THEN 'duplicate_target_username_normalized'
                ELSE 'matched'
            END AS status,
            s.source_id, s.source_username, s.source_username_norm, s.source_card_id,
            s.source_exact_count, s.source_norm_count,
            t.target_id, t.target_username, t.target_username_norm, t.target_external_id,
            t.target_exact_count, t.target_norm_count
        FROM src_ranked s
        LEFT JOIN tgt t ON t.target_username = s.source_username
        WHERE s.source_username IS NULL
           OR t.target_id IS NULL
           OR s.source_exact_count > 1
           OR s.source_norm_count > 1
           OR COALESCE(t.target_norm_count, 0) > 1
        ORDER BY s.source_username_norm NULLS FIRST, s.source_id, t.target_id
    """
    with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q_detail, filter_params + [BRAND])
        detail_rows = cur.fetchall()
    for row in detail_rows:
        status = row.get("status") or "detail"
        if status == "not_inserted_or_missing_target":
            counts["missing_target_detail"] += 1
        counts["source_vs_target_detail"] += 1
        write_reconciliation_trace_row(
            check_name="1_source_vs_target_audit_detail",
            status=status,
            source_table=PLAYER_REGISTRATION_SOURCE_TABLE,
            source_id=row.get("source_id"),
            source_username=row.get("source_username"),
            source_username_normalized=row.get("source_username_norm"),
            source_card_id=row.get("source_card_id"),
            source_duplicate_count=row.get("source_exact_count") or row.get("source_norm_count"),
            target_table="playerDetails_final",
            target_id=row.get("target_id"),
            target_username=row.get("target_username"),
            target_username_normalized=row.get("target_username_norm"),
            target_external_id=row.get("target_external_id"),
            target_duplicate_count=row.get("target_exact_count") or row.get("target_norm_count"),
            reason="Trace row for reconciliation bucket; includes source/target IDs and usernames.",
        )

    q_source_dupe_norm = f"""
        WITH src AS (
            SELECT
                "id" AS source_id,
                {source_username_expr} AS source_username,
                LOWER(TRIM({source_username_expr})) AS source_username_norm,
                NULLIF(TRIM("data"->>'card_id'), '') AS source_card_id
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE "data" IS NOT NULL{filter_sql}
        ), dupes AS (
            SELECT source_username_norm
            FROM src
            WHERE source_username_norm IS NOT NULL
            GROUP BY source_username_norm
            HAVING COUNT(*) > 1
        )
        SELECT s.*, COUNT(*) OVER (PARTITION BY s.source_username_norm) AS source_duplicate_count
        FROM src s
        JOIN dupes d ON d.source_username_norm = s.source_username_norm
        ORDER BY s.source_username_norm, s.source_username, s.source_id
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q_source_dupe_norm, filter_params)
        rows = cur.fetchall()
    for row in rows:
        counts["source_duplicate_normalized_username_detail"] += 1
        write_reconciliation_trace_row(
            check_name="2b_duplicate_source_username_normalized_detail",
            status="duplicate_source_username_normalized",
            source_table=PLAYER_REGISTRATION_SOURCE_TABLE,
            source_id=row.get("source_id"),
            source_username=row.get("source_username"),
            source_username_normalized=row.get("source_username_norm"),
            source_card_id=row.get("source_card_id"),
            source_duplicate_count=row.get("source_duplicate_count"),
            reason="Normalized duplicate source username; catches case/space variants such as abc vs ABC.",
        )

    q_source_dupe_exact = f"""
        WITH src AS (
            SELECT
                "id" AS source_id,
                {source_username_expr} AS source_username,
                LOWER(TRIM({source_username_expr})) AS source_username_norm,
                NULLIF(TRIM("data"->>'card_id'), '') AS source_card_id
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE "data" IS NOT NULL{filter_sql}
        ), dupes AS (
            SELECT source_username
            FROM src
            WHERE source_username IS NOT NULL
            GROUP BY source_username
            HAVING COUNT(*) > 1
        )
        SELECT s.*, COUNT(*) OVER (PARTITION BY s.source_username) AS source_duplicate_count
        FROM src s
        JOIN dupes d ON d.source_username = s.source_username
        ORDER BY s.source_username, s.source_id
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q_source_dupe_exact, filter_params)
        rows = cur.fetchall()
    for row in rows:
        counts["source_duplicate_username_detail"] += 1
        write_reconciliation_trace_row(
            check_name="2_duplicate_source_username_detail",
            status="duplicate_source_username_exact",
            source_table=PLAYER_REGISTRATION_SOURCE_TABLE,
            source_id=row.get("source_id"),
            source_username=row.get("source_username"),
            source_username_normalized=row.get("source_username_norm"),
            source_card_id=row.get("source_card_id"),
            source_duplicate_count=row.get("source_duplicate_count"),
            reason="Exact duplicate source username; each source row is listed for manual tracing.",
        )

    q_target_dupe_norm = """
        WITH tgt AS (
            SELECT
                id AS target_id,
                "userName" AS target_username,
                LOWER(TRIM("userName")) AS target_username_norm,
                "externalId" AS target_external_id,
                "brandName"
            FROM kemet."playerDetails_final"
            WHERE "brandName" = %s
        ), dupes AS (
            SELECT target_username_norm
            FROM tgt
            WHERE target_username_norm IS NOT NULL
            GROUP BY target_username_norm
            HAVING COUNT(*) > 1
        )
        SELECT t.*, COUNT(*) OVER (PARTITION BY t.target_username_norm) AS target_duplicate_count
        FROM tgt t
        JOIN dupes d ON d.target_username_norm = t.target_username_norm
        ORDER BY t.target_username_norm, t.target_username, t.target_id
    """
    with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q_target_dupe_norm, [BRAND])
        rows = cur.fetchall()
    for row in rows:
        counts["target_duplicate_normalized_username_detail"] += 1
        write_reconciliation_trace_row(
            check_name="2c_duplicate_target_username_normalized_detail",
            status="duplicate_target_username_normalized",
            target_table="playerDetails_final",
            target_id=row.get("target_id"),
            target_username=row.get("target_username"),
            target_username_normalized=row.get("target_username_norm"),
            target_external_id=row.get("target_external_id"),
            target_duplicate_count=row.get("target_duplicate_count"),
            reason="Normalized duplicate target username; target IDs are listed to support canonical-ID decisions.",
        )

    q_external_detail = f"""
        WITH mt AS (
            SELECT
                id AS target_id,
                "userName" AS target_username,
                LOWER(TRIM("userName")) AS target_username_norm,
                "externalId" AS target_external_id
            FROM kemet."playerDetails_final"
            WHERE "brandName" = %s
              AND ("externalId" IS NULL OR TRIM("externalId"::text) = '')
        ), src AS (
            SELECT
                "id" AS source_id,
                {source_username_expr} AS source_username,
                LOWER(TRIM({source_username_expr})) AS source_username_norm,
                NULLIF(TRIM("data"->>'card_id'), '') AS source_card_id,
                COUNT(*) OVER (PARTITION BY LOWER(TRIM({source_username_expr}))) AS source_username_norm_count
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE "data" IS NOT NULL{filter_sql}
        )
        SELECT
            CASE
                WHEN src.source_id IS NULL THEN 'target_missing_externalId_source_username_not_found'
                WHEN src.source_card_id IS NOT NULL THEN 'target_missing_externalId_source_has_card_id'
                ELSE 'target_missing_externalId_source_missing_card_id'
            END AS status,
            src.source_id, src.source_username, src.source_username_norm, src.source_card_id, src.source_username_norm_count,
            mt.target_id, mt.target_username, mt.target_username_norm, mt.target_external_id
        FROM mt
        LEFT JOIN src ON src.source_username_norm = mt.target_username_norm
        ORDER BY mt.target_username_norm, mt.target_username, src.source_username, src.source_id
    """
    with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q_external_detail, [BRAND] + filter_params)
        rows = cur.fetchall()
    for row in rows:
        counts["external_id_missing_detail"] += 1
        write_reconciliation_trace_row(
            check_name="4_external_id_post_phase_detail",
            status=row.get("status"),
            source_table=PLAYER_REGISTRATION_SOURCE_TABLE,
            source_id=row.get("source_id"),
            source_username=row.get("source_username"),
            source_username_normalized=row.get("source_username_norm"),
            source_card_id=row.get("source_card_id"),
            source_duplicate_count=row.get("source_username_norm_count"),
            target_table="playerDetails_final",
            target_id=row.get("target_id"),
            target_username=row.get("target_username"),
            target_username_normalized=row.get("target_username_norm"),
            target_external_id=row.get("target_external_id"),
            reason="Target player externalId is blank; source username/sourceId/source card_id are included when a normalized source username match exists."
,
        )

    q_missing_username = f"""
        SELECT "id" AS source_id, "data"->>'card_id' AS source_card_id
        FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
        WHERE ("data" IS NULL OR {source_username_expr} IS NULL){filter_sql}
        ORDER BY "id"
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q_missing_username, filter_params)
        rows = cur.fetchall()
    for row in rows:
        counts["missing_username_detail"] += 1
        write_reconciliation_trace_row(
            check_name="5_missing_username_rows_detail",
            status="source_missing_username",
            source_table=PLAYER_REGISTRATION_SOURCE_TABLE,
            source_id=row.get("source_id"),
            source_card_id=row.get("source_card_id"),
            reason="Source row has no usable username and cannot be matched/inserted by username.",
        )

    return counts

def run_player_reconciliation_checks(
        src_conn,
        tgt_conn,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        dry_run: bool = False,
) -> None:
    """Run player reconciliation checks and write results to CSV, trace, and email summary.

    Checks included:
      1. Source-vs-target audit buckets.
      2. True duplicate summary using SUM(count - 1).
      3. Source usernames missing from target.
      4. Post-phase externalId/card_id validation.
      5. Missing/blank username rows.
      6. Brand mismatch distribution.

    This is intentionally post-player-phase so it can catch cases where source
    card_id exists but target externalId stayed NULL after insert/upsert.
    """
    trace_print("[RECONCILIATION] Starting playerDetails reconciliation checks.")

    source_filter_params: List[Any] = []
    source_filter_sql = _source_registration_date_filter_sql(date_from, date_to, source_filter_params)

    q1 = f"""
        WITH src AS (
            SELECT
                "id" AS source_id,
                NULLIF(TRIM("data"->>'username'), '') AS username,
                "data"
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE "data" IS NOT NULL{source_filter_sql}
        ),
        src_ranked AS (
            SELECT
                source_id,
                username,
                data,
                COUNT(*) OVER (PARTITION BY username) AS source_username_count,
                ROW_NUMBER() OVER (PARTITION BY username ORDER BY source_id) AS source_username_rownum
            FROM src
        ),
        tgt AS (
            SELECT id AS target_id, "userName" AS username, "brandName"
            FROM kemet."playerDetails_final"
            WHERE "brandName" = %s
        )
        SELECT
            CASE
                WHEN s.username IS NULL THEN 'source_missing_username'
                WHEN s.source_username_count > 1 AND t.target_id IS NOT NULL AND s.source_username_rownum = 1 THEN 'duplicate_username_can
onical_target_exists'
                WHEN s.source_username_count > 1 AND t.target_id IS NOT NULL AND s.source_username_rownum > 1 THEN 'duplicate_extra_row_ta
rget_exists'
                WHEN s.source_username_count > 1 AND t.target_id IS NULL THEN 'duplicate_username_target_missing'
                WHEN s.source_username_count = 1 AND t.target_id IS NULL THEN 'unique_username_target_missing'
                WHEN s.source_username_count = 1 AND t.target_id IS NOT NULL THEN 'unique_username_target_exists'
                ELSE 'unknown'
            END AS audit_status,
            COUNT(*) AS source_rows
        FROM src_ranked s
        LEFT JOIN tgt t ON t.username = s.username
        GROUP BY audit_status
        ORDER BY audit_status
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q1, source_filter_params + [BRAND])
        rows = cur.fetchall()
    for row in rows:
        write_reconciliation_row("1_source_vs_target_audit", row["audit_status"], row["source_rows"], "Player source rows classified against target playerDe
tails_final")
    add_reconciliation_summary("Player reconciliation audit buckets written to CSV.")

    q2 = f"""
        WITH dupes AS (
            SELECT
                NULLIF(TRIM("data"->>'username'), '') AS username,
                COUNT(*) AS cnt
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE "data" IS NOT NULL
              AND NULLIF(TRIM("data"->>'username'), '') IS NOT NULL{source_filter_sql}
            GROUP BY NULLIF(TRIM("data"->>'username'), '')
            HAVING COUNT(*) > 1
        )
        SELECT
            COUNT(*) AS duplicate_username_groups,
            COALESCE(SUM(cnt), 0) AS source_rows_in_duplicate_groups,
            COALESCE(SUM(cnt - 1), 0) AS duplicate_extra_rows
        FROM dupes
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q2, source_filter_params)
        dup_summary = cur.fetchone() or {}
    for metric in ("duplicate_username_groups", "source_rows_in_duplicate_groups", "duplicate_extra_rows"):
        write_reconciliation_row("2_duplicate_summary", metric, dup_summary.get(metric), "duplicate_extra_rows is SUM(count - 1), the value to compare again
st source-target row gap")
    add_reconciliation_summary(
        f"Duplicate summary: groups={dup_summary.get('duplicate_username_groups', 0)}, "
        f"extra_rows={dup_summary.get('duplicate_extra_rows', 0)}."
    )

    q3 = f"""
        WITH src AS (
            SELECT
                "id" AS source_id,
                NULLIF(TRIM("data"->>'username'), '') AS username
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE "data" IS NOT NULL
              AND NULLIF(TRIM("data"->>'username'), '') IS NOT NULL{source_filter_sql}
        ),
        src_counts AS (
            SELECT source_id, username, COUNT(*) OVER (PARTITION BY username) AS source_username_count
            FROM src
        ),
        tgt AS (
            SELECT "userName" AS username
            FROM kemet."playerDetails_final"
            WHERE "brandName" = %s
        )
        SELECT
            COUNT(DISTINCT s.username) AS missing_username_count,
            COUNT(*) AS missing_source_rows
        FROM src_counts s
        LEFT JOIN tgt t ON t.username = s.username
        WHERE t.username IS NULL
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q3, source_filter_params + [BRAND])
        missing_summary = cur.fetchone() or {}
    for metric in ("missing_username_count", "missing_source_rows"):
        write_reconciliation_row("3_missing_source_usernames_from_target", metric, missing_summary.get(metric), "Source usernames not found in target player
Details_final for brand")
    add_reconciliation_summary(
        f"Missing target usernames: unique_usernames={missing_summary.get('missing_username_count', 0)}, "
        f"source_rows={missing_summary.get('missing_source_rows', 0)}."
    )

    q4 = f"""
        WITH missing_target AS (
            SELECT "userName" AS username
            FROM kemet."playerDetails_final"
            WHERE "brandName" = %s
              AND ("externalId" IS NULL OR TRIM("externalId"::text) = '')
        ),
        src AS (
            SELECT
                NULLIF(TRIM("data"->>'username'), '') AS username,
                NULLIF(TRIM("data"->>'card_id'), '') AS card_id
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE "data" IS NOT NULL{source_filter_sql}
        )
        SELECT
            CASE
                WHEN NULLIF(TRIM(src.card_id), '') IS NOT NULL THEN 'source_has_card_id_but_target_missing_externalId'
                ELSE 'source_missing_card_id'
            END AS issue_type,
            COUNT(DISTINCT mt.username) AS username_count
        FROM missing_target mt
        LEFT JOIN src ON src.username = mt.username
        GROUP BY issue_type
        ORDER BY issue_type
    """
    with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q4, [BRAND] + source_filter_params)
        ext_rows = cur.fetchall()
    ext_issue_total = 0
    source_has_card_but_missing = 0
    for row in ext_rows:
        count_val = int(row["username_count"] or 0)
        ext_issue_total += count_val
        if row["issue_type"] == "source_has_card_id_but_target_missing_externalId":
            source_has_card_but_missing = count_val
        write_reconciliation_row("4_external_id_post_phase_check", row["issue_type"], count_val, "Post-player-phase externalId/card_id validation")
    write_reconciliation_row(
        "4_external_id_post_phase_check",
        "target_missing_externalId",
        ext_issue_total,
        "Total target playerDetails_final rows with blank externalId; detail rows are written under 4_external_id_post_phase_detail."
    )
    add_reconciliation_summary(
        f"ExternalId post-check: target_missing_externalId={ext_issue_total}, "
        f"source_has_card_id_but_target_missing_externalId={source_has_card_but_missing}."
    )

    q5 = f"""
        SELECT COUNT(*) AS source_rows_missing_username
        FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
        WHERE ("data" IS NULL OR NULLIF(TRIM("data"->>'username'), '') IS NULL){source_filter_sql}
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q5, source_filter_params)
        missing_user_row = cur.fetchone() or {}
    write_reconciliation_row("5_missing_username_rows", "source_rows_missing_username", missing_user_row.get("source_rows_missing_username"), "Rows that can
not be migrated/matched by username")
    add_reconciliation_summary(f"Missing/blank username rows={missing_user_row.get('source_rows_missing_username', 0)}.")

    q6 = f"""
        WITH src AS (
            SELECT DISTINCT NULLIF(TRIM("data"->>'username'), '') AS username
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE "data" IS NOT NULL
              AND NULLIF(TRIM("data"->>'username'), '') IS NOT NULL{source_filter_sql}
        )
        SELECT b."brandName" AS brand_name, COUNT(*) AS matched_usernames
        FROM src s
        JOIN kemet."playerDetails_final" b ON b."userName" = s.username
        GROUP BY b."brandName"
        ORDER BY matched_usernames DESC
    """
    with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q6, source_filter_params)
        brand_rows = cur.fetchall()
    for row in brand_rows:
        write_reconciliation_row("6_brand_mismatch_distribution", row["brand_name"], row["matched_usernames"], "Shows where source usernames matched target
by brandName")

    detail_counts = _emit_player_reconciliation_trace_details(
        src_conn=src_conn,
        tgt_conn=tgt_conn,
        date_from=date_from,
        date_to=date_to,
    )
    add_reconciliation_summary(
        "Trace details written to reconciliation CSV: "
        f"source_vs_target={detail_counts.get('source_vs_target_detail', 0)}, "
        f"not_inserted_or_missing_target={detail_counts.get('missing_target_detail', 0)}, "
        f"source_duplicate_username={detail_counts.get('source_duplicate_username_detail', 0)}, "
        f"source_duplicate_normalized_username={detail_counts.get('source_duplicate_normalized_username_detail', 0)}, "
        f"target_duplicate_normalized_username={detail_counts.get('target_duplicate_normalized_username_detail', 0)}, "
        f"externalId_missing_detail={detail_counts.get('external_id_missing_detail', 0)}, "
        f"missing_username_detail={detail_counts.get('missing_username_detail', 0)}."
    )
    add_reconciliation_summary(f"Brand distribution rows written to reconciliation CSV: {CSV_RECONCILIATION_PATH}.")
    trace_print(f"[RECONCILIATION] Completed playerDetails checks. csv={CSV_RECONCILIATION_PATH}")


def _source_game_date_filter_sql(date_from: Optional[str], date_to: Optional[str], params: List[Any], alias: str = "g") -> str:
    """Build optional game-source date predicates for reference/dimension reconciliation."""
    date_col = _source_date_expr_for_table(GAME_TRANSACTION_SOURCE_TABLE).replace('"data"', f'{alias}."data"')
    clauses: List[str] = []
    if date_from is not None:
        clauses.append(f"{date_col} >= %s::timestamptz")
        params.append(date_from)
    if date_to is not None:
        clauses.append(f"{date_col} <= %s::timestamptz")
        params.append(date_to)
    return (" AND " + " AND ".join(clauses)) if clauses else ""


def run_dimension_reference_reconciliation_checks(
        src_conn,
        tgt_conn,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        dry_run: bool = False,
) -> None:
    """Append reconciliation detail rows for reference tables consumed by gameTransaction.

    This is intentionally read-only and isolated from the working migration logic.
    It does not change provider/type/game/outlet upsert behavior; it only writes
    trace rows to the existing reconciliation CSV for manual checking.
    """
    trace_print("[RECONCILIATION][DIMENSIONS] Starting game reference/dimension checks.")
    counts: Dict[str, int] = {
        "missing_gameProvider": 0,
        "duplicate_gameProvider": 0,
        "missing_gameType": 0,
        "duplicate_gameType": 0,
        "missing_gameList": 0,
        "duplicate_gameList": 0,
        "missing_outletList": 0,
        "duplicate_outletList": 0,
    }

    try:
        game_filter_params: List[Any] = []
        game_filter_sql = _source_game_date_filter_sql(date_from, date_to, game_filter_params, alias="g")

        # 7a. gameProvider_final: source game provider references missing from target, plus duplicate target providers.
        q_provider_missing = f"""
            WITH src AS (
                SELECT
                    UPPER(TRIM(COALESCE(
                        g."data"->>'GameProvider',
                        g."data"->>'gameProvider',
                        g."data"->>'provider',
                        g."data"->>'Provider',
                        g."data"->'game'->>'provider',
                        'UNKNOWN'
                    ))) AS provider_key,
                    MIN(g."id"::text) AS sample_source_id,
                    COUNT(*) AS source_usage_count
                FROM {source_table_ref(GAME_TRANSACTION_SOURCE_TABLE)} g
                WHERE g."data" IS NOT NULL{game_filter_sql}
                GROUP BY UPPER(TRIM(COALESCE(
                    g."data"->>'GameProvider',
                    g."data"->>'gameProvider',
                    g."data"->>'provider',
                    g."data"->>'Provider',
                    g."data"->'game'->>'provider',
                    'UNKNOWN'
                )))
            )
            SELECT s.*, gp.id AS target_id, gp."gameProvider" AS target_provider
            FROM src s
            LEFT JOIN kemet."gameProvider_final" gp ON gp."gameProvider" = s.provider_key
            WHERE s.provider_key IS NOT NULL AND s.provider_key <> '' AND gp.id IS NULL
            ORDER BY s.provider_key
        """
        with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(q_provider_missing, game_filter_params)
            rows = cur.fetchall()
        for row in rows:
            counts["missing_gameProvider"] += 1
            write_reconciliation_trace_row(
                check_name="7_gameProvider_reference_detail",
                status="missing_target_gameProvider",
                source_table=GAME_TRANSACTION_SOURCE_TABLE,
                source_id=row.get("sample_source_id"),
                source_duplicate_count=row.get("source_usage_count"),
                source_reference_type="gameProvider",
                source_reference_value=row.get("provider_key"),
                target_table="gameProvider_final",
                reason="Source gameTransaction provider reference was not found in gameProvider_final.",
            )

        q_provider_dupe = """
            WITH tgt AS (
                SELECT id, "gameProvider", UPPER(TRIM("gameProvider")) AS provider_key
                FROM kemet."gameProvider_final"
            ), dupes AS (
                SELECT provider_key FROM tgt WHERE provider_key IS NOT NULL AND provider_key <> '' GROUP BY provider_key HAVING COUNT(*) > 1
            )
            SELECT t.*, COUNT(*) OVER (PARTITION BY t.provider_key) AS duplicate_count
            FROM tgt t JOIN dupes d ON d.provider_key = t.provider_key
            ORDER BY t.provider_key, t.id
        """
        with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(q_provider_dupe)
            rows = cur.fetchall()
        for row in rows:
            counts["duplicate_gameProvider"] += 1
            write_reconciliation_trace_row(
                check_name="7b_gameProvider_duplicate_detail",
                status="duplicate_target_gameProvider_normalized",
                target_table="gameProvider_final",
                target_id=row.get("id"),
                target_duplicate_count=row.get("duplicate_count"),
                target_reference_type="gameProvider",
                target_reference_value=row.get("gameProvider"),
                reason="Duplicate normalized gameProvider value exists in target reference table.",
            )

        # 8a. gameType_final: compare normalized source type to target gameType.
        game_type_expr = """CASE
                    WHEN UPPER(TRIM(COALESCE(g."data"->>'GameType', g."data"->>'gameType', g."data"->>'type', g."data"->'game'->>'type', 'Slots'))) IN ('SLO
TS','SLOT') THEN 'Slots'
                    WHEN UPPER(TRIM(COALESCE(g."data"->>'GameType', g."data"->>'gameType', g."data"->>'type', g."data"->'game'->>'type', 'Slots'))) IN ('LIV
E','LIVE_CASINO','CASINO') THEN 'Live'
                    WHEN UPPER(TRIM(COALESCE(g."data"->>'GameType', g."data"->>'gameType', g."data"->>'type', g."data"->'game'->>'type', 'Slots'))) IN ('SPO
RTS','SPORT') THEN 'Sports'
                    ELSE INITCAP(TRIM(COALESCE(g."data"->>'GameType', g."data"->>'gameType', g."data"->>'type', g."data"->'game'->>'type', 'Slots')))
                END"""
        q_type_missing = f"""
            WITH src AS (
                SELECT
                    {game_type_expr} AS game_type_key,
                    MIN(g."id"::text) AS sample_source_id,
                    COUNT(*) AS source_usage_count
                FROM {source_table_ref(GAME_TRANSACTION_SOURCE_TABLE)} g
                WHERE g."data" IS NOT NULL{game_filter_sql}
                GROUP BY {game_type_expr}
            )
            SELECT s.*, gt.id AS target_id, gt."gameType" AS target_game_type
            FROM src s
            LEFT JOIN kemet."gameType_final" gt ON gt."gameType" = s.game_type_key
            WHERE s.game_type_key IS NOT NULL AND s.game_type_key <> '' AND gt.id IS NULL
            ORDER BY s.game_type_key
        """
        with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(q_type_missing, game_filter_params)
            rows = cur.fetchall()
        for row in rows:
            counts["missing_gameType"] += 1
            write_reconciliation_trace_row(
                check_name="8_gameType_reference_detail",
                status="missing_target_gameType",
                source_table=GAME_TRANSACTION_SOURCE_TABLE,
                source_id=row.get("sample_source_id"),
                source_duplicate_count=row.get("source_usage_count"),
                source_reference_type="gameType",
                source_reference_value=row.get("game_type_key"),
                target_table="gameType_final",
                reason="Source gameTransaction game type reference was not found in gameType_final.",
            )

        q_type_dupe = """
            WITH tgt AS (
                SELECT id, "gameType", LOWER(TRIM("gameType")) AS game_type_key
                FROM kemet."gameType_final"
            ), dupes AS (
                SELECT game_type_key FROM tgt WHERE game_type_key IS NOT NULL AND game_type_key <> '' GROUP BY game_type_key HAVING COUNT(*) > 1
            )
            SELECT t.*, COUNT(*) OVER (PARTITION BY t.game_type_key) AS duplicate_count
            FROM tgt t JOIN dupes d ON d.game_type_key = t.game_type_key
            ORDER BY t.game_type_key, t.id
        """
        with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(q_type_dupe)
            rows = cur.fetchall()
        for row in rows:
            counts["duplicate_gameType"] += 1
            write_reconciliation_trace_row(
                check_name="8b_gameType_duplicate_detail",
                status="duplicate_target_gameType_normalized",
                target_table="gameType_final",
                target_id=row.get("id"),
                target_duplicate_count=row.get("duplicate_count"),
                target_reference_type="gameType",
                target_reference_value=row.get("gameType"),
                reason="Duplicate normalized gameType value exists in target reference table.",
            )

        # 9a. gameList_final: source provider+gameName references missing from target game list.
        q_game_missing = f"""
            WITH src AS (
                SELECT
                    UPPER(TRIM(COALESCE(
                        g."data"->>'GameProvider', g."data"->>'gameProvider', g."data"->>'provider', g."data"->>'Provider', g."data"->'game'->>'provider', '
UNKNOWN'
                    ))) AS provider_key,
                    TRIM(COALESCE(g."data"->>'GameName', g."data"->>'gameName', g."data"->>'name', g."data"->>'GameTitle', g."data"->'game'->>'name', 'UNKNO
WN')) AS game_name_key,
                    MIN(g."id"::text) AS sample_source_id,
                    COUNT(*) AS source_usage_count
                FROM {source_table_ref(GAME_TRANSACTION_SOURCE_TABLE)} g
                WHERE g."data" IS NOT NULL{game_filter_sql}
                GROUP BY
                    UPPER(TRIM(COALESCE(g."data"->>'GameProvider', g."data"->>'gameProvider', g."data"->>'provider', g."data"->>'Provider', g."data"->'game'
->>'provider', 'UNKNOWN'))),
                    TRIM(COALESCE(g."data"->>'GameName', g."data"->>'gameName', g."data"->>'name', g."data"->>'GameTitle', g."data"->'game'->>'name', 'UNKNO
WN'))
            )
            SELECT s.*, gp.id AS provider_id, gl.id AS target_id, gl."gameName" AS target_game_name
            FROM src s
            LEFT JOIN kemet."gameProvider_final" gp ON gp."gameProvider" = s.provider_key
            LEFT JOIN kemet."gameList_final" gl ON gl."gameProviderId" = gp.id AND gl."gameName" = s.game_name_key
            WHERE s.game_name_key IS NOT NULL AND s.game_name_key <> '' AND gl.id IS NULL
            ORDER BY s.provider_key, s.game_name_key
        """
        with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(q_game_missing, game_filter_params)
            rows = cur.fetchall()
        for row in rows:
            counts["missing_gameList"] += 1
            write_reconciliation_trace_row(
                check_name="9_gameList_reference_detail",
                status="missing_target_gameList",
                source_table=GAME_TRANSACTION_SOURCE_TABLE,
                source_id=row.get("sample_source_id"),
                source_duplicate_count=row.get("source_usage_count"),
                source_reference_type="gameProvider|gameName",
                source_reference_value=f"{row.get('provider_key') or ''}|{row.get('game_name_key') or ''}",
                target_table="gameList_final",
                target_id=row.get("target_id"),
                target_reference_type="gameName",
                target_reference_value=row.get("target_game_name"),
                reason="Source gameTransaction provider/game reference was not found in gameList_final.",
            )

        q_game_dupe = """
            WITH tgt AS (
                SELECT id, "gameProviderId", "gameName", LOWER(TRIM("gameName")) AS game_name_key
                FROM kemet."gameList_final"
            ), dupes AS (
                SELECT "gameProviderId", game_name_key FROM tgt
                WHERE game_name_key IS NOT NULL AND game_name_key <> ''
                GROUP BY "gameProviderId", game_name_key HAVING COUNT(*) > 1
            )
            SELECT t.*, COUNT(*) OVER (PARTITION BY t."gameProviderId", t.game_name_key) AS duplicate_count
            FROM tgt t JOIN dupes d ON d."gameProviderId" = t."gameProviderId" AND d.game_name_key = t.game_name_key
            ORDER BY t."gameProviderId", t.game_name_key, t.id
        """
        with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(q_game_dupe)
            rows = cur.fetchall()
        for row in rows:
            counts["duplicate_gameList"] += 1
            write_reconciliation_trace_row(
                check_name="9b_gameList_duplicate_detail",
                status="duplicate_target_gameList_normalized",
                target_table="gameList_final",
                target_id=row.get("id"),
                target_duplicate_count=row.get("duplicate_count"),
                target_reference_type="gameProviderId|gameName",
                target_reference_value=f"{row.get('gameProviderId') or ''}|{row.get('gameName') or ''}",
                reason="Duplicate normalized gameList value exists for the same provider in target reference table.",
            )

        # 10a. outletList_final: compare outlet codes used by gameTransaction and player registrations to target outlet list.
        player_filter_params: List[Any] = []
        player_filter_sql = _source_registration_date_filter_sql(date_from, date_to, player_filter_params)
        q_outlet_missing = f"""
            WITH src AS (
                SELECT
                    'gameTransaction'::text AS source_kind,
                    UPPER(TRIM(COALESCE(g."data"->>'Outlet', g."data"->>'outlet', g."data"->>'tableRoomId'))) AS outlet_code,
                    MIN(g."id"::text) AS sample_source_id,
                    COUNT(*) AS source_usage_count
                FROM {source_table_ref(GAME_TRANSACTION_SOURCE_TABLE)} g
                WHERE g."data" IS NOT NULL{game_filter_sql}
                GROUP BY UPPER(TRIM(COALESCE(g."data"->>'Outlet', g."data"->>'outlet', g."data"->>'tableRoomId')))
                UNION ALL
                SELECT
                    'playerRegistration'::text AS source_kind,
                    UPPER(TRIM(COALESCE(p."data"->>'outlet_id', p."data"->>'outletCode', p."data"->>'outlet_code', p."data"->>'outlet'))) AS outlet_code,
                    MIN(p."id"::text) AS sample_source_id,
                    COUNT(*) AS source_usage_count
                FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)} p
                WHERE p."data" IS NOT NULL{player_filter_sql}
                GROUP BY UPPER(TRIM(COALESCE(p."data"->>'outlet_id', p."data"->>'outletCode', p."data"->>'outlet_code', p."data"->>'outlet')))
            ), src_rollup AS (
                SELECT outlet_code, MIN(source_kind) AS source_kind, MIN(sample_source_id) AS sample_source_id, SUM(source_usage_count) AS source_usage_coun
t
                FROM src
                WHERE outlet_code IS NOT NULL AND outlet_code <> ''
                GROUP BY outlet_code
            )
            SELECT s.*, ol.id AS target_id, ol."outletCode" AS target_outlet_code
            FROM src_rollup s
            LEFT JOIN kemet."outletList_final" ol ON UPPER(TRIM(ol."outletCode")) = s.outlet_code
            WHERE ol.id IS NULL
            ORDER BY s.outlet_code
        """
        with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(q_outlet_missing, game_filter_params + player_filter_params)
            rows = cur.fetchall()
        for row in rows:
            counts["missing_outletList"] += 1
            write_reconciliation_trace_row(
                check_name="10_outletList_reference_detail",
                status="missing_target_outletList",
                source_table=f"{GAME_TRANSACTION_SOURCE_TABLE}|{PLAYER_REGISTRATION_SOURCE_TABLE}",
                source_id=row.get("sample_source_id"),
                source_duplicate_count=row.get("source_usage_count"),
                source_reference_type="outletCode",
                source_reference_value=row.get("outlet_code"),
                target_table="outletList_final",
                reason="Source outlet code used by migration was not found in outletList_final.",
            )

        q_outlet_dupe = """
            WITH tgt AS (
                SELECT id, "outletCode", UPPER(TRIM("outletCode")) AS outlet_key
                FROM kemet."outletList_final"
            ), dupes AS (
                SELECT outlet_key FROM tgt WHERE outlet_key IS NOT NULL AND outlet_key <> '' GROUP BY outlet_key HAVING COUNT(*) > 1
            )
            SELECT t.*, COUNT(*) OVER (PARTITION BY t.outlet_key) AS duplicate_count
            FROM tgt t JOIN dupes d ON d.outlet_key = t.outlet_key
            ORDER BY t.outlet_key, t.id
        """
        with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(q_outlet_dupe)
            rows = cur.fetchall()
        for row in rows:
            counts["duplicate_outletList"] += 1
            write_reconciliation_trace_row(
                check_name="10b_outletList_duplicate_detail",
                status="duplicate_target_outletList_normalized",
                target_table="outletList_final",
                target_id=row.get("id"),
                target_duplicate_count=row.get("duplicate_count"),
                target_reference_type="outletCode",
                target_reference_value=row.get("outletCode"),
                reason="Duplicate normalized outletCode exists in target outlet list.",
            )

        for metric, value in counts.items():
            write_reconciliation_row("7_10_dimension_reference_summary", metric, value, "Reference-table reconciliation for gameProvider, gameType, gameList
, and outletList.")
        add_reconciliation_summary(
            "Dimension/reference reconciliation rows written: "
            f"gameProvider missing={counts['missing_gameProvider']} duplicate={counts['duplicate_gameProvider']}; "
            f"gameType missing={counts['missing_gameType']} duplicate={counts['duplicate_gameType']}; "
            f"gameList missing={counts['missing_gameList']} duplicate={counts['duplicate_gameList']}; "
            f"outletList missing={counts['missing_outletList']} duplicate={counts['duplicate_outletList']}."
        )
        trace_print(f"[RECONCILIATION][DIMENSIONS] Completed. csv={CSV_RECONCILIATION_PATH}")

    except Exception as e:
        try:
            tgt_conn.rollback()
        except Exception:
            pass
        add_reconciliation_summary(f"Dimension/reference reconciliation failed safely: {e}")
        trace_print(f"[RECONCILIATION][DIMENSIONS][WARN] Failed safely: {e}", level=logging.WARNING)


# ============================================================================
# Post-migration data quality checker requested 2026-05-21
# - Separate from reconciliation: reconciliation explains row-count gaps;
#   this checker validates column-level source-to-target values.
# - Skips gameTransaction per request.
# - Covers playerDetails_final, walletTransaction_final deposits, withdrawals.
# - Writes only mismatched values to reports/data_quality_*.csv.
# - Uses bounded samples and bounded source-row scans to avoid resource hogging.
# REVERT NOTE: remove run_post_migration_data_quality_checks() call and this
# block if you need to disable the DQ pass quickly.
# ============================================================================

def _dq_int_env(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, str(default))))
    except Exception:
        return default


def _dq_limits() -> Tuple[int, int]:
    """Return bounded DQ limits to protect the system from large output/scans."""
    sample_limit = _dq_int_env("DQ_SAMPLE_LIMIT", 500)
    max_source_rows = _dq_int_env("DQ_MAX_SOURCE_ROWS", 200000)
    return sample_limit, max_source_rows


def _dq_date_filter_sql(table: str, date_from: Optional[str], date_to: Optional[str], params: List[Any], alias: str = "a") -> str:
    """Build a source date-window predicate for DQ checks using the same source date keys as migration."""
    date_col = _source_date_expr_for_table(table).replace('"data"', f'{alias}."data"')
    parts: List[str] = []
    if date_from is not None:
        parts.append(f"{date_col} >= %s::timestamptz")
        params.append(date_from)
    if date_to is not None:
        parts.append(f"{date_col} <= %s::timestamptz")
        params.append(date_to)
    return (" AND " + " AND ".join(parts)) if parts else ""


def _write_dq_rows(rows: List[Dict[str, Any]], default_table: str, default_phase: str) -> int:
    """Write DQ mismatch rows defensively; never let report writing crash migration."""
    written = 0
    for row in rows:
        try:
            write_data_quality_row(
                table_name=row.get("table_name") or default_table,
                phase=row.get("phase") or default_phase,
                source_id=row.get("source_id"),
                business_key=row.get("business_key"),
                target_id=row.get("target_id"),
                column_name=row.get("column_name"),
                source_value=row.get("source_value"),
                target_value=row.get("target_value"),
                issue_type=row.get("issue_type") or "mismatch",
                notes=row.get("notes") or "",
            )
            written += 1
        except Exception as e:
            trace_print(f"[DATA QUALITY][WARN] Failed writing DQ row: {e}", level=logging.WARNING)
    return written


def run_player_column_data_quality_check(tgt_conn, date_from: Optional[str] = None, date_to: Optional[str] = None) -> int:
    """Compare PlayerRegistrationsInplayV1_dev values to playerDetails_final.

    Checks username, first/middle/last, externalId/card_id, mobile, email,
    registrationDate, outlet, address, birthdate, walletBalance, and isActive.
    Name migration logic is not changed; this only audits the expected values.
    """
    sample_limit, max_source_rows = _dq_limits()
    params: List[Any] = []
    date_sql = _dq_date_filter_sql(PLAYER_REGISTRATION_SOURCE_TABLE, date_from, date_to, params, alias="a")
    q = f"""
        WITH src AS (
            SELECT
                a."id" AS source_id,
                a."data",
                NULLIF(TRIM(COALESCE(
                    a."data"->>'username',
                    a."data"->>'name',
                    a."data"->>'loginName',
                    a."data"->>'userName',
                    a."data"->>'userid'
                )), '') AS username,
                NULLIF(TRIM(COALESCE(
                    a."data"->>'card_id',
                    a."data"->>'cardId',
                    a."data"->>'memberId',
                    a."data"->>'externalId'
                )), '') AS expected_external_id,
                COALESCE(NULLIF(TRIM(a."data"->>'first_name'), ''), 'Unknown') AS expected_first_name,
                COALESCE(a."data"->>'middle_name', '') AS expected_middle_name,
                COALESCE(NULLIF(TRIM(a."data"->>'last_name'), ''), 'Unknown') AS expected_last_name,
                CASE
                    WHEN regexp_replace(COALESCE(a."data"->>'contact_number',''), '[^0-9]', '', 'g') = '' THEN '0000000000'
                    WHEN length(regexp_replace(COALESCE(a."data"->>'contact_number',''), '[^0-9]', '', 'g')) >= 10
                        THEN right(regexp_replace(COALESCE(a."data"->>'contact_number',''), '[^0-9]', '', 'g'), 10)
                    ELSE '0000000000'
                END AS expected_mobile_number,
                CASE
                    WHEN trim(trailing '.' from COALESCE(a."data"->>'email','')) ~* '^[A-Z0-9._%%+-]+@[A-Z0-9.-]+\\.[A-Z]{{2,}}$'
                        THEN trim(trailing '.' from a."data"->>'email')
                    ELSE regexp_replace(
                        regexp_replace(
                            lower(COALESCE(NULLIF(TRIM(a."data"->>'username'), ''), 'unknown')),
                            '[^a-z0-9._%%+-]+', '_', 'g'
                        ),
                        '_+', '_', 'g'
                    ) || '@unknown.local'
                END AS expected_email,
                COALESCE(
                    NULLIF(a."data"->>'createddate','')::timestamptz,
                    NULLIF(a."data"->>'updatedate','')::timestamptz
                ) AS expected_registration_date,
                NULLIF(TRIM(a."data"->>'outlet_id'), '') AS expected_outlet_code,
                COALESCE(NULLIF(TRIM(a."data"->>'current_address'), ''), 'N/A') AS expected_address_street,
                COALESCE(NULLIF(TRIM(a."data"->>'permanent_address'), ''), 'N/A') AS expected_address_province,
                COALESCE(NULLIF(a."data"->>'birthdate','')::date, DATE '1900-01-01') AS expected_birthdate,
                COALESCE(NULLIF(a."data"->>'balance','')::numeric, 0) AS expected_wallet_balance,
                CASE
                    WHEN a."data" ? 'suspended' THEN (a."data"->>'suspended' = '0')
                    WHEN a."data" ? 'closed' THEN NOT ((a."data"->>'closed') IN ('1','true','t','yes','y','active','approved','verified'))
                    ELSE false
                END AS expected_is_active
            FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)} a
            WHERE a."data" IS NOT NULL{date_sql}
            ORDER BY {_source_date_expr_for_table(PLAYER_REGISTRATION_SOURCE_TABLE).replace('"data"', 'a."data"')} ASC NULLS LAST, a."id" ASC
            LIMIT %s
        ), joined AS (
            SELECT
                src.*,
                t.id AS target_id,
                t."userName" AS actual_username,
                t."externalId" AS actual_external_id,
                t."firstName" AS actual_first_name,
                t."middleName" AS actual_middle_name,
                t."lastName" AS actual_last_name,
                t."mobileNumber" AS actual_mobile_number,
                t."emailAddress" AS actual_email,
                t."registrationDate" AS actual_registration_date,
                t."outletCode" AS actual_outlet_code,
                t."addressStreet" AS actual_address_street,
                t."addressProvince" AS actual_address_province,
                t."birthdate" AS actual_birthdate,
                t."walletBalance" AS actual_wallet_balance,
                t."isActive" AS actual_is_active
            FROM src
            LEFT JOIN kemet."playerDetails_final" t
                ON t."brandName" = %s
               AND t."userName" = src.username
        ), mismatches AS (
            SELECT
                'playerDetails_final'::text AS table_name,
                'playerDetails'::text AS phase,
                source_id::text,
                username::text AS business_key,
                target_id::text,
                v.column_name,
                v.source_value,
                v.target_value,
                CASE WHEN target_id IS NULL THEN 'missing_target_row' ELSE 'column_value_mismatch' END AS issue_type,
                'Post-migration DQ column comparison for playerDetails; bounded by DQ_MAX_SOURCE_ROWS/DQ_SAMPLE_LIMIT.'::text AS notes
            FROM joined
            CROSS JOIN LATERAL (VALUES
                ('userName', username::text, actual_username::text),
                ('externalId', expected_external_id::text, actual_external_id::text),
                ('firstName', expected_first_name::text, actual_first_name::text),
                ('middleName', expected_middle_name::text, COALESCE(actual_middle_name, '')::text),
                ('lastName', expected_last_name::text, actual_last_name::text),
                ('mobileNumber', expected_mobile_number::text, actual_mobile_number::text),
                ('emailAddress', expected_email::text, actual_email::text),
                ('registrationDate', expected_registration_date::text, actual_registration_date::text),
                ('outletCode', expected_outlet_code::text, actual_outlet_code::text),
                ('addressStreet', expected_address_street::text, actual_address_street::text),
                ('addressProvince', expected_address_province::text, actual_address_province::text),
                ('birthdate', expected_birthdate::text, actual_birthdate::date::text),
                ('walletBalance', expected_wallet_balance::text, actual_wallet_balance::numeric::text),
                ('isActive', expected_is_active::text, actual_is_active::text)
            ) AS v(column_name, source_value, target_value)
            WHERE target_id IS NULL OR v.source_value IS DISTINCT FROM v.target_value
        )
        SELECT * FROM mismatches
        ORDER BY source_id, column_name
        LIMIT %s
    """
    try:
        with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(q, params + [max_source_rows, BRAND, sample_limit])
            rows = cur.fetchall()
        written = _write_dq_rows(rows, "playerDetails_final", "playerDetails")
        add_data_quality_summary(f"playerDetails DQ mismatches written={written}; csv={CSV_DATA_QUALITY_PATH}; sampleLimit={sample_limit}; maxSourceRows={ma
x_source_rows}.")
        return written
    except Exception as e:
        try:
            tgt_conn.rollback()
        except Exception:
            pass
        add_data_quality_summary(f"playerDetails DQ check failed safely: {e}")
        return 0

def run_wallet_column_data_quality_check(tgt_conn, kind: str, date_from: Optional[str] = None, date_to: Optional[str] = None) -> int:
    """Compare Deposits/Withdrawals source values to walletTransaction_final; gameTransaction is skipped."""
    sample_limit, max_source_rows = _dq_limits()
    table = DEPOSITS_SOURCE_TABLE if kind == "deposit" else WITHDRAWALS_SOURCE_TABLE
    params: List[Any] = []
    date_sql = _dq_date_filter_sql(table, date_from, date_to, params, alias="a")
    q = f"""
        WITH src AS (
            SELECT
                a."id" AS source_id,
                a."data",
                NULLIF(TRIM(COALESCE(
                    a."data"->>'username',
                    a."data"->>'userName',
                    a."data"->>'name',
                    a."data"->>'loginName',
                    a."data"->>'userid',
                    a."data"->>'userId',
                    a."data"->>'PlayerAccount',
                    a."data"->>'playerAccount',
                    a."data"->'member'->>'username',
                    a."data"->'member'->>'name',
                    a."data"->'member'->>'userName',
                    a."data"->'member'->>'loginName',
                    a."data"->'member'->>'userid'
                )), '') AS expected_username,
                NULLIF(TRIM(COALESCE(
                    a."data"->>'id',
                    a."data"->>'referenceId',
                    a."data"->>'transactionId'
                )), '') AS expected_reference_id,
                ABS(COALESCE(NULLIF(COALESCE(
                    a."data"->>'amount',
                    a."data"->>'TotalAmount',
                    a."data"->>'totalAmount'
                ), '')::numeric, 0)) AS expected_amount,
                COALESCE(NULLIF(COALESCE(
                    a."data"->>'transferDate',
                    a."data"->>'transferdate',
                    a."data"->>'TransferDate'
                ), '')::timestamptz, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS expected_created_datetime,
                COALESCE(NULLIF(COALESCE(
                    a."data"->>'payment',
                    a."data"->>'paymentMethod',
                    a."data"->>'paymentGateway'
                ), ''), 'N/A') AS expected_payment_gateway
            FROM {source_table_ref(table)} a
            WHERE a."data" IS NOT NULL{date_sql}
            ORDER BY {_source_date_expr_for_table(table).replace('"data"', 'a."data"')} ASC NULLS LAST, a."id" ASC
            LIMIT %s
        ), joined AS (
            SELECT
                src.*,
                wt.id AS target_id,
                wt."referenceId" AS actual_reference_id,
                wt."transactionType" AS actual_transaction_type,
                wt.platform AS actual_platform,
                wt.amount AS actual_amount,
                wt.status AS actual_status,
                wt."createdDatetime" AS actual_created_datetime,
                wt."confirmedDatetime" AS actual_confirmed_datetime,
                wt."updatedAt" AS actual_updated_at,
                wt."paymentGateway" AS actual_payment_gateway,
                wt.domain AS actual_domain,
                pd."userName" AS actual_username
            FROM src
            LEFT JOIN kemet."walletTransaction_final" wt
                ON wt.platform = %s
               AND wt."transactionType" = %s
               AND wt."referenceId" = src.expected_reference_id
            LEFT JOIN kemet."playerDetails_final" pd
                ON pd.id = wt."playerId"
        ), mismatches AS (
            SELECT
                'walletTransaction_final'::text AS table_name,
                %s::text AS phase,
                source_id::text,
                COALESCE(expected_reference_id, expected_username, source_id::text) AS business_key,
                target_id::text,
                v.column_name,
                v.source_value,
                v.target_value,
                CASE WHEN target_id IS NULL THEN 'missing_target_row' ELSE 'column_value_mismatch' END AS issue_type,
                'Post-migration DQ column comparison for walletTransaction; bounded by DQ_MAX_SOURCE_ROWS/DQ_SAMPLE_LIMIT. gameTransaction intentionally ski
pped.'::text AS notes
            FROM joined
            CROSS JOIN LATERAL (VALUES
                ('transactionType', %s::text, actual_transaction_type::text),
                ('platform', %s::text, actual_platform::text),
                ('referenceId', expected_reference_id::text, actual_reference_id::text),
                ('playerUserName', expected_username::text, actual_username::text),
                ('amount', expected_amount::text, actual_amount::numeric::text),
                ('status', 'confirmed'::text, actual_status::text),
                ('createdDatetime', expected_created_datetime::text, actual_created_datetime::text),
                ('confirmedDatetime', expected_created_datetime::text, actual_confirmed_datetime::text),
                ('updatedAt', expected_created_datetime::text, actual_updated_at::text),
                ('paymentGateway', expected_payment_gateway::text, actual_payment_gateway::text),
                ('domain', 'www.inplay.com.ph'::text, actual_domain::text)
            ) AS v(column_name, source_value, target_value)
            WHERE target_id IS NULL OR v.source_value IS DISTINCT FROM v.target_value
        )
        SELECT * FROM mismatches
        ORDER BY source_id, column_name
        LIMIT %s
    """
    query_params = params + [max_source_rows, WALLET_PLATFORM, kind, f"walletTransaction.{kind}", kind, WALLET_PLATFORM, sample_limit]
    try:
        with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(q, query_params)
            rows = cur.fetchall()
        written = _write_dq_rows(rows, "walletTransaction_final", f"walletTransaction.{kind}")
        add_data_quality_summary(f"walletTransaction.{kind} DQ mismatches written={written}; csv={CSV_DATA_QUALITY_PATH}; sampleLimit={sample_limit}; maxSou
rceRows={max_source_rows}.")
        return written
    except Exception as e:
        try:
            tgt_conn.rollback()
        except Exception:
            pass
        add_data_quality_summary(f"walletTransaction.{kind} DQ check failed safely: {e}")
        return 0


def run_post_migration_data_quality_checks(tgt_conn, date_from: Optional[str] = None, date_to: Optional[str] = None, dry_run: bool = False
) -> None:
    """Run bounded post-migration DQ checks for migrated tables.

    DATA QUALITY CHECKER NOTE:
    This is the new column-value mismatch checker requested after the externalId
    reconciliation work. It writes mismatches only, not matches, to keep output
    small. gameTransaction is intentionally skipped per latest instruction.
    """
    trace_print("[DATA QUALITY] Starting post-migration column-value checks. gameTransaction intentionally skipped.")
    if dry_run:
        add_data_quality_summary("Data quality checks executed in dry-run context; no migration writes are committed by this checker.")
    total_written = 0
    total_written += run_player_column_data_quality_check(tgt_conn, date_from=date_from, date_to=date_to)
    total_written += run_wallet_column_data_quality_check(tgt_conn, "deposit", date_from=date_from, date_to=date_to)
    total_written += run_wallet_column_data_quality_check(tgt_conn, "withdrawal", date_from=date_from, date_to=date_to)
    add_data_quality_summary(f"Total DQ mismatch rows written={total_written}; dataQualityCsvPath={CSV_DATA_QUALITY_PATH}.")
    trace_print(f"[DATA QUALITY] Completed post-migration checks. csv={CSV_DATA_QUALITY_PATH}")

def migrate_single_user(
        src_conn,
        tgt_conn,
        username: str,
        dry_run: bool,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
) -> None:
    SOURCE_QUERY_TRACE.clear()
    username = username.strip()
    print(f"\n[Single User] Migrating targeted username: '{username}'", flush=True)
    from_dt_iso: Optional[str] = date_from.isoformat() if date_from is not None else None
    until_dt_iso: Optional[str] = date_to.isoformat() if date_to is not None else None

    def _append_date_filters(table: str, conditions: List[str], params: List[Any]) -> None:
        date_col = _source_date_expr_for_table(table)
        if from_dt_iso is not None:
            conditions.append(f"{date_col} >= %s::timestamptz")
            params.append(from_dt_iso)
        if until_dt_iso is not None:
            conditions.append(f"{date_col} <= %s::timestamptz")
            params.append(until_dt_iso)

    detail_map = fetch_player_detail_map(src_conn, from_dt_iso, until_dt_iso)
    registration_params: List[Any] = [username]
    registration_conditions = [
        '"data" IS NOT NULL',
        "COALESCE(\"data\"->>'name', \"data\"->>'username', \"data\"->>'userName', \"data\"->>'loginName', \"data\"->>'userid') = %s",
    ]
    _append_date_filters(PLAYER_REGISTRATION_SOURCE_TABLE, registration_conditions, registration_params)
    registration_sql = f"""
            SELECT "id", "data" FROM {source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)}
            WHERE {' AND '.join(registration_conditions)}
            ORDER BY {_source_date_expr_for_table(PLAYER_REGISTRATION_SOURCE_TABLE)} ASC NULLS LAST, "id" ASC
            LIMIT 1
            """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        print_source_query(cur, f"single-user:{PLAYER_REGISTRATION_SOURCE_TABLE}", registration_sql, registration_params)
        cur.execute(registration_sql, registration_params)
        reg_row = cur.fetchone()
    print_source_query_summary("single-user playerRegistration", [f"{PLAYER_REGISTRATION_SOURCE_TABLE} detail-map", f"single-user:{PLAYER_REGISTRATION_SOURC
E_TABLE}"])

    player_map: Dict[str, uuid.UUID] = build_player_map(tgt_conn)
    registered_usernames = set()
    player_upserted = 0
    if reg_row:
        data = as_dict(reg_row["data"])
        mapped_username = extract_username(data) or username
        if mapped_username:
            registered_usernames.add(mapped_username)
        pid = upsert_player_from_member(tgt_conn, data, BRAND, dry_run=dry_run, detail_map=detail_map, source_id=str(reg_row.get("id") or
"N/A"))
        if pid:
            player_map[mapped_username] = pid
            player_upserted = 1
        print(("[DRY-RUN] Would upsert" if dry_run else "Upserted") + f" player entry username={mapped_username} id={pid}", flush=True)
    else:
        record_player_skip(
            source_id="N/A",
            username=username,
            reason=f"No canonical registration row found inside {PLAYER_REGISTRATION_SOURCE_TABLE}",
            dry_run=dry_run,
            source_table=PLAYER_REGISTRATION_SOURCE_TABLE,
            action="skipped",
        )

    provider_cache: Dict[str, uuid.UUID] = {}
    gametype_cache: Dict[str, uuid.UUID] = {}
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID] = {}
    member_name_expr = (
        "COALESCE(\"data\"->'member'->>'name', \"data\"->'member'->>'username', "
        "\"data\"->'member'->>'userName', \"data\"->'member'->>'loginName', "
        "\"data\"->'member'->>'userid', \"data\"->>'PlayerAccount', \"data\"->>'username', \"data\"->>'userName')"
    )

    game_params: List[Any] = [username]
    game_conditions = ['"data" IS NOT NULL', f"{member_name_expr} = %s"]
    _append_date_filters(GAME_TRANSACTION_SOURCE_TABLE, game_conditions, game_params)
    game_sql = f"""
            SELECT "id", "data" FROM {source_table_ref(GAME_TRANSACTION_SOURCE_TABLE)}
            WHERE {' AND '.join(game_conditions)}
            ORDER BY {_source_date_expr_for_table(GAME_TRANSACTION_SOURCE_TABLE)} ASC NULLS LAST, "id" ASC
            """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        print_source_query(cur, f"single-user:{GAME_TRANSACTION_SOURCE_TABLE}", game_sql, game_params)
        cur.execute(game_sql, game_params)
        tx_rows = cur.fetchall()
    inserted_gt, skipped_gt = insert_game_tx_batch(tgt_conn, tx_rows, player_map, provider_cache, gametype_cache, gamelist_cache, dry_run=
dry_run, registered_usernames=registered_usernames)
    print_source_query_summary("single-user gameTransaction", [f"single-user:{GAME_TRANSACTION_SOURCE_TABLE}"])

    deposit_params: List[Any] = [username]
    deposit_conditions = ['"data" IS NOT NULL', f"{member_name_expr} = %s"]
    _append_date_filters(DEPOSITS_SOURCE_TABLE, deposit_conditions, deposit_params)
    deposit_sql = f"""
            SELECT "id", "data" FROM {source_table_ref(DEPOSITS_SOURCE_TABLE)}
            WHERE {' AND '.join(deposit_conditions)}
            ORDER BY {_source_date_expr_for_table(DEPOSITS_SOURCE_TABLE)} ASC NULLS LAST, "id" ASC
            """
    withdrawal_params: List[Any] = [username]
    withdrawal_conditions = ['"data" IS NOT NULL', f"{member_name_expr} = %s"]
    _append_date_filters(WITHDRAWALS_SOURCE_TABLE, withdrawal_conditions, withdrawal_params)
    withdrawal_sql = f"""
            SELECT "id", "data" FROM {source_table_ref(WITHDRAWALS_SOURCE_TABLE)}
            WHERE {' AND '.join(withdrawal_conditions)}
            ORDER BY {_source_date_expr_for_table(WITHDRAWALS_SOURCE_TABLE)} ASC NULLS LAST, "id" ASC
            """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        print_source_query(cur, f"single-user:{DEPOSITS_SOURCE_TABLE}", deposit_sql, deposit_params)
        cur.execute(deposit_sql, deposit_params)
        dep_rows = cur.fetchall()
        print_source_query(cur, f"single-user:{WITHDRAWALS_SOURCE_TABLE}", withdrawal_sql, withdrawal_params)
        cur.execute(withdrawal_sql, withdrawal_params)
        wd_rows = cur.fetchall()
    inserted_dep, skipped_dep = insert_wallet_batch(tgt_conn, dep_rows, "deposit", player_map, dry_run=dry_run, registered_usernames=registered_usernames)
    inserted_wd, skipped_wd = insert_wallet_batch(tgt_conn, wd_rows, "withdrawal", player_map, dry_run=dry_run, registered_usernames=registered_usernames)
    print_source_query_summary("single-user walletTransaction", [f"single-user:{DEPOSITS_SOURCE_TABLE}", f"single-user:{WITHDRAWALS_SOURCE_TABLE}"])

    if dry_run:
        tgt_conn.rollback()
        print("[DRY-RUN] rolled back single user writes.")
    else:
        tgt_conn.commit()
        print("Committed successfully.")
    total_inserted = player_upserted + inserted_gt + inserted_dep + inserted_wd
    summary_msg = (
        "\n[RUN SUMMARY][single-user]\n"
        f"  playerDetails_final inserted_or_updated={player_upserted}, reportCsvRows={player_report_total()}, "
        f"fixed={player_report_fixed_total()}, total_fixed={player_report_fixed_total()}, "
        f"duplicates={player_report_duplicate_total()}, total_duplicates={player_report_duplicate_total()}, "
        f"emailCorrectionRows={player_report_fixed_total()}, duplicateRows={player_report_duplicate_total()}, "
        f"failureReportRows={player_report_failure_total()}\n"
        f"  playerDetails_final reportActions=[{player_report_counts_text()}]\n"
        f"  playerDetails_final reportIssues=[{player_report_issue_counts_text()}]\n"
        f"  playerDetails_final reportCsvPath={CSV_PLAYERS_PATH}\n"
        f"  gameTransaction_final inserted_or_would_insert={inserted_gt}, skipped={skipped_gt}\n"
        f"  walletTransaction_final deposits inserted_or_would_insert={inserted_dep}, skipped={skipped_dep}\n"
        f"  walletTransaction_final withdrawals inserted_or_would_insert={inserted_wd}, skipped={skipped_wd}\n"
        f"  TOTAL records inserted_or_would_insert={total_inserted}"
    )
    trace_print(summary_msg)


# ---------------------------------------------------------------------------
# Source preflight audit checks (additive; read-only)
# ---------------------------------------------------------------------------
def _preflight_detail_limit() -> int:
    """Bound preflight detail rows per check to protect smaller ETL hosts."""
    try:
        return max(1, int(os.getenv("PREFLIGHT_DETAIL_LIMIT", "20000")))
    except Exception:
        return 20000


def _source_date_filter_for_alias(table: str, date_from: Optional[str], date_to: Optional[str], params: List[Any], alias: str = "s") -> str:
    """Build source date-window predicate for preflight checks using SOURCE_SCHEMA-aware source tables."""
    date_expr = _source_date_expr_for_table(table).replace('"data"', f'{alias}."data"')
    clauses: List[str] = []
    if date_from is not None:
        clauses.append(f"{date_expr} >= %s::timestamptz")
        params.append(date_from)
    if date_to is not None:
        clauses.append(f"{date_expr} <= %s::timestamptz")
        params.append(date_to)
    return (" AND " + " AND ".join(clauses)) if clauses else ""


def _preflight_trace_rows(check_name: str, rows: List[Dict[str, Any]], source_table: str, status: str, reason: str, reference_type: str = "") -> int:
    """Write preflight detail rows into the compact reconciliation CSV and trace count."""
    written = 0
    for row in rows:
        write_reconciliation_trace_row(
            check_name=check_name,
            status=row.get("status") or status,
            source_table=source_table,
            source_id=row.get("source_id") or row.get("sourceId") or "",
            source_username=row.get("source_username") or row.get("sourceUsername") or "",
            source_username_normalized=row.get("source_username_normalized") or row.get("sourceUsernameNormalized") or "",
            source_card_id=row.get("source_card_id") or row.get("sourceCardId") or "",
            source_duplicate_count=row.get("duplicate_count") or row.get("source_duplicate_count") or "",
            source_reference_type=row.get("source_reference_type") or reference_type,
            source_reference_value=row.get("source_reference_value") or row.get("reference_value") or "",
            reason=row.get("reason") or reason,
            notes=row.get("notes") or "Source preflight audit before migration write phases; no target writes performed.",
        )
        written += 1
    trace_print(f"[PREFLIGHT][{check_name}] detailRowsWritten={written} status={status}")
    return written


def _run_preflight_detail_query(src_conn, check_name: str, query: str, params: List[Any], source_table: str, status: str, reason: str, reference_type: str =
 "") -> int:
    """Run a bounded source preflight query safely and write trace rows to reconciliation CSV."""
    try:
        with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        try:
            src_conn.rollback()
        except Exception:
            pass
        write_reconciliation_row(check_name, "detail_rows", len(rows), reason)
        return _preflight_trace_rows(check_name, rows, source_table, status, reason, reference_type=reference_type)
    except Exception as e:
        try:
            src_conn.rollback()
        except Exception:
            pass
        msg = f"Source preflight check failed safely: {e}"
        write_reconciliation_row(check_name, "preflight_failed", 1, msg)
        trace_print(f"[PREFLIGHT][WARN][{check_name}] {msg}", level=logging.WARNING)
        return 0


def run_source_preflight_checks(src_conn, date_from: Optional[str] = None, date_to: Optional[str] = None, dry_run: bool = False) -> None:
    """Read-only source preflight checks before migration phases.

    Purpose:
      - Identify source-side duplicates, missing keys, and field issues before any inserts/upserts.
      - Write traceable rows to the reconciliation CSV with sourceId/sourceUsername/card/reference values.
      - Do not change migration behavior and do not write to target tables.
    """
    trace_print("[PREFLIGHT] Starting source duplicate/missing-field checks before migration phases.")
    limit = _preflight_detail_limit()
    total_written = 0

    player_username_expr = "NULLIF(TRIM(COALESCE(s.\"data\"->>'name', s.\"data\"->>'username', s.\"data\"->>'userName', s.\"data\"->>'loginName', s.\"data\"
->>'userid', s.\"data\"->>'userId')), '')"
    player_source_ref = source_table_ref(PLAYER_REGISTRATION_SOURCE_TABLE)

    # Player source: missing username
    params: List[Any] = []
    date_sql = _source_date_filter_for_alias(PLAYER_REGISTRATION_SOURCE_TABLE, date_from, date_to, params, alias="s")
    q = f"""
        SELECT
            s."id"::text AS source_id,
            {player_username_expr} AS source_username,
            LOWER(TRIM({player_username_expr})) AS source_username_normalized,
            NULLIF(TRIM(s."data"->>'card_id'), '') AS source_card_id,
            'missing_source_username' AS status,
            'Player registration source row has blank/missing username identity keys before migration.' AS reason
        FROM {player_source_ref} s
        WHERE s."data" IS NOT NULL{date_sql}
          AND {player_username_expr} IS NULL
        ORDER BY s."id"
        LIMIT %s
    """
    total_written += _run_preflight_detail_query(src_conn, "0a_preflight_player_missing_username", q, params + [limit], PLAYER_REGISTRATION_SOURCE_TABLE, "m
issing_source_username", "Player source rows with missing username identity keys", "username")

    # Player source: duplicate exact username
    params = []
    date_sql = _source_date_filter_for_alias(PLAYER_REGISTRATION_SOURCE_TABLE, date_from, date_to, params, alias="s")
    q = f"""
        WITH base AS (
            SELECT s."id"::text AS source_id,
                   {player_username_expr} AS source_username,
                   LOWER(TRIM({player_username_expr})) AS source_username_normalized,
                   NULLIF(TRIM(s."data"->>'card_id'), '') AS source_card_id
            FROM {player_source_ref} s
            WHERE s."data" IS NOT NULL{date_sql}
              AND {player_username_expr} IS NOT NULL
        ), dupes AS (
            SELECT source_username, COUNT(*) AS duplicate_count
            FROM base
            GROUP BY source_username
            HAVING COUNT(*) > 1
        )
        SELECT b.*, d.duplicate_count,
               b.source_username AS source_reference_value,
               'duplicate_source_username_exact' AS status,
               'Exact source username appears more than once before migration.' AS reason
        FROM base b
        JOIN dupes d ON d.source_username = b.source_username
        ORDER BY b.source_username, b.source_id
        LIMIT %s
    """
    total_written += _run_preflight_detail_query(src_conn, "0b_preflight_player_duplicate_username_exact", q, params + [limit], PLAYER_REGISTRATION_SOURCE_T
ABLE, "duplicate_source_username_exact", "Exact source username duplicate rows before player migration", "username")

    # Player source: duplicate normalized username (case/space variants)
    params = []
    date_sql = _source_date_filter_for_alias(PLAYER_REGISTRATION_SOURCE_TABLE, date_from, date_to, params, alias="s")
    q = f"""
        WITH base AS (
            SELECT s."id"::text AS source_id,
                   {player_username_expr} AS source_username,
                   LOWER(TRIM({player_username_expr})) AS source_username_normalized,
                   NULLIF(TRIM(s."data"->>'card_id'), '') AS source_card_id
            FROM {player_source_ref} s
            WHERE s."data" IS NOT NULL{date_sql}
              AND {player_username_expr} IS NOT NULL
        ), dupes AS (
            SELECT source_username_normalized, COUNT(*) AS duplicate_count, COUNT(DISTINCT source_username) AS variant_count
            FROM base
            GROUP BY source_username_normalized
            HAVING COUNT(*) > 1
        )
        SELECT b.*, d.duplicate_count,
               b.source_username_normalized AS source_reference_value,
               'duplicate_source_username_normalized' AS status,
               'Normalized source username appears more than once; may be case/space variants.' AS reason
        FROM base b
        JOIN dupes d ON d.source_username_normalized = b.source_username_normalized
        ORDER BY b.source_username_normalized, b.source_username, b.source_id
        LIMIT %s
    """
    total_written += _run_preflight_detail_query(src_conn, "0c_preflight_player_duplicate_username_normalized", q, params + [limit], PLAYER_REGISTRATION_SOU
RCE_TABLE, "duplicate_source_username_normalized", "Normalized source username duplicate rows before player migration", "normalized_username")

    # Player source: duplicate card_id
    params = []
    date_sql = _source_date_filter_for_alias(PLAYER_REGISTRATION_SOURCE_TABLE, date_from, date_to, params, alias="s")
    q = f"""
        WITH base AS (
            SELECT s."id"::text AS source_id,
                   {player_username_expr} AS source_username,
                   LOWER(TRIM({player_username_expr})) AS source_username_normalized,
                   NULLIF(TRIM(s."data"->>'card_id'), '') AS source_card_id
            FROM {player_source_ref} s
            WHERE s."data" IS NOT NULL{date_sql}
              AND NULLIF(TRIM(s."data"->>'card_id'), '') IS NOT NULL
        ), dupes AS (
            SELECT source_card_id, COUNT(*) AS duplicate_count
            FROM base
            GROUP BY source_card_id
            HAVING COUNT(*) > 1
        )
        SELECT b.*, d.duplicate_count,
               b.source_card_id AS source_reference_value,
               'duplicate_source_card_id' AS status,
               'Source card_id appears on more than one registration row.' AS reason
        FROM base b
        JOIN dupes d ON d.source_card_id = b.source_card_id
        ORDER BY b.source_card_id, b.source_id
        LIMIT %s
    """
    total_written += _run_preflight_detail_query(src_conn, "0d_preflight_player_duplicate_card_id", q, params + [limit], PLAYER_REGISTRATION_SOURCE_TABLE, "
duplicate_source_card_id", "Duplicate card_id rows before player migration", "card_id")

    game_username_expr = "NULLIF(TRIM(COALESCE(s.\"data\"->>'PlayerAccount', s.\"data\"->>'playerAccount', s.\"data\"->>'username', s.\"data\"->>'userName',
 s.\"data\"->>'name', s.\"data\"->>'loginName', s.\"data\"->>'userid', s.\"data\"->>'userId', s.\"data\"->'member'->>'name', s.\"data\"->'member'->>'usernam
e', s.\"data\"->'member'->>'userName', s.\"data\"->'member'->>'loginName', s.\"data\"->'member'->>'userid')), '')"
    game_external_expr = "NULLIF(TRIM(COALESCE(s.\"data\"->>'TransactionID', s.\"data\"->>'transactionId', s.\"data\"->>'externalId', s.\"data\"->>'id')), '
')"
    game_source_ref = source_table_ref(GAME_TRANSACTION_SOURCE_TABLE)

    game_checks = [
        ("0e_preflight_game_missing_username", f"{game_username_expr} IS NULL", "missing_game_username", "Game source row has missing PlayerAccount/username
 before migration.", "username"),
        ("0f_preflight_game_missing_external_id", f"{game_external_expr} IS NULL", "missing_game_external_id", "Game source row has missing TransactionID/ex
ternalId before migration.", "externalId"),
        ("0g_preflight_game_missing_provider", "NULLIF(TRIM(COALESCE(s.\"data\"->>'GameProvider', s.\"data\"->>'gameProvider', s.\"data\"->>'provider', s.\"
data\"->>'Provider', s.\"data\"->'game'->>'provider')), '') IS NULL", "missing_game_provider", "Game source row has missing provider before dimension upsert
.", "gameProvider"),
        ("0h_preflight_game_missing_game_name", "NULLIF(TRIM(COALESCE(s.\"data\"->>'GameName', s.\"data\"->>'gameName', s.\"data\"->>'name', s.\"data\"->>'G
ameTitle', s.\"data\"->'game'->>'name')), '') IS NULL", "missing_game_name", "Game source row has missing game name before gameList upsert.", "gameName"),
        ("0i_preflight_game_missing_game_date", "NULLIF(TRIM(COALESCE(s.\"data\"->>'GameDate', s.\"data\"->>'gameDate', s.\"data\"->>'gamedate')), '') IS NU
LL", "missing_game_date", "Game source row has missing GameDate before migration.", "GameDate"),
    ]
    for check_name, predicate, status, reason, reference_type in game_checks:
        params = []
        date_sql = _source_date_filter_for_alias(GAME_TRANSACTION_SOURCE_TABLE, date_from, date_to, params, alias="s")
        q = f"""
            SELECT s."id"::text AS source_id,
                   {game_username_expr} AS source_username,
                   LOWER(TRIM({game_username_expr})) AS source_username_normalized,
                   {game_external_expr} AS source_reference_value,
                   {game_external_expr} AS source_card_id,
                   %s AS status,
                   %s AS reason
            FROM {game_source_ref} s
            WHERE s."data" IS NOT NULL{date_sql}
              AND {predicate}
            ORDER BY s."id"
            LIMIT %s
        """
        total_written += _run_preflight_detail_query(src_conn, check_name, q, [status, reason] + params + [limit], GAME_TRANSACTION_SOURCE_TABLE, status, re
ason, reference_type)

    # Game source: duplicate externalId/TransactionID
    params = []
    date_sql = _source_date_filter_for_alias(GAME_TRANSACTION_SOURCE_TABLE, date_from, date_to, params, alias="s")
    q = f"""
        WITH base AS (
            SELECT s."id"::text AS source_id,
                   {game_username_expr} AS source_username,
                   LOWER(TRIM({game_username_expr})) AS source_username_normalized,
                   {game_external_expr} AS source_reference_value
            FROM {game_source_ref} s
            WHERE s."data" IS NOT NULL{date_sql}
              AND {game_external_expr} IS NOT NULL
        ), dupes AS (
            SELECT source_reference_value, COUNT(*) AS duplicate_count
            FROM base
            GROUP BY source_reference_value
            HAVING COUNT(*) > 1
        )
        SELECT b.*, d.duplicate_count,
               'duplicate_game_external_id' AS status,
               'Duplicate game TransactionID/externalId appears in source before migration.' AS reason
        FROM base b
        JOIN dupes d ON d.source_reference_value = b.source_reference_value
        ORDER BY b.source_reference_value, b.source_id
        LIMIT %s
    """
    total_written += _run_preflight_detail_query(src_conn, "0j_preflight_game_duplicate_external_id", q, params + [limit], GAME_TRANSACTION_SOURCE_TABLE, "d
uplicate_game_external_id", "Duplicate game TransactionID/externalId rows before migration", "externalId")

    def _wallet_preflight(kind: str, table: str) -> int:
        written = 0
        wallet_username_expr = "NULLIF(TRIM(COALESCE(s.\"data\"->>'username', s.\"data\"->>'userName', s.\"data\"->>'name', s.\"data\"->>'loginName', s.\"da
ta\"->>'userid', s.\"data\"->>'userId', s.\"data\"->>'PlayerAccount', s.\"data\"->>'playerAccount', s.\"data\"->'member'->>'username', s.\"data\"->'member'-
>>'name', s.\"data\"->'member'->>'userName', s.\"data\"->'member'->>'loginName', s.\"data\"->'member'->>'userid')), '')"
        wallet_ref_expr = "NULLIF(TRIM(COALESCE(s.\"data\"->>'id', s.\"data\"->>'referenceId', s.\"data\"->>'transactionId')), '')"
        wallet_amount_expr = "NULLIF(TRIM(COALESCE(s.\"data\"->>'amount', s.\"data\"->>'TotalAmount', s.\"data\"->>'totalAmount')), '')"
        wallet_date_expr = "NULLIF(TRIM(COALESCE(s.\"data\"->>'transferDate', s.\"data\"->>'transferdate', s.\"data\"->>'TransferDate')), '')"
        wallet_source_ref = source_table_ref(table)
        checks = [
            (f"0k_preflight_wallet_{kind}_missing_username", f"{wallet_username_expr} IS NULL", f"missing_wallet_{kind}_username", f"{kind} source row has m
issing username before player_map lookup.", "username"),
            (f"0l_preflight_wallet_{kind}_missing_reference_id", f"{wallet_ref_expr} IS NULL", f"missing_wallet_{kind}_reference_id", f"{kind} source row ha
s missing referenceId/id before insert.", "referenceId"),
            (f"0m_preflight_wallet_{kind}_missing_amount", f"{wallet_amount_expr} IS NULL", f"missing_wallet_{kind}_amount", f"{kind} source row has missing
 amount before insert.", "amount"),
            (f"0n_preflight_wallet_{kind}_invalid_amount", f"{wallet_amount_expr} IS NOT NULL AND {wallet_amount_expr} !~ '^[-+]?[0-9]+(\\.[0-9]+)?$'", f"in
valid_wallet_{kind}_amount", f"{kind} source row has non-numeric amount before insert.", "amount"),
            (f"0o_preflight_wallet_{kind}_missing_transfer_date", f"{wallet_date_expr} IS NULL", f"missing_wallet_{kind}_transfer_date", f"{kind} source row
 has missing transferDate; runtime defaults to 1970-01-01.", "transferDate"),
        ]
        for check_name, predicate, status, reason, reference_type in checks:
            params: List[Any] = []
            date_sql = _source_date_filter_for_alias(table, date_from, date_to, params, alias="s")
            q = f"""
                SELECT s."id"::text AS source_id,
                       {wallet_username_expr} AS source_username,
                       LOWER(TRIM({wallet_username_expr})) AS source_username_normalized,
                       {wallet_ref_expr} AS source_reference_value,
                       %s AS status,
                       %s AS reason
                FROM {wallet_source_ref} s
                WHERE s."data" IS NOT NULL{date_sql}
                  AND {predicate}
                ORDER BY s."id"
                LIMIT %s
            """
            written += _run_preflight_detail_query(src_conn, check_name, q, [status, reason] + params + [limit], table, status, reason, reference_type)

        params = []
        date_sql = _source_date_filter_for_alias(table, date_from, date_to, params, alias="s")
        q = f"""
            WITH base AS (
                SELECT s."id"::text AS source_id,
                       {wallet_username_expr} AS source_username,
                       LOWER(TRIM({wallet_username_expr})) AS source_username_normalized,
                       {wallet_ref_expr} AS source_reference_value
                FROM {wallet_source_ref} s
                WHERE s."data" IS NOT NULL{date_sql}
                  AND {wallet_ref_expr} IS NOT NULL
            ), dupes AS (
                SELECT source_reference_value, COUNT(*) AS duplicate_count
                FROM base
                GROUP BY source_reference_value
                HAVING COUNT(*) > 1
            )
            SELECT b.*, d.duplicate_count,
                   %s AS status,
                   %s AS reason
            FROM base b
            JOIN dupes d ON d.source_reference_value = b.source_reference_value
            ORDER BY b.source_reference_value, b.source_id
            LIMIT %s
        """
        written += _run_preflight_detail_query(src_conn, f"0p_preflight_wallet_{kind}_duplicate_reference_id", q, params + [f"duplicate_wallet_{kind}_refere
nce_id", f"Duplicate {kind} referenceId/id appears in source before migration.", limit], table, f"duplicate_wallet_{kind}_reference_id", f"Duplicate {kind}
referenceId/id rows before migration", "referenceId")
        return written

    total_written += _wallet_preflight("deposit", DEPOSITS_SOURCE_TABLE)
    total_written += _wallet_preflight("withdrawal", WITHDRAWALS_SOURCE_TABLE)

    write_reconciliation_row("0_preflight_source_summary", "preflight_detail_rows_written", total_written, "Source preflight checks completed before migrati
on phases.")
    add_reconciliation_summary(f"Source preflight checks completed; detailRowsWritten={total_written}; csv={CSV_RECONCILIATION_PATH}.")
    trace_print(f"[PREFLIGHT] Completed source duplicate/missing-field checks. detailRowsWritten={total_written} csv={CSV_RECONCILIATION_PATH}")

def migrate_all(
        src_conn,
        tgt_conn,
        dry_run: bool,
        batch_size: int,
        commit_every: int,
        resume: bool,
        start_after_id: Optional[str],
        max_rows_total: Optional[int],
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
) -> int:
    SOURCE_QUERY_TRACE.clear()
    ensure_wallet_dedupe_index(tgt_conn, dry_run=dry_run)
    from_dt_iso: Optional[str] = date_from.isoformat() if date_from is not None else None
    until_dt_iso: Optional[str] = date_to.isoformat() if date_to is not None else None
    date_window_requested = (from_dt_iso is not None) or (until_dt_iso is not None)
    if date_window_requested:
        print("[DATE WINDOW] --date-from/--date-to enabled. Source reads use PlayerRegistrations.createddate, GameTransaction.GameDate, and Deposits/Withdra
wals.transferDate. Checkpoints are ignored for source reads in this run.", flush=True)

    def _initial_cursor(phase_name: str) -> Tuple[Optional[str], Optional[str]]:
        if date_window_requested:
            return (None, start_after_id)
        if resume:
            cp = checkpoint_get(tgt_conn, phase_name)
            if cp:
                print(f"Resuming phase '{phase_name}' from checkpoint pointer: {cp}", flush=True)
                return parse_inplayv2_checkpoint(cp)
        return (None, start_after_id)

    player_map = build_player_map(tgt_conn)
    print(f"Initial target player mapping lookup loaded: {len(player_map)} items mapped", flush=True)
    provider_cache: Dict[str, uuid.UUID] = {}
    gametype_cache: Dict[str, uuid.UUID] = {}
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID] = {}
    detail_map = fetch_player_detail_map(src_conn, from_dt_iso, until_dt_iso)
    registered_usernames = set()

    # PREFLIGHT CHECKS: read-only source duplicate/missing-field audit before migration writes.
    # This does not modify target tables or change migration behavior.
    run_source_preflight_checks(src_conn, from_dt_iso, until_dt_iso, dry_run=dry_run)

    phase = "player"
    after_dt, after_id = _initial_cursor(phase)
    processed = player_upserted = player_skipped = 0
    last_dt, last_id = after_dt, after_id or ""
    while True:
        if max_rows_total is not None and processed >= max_rows_total:
            break
        fetch_limit = min(batch_size, max_rows_total - processed) if max_rows_total is not None else batch_size
        rows = fetch_json_table_batch(src_conn, PLAYER_REGISTRATION_SOURCE_TABLE, last_dt, last_id or None, fetch_limit, from_dt=from_dt_iso, until_dt=until
_dt_iso)
        if not rows:
            break
        for r in rows:
            rid = str(r["id"])
            data = as_dict(r.get("data"))
            if not data:
                player_skipped += 1
                record_player_skip(
                    source_id=rid,
                    username=None,
                    reason="Missing or invalid source JSON data",
                    dry_run=dry_run,
                    source_table=PLAYER_REGISTRATION_SOURCE_TABLE,
                    action="skipped",
                    data=data,
                )
                processed += 1
                last_id = rid
                continue
            username = extract_username(data)
            if username:
                registered_usernames.add(username)
            try:
                pid = upsert_player_from_member(tgt_conn, data, BRAND, dry_run=dry_run, detail_map=detail_map, source_id=rid)
                if pid and username:
                    player_map[username] = pid
                    player_upserted += 1
                else:
                    player_skipped += 1
            except Exception as e:
                player_skipped += 1
                record_player_skip(
                    source_id=rid,
                    username=username if 'username' in locals() else None,
                    reason="Player upsert failed",
                    dry_run=dry_run,
                    source_table=PLAYER_REGISTRATION_SOURCE_TABLE,
                    action="upsert_failed",
                    error=e,
                    data=data,
                )
                tgt_conn.rollback()
            processed += 1
            last_id = rid
            last_dt = source_dt_value(data, PLAYER_REGISTRATION_SOURCE_TABLE)
            if (not dry_run) and (processed % commit_every == 0):
                checkpoint_set(tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run)
                tgt_conn.commit()
        print(f"Progress players: processed={processed} upserted={player_upserted} skipped={player_skipped} lastId={last_id}", flush=True)
    if not dry_run:
        if processed > 0:
            checkpoint_set(tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run)
        tgt_conn.commit()
    print_source_query_summary("playerRegistration", [f"{PLAYER_REGISTRATION_SOURCE_TABLE} detail-map", f"{PLAYER_REGISTRATION_SOURCE_TABLE} batch"])
    emit_player_phase_summary(processed, player_upserted, player_skipped)
    if not dry_run:
        player_map = build_player_map(tgt_conn)

    # RECONCILIATION CHECKS 1-6: post-player phase validation and CSV/email summary.
    # This catches source card_id present but target externalId still NULL before the run proceeds.
    run_player_reconciliation_checks(src_conn, tgt_conn, from_dt_iso, until_dt_iso, dry_run=dry_run)
    run_dimension_reference_reconciliation_checks(src_conn, tgt_conn, from_dt_iso, until_dt_iso, dry_run=dry_run)

    phase = "gameTx"
    after_dt, after_id = _initial_cursor(phase)
    processed_gt = inserted_gt_total = skipped_gt_total = 0
    last_dt, last_id = after_dt, after_id or ""
    while True:
        if max_rows_total is not None and processed_gt >= max_rows_total:
            break
        fetch_limit = min(batch_size, max_rows_total - processed_gt) if max_rows_total is not None else batch_size
        rows = fetch_json_table_batch(src_conn, GAME_TRANSACTION_SOURCE_TABLE, last_dt, last_id or None, fetch_limit, from_dt=from_dt_iso, until_dt=until_dt
_iso)
        if not rows:
            break
        inserted, skipped = insert_game_tx_batch(tgt_conn, rows, player_map, provider_cache, gametype_cache, gamelist_cache, dry_run=dry_run, registered_use
rnames=registered_usernames)
        inserted_gt_total += inserted
        processed_gt += len(rows)
        skipped_gt_total += skipped
        last_row_data = as_dict(rows[-1].get("data"))
        last_id = str(rows[-1]["id"])
        last_dt = source_dt_value(last_row_data, GAME_TRANSACTION_SOURCE_TABLE)
        if not dry_run:
            checkpoint_set(tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run)
            if (processed_gt % commit_every) < batch_size:
                tgt_conn.commit()
        print(f"Progress gameTx: processed={processed_gt} inserted={inserted_gt_total} skipped={skipped_gt_total} lastId={last_id}", flush
=True)
    if not dry_run:
        tgt_conn.commit()
    print_source_query_summary("gameTransaction", [f"{GAME_TRANSACTION_SOURCE_TABLE} batch"])
    trace_print(f"Completed gameTx phase. sourceProcessed={processed_gt} inserted_or_would_insert={inserted_gt_total} skipped={skipped_gt_total} duplicates=
{phase_duplicate_total('gameTransaction')} total_duplicates={phase_duplicate_total('gameTransaction')} reportCsvRows={phase_report_total('gameTransaction')}
 reportActions=[{phase_report_counts_text('gameTransaction')}] reportIssues=[{phase_report_issue_counts_text('gameTransaction')}] reportCsvPath={CSV_GAMETX_
PATH}")

    phase = "deposits"
    after_dt, after_id = _initial_cursor(phase)
    processed_dep = inserted_dep_total = skipped_dep_total = 0
    last_dt, last_id = after_dt, after_id or ""
    while True:
        if max_rows_total is not None and processed_dep >= max_rows_total:
            break
        fetch_limit = min(batch_size, max_rows_total - processed_dep) if max_rows_total is not None else batch_size
        rows = fetch_json_table_batch(src_conn, DEPOSITS_SOURCE_TABLE, last_dt, last_id or None, fetch_limit, from_dt=from_dt_iso, until_dt=until_dt_iso)
        if not rows:
            break
        inserted, skipped = insert_wallet_batch(tgt_conn, rows, "deposit", player_map, dry_run=dry_run, registered_usernames=registered_usernames)
        inserted_dep_total += inserted
        skipped_dep_total += skipped
        processed_dep += len(rows)
        last_row_data = as_dict(rows[-1].get("data"))
        last_id = str(rows[-1]["id"])
        last_dt = source_dt_value(last_row_data, DEPOSITS_SOURCE_TABLE)
        if not dry_run:
            checkpoint_set(tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run)
            if (processed_dep % commit_every) < batch_size:
                tgt_conn.commit()
        print(f"Progress deposits: processed={processed_dep} inserted={inserted_dep_total} skipped={skipped_dep_total} lastId={last_id}",
flush=True)
    if not dry_run:
        tgt_conn.commit()
    print_source_query_summary("walletTransaction.deposits", [f"{DEPOSITS_SOURCE_TABLE} batch"])
    trace_print(f"Completed deposits phase. sourceProcessed={processed_dep} inserted_or_would_insert={inserted_dep_total} skipped={skipped_dep_total} duplic
ates={phase_duplicate_total('walletTransaction.deposit')} total_duplicates={phase_duplicate_total('walletTransaction.deposit')} reportCsvRows={phase_report_
total('walletTransaction.deposit')} reportActions=[{phase_report_counts_text('walletTransaction.deposit')}] reportIssues=[{phase_report_issue_counts_text('w
alletTransaction.deposit')}] reportCsvPath={CSV_DEPOSITS_PATH}")

    phase = "withdrawals"
    after_dt, after_id = _initial_cursor(phase)
    processed_wd = inserted_wd_total = skipped_wd_total = 0
    last_dt, last_id = after_dt, after_id or ""
    while True:
        if max_rows_total is not None and processed_wd >= max_rows_total:
            break
        fetch_limit = min(batch_size, max_rows_total - processed_wd) if max_rows_total is not None else batch_size
        rows = fetch_json_table_batch(src_conn, WITHDRAWALS_SOURCE_TABLE, last_dt, last_id or None, fetch_limit, from_dt=from_dt_iso, until_dt=until_dt_iso)
        if not rows:
            break
        inserted, skipped = insert_wallet_batch(tgt_conn, rows, "withdrawal", player_map, dry_run=dry_run, registered_usernames=registered_usernames)
        inserted_wd_total += inserted
        skipped_wd_total += skipped
        processed_wd += len(rows)
        last_row_data = as_dict(rows[-1].get("data"))
        last_id = str(rows[-1]["id"])
        last_dt = source_dt_value(last_row_data, WITHDRAWALS_SOURCE_TABLE)
        if not dry_run:
            checkpoint_set(tgt_conn, phase, format_inplayv2_checkpoint(last_dt, last_id), dry_run=dry_run)
            if (processed_wd % commit_every) < batch_size:
                tgt_conn.commit()
        print(f"Progress withdrawals: processed={processed_wd} inserted={inserted_wd_total} skipped={skipped_wd_total} lastId={last_id}",
flush=True)
    if not dry_run:
        tgt_conn.commit()
    print_source_query_summary("walletTransaction.withdrawals", [f"{WITHDRAWALS_SOURCE_TABLE} batch"])
    trace_print(f"Completed withdrawals phase. sourceProcessed={processed_wd} inserted_or_would_insert={inserted_wd_total} skipped={skipped_wd_total} duplic
ates={phase_duplicate_total('walletTransaction.withdrawal')} total_duplicates={phase_duplicate_total('walletTransaction.withdrawal')} reportCsvRows={phase_r
eport_total('walletTransaction.withdrawal')} reportActions=[{phase_report_counts_text('walletTransaction.withdrawal')}] reportIssues=[{phase_report_issue_co
unts_text('walletTransaction.withdrawal')}] reportCsvPath={CSV_WITHDRAWALS_PATH}")

    # DATA QUALITY CHECKER NOTE 2026-05-21:
    # Run after player + wallet phases so the target rows exist.
    # gameTransaction is intentionally skipped per request.
    run_post_migration_data_quality_checks(tgt_conn, from_dt_iso, until_dt_iso, dry_run=dry_run)

    total_inserted = player_upserted + inserted_gt_total + inserted_dep_total + inserted_wd_total
    total_source_processed = processed + processed_gt + processed_dep + processed_wd
    total_skipped = player_skipped + skipped_gt_total + skipped_dep_total + skipped_wd_total
    summary_msg = (
        "\n[RUN SUMMARY]\n"
        f"  playerDetails_final inserted_or_updated={player_upserted}, sourceProcessed={processed}, skipped={player_skipped}, "
        f"reportCsvRows={player_report_total()}, fixed={player_report_fixed_total()}, total_fixed={player_report_fixed_total()}, "
        f"duplicates={player_report_duplicate_total()}, total_duplicates={player_report_duplicate_total()}, "
        f"emailCorrectionRows={player_report_fixed_total()}, duplicateRows={player_report_duplicate_total()}, "
        f"failureReportRows={player_report_failure_total()}\n"
        f"  playerDetails_final reportActions=[{player_report_counts_text()}]\n"
        f"  playerDetails_final reportIssues=[{player_report_issue_counts_text()}]\n"
        f"  playerDetails_final reportCsvPath={CSV_PLAYERS_PATH}\n"
        f"  gameTransaction_final inserted_or_would_insert={inserted_gt_total}, sourceProcessed={processed_gt}, skipped={skipped_gt_total}, "
        f"duplicates={phase_duplicate_total('gameTransaction')}, total_duplicates={phase_duplicate_total('gameTransaction')}, "
        f"reportCsvRows={phase_report_total('gameTransaction')}, reportCsvPath={CSV_GAMETX_PATH}\n"
        f"  gameTransaction_final reportActions=[{phase_report_counts_text('gameTransaction')}]\n"
        f"  gameTransaction_final reportIssues=[{phase_report_issue_counts_text('gameTransaction')}]\n"
        f"  walletTransaction_final deposits inserted_or_would_insert={inserted_dep_total}, sourceProcessed={processed_dep}, skipped={skipped_dep_total}, "
        f"duplicates={phase_duplicate_total('walletTransaction.deposit')}, total_duplicates={phase_duplicate_total('walletTransaction.deposit')}, "
        f"reportCsvRows={phase_report_total('walletTransaction.deposit')}, reportCsvPath={CSV_DEPOSITS_PATH}\n"
        f"  walletTransaction_final deposits reportActions=[{phase_report_counts_text('walletTransaction.deposit')}]\n"
        f"  walletTransaction_final deposits reportIssues=[{phase_report_issue_counts_text('walletTransaction.deposit')}]\n"
        f"  walletTransaction_final withdrawals inserted_or_would_insert={inserted_wd_total}, sourceProcessed={processed_wd}, skipped={skipped_wd_total}, "
        f"duplicates={phase_duplicate_total('walletTransaction.withdrawal')}, total_duplicates={phase_duplicate_total('walletTransaction.withdrawal')}, "
        f"reportCsvRows={phase_report_total('walletTransaction.withdrawal')}, reportCsvPath={CSV_WITHDRAWALS_PATH}\n"
        f"  walletTransaction_final withdrawals reportActions=[{phase_report_counts_text('walletTransaction.withdrawal')}]\n"
        f"  walletTransaction_final withdrawals reportIssues=[{phase_report_issue_counts_text('walletTransaction.withdrawal')}]\n"
        f"  TOTAL sourceProcessed={total_source_processed}\n"
        f"  TOTAL skipped={total_skipped}\n"
        f"  TOTAL fixed={player_report_fixed_total()}\n"
        f"  TOTAL player_duplicates={player_report_duplicate_total()}\n"
        f"  TOTAL transaction_duplicates={all_phase_duplicate_total()}\n"
        f"  TOTAL duplicates={player_report_duplicate_total() + all_phase_duplicate_total()}\n"
        f"  TOTAL total_duplicates={player_report_duplicate_total() + all_phase_duplicate_total()}\n"
        f"  TOTAL records inserted_or_would_insert={total_inserted}"
    )
    trace_print(summary_msg)
    return total_source_processed



# ============================================================================
# Additive audit/reporting overrides requested 2026-05-26
# - Keep existing migration business logic intact.
# - Add richer CSV + trace details for wallet/game skips, duplicate ignores,
#   dimension upsert failures, and batch insert failures.
# - These override earlier function names at runtime; migrate_all resolves these
#   global functions when it executes, so no existing call sites need to change.
# ============================================================================

GAME_REPORT_FIELDNAMES_EXT = [
    "sourceTable", "sourceId", "externalId", "username", "sourceUsername",
    "targetId", "targetUsername", "targetPlayerId", "targetExternalId",
    "providerName", "gameName", "gameType", "outlet", "roundId",
    "action", "issueType", "reason", "error", "dryRun", "timestamp", "sourcePayload",
]

WALLET_REPORT_FIELDNAMES_EXT = [
    "sourceTable", "sourceId", "referenceId", "username", "sourceUsername",
    "targetId", "targetUsername", "targetPlayerId", "targetReferenceId",
    "transactionType", "amount", "paymentGateway",
    "action", "issueType", "reason", "error", "dryRun", "timestamp", "sourcePayload",
]


def _norm_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _fetch_existing_game_targets(tgt_conn, external_ids: List[Any]) -> Dict[str, Dict[str, Any]]:
    refs = sorted({str(x).strip() for x in external_ids if str(x or "").strip()})
    if not refs:
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    try:
        with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    gt.id AS target_id,
                    gt."externalId" AS target_external_id,
                    gt."playerId" AS target_player_id,
                    gt."playerUserName" AS target_username,
                    pd."userName" AS playerdetails_username
                FROM kemet."gameTransaction_final" gt
                LEFT JOIN kemet."playerDetails_final" pd ON pd.id = gt."playerId"
                WHERE gt."externalId" = ANY(%s)
                """,
                (refs,),
            )
            for row in cur.fetchall():
                ref = str(row.get("target_external_id") or "").strip()
                if ref and ref not in result:
                    result[ref] = row
    except Exception as e:
        trace_print(f"[AUDIT][gameTransaction][WARN] unable to fetch existing duplicate targets: {e}", level=logging.WARNING)
        try:
            tgt_conn.rollback()
        except Exception:
            pass
    return result


def _fetch_existing_wallet_targets(tgt_conn, kind: str, reference_ids: List[Any]) -> Dict[str, Dict[str, Any]]:
    refs = sorted({str(x).strip() for x in reference_ids if str(x or "").strip()})
    if not refs:
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    try:
        with tgt_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    wt.id AS target_id,
                    wt."referenceId" AS target_reference_id,
                    wt."playerId" AS target_player_id,
                    pd."userName" AS target_username,
                    wt.amount AS target_amount,
                    wt."paymentGateway" AS target_payment_gateway
                FROM kemet."walletTransaction_final" wt
                LEFT JOIN kemet."playerDetails_final" pd ON pd.id = wt."playerId"
                WHERE wt.platform = %s
                  AND wt."transactionType" = %s
                  AND wt."referenceId" = ANY(%s)
                """,
                (WALLET_PLATFORM, kind, refs),
            )
            for row in cur.fetchall():
                ref = str(row.get("target_reference_id") or "").strip()
                if ref and ref not in result:
                    result[ref] = row
    except Exception as e:
        trace_print(f"[AUDIT][walletTransaction.{kind}][WARN] unable to fetch existing duplicate targets: {e}", level=logging.WARNING)
        try:
            tgt_conn.rollback()
        except Exception:
            pass
    return result


def record_dimension_report(
        source_id: Any,
        username: Optional[str],
        source_reference_type: str,
        source_reference_value: Any,
        target_table: str,
        reason: str,
        action: str,
        issue_type: str,
        dry_run: bool,
        error: Optional[Any] = None,
        data: Optional[Dict[str, Any]] = None,
        target_id: Any = "",
        target_reference_value: Any = "",
) -> None:
    """Trace and export dimension/reference failures into reconciliation CSV."""
    msg = (
        f"[REPORT][dimensionReference] sourceTable={GAME_TRANSACTION_SOURCE_TABLE} sourceId={source_id or 'N/A'} "
        f"username={username or ''} sourceReferenceType={source_reference_type} "
        f"sourceReferenceValue={source_reference_value or ''} targetTable={target_table} "
        f"targetId={target_id or ''} action={action} issueType={issue_type} reason={reason}"
    )
    if error is not None:
        msg += f" error={error}"
    trace_print(msg)
    write_reconciliation_trace_row(
        check_name="11_dimension_reference_runtime_report",
        status=issue_type,
        source_table=GAME_TRANSACTION_SOURCE_TABLE,
        source_id=source_id or "",
        source_username=username or "",
        source_username_normalized=_norm_key(username),
        source_reference_type=source_reference_type,
        source_reference_value=source_reference_value or "",
        target_table=target_table,
        target_id=target_id or "",
        target_username="",
        target_reference_type=source_reference_type,
        target_reference_value=target_reference_value or "",
        reason=reason if error is None else f"{reason}; error={error}",
        notes="Runtime dimension/reference report generated during gameTransaction transformation. Source payload retained in phase CSV when available.",
    )


def record_game_report(
        source_id: Any,
        external_id: Any,
        username: Optional[str],
        reason: str,
        dry_run: bool,
        action: str = "skipped",
        issue_type: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
        target_id: Optional[Any] = None,
        target_username: Optional[Any] = None,
        target_player_id: Optional[Any] = None,
        target_external_id: Optional[Any] = None,
        provider_name: Optional[Any] = None,
        game_name: Optional[Any] = None,
        game_type: Optional[Any] = None,
        outlet: Optional[Any] = None,
        round_id: Optional[Any] = None,
        error: Optional[Any] = None,
) -> None:
    """Print/log/export gameTransaction skip, duplicate, and failure rows with target trace fields."""
    phase = "gameTransaction"
    event_label = "[REPORT][gameTransaction]" if action != "skipped" else "[SKIP][gameTransaction]"
    msg = (
        f"{event_label} sourceTable={GAME_TRANSACTION_SOURCE_TABLE} sourceId={source_id or 'N/A'} "
        f"externalId={external_id or ''} sourceUsername={username or ''} "
        f"targetId={target_id or ''} targetUsername={target_username or ''} targetPlayerId={target_player_id or ''} "
        f"action={action} issueType={issue_type or action} reason={reason}"
    )
    if error is not None:
        msg += f" error={error}"
    trace_print(msg)
    _counter_inc(PHASE_REPORT_COUNTS.setdefault(phase, {}), action)
    _counter_inc(PHASE_REPORT_ISSUE_COUNTS.setdefault(phase, {}), issue_type or action)
    write_skipped_to_csv(
        filepath=CSV_GAMETX_PATH,
        fieldnames=GAME_REPORT_FIELDNAMES_EXT,
        row_data={
            "sourceTable": GAME_TRANSACTION_SOURCE_TABLE,
            "sourceId": source_id or "",
            "externalId": external_id or "",
            "username": username or "",
            "sourceUsername": username or "",
            "targetId": target_id or "",
            "targetUsername": target_username or "",
            "targetPlayerId": target_player_id or "",
            "targetExternalId": target_external_id or "",
            "providerName": provider_name or "",
            "gameName": game_name or "",
            "gameType": game_type or "",
            "outlet": outlet or "",
            "roundId": round_id or "",
            "action": action,
            "issueType": issue_type or action,
            "reason": reason,
            "error": str(error or ""),
            "dryRun": str(bool(dry_run)),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sourcePayload": _csv_safe_json(data or {}),
        },
    )
    trace_print(f"[CSV][gameTransaction] wrote report row path={CSV_GAMETX_PATH} sourceId={source_id or 'N/A'} action={action}")


def record_game_skip(source_id: Any, external_id: Any, username: Optional[str], reason: str, dry_run: bool, data: Optional[Dict[str, Any]] = None, **extra:
Any) -> None:
    """Print/log/export a gameTransaction skip reason."""
    record_game_report(
        source_id=source_id,
        external_id=external_id,
        username=username,
        reason=reason,
        dry_run=dry_run,
        action="skipped",
        issue_type=extra.pop("issue_type", "skipped"),
        data=data,
        **extra,
    )


def record_wallet_report(
        kind: str,
        source_id: Any,
        username: Optional[str],
        reason: str,
        dry_run: bool,
        reference_id: Optional[Any] = None,
        action: str = "skipped",
        issue_type: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
        target_id: Optional[Any] = None,
        target_username: Optional[Any] = None,
        target_player_id: Optional[Any] = None,
        target_reference_id: Optional[Any] = None,
        amount: Optional[Any] = None,
        payment_gateway: Optional[Any] = None,
        error: Optional[Any] = None,
) -> None:
    """Print/log/export walletTransaction skip, duplicate, and failure rows with target trace fields."""
    phase = f"walletTransaction.{kind}"
    source_table = DEPOSITS_SOURCE_TABLE if kind == "deposit" else WITHDRAWALS_SOURCE_TABLE
    event_label = f"[REPORT][walletTransaction.{kind}]" if action != "skipped" else f"[SKIP][walletTransaction.{kind}]"
    msg = (
        f"{event_label} sourceTable={source_table} sourceId={source_id or 'N/A'} "
        f"referenceId={reference_id or ''} sourceUsername={username or ''} targetId={target_id or ''} "
        f"targetUsername={target_username or ''} targetPlayerId={target_player_id or ''} transactionType={kind} "
        f"action={action} issueType={issue_type or action} reason={reason}"
    )
    if error is not None:
        msg += f" error={error}"
    trace_print(msg)
    _counter_inc(PHASE_REPORT_COUNTS.setdefault(phase, {}), action)
    _counter_inc(PHASE_REPORT_ISSUE_COUNTS.setdefault(phase, {}), issue_type or action)
    target_csv = CSV_DEPOSITS_PATH if kind == "deposit" else CSV_WITHDRAWALS_PATH
    write_skipped_to_csv(
        filepath=target_csv,
        fieldnames=WALLET_REPORT_FIELDNAMES_EXT,
        row_data={
            "sourceTable": source_table,
            "sourceId": source_id or "",
            "referenceId": reference_id or "",
            "username": username or "",
            "sourceUsername": username or "",
            "targetId": target_id or "",
            "targetUsername": target_username or "",
            "targetPlayerId": target_player_id or "",
            "targetReferenceId": target_reference_id or "",
            "transactionType": kind,
            "amount": amount if amount is not None else "",
            "paymentGateway": payment_gateway or "",
            "action": action,
            "issueType": issue_type or action,
            "reason": reason,
            "error": str(error or ""),
            "dryRun": str(bool(dry_run)),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sourcePayload": _csv_safe_json(data or {}),
        },
    )
    trace_print(f"[CSV][walletTransaction.{kind}] wrote report row path={target_csv} sourceId={source_id or 'N/A'} action={action}")


def record_wallet_skip(kind: str, source_id: Any, username: Optional[str], reason: str, dry_run: bool, data: Optional[Dict[str, Any]] = None, reference_id:
Optional[Any] = None, **extra: Any) -> None:
    """Print/log/export a walletTransaction skip reason."""
    record_wallet_report(
        kind=kind,
        source_id=source_id,
        username=username,
        reason=reason,
        dry_run=dry_run,
        reference_id=reference_id,
        action="skipped",
        issue_type=extra.pop("issue_type", "skipped"),
        data=data,
        **extra,
    )


def insert_game_tx_batch(
    tgt_conn,
    rows: List[Dict[str, Any]],
    player_map: Dict[str, uuid.UUID],
    provider_cache: Dict[str, uuid.UUID],
    gametype_cache: Dict[str, uuid.UUID],
    gamelist_cache: Dict[Tuple[uuid.UUID, str], uuid.UUID],
    dry_run: bool,
    registered_usernames: Optional[set] = None,
) -> Tuple[int, int]:
    """Audited override: same game insert rules, richer skip/duplicate/failure reporting."""
    values: List[Tuple[Any, ...]] = []
    value_meta: List[Dict[str, Any]] = []
    skipped_rows = 0

    for r in rows:
        source_id = str(r.get("id") or r.get("src_id") or "").strip()
        data = as_dict(r.get("data"))
        if not source_id:
            skipped_rows += 1
            record_game_skip(None, None, None, "Missing source row id", dry_run, data=data, issue_type="missing_source_id")
            continue
        if not data:
            skipped_rows += 1
            record_game_skip(source_id, None, None, "Missing or invalid source JSON data", dry_run, data=data, issue_type="missing_or_invalid_json")
            continue

        member = as_dict(data.get("member"))
        game = as_dict(data.get("game"))
        external_id = str(_first_present(data, "TransactionID", "transactionId", "externalId", "id") or source_id).strip()
        if not external_id:
            skipped_rows += 1
            record_game_skip(source_id, None, None, "Missing game transaction externalId/id", dry_run, data=data, issue_type="missing_external_id")
            continue

        username = str(
            _first_present(data, "PlayerAccount", "playerAccount", "username", "userName", "name", "loginName", "userid", "userId")
            or extract_username(member)
            or ""
        ).strip()
        if not username:
            skipped_rows += 1
            record_game_skip(source_id, external_id, None, "Missing username in game transaction source payload", dry_run, data=data, issue_type="missing_us
ername")
            continue

        player_id = player_map.get(username)
        if not player_id:
            skipped_rows += 1
            issue = "player_not_in_player_map"
            if registered_usernames is not None and username in registered_usernames:
                issue = "player_exists_in_source_registration_but_not_target_player_map"
            record_game_skip(source_id, external_id, username, _skip_reason_missing_player(username, registered_usernames), dry_run, data=data, issue_type=i
ssue)
            continue

        provider_name = str(_first_present(data, "GameProvider", "gameProvider", "provider", "Provider") or game.get("provider") or "UNKNOWN").strip() or "U
NKNOWN"
        game_name = str(_first_present(data, "GameName", "gameName", "name", "GameTitle") or game.get("name") or "UNKNOWN").strip() or "UNKNOWN"
        game_type_raw = str(_first_present(data, "GameType", "gameType", "type") or game.get("type") or "Slots")
        outlet = str(_first_present(data, "Outlet", "outlet", "tableRoomId") or "").strip() or None
        round_id = str(_first_present(data, "SessionID", "sessionId", "vendorRoundId", "roundId") or "").strip() or None

        try:
            provider_id = get_or_create_game_provider(tgt_conn, provider_name, provider_cache, dry_run)
            game_type_id = get_or_create_game_type(tgt_conn, game_type_raw, gametype_cache, dry_run)
            game_id = get_or_create_game_list(tgt_conn, game_name, provider_id, game_type_id, gamelist_cache, dry_run)
        except Exception as e:
            skipped_rows += 1
            try:
                tgt_conn.rollback()
            except Exception:
                pass
            record_game_report(
                source_id=source_id,
                external_id=external_id,
                username=username,
                reason="Dimension/reference upsert failed before gameTransaction insert",
                dry_run=dry_run,
                action="dimension_upsert_failed",
                issue_type="dimension_reference_upsert_failed",
                data=data,
                provider_name=provider_name,
                game_name=game_name,
                game_type=game_type_raw,
                outlet=outlet,
                round_id=round_id,
                error=e,
            )
            record_dimension_report(source_id, username, "gameProvider/gameType/gameList", f"provider={provider_name}; type={game_type_raw}; game={game_name
}", "gameProvider_final/gameType_final/gameList_final", "Dimension/reference upsert failed", "dimension_upsert_failed", "dimension_reference_upsert_failed",
 dry_run, error=e, data=data)
            continue

        start_dt = parse_iso_dt(_first_present(data, "GameDate", "gameDate", "gamedate", "dateTimeCreated", "createdDateTime")) or datetime.now(timezone.utc
)
        end_dt = parse_iso_dt(_first_present(data, "UpdateDateTime", "updatedAt", "dateTimeSettled", "settledDate", "GameDate")) or start_dt
        bet_amount = to_decimal_str(_first_present(data, "TotalStakes", "bet", "betAmount", "stake"))
        payout_amount = to_decimal_str(_first_present(data, "TotalWins", "payout", "payoutAmount", "win"))
        valid_bet = to_decimal_str(_first_present(data, "ValidBet", "validBet", "TotalStakes", "bet", "betAmount", "stake"))
        pc1, pc2, pc3, pc4 = (to_decimal_str(data.get(k)) for k in ("PC1", "PC2", "PC3", "PC4"))
        pc5 = to_decimal_str(_first_present(data, "PC5", "jackpotContribution"))
        jw1, jw2, jw3, jw4 = (to_decimal_str(data.get(k)) for k in ("JW1", "JW2", "JW3", "JW4"))
        jw5 = to_decimal_str(_first_present(data, "JW5", "jackpotPayout"))
        progression_paid = to_decimal_str(_first_present(data, "PROGRESSIVE_CONTRIBUTION_PAID", "progressionContributionPaid"))
        seed_won = to_decimal_str(_first_present(data, "SEED_MONEY_WON", "seedMoneyWon"))
        seed_over_raw = data.get("SEED_MONEY_JACKPOT_WON_OVER_1000") or data.get("seedMoneyJackpotOver1000")
        try:
            seed_over_1000_bool = bool(int(float(seed_over_raw or 0)))
        except Exception:
            seed_over_1000_bool = False
        seed_over_1000_int = 1 if seed_over_1000_bool else 0

        values.append((
            start_dt, provider_id, game_id, game_type_id, player_id,
            username, outlet, "0", bet_amount, valid_bet, payout_amount,
            pc1, pc2, pc3, pc4, pc5,
            jw1, jw2, jw3, jw4, jw5,
            progression_paid, seed_won, seed_over_1000_int,
            end_dt, external_id, False, None, None,
            BRAND, PLATFORM, round_id,
        ))
        value_meta.append({
            "sourceId": source_id,
            "externalId": external_id,
            "username": username,
            "targetPlayerId": player_id,
            "providerName": provider_name,
            "gameName": game_name,
            "gameType": game_type_raw,
            "outlet": outlet,
            "roundId": round_id,
            "data": data,
        })

    if skipped_rows:
        trace_print(f"[SUMMARY][gameTransaction] insertable={len(values)} skipped={skipped_rows}")
    if dry_run:
        trace_print(f"[DRY-RUN] Would insert {len(values)} gameTransaction_final rows. skipped={skipped_rows}")
        return (len(values), skipped_rows)
    if not values:
        return (0, skipped_rows)

    sql = """
    INSERT INTO kemet."gameTransaction_final" (
        "startDateTime", "providerId", "gameId", "gameTypeId", "playerId",
        "playerUserName", "tableRoomId", "sideBetAmount", "validBet", "betAmount",
        "payoutAmount", "PC1","PC2","PC3","PC4","PC5",
        "JW1","JW2","JW3","JW4","JW5",
        "progressionContributionPaid", "seedMoneyWon", "seedMoneyJackpotOver1000",
        "endDateTime", "externalId", "parlay", "betDetails", "betTiming",
        "brand", "platform", "roundId"
    )
    VALUES %s
    ON CONFLICT ("externalId") DO NOTHING
    RETURNING "externalId"
    """
    try:
        with tgt_conn.cursor() as cur:
            inserted_rows = execute_values(cur, sql, values, page_size=500, fetch=True)
    except Exception as e:
        try:
            tgt_conn.rollback()
        except Exception:
            pass
        for meta in value_meta:
            record_game_report(
                source_id=meta.get("sourceId"),
                external_id=meta.get("externalId"),
                username=meta.get("username"),
                reason="gameTransaction batch insert failed; no rows from this prepared batch were inserted",
                dry_run=dry_run,
                action="insert_failed",
                issue_type="game_transaction_insert_failed",
                data=meta.get("data") or {},
                target_player_id=meta.get("targetPlayerId"),
                provider_name=meta.get("providerName"),
                game_name=meta.get("gameName"),
                game_type=meta.get("gameType"),
                outlet=meta.get("outlet"),
                round_id=meta.get("roundId"),
                error=e,
            )
        trace_print(f"[LIVE][gameTransaction] batch insert failed attempted={len(values)} skipped={skipped_rows} error={e}", level=logging.ERROR)
        return (0, skipped_rows + len(values))

    inserted_counts: Dict[str, int] = {}
    for row in inserted_rows or []:
        inserted_ref = str(row[0])
        inserted_counts[inserted_ref] = inserted_counts.get(inserted_ref, 0) + 1

    duplicate_targets = _fetch_existing_game_targets(tgt_conn, [m.get("externalId") for m in value_meta])
    duplicate_count = 0
    for meta in value_meta:
        ext = str(meta["externalId"])
        if inserted_counts.get(ext, 0) > 0:
            inserted_counts[ext] -= 1
            continue
        duplicate_count += 1
        tgt = duplicate_targets.get(ext) or {}
        record_game_report(
            source_id=meta.get("sourceId"),
            external_id=ext,
            username=meta.get("username"),
            reason="Duplicate gameTransaction externalId encountered; target row already existed and ON CONFLICT DO NOTHING ignored this source row",
            dry_run=dry_run,
            action="duplicate_key_ignored",
            issue_type="duplicate_external_id_ignored",
            data=meta.get("data") or {},
            target_id=tgt.get("target_id"),
            target_username=tgt.get("playerdetails_username") or tgt.get("target_username"),
            target_player_id=tgt.get("target_player_id"),
            target_external_id=tgt.get("target_external_id"),
            provider_name=meta.get("providerName"),
            game_name=meta.get("gameName"),
            game_type=meta.get("gameType"),
            outlet=meta.get("outlet"),
            round_id=meta.get("roundId"),
        )

    inserted_count = len(inserted_rows or [])
    trace_print(f"[LIVE] Inserted {inserted_count} gameTransaction_final rows. duplicates={duplicate_count} skipped={skipped_rows}")
    return (inserted_count, skipped_rows)


def wallet_row_to_values(
        tgt_conn,
        kind: str,
        src_id: str,
        data: Dict[str, Any],
        player_map: Dict[str, uuid.UUID],
        dry_run: bool,
        registered_usernames: Optional[set] = None,
) -> Optional[Tuple[Any, ...]]:
    """Audited override: same wallet transform rules, richer skip reasons."""
    try:
        member = as_dict(data.get("member"))
        username = str(
            _first_present(data, "username", "userName", "name", "loginName", "userid", "userId", "PlayerAccount", "playerAccount")
            or extract_username(member)
            or ""
        ).strip()
        ref_id = str(_first_present(data, "id", "referenceId", "transactionId") or src_id or "").strip()
        amount_raw = _first_present(data, "amount", "TotalAmount", "totalAmount")
        payment_gateway = str(_first_present(data, "payment", "paymentMethod", "paymentGateway") or "N/A")
        if not username:
            record_wallet_skip(kind, src_id, None, "Missing username in wallet source payload", dry_run, data=data, reference_id=ref_id, issue_type="missing
_username", amount=amount_raw, payment_gateway=payment_gateway)
            return None
        player_id = player_map.get(username)
        if not player_id:
            issue = "player_not_in_player_map"
            if registered_usernames is not None and username in registered_usernames:
                issue = "player_exists_in_source_registration_but_not_target_player_map"
            record_wallet_skip(kind, src_id, username, _skip_reason_missing_player(username, registered_usernames), dry_run, data=data, reference_id=ref_id,
 issue_type=issue, amount=amount_raw, payment_gateway=payment_gateway)
            return None
        if not ref_id:
            record_wallet_skip(kind, src_id, username, "Missing wallet referenceId/id", dry_run, data=data, reference_id=ref_id, issue_type="missing_referen
ce_id", target_player_id=player_id, amount=amount_raw, payment_gateway=payment_gateway)
            return None
        try:
            amount = abs(float(amount_raw or 0))
        except Exception:
            record_wallet_skip(kind, src_id, username, f"Invalid amount value: {amount_raw!r}", dry_run, data=data, reference_id=ref_id, issue_type="invalid
_amount", target_player_id=player_id, amount=amount_raw, payment_gateway=payment_gateway)
            return None
        raw_date = _first_present(data, "transferDate", "transferdate", "TransferDate")
        t_date = parse_iso_dt(raw_date)
        if t_date is None:
            t_date = datetime(1970, 1, 1, tzinfo=timezone.utc)
            record_wallet_report(
                kind=kind,
                source_id=src_id,
                username=username,
                reference_id=ref_id,
                reason="Missing/invalid transferDate; defaulting createdDatetime/confirmedDatetime to 1970-01-01",
                dry_run=dry_run,
                action="date_defaulted",
                issue_type="missing_transfer_date_defaulted",
                data=data,
                target_player_id=player_id,
                amount=amount,
                payment_gateway=payment_gateway,
            )
        return (
            kind.lower(), WALLET_PLATFORM, player_id, payment_gateway, "www.inplay.com.ph",
            amount, "confirmed", None, t_date, t_date, None, None, ref_id, t_date,
        )
    except Exception as e:
        try:
            tgt_conn.rollback()
        except Exception:
            pass
        record_wallet_report(
            kind=kind,
            source_id=src_id,
            username="",
            reference_id="",
            reason="Unexpected wallet transform failure before insert",
            dry_run=dry_run,
            action="transform_failed",
            issue_type="wallet_transform_failed",
            data=data,
            error=e,
        )
        return None


def insert_wallet_batch(
    tgt_conn,
    rows: List[Dict[str, Any]],
    kind: str,
    player_map: Dict[str, uuid.UUID],
    dry_run: bool,
    registered_usernames: Optional[set] = None,
) -> Tuple[int, int]:
    """Audited override: same wallet insert rules, richer skip/duplicate/failure reporting."""
    values: List[Tuple[Any, ...]] = []
    value_meta: List[Dict[str, Any]] = []
    skipped_rows = 0
    for r in rows:
        src_id = str(r.get("id") or "").strip()
        data = as_dict(r.get("data"))
        if not src_id:
            skipped_rows += 1
            record_wallet_skip(kind, None, None, "Missing source row id", dry_run, data=data, issue_type="missing_source_id")
            continue
        if not data:
            skipped_rows += 1
            record_wallet_skip(kind, src_id, None, "Missing or invalid source JSON data", dry_run, data=data, issue_type="missing_or_invalid_json")
            continue
        v = wallet_row_to_values(tgt_conn, kind, src_id, data, player_map, dry_run=dry_run, registered_usernames=registered_usernames)
        if v:
            values.append(v)
            value_meta.append({
                "sourceId": src_id,
                "referenceId": v[12],
                "username": str(
                    _first_present(data, "username", "userName", "name", "loginName", "userid", "userId", "PlayerAccount", "playerAccount")
                    or extract_username(as_dict(data.get("member")))
                    or ""
                ).strip(),
                "targetPlayerId": v[2],
                "amount": v[5],
                "paymentGateway": v[3],
                "data": data,
            })
        else:
            skipped_rows += 1

    if skipped_rows:
        trace_print(f"[SUMMARY][walletTransaction.{kind}] insertable={len(values)} skipped={skipped_rows}")
    if dry_run:
        trace_print(f"[DRY-RUN] Would insert {len(values)} walletTransaction_final rows for {kind}. skipped={skipped_rows}")
        return (len(values), skipped_rows)
    if not values:
        return (0, skipped_rows)

    sql = f"""
    INSERT INTO kemet."walletTransaction_final" (
        "transactionType", "platform", "playerId", "paymentGateway", "domain",
        "amount", "status", "bettingPhase", "createdDatetime", "confirmedDatetime",
        "cancelledDatetime", "failedDatetime", "referenceId", "updatedAt"
    )
    VALUES %s
    ON CONFLICT ("platform", "referenceId")
    WHERE ("platform" = '{WALLET_PLATFORM}' AND "referenceId" IS NOT NULL) DO NOTHING
    RETURNING "referenceId"
    """
    try:
        with tgt_conn.cursor() as cur:
            inserted_rows = execute_values(cur, sql, values, page_size=1000, fetch=True)
    except Exception as e:
        try:
            tgt_conn.rollback()
        except Exception:
            pass
        for meta in value_meta:
            record_wallet_report(
                kind=kind,
                source_id=meta.get("sourceId"),
                username=meta.get("username"),
                reference_id=meta.get("referenceId"),
                reason="walletTransaction batch insert failed; no rows from this prepared batch were inserted",
                dry_run=dry_run,
                action="insert_failed",
                issue_type="wallet_transaction_insert_failed",
                data=meta.get("data") or {},
                target_player_id=meta.get("targetPlayerId"),
                amount=meta.get("amount"),
                payment_gateway=meta.get("paymentGateway"),
                error=e,
            )
        trace_print(f"[LIVE][walletTransaction.{kind}] batch insert failed attempted={len(values)} skipped={skipped_rows} error={e}", level=logging.ERROR)
        return (0, skipped_rows + len(values))

    inserted_counts: Dict[str, int] = {}
    for row in inserted_rows or []:
        inserted_ref = str(row[0])
        inserted_counts[inserted_ref] = inserted_counts.get(inserted_ref, 0) + 1

    duplicate_targets = _fetch_existing_wallet_targets(tgt_conn, kind, [m.get("referenceId") for m in value_meta])
    duplicate_count = 0
    for meta in value_meta:
        ref = str(meta["referenceId"])
        if inserted_counts.get(ref, 0) > 0:
            inserted_counts[ref] -= 1
            continue
        duplicate_count += 1
        tgt = duplicate_targets.get(ref) or {}
        record_wallet_report(
            kind=kind,
            source_id=meta.get("sourceId"),
            username=meta.get("username"),
            reference_id=ref,
            reason="Duplicate walletTransaction platform/referenceId encountered; target row already existed and ON CONFLICT DO NOTHING ignored this source
row",
            dry_run=dry_run,
            action="duplicate_key_ignored",
            issue_type="duplicate_reference_id_ignored",
            data=meta.get("data") or {},
            target_id=tgt.get("target_id"),
            target_username=tgt.get("target_username"),
            target_player_id=tgt.get("target_player_id"),
            target_reference_id=tgt.get("target_reference_id"),
            amount=meta.get("amount"),
            payment_gateway=meta.get("paymentGateway"),
        )

    inserted_count = len(inserted_rows or [])
    trace_print(f"[LIVE] Inserted {inserted_count} walletTransaction_final rows for {kind}. duplicates={duplicate_count} skipped={skipped_rows}")
    return (inserted_count, skipped_rows)



# ----------------------------
# Main Execution Entry Point
# ----------------------------
def main():
    # Initialize the parser
    ap = argparse.ArgumentParser(description="InPlayV1 Production Migration Pipeline")
    # Define the execution group (optional, but good for organization)
    execution_group = ap.add_argument_group("Execution")

    # Add all arguments to 'ap' (the main parser)
    execution_group.add_argument("--username", help="Run data migration operations exclusively targeting a specific user record")
    execution_group.add_argument("--dry-run", action="store_true", help="Execute structural validation logic in dry-run mode without writing to target stora
ge")

    # Add remaining arguments
    ap.add_argument("--migrate-all", action="store_true", help="Process total historical sequences across records iteratively")
    ap.add_argument("--delete-first", action="store_true", help="Wipes target data matching constraints cleanly before bootstrapping insertions")
    ap.add_argument("--repair-data", action="store_true", help="Executes downstream field backfills and schematic structural conversions")
    ap.add_argument("--repair-status", action="store_true", help="Sync status mappings from source state schemas")

    ap.add_argument("--keep-from", type=_parse_date_arg, default=None, metavar="YYYY-MM-DD",
                    help="With --delete/--delete-first: keep records whose date is on or after this UTC date")
    ap.add_argument("--keep-to", type=_parse_date_arg, default=None, metavar="YYYY-MM-DD",
                    help="With --delete/--delete-first: keep records whose date is on or before this UTC date")
    ap.add_argument("--date-from", type=_parse_date_arg, default=None, metavar="YYYY-MM-DD",
                    help="With --migrate-all: only migrate source rows on/after this UTC date using createddate/GameDate/transferDate; checkpoints are ignor
ed for the source read")
    ap.add_argument("--date-to", type=_parse_date_arg, default=None, metavar="YYYY-MM-DD",
                    help="With --migrate-all: only migrate source rows on/before this UTC date using createddate/GameDate/transferDate")
    ap.add_argument("--batch-size", type=int, default=10000, help="Batch size for source scans")
    ap.add_argument("--commit-every", type=int, default=200000,
                    help="Commit+checkpoint after this many processed rows per phase")
    ap.add_argument("--resume", type=lambda x: str(x).lower() not in ("0", "false", "no"), default=True,
                    help="Resume from migrationCheckpoint (default true)")
    ap.add_argument("--start-after-id", type=str, default=None,
                    help="Override checkpoint and start from id > this value (applies to each phase)")
    ap.add_argument("--max-rows-total", dest="max_rows_total", type=int, default=None,
                    help="Max data iteration ingestion threshold caps")
    ap.add_argument("--loop-forever", action="store_true",
                    help="Continuously loop migration process over 9-hour windows")

    args = ap.parse_args()

    # Dynamic Logger Path Ingestion Setup
    log_dir = os.path.dirname(LOG_FILE_PATH)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=LOG_FILE_PATH,
        filemode="a",
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=logging.INFO
    )

    if args.dry_run:
        logging.info("==============================================")
        logging.info(f"INITIALIZING PIPELINE SIMULATION IN --dry-run MODE. TARGET: {LOG_FILE_PATH}")
        logging.info("==============================================")
        print(f"[DRY-RUN LOGGING ACTIVE] Target logs routing directly into: {LOG_FILE_PATH}", flush=True)

    src = connect("iestdl")
    tgt = connect("iestdl")

    try:
        if args.delete_first:
            delete_target_data(tgt, args.dry_run, args.keep_from, args.keep_to)

        if args.repair_data:
            repair_existing_data(src, tgt, args.dry_run, args.batch_size, args.commit_every)

        if args.repair_status:
            repair_wallet_status(src, tgt, args.dry_run, args.batch_size, args.commit_every)

        if args.migrate_all:
            if args.loop_forever:
                print("[LOOP] Starting continuous infinite execution loop over 9-hour operational windows...",
                      flush=True)
                start_loop_time = time.time()
                iteration = 0
                while (time.time() - start_loop_time) < (9 * 3600):
                    iteration += 1
                    print(f"\n[LOOP] Executing migration sequence iteration cycle: #{iteration}", flush=True)
                    total = migrate_all(
                        src_conn=src, tgt_conn=tgt, dry_run=args.dry_run,
                        batch_size=args.batch_size, commit_every=args.commit_every,
                        resume=args.resume if iteration == 1 else True,
                        start_after_id=args.start_after_id if iteration == 1 else None,
                        max_rows_total=args.max_rows_total,
                        date_from=args.date_from, date_to=args.date_to,
                    )
                    if total == 0:
                        print("[LOOP] No new changes intercepted. Hibernating loop thread for 30s...", flush=True)
                        time.sleep(30)
                print("[LOOP] 9-hour operational lifespan windows completed. Shutting down.", flush=True)
            else:
                migrate_all(
                    src_conn=src, tgt_conn=tgt, dry_run=args.dry_run,
                    batch_size=args.batch_size, commit_every=args.commit_every,
                    resume=args.resume, start_after_id=args.start_after_id,
                    max_rows_total=args.max_rows_total,
                    date_from=args.date_from, date_to=args.date_to,
                )
        elif args.username:
            migrate_single_user(
                src,
                tgt,
                args.username,
                dry_run=args.dry_run,
                date_from=args.date_from,
                date_to=args.date_to,
            )

        # =============================================================
        # ---> PLACE THE MAIL CALL HERE (AT THE END OF THE TRY BLOCK) <---
        # =============================================================
        print("[INFO] Migration loop finished. Checking report files for dispatch...", flush=True)

        # References the dynamic global string variables defined at the top of your script
        active_reports = [CSV_GAMETX_PATH, CSV_DEPOSITS_PATH, CSV_WITHDRAWALS_PATH, CSV_PLAYERS_PATH, CSV_RECONCILIATION_PATH, CSV_DATA_QUALITY_PATH]
        active_reports = package_reports_if_needed(active_reports)

        email_body = f"""Hello Team,

                    The database migration pipeline execution run has completed successfully.
                    Timestamp Group Identifier: {TIMESTAMP_STR}
                    Dry Run Configuration Flag: {args.dry_run}

                    Attached are the generated CSV reports listing any records skipped or handled during this sequence.
                    Player registration skip/upsert-failure diagnostics are included when generated.
                    Reconciliation CSV is included when generated.

                    {reconciliation_email_summary()}

                    {data_quality_email_summary()}

                    If the combined report payload reached 17MB, the reports were packaged into a single ZIP file.
        """

        # Invoke the function imported from utilities/mailer.py
        send_migration_reports(
            subject=f"[{BRAND} Migration Notification] Phase Run Complete - {TIMESTAMP_STR}",
            body_text=email_body,
            #to_emails=["admin@yourdomain.com", "lead_engineer@yourdomain.com"],
            to_emails=["allan.faylona@iest.com.ph"],
            cc_emails=[""],
            file_paths=active_reports,
            smtp_host="smtp.gmail.com",
            smtp_port=587,
            smtp_user="palaciodefaylona@gmail.com",
            smtp_password="ywsrnqcbnwgfxfmn"  # Use environment variables here if preferred
        )

    except Exception:
        try:
            tgt.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            src.close()
        except Exception:
            pass
        try:
            tgt.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
