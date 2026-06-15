# Migration Engine Multi-Brand 

This package was rebuilt from the four current working scripts supplied in the chat. The uploaded scripts are included unchanged for audit under `scripts/`:

- `scripts/inplayv2.py`
- `scripts/inplayv1.py`
- `scripts/instaplay.py`
- `scripts/1play.py`

The files under `scripts/` are reference/audit copies only. The runtime engine does not import them directly. The runnable implementation is the shared engine plus the brand adapters under `helpers/brands/`.

Brand-specific business rules, source table names, source keys, and mappings are isolated in:

```text
helpers/brands/inplay/      # InplayV2
helpers/brands/inplayv1/    # InplayV1
helpers/brands/instaplay/   # Instaplay / 88Play
helpers/brands/oneplay/     # internal module for public --brand 1play
```

Use `--brand 1play` for 1Play. The internal Python module remains `oneplay` because Python package names beginning with a digit are unsafe for normal imports.

## Target routing

For this build, all brands write to target database `iestdbrds` and target schema `migration_repair` by default. Source database defaults to `iestdl`. Source schema remains configurable per brand through environment variables.

## PHT date window

When `--date-from` / `--date-to` are provided, the engine uses the same InplayV2-style Philippine business window:

```text
--date-from YYYY-MM-DD -> YYYY-MM-DD 06:00:00+08:00 inclusive
--date-to   YYYY-MM-DD -> YYYY-MM-DD 06:00:00+08:00 exclusive
```

JSONB brands compare those bounds as `timestamptz`. 1Play uses flat timestamp columns, so the same PHT bounds are converted back to Asia/Manila local timestamp before comparison.

## Low-memory defaults

The CLI defaults are intentionally conservative for a 4GB EC2 instance:

```text
--batch-size 100
--commit-every 500
--tx-limit 50
--wallet-limit 50
INSERT_PAGE_SIZE 100
reconciliation/data-quality OFF by default
```

Run reconciliation after a migration using `--run-recon`; run DQ after a migration using `--run-dq`. For completed migrations, use `--recon-only` and/or `--dq-only` so the engine does not reprocess rows.

## Examples

```bash
python3 main.py --brand inplay --migrate-all --date-from 2026-05-26 --date-to 2026-05-27 --dry-run
python3 main.py --brand inplayv1 --migrate-all --date-from 2026-05-26 --date-to 2026-05-27 --dry-run
python3 main.py --brand instaplay --migrate-all --date-from 2026-05-26 --date-to 2026-05-27 --dry-run
python3 main.py --brand 1play --migrate-all --date-from 2026-05-26 --date-to 2026-05-27 --dry-run

# post-run checks only
python3 main.py --brand inplay --recon-only --date-from 2026-05-26 --date-to 2026-05-27
python3 main.py --brand inplay --dq-only --date-from 2026-05-26 --date-to 2026-05-27
```

## Preserved mapping notes

- InplayV2: JSONB source tables, PHT 06:00 business window, `immutable_json_timestamp(...)` for JSONB source-date filtering, registration-only player creation, game/wallet rows skip if player is missing.
- InplayV1: JSONB source tables with `createddate`, `GameDate`, `transferDate`; registration-only player creation; game/wallet no ghost/shadow player creation.
- Instaplay/88Play: member-driven source flow; registration/member row drives child game and wallet fetches; mapping follows the supplied 88Play/Instaplay script.
- 1Play: flat RDBMS source tables; `LOGIN_NAME`/`PLAYER_ACCOUNT`/`PLAYER_NAME` based joining; game `TRANSACTION_ID` to externalId; wallet `TRANSACTION_ID` to referenceId.

## Phase summary and CSV reporting

This package writes CSV report files under `MIGRATION_REPORT_DIR` (default: `reports`) and log files under `MIGRATION_LOG_DIR` (default: `logs`).

At run startup, the engine creates header-only CSV files immediately, including a run summary file:

```text
<brand>_runSummary_<date-window>-rundate_<timestamp>.csv
```

Each phase writes one lightweight summary row instead of accumulating row details in memory. The summary includes:

- `sourceRows`
- `mappedRows`
- `insertedRows`
- `duplicateRows`
- `skippedRows`
- `missingPlayerRows`
- `missingUsernameRows`
- `missingRequiredRows`
- `mappingErrorRows`
- `insertErrorRows`

The same phase summary is also printed once per phase to the screen/log as:

```text
[PHASE SUMMARY][<brand> <phase>] ...
```

This is designed for 4GB EC2 hosts: it keeps only integer counters in memory and avoids full SQL/log spam inside every fetch loop.
