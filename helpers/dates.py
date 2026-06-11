import argparse
from datetime import datetime, timezone, timedelta
from typing import Any, Optional, Tuple

PHT_TZ = timezone(timedelta(hours=8))


def parse_iso_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return None


def parse_date_arg(text: str) -> datetime:
    text = str(text).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise argparse.ArgumentTypeError(
        f"Invalid date: {text!r}. Expected YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS"
    )


def business_window_bounds(
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    start_hour: int = 6,
    tz=PHT_TZ,
) -> Tuple[Optional[str], Optional[str]]:
    """Return PHT-aware timestamptz text bounds.

    date_from YYYY-MM-DD -> YYYY-MM-DD start_hour:00:00+08:00 inclusive
    date_to   YYYY-MM-DD -> YYYY-MM-DD start_hour:00:00+08:00 exclusive
    """
    def boundary(value: datetime) -> str:
        dt = datetime.combine(value.date(), datetime.min.time(), tzinfo=tz)
        dt = dt.replace(hour=start_hour, minute=0, second=0, microsecond=0)
        return dt.isoformat(sep=" ", timespec="seconds")

    return (
        boundary(date_from) if date_from is not None else None,
        boundary(date_to) if date_to is not None else None,
    )
