from __future__ import annotations
import pandas as pd
from datetime import datetime, timezone

def to_utc(ts) -> pd.Timestamp:
    """Coerce to timezone-aware UTC pandas Timestamp."""
    return pd.to_datetime(ts, errors="coerce", utc=True)

def parse_day_date(day_str: str) -> pd.Timestamp:
    """
    Raw files use day strings like 'YYYY-MM-DD'.
    Normalize to UTC midnight for that date.
    """
    ts = pd.to_datetime(day_str, format="%Y-%m-%d", errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    return pd.Timestamp(ts).tz_localize("UTC")

def ensure_datetime_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)
