from __future__ import annotations
import pandas as pd

def taps_have_prints(taps: pd.DataFrame, prints: pd.DataFrame) -> float:
    """Return share of taps whose (user_id, value_prop_id, date) exists in prints."""
    if taps.empty:
        return 1.0
    t = taps.copy()
    p = prints.copy()
    t["d"] = t["timestamp"].dt.date
    p["d"] = p["timestamp"].dt.date
    merged = t.merge(p[["user_id", "value_prop_id", "d"]].drop_duplicates(),
                     on=["user_id", "value_prop_id", "d"], how="left", indicator=True)
    ok_rate = (merged["_merge"] == "both").mean()
    return float(ok_rate)
