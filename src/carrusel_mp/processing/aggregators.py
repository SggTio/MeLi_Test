from __future__ import annotations

from typing import List

import pandas as pd

#================================================================================

def day_key(ts_series: pd.Series) -> pd.Series:
    """Return UTC date component to join by day."""
    return pd.to_datetime(ts_series, utc=True).dt.date

#--------------------------------------------------------------------------------

def daily_counts(df: pd.DataFrame, group_cols: List[str], out_col: str) -> pd.DataFrame:
    g = df.groupby(group_cols, as_index=False).size()
    g = g.rename(columns={"size": out_col})
    return g

#--------------------------------------------------------------------------------

def daily_sums(df: pd.DataFrame, group_cols: List[str], value_col: str, out_col: str) -> pd.DataFrame:
    g = df.groupby(group_cols, as_index=False)[value_col].sum()
    g = g.rename(columns={value_col: out_col})
    return g

#--------------------------------------------------------------------------------

def trailing_sum_on_calendar_days(
    df_daily: pd.DataFrame,
    on_cols: list[str],
    value_col: str,
    asof_date_col: str,
    window_days: int,
) -> pd.DataFrame:
    """
    Compute trailing sum over the previous N calendar days, EXCLUDING the as-of date.
    Assumes df_daily is at daily grain per on_cols + asof_date_col, with a value_col to sum.
    """
    df = df_daily.copy()
    df = df.sort_values(on_cols + [asof_date_col])

    # build a shifted rolling window by self-joining on date ranges
    # For performance on large data: switch to ASOF merges or window functions in Spark later.
    rows = []
    for key_vals, grp in df.groupby(on_cols, sort=False):
        grp = grp.sort_values(asof_date_col)
        dates = pd.to_datetime(grp[asof_date_col])
        vals = grp[value_col].values

        # Precompute cumulative sums to compute trailing in O(1)
        csum = pd.Series(vals).cumsum()
        out_vals = []
        for i, d in enumerate(dates):
            start = d - pd.Timedelta(days=window_days)
            # exclude current date; window is [start, d)
            mask = (dates >= start) & (dates < d)
            if not mask.any():
                out_vals.append(0.0)
            else:
                idx = mask.to_numpy().nonzero()[0]
                j0, j1 = idx[0], idx[-1]
                s = csum.iloc[j1] - (csum.iloc[j0 - 1] if j0 > 0 else 0)
                out_vals.append(float(s))
        grp[f"{value_col}_trailing_{window_days}d"] = out_vals
        rows.append(grp)

    out = pd.concat(rows, ignore_index=True)
    return out[on_cols + [asof_date_col, f"{value_col}_trailing_{window_days}d"]]
