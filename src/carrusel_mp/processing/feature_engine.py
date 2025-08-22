from __future__ import annotations

from typing import Optional

import pandas as pd

from src.carrusel_mp.config import get_config

from models.contracts import DataFrameContracts
from src.carrusel_mp.processing.aggregators import (
    day_key,
    daily_counts,
    daily_sums,
    trailing_sum_on_calendar_days,
)

#================================================================================

class FeatureEngineer:
    """
    Build Gold dataset:
      - base = prints in last N days (target_window_days)
      - clicked = any tap on same (user_id, value_prop_id, date)
      - 3w lookbacks on prints/taps/payments
    """

    def __init__(self, lookback_days: Optional[int] = None, target_window_days: Optional[int] = None) -> None:
        cfg = get_config()
        self.lookback_days = lookback_days or cfg.processing.lookback_window_days
        self.target_window_days = target_window_days or cfg.processing.target_window_days


    def build(self, prints_df: pd.DataFrame, taps_df: pd.DataFrame, pays_df: pd.DataFrame) -> pd.DataFrame:
        if prints_df.empty:
            return pd.DataFrame(columns=[
                "timestamp","user_id","value_prop_id","clicked",
                "prints_count_3w","taps_count_3w","payments_count_3w","payments_amount_3w"
            ])

        # Add day keys for joining
        p = prints_df.copy()
        t = taps_df.copy()
        y = pays_df.copy()

        p["day"] = day_key(p["timestamp"])
        if not t.empty:
            t["day"] = day_key(t["timestamp"])
        if not y.empty:
            y["day"] = day_key(y["timestamp"])

        # Base population: prints in last target_window_days relative to max print date
        max_day = p["day"].max()
        min_day = max_day - pd.Timedelta(days=self.target_window_days - 1)
        p_base = p[(p["day"] >= min_day) & (p["day"] <= max_day)].copy()

        # Target: clicked = any tap on same (user_id, value_prop_id, day)
        if t.empty:
            p_base["clicked"] = False
        else:
            taps_day = t[["user_id", "value_prop_id", "day"]].drop_duplicates()
            p_base = p_base.merge(
                taps_day.assign(clicked=True),
                on=["user_id", "value_prop_id", "day"],
                how="left",
            )
            p_base["clicked"] = p_base["clicked"].fillna(False)

        # 3-week lookbacks (strictly prior to print day)
        # Build daily counts/sums for full history first
        prints_daily = daily_counts(p, ["user_id", "value_prop_id", "day"], "prints_daily")
        taps_daily = daily_counts(t, ["user_id", "value_prop_id", "day"], "taps_daily") if not t.empty else pd.DataFrame(columns=["user_id","value_prop_id","day","taps_daily"])
        pays_cnt_daily = daily_counts(y, ["user_id", "value_prop_id", "day"], "payments_daily") if not y.empty else pd.DataFrame(columns=["user_id","value_prop_id","day","payments_daily"])
        pays_amt_daily = daily_sums(y, ["user_id", "value_prop_id", "day"], "amount", "amount_daily") if not y.empty else pd.DataFrame(columns=["user_id","value_prop_id","day","amount_daily"])

        # trailing windows: produce trailing sums per (user_id, value_prop_id, day)
        p_tr = trailing_sum_on_calendar_days(prints_daily, ["user_id","value_prop_id"], "prints_daily", "day", self.lookback_days)
        t_tr = trailing_sum_on_calendar_days(taps_daily, ["user_id","value_prop_id"], "taps_daily", "day", self.lookback_days) if not t.empty else pd.DataFrame(columns=["user_id","value_prop_id","day","taps_daily_trailing_{}d".format(self.lookback_days)])
        pc_tr = trailing_sum_on_calendar_days(pays_cnt_daily, ["user_id","value_prop_id"], "payments_daily", "day", self.lookback_days) if not y.empty else pd.DataFrame(columns=["user_id","value_prop_id","day","payments_daily_trailing_{}d".format(self.lookback_days)])
        pa_tr = trailing_sum_on_calendar_days(pays_amt_daily, ["user_id","value_prop_id"], "amount_daily", "day", self.lookback_days) if not y.empty else pd.DataFrame(columns=["user_id","value_prop_id","day","amount_daily_trailing_{}d".format(self.lookback_days)])

        # Merge trailing features onto base prints (match by user_id, vp, day)
        gold = p_base[["timestamp", "user_id", "value_prop_id", "day", "clicked"]].copy()

        # helper to merge if not empty
        def merge_feat(left, right, colname):
            if right.empty:
                left[colname] = 0.0
                return left
            return left.merge(right, on=["user_id","value_prop_id","day"], how="left")

        gold = merge_feat(gold, p_tr.rename(columns={f"prints_daily_trailing_{self.lookback_days}d":"prints_count_3w"}), "prints_count_3w")
        # if column didn't exist (empty), fill added by default above; otherwise rename now:
        if f"prints_daily_trailing_{self.lookback_days}d" in gold.columns:
            gold = gold.rename(columns={f"prints_daily_trailing_{self.lookback_days}d":"prints_count_3w"})

        gold = merge_feat(gold, t_tr.rename(columns={f"taps_daily_trailing_{self.lookback_days}d":"taps_count_3w"}), "taps_count_3w")
        if f"taps_daily_trailing_{self.lookback_days}d" in gold.columns:
            gold = gold.rename(columns={f"taps_daily_trailing_{self.lookback_days}d":"taps_count_3w"})

        gold = merge_feat(gold, pc_tr.rename(columns={f"payments_daily_trailing_{self.lookback_days}d":"payments_count_3w"}), "payments_count_3w")
        if f"payments_daily_trailing_{self.lookback_days}d" in gold.columns:
            gold = gold.rename(columns={f"payments_daily_trailing_{self.lookback_days}d":"payments_count_3w"})

        gold = merge_feat(gold, pa_tr.rename(columns={f"amount_daily_trailing_{self.lookback_days}d":"payments_amount_3w"}), "payments_amount_3w")
        if f"amount_daily_trailing_{self.lookback_days}d" in gold.columns:
            gold = gold.rename(columns={f"amount_daily_trailing_{self.lookback_days}d":"payments_amount_3w"})

        # Fill NaNs for empty joins
        for c in ["prints_count_3w","taps_count_3w","payments_count_3w","payments_amount_3w"]:
            if c in gold.columns:
                gold[c] = gold[c].fillna(0)

        # Keep final columns in required order
        gold = gold[[
            "timestamp","user_id","value_prop_id","clicked",
            "prints_count_3w","taps_count_3w","payments_count_3w","payments_amount_3w"
        ]]

        # Validate by final_dataset contract (strict)
        DataFrameContracts.validate_dataframe(gold, "final_dataset")
        return gold
    
#================================================================================
