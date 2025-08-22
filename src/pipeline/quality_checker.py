from __future__ import annotations

from typing import Dict, Any

import pandas as pd

from src.utils.quality_utils import compute_quality_summary
from src.data.validators import taps_have_prints

#================================================================================

class QualityChecker:
    """
    Aggregates a compact quality report for prints/taps/payments and the gold dataset.
    """

    def __init__(self, thresholds: Dict[str, float]) -> None:
        self.max_null_pct = float(thresholds.get("max_null_pct", 0.1))
        self.max_dup_pct = float(thresholds.get("max_dup_pct", 0.1))
        self.min_quality = float(thresholds.get("min_quality_score", 0.8))

    #--------------------------------------------------------------------------------

    def evaluate(
        self,
        prints: pd.DataFrame,
        taps: pd.DataFrame,
        payments: pd.DataFrame,
        gold: pd.DataFrame,
    ) -> Dict[str, Any]:
        res: Dict[str, Any] = {}

        # per-source summaries
        p_sum = compute_quality_summary(prints)
        t_sum = compute_quality_summary(taps)
        y_sum = compute_quality_summary(payments)
        g_sum = compute_quality_summary(gold)

        res["row_counts"] = {
            "prints": p_sum["rows"],
            "taps": t_sum["rows"],
            "payments": y_sum["rows"],
            "gold": g_sum["rows"],
        }

        res["null_rates"] = {
            "prints": max(p_sum["null_rates"].values(), default=0.0),
            "taps": max(t_sum["null_rates"].values(), default=0.0),
            "payments": max(y_sum["null_rates"].values(), default=0.0),
            "gold": max(g_sum["null_rates"].values(), default=0.0),
        }

        res["duplicate_rates"] = {
            "prints": p_sum["duplicate_rate"],
            "taps": t_sum["duplicate_rate"],
            "payments": y_sum["duplicate_rate"],
            "gold": g_sum["duplicate_rate"],
        }

        # business rule: taps must have prints (same user_id, value_prop_id, same day)
        res["business_rules"] = {
            "taps_have_prints_rate": taps_have_prints(taps, prints) if not taps.empty else 1.0
        }

        # crude composite quality score (you can refine)
        # penalize nulls/dups; reward taps mapping to prints
        score = (
            0.4 * (1 - res["null_rates"]["prints"])
            + 0.2 * (1 - res["duplicate_rates"]["prints"])
            + 0.2 * res["business_rules"]["taps_have_prints_rate"]
            + 0.2 * (1 - res["null_rates"]["gold"])
        )
        res["quality_score"] = round(float(score), 4)

        # thresholds verdicts
        res["thresholds"] = {
            "max_null_pct": self.max_null_pct,
            "max_dup_pct": self.max_dup_pct,
            "min_quality_score": self.min_quality,
        }
        res["passes"] = {
            "prints_nulls": res["null_rates"]["prints"] <= self.max_null_pct,
            "prints_dups": res["duplicate_rates"]["prints"] <= self.max_dup_pct,
            "gold_nulls": res["null_rates"]["gold"] <= self.max_null_pct,
            "score": res["quality_score"] >= self.min_quality,
        }
        return res
    
#================================================================================