# src/mp_carousel/utils/quality_utils.py
from __future__ import annotations
import pandas as pd
from typing import Dict, Any
import logging

def compute_quality_summary(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {"rows": 0, "cols": 0, "null_rates": {}, "duplicate_rate": 0.0}
    null_rates = df.isna().mean().round(4).to_dict()
    dup_rate = float((df.duplicated().sum() / len(df)))
    return {
        "rows": int(len(df)),
        "cols": int(df.shape[1]),
        "null_rates": null_rates,
        "duplicate_rate": dup_rate,
    }

def log_quality_summary(logger: logging.Logger, summary: Dict[str, Any], prefix: str = "") -> None:
    p = f"{prefix}: " if prefix else ""
    logger.info(f"{p}rows={summary['rows']}, cols={summary['cols']}, dup_rate={summary['duplicate_rate']:.4f}")
