from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from src.carrusel_mp.config import get_config

from src.utils.time_utils import ensure_datetime_utc, now_utc
from src.models.contracts import DataFrameContracts

#================================================================================

class PreprocessingEngine:
    """
    Silver-layer cleaner/structurer:
      - tz normalization (UTC)
      - dedupe + duplicate flag
      - metadata columns
      - dtype tweaks
      - schema validation (processed prints only; taps/payments use raw schema)
    """

    def __init__(self, source_name: str, logger: Optional[logging.Logger] = None) -> None:
        self.cfg = get_config()
        self.source_name = source_name
        self.logger = logger or logging.getLogger(f"mp_carousel.preproc.{source_name}")

    #------------------------------------------------------------------------

    def process_prints(self, df_raw: pd.DataFrame, source_file: str) -> pd.DataFrame:
        if df_raw.empty:
            return self._attach_metadata(df_raw.copy(), source_file)

        df = df_raw.copy()

        # normalize timestamp to UTC
        df["timestamp"] = ensure_datetime_utc(df["timestamp"])

        # dedupe
        dup_mask = df.duplicated()
        df["duplicate_flag"] = dup_mask
        df = df.drop_duplicates(ignore_index=True)

        # enforce dtypes (optional small tweaks)
        if "position" in df.columns:
            df["position"] = pd.to_numeric(df["position"], errors="coerce").astype("Int64")

        df = self._attach_metadata(df, source_file)

        # processed prints have a strict schema
        DataFrameContracts.validate_dataframe(df[[
            "timestamp", "user_id", "value_prop_id", "position",
            "processed_at", "source_file", "row_number", "duplicate_flag", "data_quality_score"
        ]], "processed_prints")

        self.logger.info(f"[prints] processed rows={len(df)} (deduped)")
        return df
    
    #------------------------------------------------------------------------

    def process_taps(self, df_raw: pd.DataFrame, source_file: str) -> pd.DataFrame:
        if df_raw.empty:
            return self._attach_metadata(df_raw.copy(), source_file)

        df = df_raw.copy()
        df["timestamp"] = ensure_datetime_utc(df["timestamp"])
        dup_mask = df.duplicated()
        df["duplicate_flag"] = dup_mask
        df = df.drop_duplicates(ignore_index=True)

        if "position" in df.columns:
            df["position"] = pd.to_numeric(df["position"], errors="coerce").astype("Int64")

        df = self._attach_metadata(df, source_file)

        # taps do not yet have a strict "processed" schema; validate against raw canonical
        DataFrameContracts.validate_dataframe(df[["timestamp", "user_id", "value_prop_id", "position"]], "taps_raw")
        self.logger.info(f"[taps] processed rows={len(df)} (deduped)")
        return df
    
    #------------------------------------------------------------------------

    def process_payments(self, df_raw: pd.DataFrame, source_file: str) -> pd.DataFrame:
        if df_raw.empty:
            return self._attach_metadata(df_raw.copy(), source_file)

        df = df_raw.copy()
        df["timestamp"] = ensure_datetime_utc(df["timestamp"])
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        df = df[df["amount"].notna()]

        dup_mask = df.duplicated()
        df["duplicate_flag"] = dup_mask
        df = df.drop_duplicates(ignore_index=True)

        df = self._attach_metadata(df, source_file)

        # payments do not yet have a strict "processed" schema; validate against raw canonical
        DataFrameContracts.validate_dataframe(df[["timestamp", "user_id", "value_prop_id", "amount"]], "payments_raw")
        self.logger.info(f"[payments] processed rows={len(df)} (deduped)")
        return df

    #------------------------------------------------------------------------

    def _attach_metadata(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        df = df.copy()
        df["processed_at"] = now_utc()
        df["source_file"] = source_file
        df["row_number"] = pd.RangeIndex(start=0, stop=len(df), step=1)
        # simple default; can be updated by quality engine
        df["data_quality_score"] = 1.0
        return df

#================================================================================