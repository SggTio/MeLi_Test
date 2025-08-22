from __future__ import annotations

import json
import os

import pandas as pd

from src.carrusel_mp.config import get_config

from management.logging_config import setup_logging, ProgressTracker

from src.data.extractors import PrintsExtractor, TapsExtractor, PaymentsExtractor
from src.carrusel_mp.processing.preprocessing_engine import PreprocessingEngine
from src.carrusel_mp.processing.feature_engine import FeatureEngineer
from src.pipeline.quality_checker import QualityChecker


def _read_all(iterator):
    chunks = list(iterator)
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def main():
    setup_logging()
    cfg = get_config()
    tr = ProgressTracker("pipeline", total_steps=5)

    raw_dir = cfg.data_paths.raw
    proc_dir = cfg.data_paths.processed
    out_dir = cfg.data_paths.output
    os.makedirs(proc_dir, exist_ok=True)
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(cfg.quality.quality_report_path, exist_ok=True)

    # 1) extract (canonical raw)
    tr.update("extract")
    prints_raw = _read_all(PrintsExtractor(f"{raw_dir}/prints.json").extract_chunked())
    taps_raw = _read_all(TapsExtractor(f"{raw_dir}/taps.json").extract_chunked())
    pays_raw = _read_all(PaymentsExtractor(f"{raw_dir}/pays.csv").extract_chunked())

    # 2) preprocess (silver)
    tr.update("preprocess")
    pre = PreprocessingEngine("mp")
    prints = pre.process_prints(prints_raw, "prints.json")
    taps = pre.process_taps(taps_raw, "taps.json")
    pays = pre.process_payments(pays_raw, "pays.csv")

    prints.to_parquet(f"{proc_dir}/prints.parquet", index=False)
    taps.to_parquet(f"{proc_dir}/taps.parquet", index=False)
    pays.to_parquet(f"{proc_dir}/payments.parquet", index=False)

    # 3) features (gold)
    tr.update("features")
    fe = FeatureEngineer(
        lookback_days=cfg.processing.lookback_window_days,
        target_window_days=cfg.processing.target_window_days,
    )
    gold = fe.build(prints, taps, pays)
    gold.to_parquet(f"{out_dir}/training_dataset.parquet", index=False)

    # 4) quality
    tr.update("quality")
    qc = QualityChecker(
        {
            "max_null_pct": cfg.quality.max_null_percentage,
            "max_dup_pct": cfg.quality.max_duplicate_percentage,
            "min_quality_score": cfg.quality.min_data_quality_score,
        }
    )
    report = qc.evaluate(prints, taps, pays, gold)

    # 5) persist report
    tr.update("report")
    with open(f"{cfg.quality.quality_report_path}/quality_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    tr.finish("ok")


if __name__ == "__main__":
    main()
