from __future__ import annotations

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

# Import your pipeline components
from management.logging_config import setup_logging
from src.carrusel_mp.config import get_config
from src.data.extractors import PrintsExtractor, TapsExtractor, PaymentsExtractor
from src.carrusel_mp.processing.preprocessing_engine import PreprocessingEngine
from src.carrusel_mp.processing.feature_engine import FeatureEngineer
from src.pipeline.quality_checker import QualityChecker

def extract(**_):
    cfg = get_config()
    prints = pd.concat(list(PrintsExtractor(f"{cfg.data_paths.raw}/prints.json").extract_chunked()), ignore_index=True)
    taps   = pd.concat(list(TapsExtractor  (f"{cfg.data_paths.raw}/taps.json").extract_chunked()),   ignore_index=True)
    pays   = pd.concat(list(PaymentsExtractor(f"{cfg.data_paths.raw}/pays.csv").extract_chunked()), ignore_index=True)
    prints.to_parquet(f"{cfg.data_paths.processed}/_bronze_prints.parquet", index=False)
    taps.to_parquet  (f"{cfg.data_paths.processed}/_bronze_taps.parquet", index=False)
    pays.to_parquet  (f"{cfg.data_paths.processed}/_bronze_pays.parquet", index=False)

def preprocess(**_):
    cfg = get_config()
    pre = PreprocessingEngine("mp")
    prints_raw = pd.read_parquet(f"{cfg.data_paths.processed}/_bronze_prints.parquet")
    taps_raw   = pd.read_parquet(f"{cfg.data_paths.processed}/_bronze_taps.parquet")
    pays_raw   = pd.read_parquet(f"{cfg.data_paths.processed}/_bronze_pays.parquet")

    prints = pre.process_prints(prints_raw, "prints.json")
    taps   = pre.process_taps(taps_raw, "taps.json")
    pays   = pre.process_payments(pays_raw, "pays.csv")

    prints.to_parquet(f"{cfg.data_paths.processed}/prints.parquet", index=False)
    taps.to_parquet  (f"{cfg.data_paths.processed}/taps.parquet", index=False)
    pays.to_parquet  (f"{cfg.data_paths.processed}/payments.parquet", index=False)

def features(**_):
    cfg = get_config()
    fe = FeatureEngineer()
    prints = pd.read_parquet(f"{cfg.data_paths.processed}/prints.parquet")
    taps   = pd.read_parquet(f"{cfg.data_paths.processed}/taps.parquet")
    pays   = pd.read_parquet(f"{cfg.data_paths.processed}/payments.parquet")
    gold = fe.build(prints, taps, pays)
    gold.to_parquet(f"{cfg.data_paths.output}/training_dataset.parquet", index=False)

def quality(**_):
    cfg = get_config()
    qc = QualityChecker({
        "max_null_pct": cfg.quality.max_null_percentage,
        "max_dup_pct": cfg.quality.max_duplicate_percentage,
        "min_quality_score": cfg.quality.min_data_quality_score,
    })
    prints = pd.read_parquet(f"{cfg.data_paths.processed}/prints.parquet")
    taps   = pd.read_parquet(f"{cfg.data_paths.processed}/taps.parquet")
    pays   = pd.read_parquet(f"{cfg.data_paths.processed}/payments.parquet")
    gold   = pd.read_parquet(f"{cfg.data_paths.output}/training_dataset.parquet")
    report = qc.evaluate(prints, taps, pays, gold)
    os.makedirs(cfg.quality.quality_report_path, exist_ok=True)
    import json
    with open(f"{cfg.quality.quality_report_path}/quality_report.json", "w") as f:
        json.dump(report, f, indent=2)

default_args = {
    "owner": "mp_data",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mp_carousel_training",
    start_date=datetime(2020, 11, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["mercadopago","training"],
) as dag:

    t_extract = PythonOperator(task_id="extract", python_callable=extract)
    t_preproc = PythonOperator(task_id="preprocess", python_callable=preprocess)
    t_features = PythonOperator(task_id="features", python_callable=features)
    t_quality = PythonOperator(task_id="quality", python_callable=quality)

    t_extract >> t_preproc >> t_features >> t_quality
