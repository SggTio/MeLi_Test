MercadoPago ML Pipeline

Goal: build a machine learning–ready dataset for ordering Value Propositions (Value Props) shown in MercadoPago’s “Descubrí Más” carousel.

The pipeline ingests prints, taps, and payments (JSONL/CSV), validates them with Pydantic (row-level) and Pandera (DataFrame-level), applies quality checks, and emits datasets ready for feature engineering (clicked target + 21-day lookbacks).

```bash
mercadopago-ml-pipeline/
├── config/
│   └── params.yaml             # Central configuration (paths, windows, thresholds)
├── management/
│   └── logging_config.py       # Logging bootstrap & ProgressTracker
├── src/
│   ├── carrusel_mp/
│   │   └── processing/
│   │   │   └── aggregators.py        # utilities for calculation in aggregation dataset
│   │   │   └── feature_engine.py     # Feature processing engine
│   │   │   └── preprocess_engine.py  # health check for data before processing
│   │   └── config.py           # Config loader, env overrides, validation bounds
│   │
│   ├── data/
│   │   ├── extractors.py       # DataExtractor base + Prints/Taps/Payments
│   │   ├── loaders.py          # JSONL/CSV chunked readers
│   │   └── validators.py       # Cross-source validators (e.g. taps must have prints)
│   │
│   ├── models/
│   │   ├── contracts.py        # Pandera schemas (raw, processed, final)
│   │   └── schemas.py          # Pydantic models (Print, Tap, Payment, Results)
│   └── utils/
│           ├── time_utils.py   # UTC/time parsing helpers
│           └── quality_utils.py # Simple quality summaries & logging
├── scripts/
│   ├── run_pipeline.py         # End-to-end runner (Bronze→Silver→Gold)
│   └── performance_analysis.py # (optional) perf tips
│
├── examples/
│   ├── input/                  # Example raw batch (prints.json, taps.json, pays.csv)
│   ├── processed/              # Example “Silver” CSVs
│   └── output/                 # Example “Gold” CSV
│
└── data/                       # Storage (gitignored in prod)
    ├── raw/                    # Bronze: raw inputs
    ├── processed/              # Silver: cleaned with metadata (+ rejects/manifests)
    └── output/                 # Gold: ML-ready dataset

```

🔑 Core Components
1) Configuration (config/params.yaml + src/carrusel_mp/config.py)

Paths, chunk sizes, memory caps, validation windows, thresholds

YAML → dataclasses + env overrides (e.g. MP_LOG_DIR, MP_VALIDATE_SCHEMA)

Validation bounds for timestamps (e.g., validation.business_rules.min_timestamp_days_ago)

Ensures data directories exist

2) Logging (management/logging_config.py)

logging.config.dictConfig structured logs

Rotating file handler + optional console output

ProgressTracker for stepwise reporting
 
 ``` yaml
🚀 start: extraction
📈 [2/5] (40.0%) processed taps
✅ done: extraction (duration: 12.34s)
```

3) Models

Pydantic (src/models/schemas.py)

Validate each row: PrintRecord, TapRecord, PaymentRecord

Enforce ID formats, timestamp bounds (UTC), amount limits

BatchProcessingResult for batch metrics

Pandera (src/models/contracts.py)

Validate entire DataFrames

strict=False for raw (optional position)

strict=True for processed/final

Leverages config bounds and max payment amount

4) Data Extractors (src/data/extractors.py)

Abstract DataExtractor (generic over Pydantic model + Pandera schema)

Chunked readers via src/data/loaders.py

Normalize raw → canonical:

prints/taps: day → timestamp (UTC), event_data.{value_prop, position} → value_prop_id, position

payments: pay_date → timestamp, total → amount

Rejects to data/processed/_rejects/*.csv, manifests to data/processed/_manifests/*.json

5) Preprocessing & Features

Preprocessing (in the runner): UTC coercion, dedupe, metadata, basic type fixes

Features (in the runner):

clicked target (tap exists same (user_id, value_prop_id, date))

21-day trailing lookbacks: prints_count_3w, taps_count_3w, payments_count_3w, payments_amount_3w

6) Validators (src/data/validators.py)

Example: taps_have_prints — % of taps that have a print same (user_id, value_prop_id, date)

🧩 Data Flow
ETL Flow (Bronze → Silver → Gold)

``` text
┌───────────────┐     ┌──────────────────┐
│  prints.json  │───► │ PrintsExtractor  │
├───────────────┤     ├──────────────────┤
│   taps.json   │───► │ TapsExtractor    │───►  Silver: processed/validated
├───────────────┤     ├──────────────────┤        (+ rejects, manifests)
│   pays.csv    │───► │ PaymentsExtractor│
└───────────────┘     └──────────────────┘
                               │
                               ▼
                  ┌─────────────────────────┐
                  │  Feature engineering    │───►  Gold: final training dataset
                  │  (clicked + 21d feats)  │
                  └─────────────────────────┘

```

Extraction Workflow

``` text
[Raw file]
   │
   ▼
[Loader (chunk)]
   │
   ▼
[Normalize]
   │
   ▼
[Pydantic row validate]
   │
   ▼
[Pandera DF validate]
   │
   ▼
[Processed chunk] → [Yield / Write]


```

⚙️ Usage.
0) Install


``` bash
python -m venv .venv
source .venv/bin/activate           # (On Windows: .venv\Scripts\activate)
python -m pip install --upgrade pip
pip install -r requirements/base.txt

```

1) Place inputs

Either copy your own month of data into data/raw/:

prints.json (JSON Lines)

taps.json (JSON Lines)

pays.csv (CSV)


Run the pipeline:
Ensure the repo root is on PYTHONPATH so src/... imports resolve:
```bash
PYTHONPATH=. python scripts/run_pipeline.py
```

4) Outputs

Silver (Parquet):

data/processed/prints.parquet

data/processed/taps.parquet

data/processed/payments.parquet

plus _rejects/*.csv and _manifests/*.json

Gold (Parquet):

data/output/training_dataset.parquet

Quality Report:

data/quality_reports/quality_report.json

Logs:

logs/pipeline.log

Quality/Validation

Row-level: Pydantic (schemas.py)

Frame-level: Pandera (contracts.py) with strict=False (raw) / strict=True (processed/final)

Business checks:

max payment <= validation.business_rules.max_payment_amount

timestamps within validation.business_rules.min_timestamp_days_ago window

optional: taps_have_prints metric in validators.py

⚙️ Configuration & Logging Flow:

```text
┌───────────────────────┐
│  config/params.yaml   │
└─────────┬─────────────┘
          │ YAML load + env overrides
          ▼
┌──────────────────────────────┐
│ src/carrusel_mp/config.py    │
│   - DataPaths                │
│   - ProcessingConfig         │
│   - QualityConfig            │
│   - LoggingCfg               │
└─────────┬────────────────────┘
          │ provides cfg objects
          ▼
┌──────────────────────────────┐
│ management/logging_config.py │
│   setup_logging()            │
│   ProgressTracker            │
└─────────┬────────────────────┘
          │ structured logs + rotation
          ▼
      Application Runtime

```

Orchestration
GitHub Actions (CI runner)

```yaml
name: ml-pipeline
on:
  push:
    branches: [ main ]
  workflow_dispatch: {}

jobs:
  run-pipeline:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install deps
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements/base.txt

      - name: Prepare example data
        run: |
          mkdir -p data/raw
          cp -r examples/input/* data/raw/
          ls -la data/raw

      - name: Run pipeline
        env:
          MP_LOG_DIR: logs
          MP_VALIDATE_SCHEMA: "true"
          PYTHONPATH: .
        run: |
          python scripts/run_pipeline.py

      - name: Upload artifacts
        uses: actions/upload-artifact@v4
        with:
          name: pipeline-artifacts
          path: |
            data/processed/**
            data/output/**
            data/quality_reports/**
            logs/**

```
Airflow DAG (concept)

```text
┌──────────────────────────┐      ┌──────────────────────────┐
│        extract           │ ───► │       preprocess         │
│  (read + normalize raw)  │      │  (UTC, dedupe, metadata) │
└──────────────────────────┘      └──────────────────────────┘
           │                                │
           └────────────────────────────────┘
                        ▼
              ┌──────────────────────────┐
              │        features          │
              │  (clicked + 21d feats)   │
              └──────────────────────────┘
                        │
                        ▼
              ┌──────────────────────────┐
              │         quality          │
              │ (rules + report JSON)    │
              └──────────────────────────┘

```

