MercadoPago ML Pipeline

Goal: build a machine learning–ready dataset for ordering Value Propositions (a.k.a. Value Props) shown in MercadoPago’s “Descubrí Más” carousel.

The pipeline ingests prints, taps, and payments (JSONL/CSV), validates them with Pydantic (row-level) and Pandera (DataFrame-level), applies quality checks, and emits datasets ready for feature engineering.

📂 Repository Structure

mercadopago-ml-pipeline/
├── config/
│   └── params.yaml             # Central configuration (paths, windows, thresholds)
├── management/
│   └── logging_config.py       # Logging bootstrap & ProgressTracker
├── src/
│   └── carrusel_mp/
│       └── config.py           # Config loader, env overrides, validation bounds
│
│   └── data/
│       ├── extractors.py       # DataExtractor base + Prints/Taps/Payments
│       ├── loaders.py          # JSONL/CSV chunked readers
│       └── validators.py       # Cross-source validators (e.g. taps must have prints)
│
│   └── models/
│       ├── contracts.py        # Pandera schemas (raw, processed, final)
│       └── schemas.py          # Pydantic models (Print, Tap, Payment, Results)
│
└── data/                       # Storage (gitignored in real usage)
    ├── raw/                    # Bronze: raw inputs
    ├── processed/              # Silver: cleaned with metadata
    └── output/                 # Gold: ML-ready dataset

🔑 Core Components
1. Configuration (config/params.yaml + src/carrusel_mp/config.py)

params.yaml defines paths, chunk sizes, memory caps, validation windows, thresholds.

Config dataclass loader:

Reads YAML and applies environment variable overrides.

Provides validation bounds (e.g., timestamp age in days).

Ensures required directories exist.

2. Logging (management/logging_config.py)

Uses logging.config.dictConfig for structured logs.

File rotation with size/backup limits.

Console logging toggle.

ProgressTracker for stepwise reporting:
 
 ``` yaml
🚀 start: extraction
📈 [2/5] (40.0%) processed taps
✅ done: extraction (duration: 12.34s)
```

3. Models

Pydantic Schemas (schemas.py)

Validate each row (PrintRecord, TapRecord, PaymentRecord).

Enforce ID formats, timestamp bounds (UTC), amount limits.

Pandera Contracts (contracts.py)

Validate entire DataFrames.

strict=False for raw (allow optional cols like position).

strict=True for processed/final (lock schema).

Capture business rules (e.g., taps ≤ prints, payments ≤ max).

4. Data Extractors (extractors.py)

Abstract DataExtractor:

Generic over Pydantic model + Pandera schema.

Reads chunked data (via loaders.py).

Normalizes raw JSON/CSV into canonical form:

timestamp (UTC, parsed from day / pay_date).

user_id, value_prop_id.

position (prints/taps) or amount (payments).

Rejects invalid rows → quarantine CSV.

Writes manifest JSON with metrics (rows read, rejected, etc.).

Concrete extractors:

PrintsExtractor → JSONL with event_data.

TapsExtractor → JSONL with event_data.

PaymentsExtractor → CSV with pay_date, total.

5. Validators (validators.py)

Example: taps_have_prints
Ensures each tap has a corresponding print (same user_id, value_prop_id, date).

🧩 Data Flow
ETL Flow (Bronze → Silver → Gold)

``` mermaid
flowchart LR
  subgraph Bronze["Bronze (raw)"]
    P[prints.jsonl] --> E1
    T[taps.jsonl] --> E2
    Y[pays.csv] --> E3
  end

  subgraph Silver["Silver (processed)"]
    E1[PrintsExtractor] --> V1
    E2[TapsExtractor] --> V2
    E3[PaymentsExtractor] --> V3
  end

  subgraph Gold["Gold (final ML dataset)"]
    V1 --> F
    V2 --> F
    V3 --> F
    F[Feature Engine / Aggregator (TBD)]
  end
```

Extraction Workflow

``` mermaid
[Raw file] → [Loader (chunk)] → [Normalize] → [Pydantic row validate]
           → [Pandera DF validate] → [Processed chunk] → [Yield / Write]
```

⚙️ Usage (so far)
Configure

Edit config/params.yaml:

``` yaml
processing:
  chunk_size: 10000
  target_window_days: 7
  lookback_window_days: 21
validation:
  business_rules:
    min_timestamp_days_ago: 1825

```
Run Extraction

Example (inside scripts/ or notebook):

``` python
from src.data.extractors import PrintsExtractor
from carrusel_mp.config import get_config

cfg = get_config()
prints_path = cfg.data_paths.raw + "/prints.json"

ext = PrintsExtractor(prints_path)
for chunk in ext.extract_chunked():
    print(chunk.head())

```


