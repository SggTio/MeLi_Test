# MercadoPago ML Pipeline (Carousel Value Props)

Pipeline to build a training-ready dataset for ordering Value Props in MercadoPago’s “Descubrí Más” carousel.  
This repo is organized in layers and follows a medallion architecture: **raw → processed → output**.

---

## Objectives (Phase 1 — implemented)

- Read **raw events** from three sources:
  - `prints.jsonl` (impressions),
  - `taps.jsonl` (clicks),
  - `pays.csv` (payments).
- Stream them **in chunks** (memory-safe) and **normalize** to a canonical schema.
- Validate each record with **Pydantic** (row-level) and each chunk with **Pandera** (dataframe-level).
- Emit **manifests** and **rejects** for transparency.

> Next phases (not in this README yet): preprocessing engine (UTC confirm, dedupe, metadata, dtypes), feature engineering (21-day lookback, label attribution), and orchestration.

---

## Repository layout (relevant parts)

mercadopago-ml-pipeline/
├── config/
│ └── params.yaml
├── data/
│ ├── raw/ # bronze
│ ├── processed/ # silver
│ └── output/ # gold
├── docs/
│ └── README.md
└── src/
└── mp_carousel/
├── config.py # Single source of truth config
├── logging_config.py # Logging setup + ProgressTracker
├── models/
│ ├── schemas.py # Pydantic row models (+ _bounds())
│ └── contracts.py # Pandera DataFrame schemas
├── data/
│ ├── loaders.py # Chunk readers (csv/jsonl)
│ ├── extractors.py # DataExtractor ABC + concrete extractors
│ └── validators.py # Soft dataset-level checks (e.g. taps-have-prints rate)
└── utils/
├── time_utils.py # UTC helpers, parse_day_date
└── quality_utils.py # Quality summaries


---

## Configuration

**`config/params.yaml`** is the primary configuration. Key entries:

- `data_paths.raw|processed|output`  
- `processing.chunk_size`, `processing.validate_schema`  
- `validation.business_rules.min_timestamp_days_ago` ← **controls allowable data recency**  
- `validation.business_rules.max_payment_amount`  

These values are also partially overrideable via environment variables (see `src/mp_carousel/config.py`).

---

## Canonical schemas

Raw sources are normalized to:

- **Prints / Taps**:
  - `timestamp: datetime (UTC)` ← from raw `day`
  - `user_id: str`              ← cast from raw
  - `value_prop_id: str`        ← from `event_data.value_prop`
  - `position: Optional[int]`   ← from `event_data.position`

- **Payments**:
  - `timestamp: datetime (UTC)` ← from raw `pay_date`
  - `user_id: str`              ← cast from raw
  - `value_prop_id: str`        ← from `value_prop`
  - `amount: float`             ← from `total` (must be > 0 and ≤ max_payment_amount)

**Why canonicalization?** The downstream pipeline (preprocessing, features, orchestrator) consumes a stable schema regardless of differences in raw formats.

---

## Validation strategy

- **Pydantic (row-level)** — `src/mp_carousel/models/schemas.py`  
  Each normalized row is validated by a Pydantic model (`PrintRecord`, `TapRecord`, `PaymentRecord`).  
  Timestamp bounds are **config-driven**, read from `params.yaml` via `get_config().get_validation_bounds()`.

- **Pandera (dataframe-level)** — `src/mp_carousel/models/contracts.py`  
  For raw canonical chunks: `strict=False` (accepts extra columns).  
  For processed/final datasets: `strict=True` (exact contract).

---

## Extraction pipeline (current)

**Class:** `DataExtractor` in `src/mp_carousel/data/extractors.py`  
**Pattern:** Template Method

Steps per chunk:
1. `_reader()` yields a raw Pandas DataFrame from source (JSONL or CSV) using iterator-based chunking.
2. `_normalize_chunk(df)` maps raw fields to canonical dicts.
3. **Pydantic** validates each dict (quarantine if invalid).
4. **Pandera** validates the resulting DataFrame (raw schema).
5. Metrics updated; chunk yielded to the caller.

On stream completion:
- Writes a per-source **manifest** to `data/processed/_manifests/<source>.json`.
- Appends invalid rows to `data/processed/_rejects/<source>.csv`.
- Hard-fails if reject rate exceeds `max_reject_rate` (default 2%).

---

## Logging & Progress

`src/mp_carousel/logging_config.py` configures file + console logging and a `ProgressTracker` with corrected timing (`datetime.now(timezone.utc)`).

Log output shows:
- per-chunk read/valid/rejected counts,
- memory-friendly chunk boundaries,
- end-of-source summaries.

---

## Quick start (extraction only)

Place raw files:

data/raw/prints.json
data/raw/taps.json
data/raw/pays.csv


Run a simple smoke script:

```python
from mp_carousel.logging_config import setup_logging
from mp_carousel.config import get_config
from mp_carousel.data.extractors import PrintsExtractor, TapsExtractor, PaymentsExtractor

def main():
    setup_logging()
    cfg = get_config()
    paths = {
        "prints": f"{cfg.data_paths.raw}/prints.json",
        "taps": f"{cfg.data_paths.raw}/taps.json",
        "payments": f"{cfg.data_paths.raw}/pays.csv",
    }
    prints = PrintsExtractor(paths["prints"]).extract_all()
    taps   = TapsExtractor(paths["taps"]).extract_all()
    pays   = PaymentsExtractor(paths["payments"]).extract_all()

    print("prints sample:\n", prints.head())
    print("taps sample:\n", taps.head())
    print("pays sample:\n", pays.head())

if __name__ == "__main__":
    main()
```


You should see canonical columns:

prints/taps: timestamp | user_id | value_prop_id | position

payments: timestamp | user_id | value_prop_id | amount

Manifests: data/processed/_manifests/
Rejects: data/processed/_rejects/

(Workflow diagram (current scope)

          +-------------------+
          |  params.yaml      |
          +---------+---------+
                    |
                    v
      +-------------+-------------+
      |  Config (single source)   |
      |  src/mp_carousel/config   |
      +------+------+-------------+
             |      |
             |      v
             |  Logging setup (setup_logging)
             |  src/mp_carousel/logging_config.py
             |
             v
  +----------+------------------------------+
  |          Data Extractors (ABC)          |
  |  src/mp_carousel/data/extractors.py     |
  |                                         |
  |  _reader()         _normalize_chunk()   |
  |    |                      |             |
  |    v                      v             |
  |  chunk df  ---> canonical dicts         |
  |         Pydantic (row)  Pandera (df)    |
  |                |            |           |
  |                v            v           |
  |            valid df -----> yield        |
  +----------------+------------------------+
                   |
                   v
        manifests + rejects written
        data/processed/_manifests/
        data/processed/_rejects/)


Design choices & rationale

Template Method + Strategy in extractors: consistent orchestration, flexible normalization.

Schema-driven development: Pydantic/Pandera keep us honest; contracts evolve in one place.

Config-driven policy: time bounds and thresholds don’t require code changes.

Iterator-based chunking: predictable memory usage and good performance.