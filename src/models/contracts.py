"""
MercadoPago ML Pipeline - Data Contracts Layer
Pandera DataFrame schemas for data validation
"""
#================================================================================

from __future__ import annotations
import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema
from datetime import datetime
from src.carrusel_mp.config import get_config

#================================================================================

def _bounds():
    return get_config().get_validation_bounds()
# ---------- raw (canonical) schemas: strict=False ----------
PrintsRawSchema = DataFrameSchema(
    {
        "timestamp": Column(pa.DateTime, nullable=False, coerce=True,
                            checks=[Check.greater_than_or_equal_to(lambda: _bounds()[0]),
                                    Check.less_than_or_equal_to(lambda: _bounds()[1])]),
        "user_id": Column(pa.String, nullable=False),
        "value_prop_id": Column(pa.String, nullable=False),
        "position": Column(pa.Int64, nullable=True),
    },
    coerce=True,
    strict=False,
    name="prints_raw",
)

TapsRawSchema = DataFrameSchema(
    {
        "timestamp": Column(pa.DateTime, nullable=False, coerce=True,
                            checks=[Check.greater_than_or_equal_to(lambda: _bounds()[0]),
                                    Check.less_than_or_equal_to(lambda: _bounds()[1])]),
        "user_id": Column(pa.String, nullable=False),
        "value_prop_id": Column(pa.String, nullable=False),
        "position": Column(pa.Int64, nullable=True),
    },
    coerce=True,
    strict=False,
    name="taps_raw",
)

PaymentsRawSchema = DataFrameSchema(
    {
        "timestamp": Column(pa.DateTime, nullable=False, coerce=True,
                            checks=[Check.greater_than_or_equal_to(lambda: _bounds()[0]),
                                    Check.less_than_or_equal_to(lambda: _bounds()[1])]),
        "user_id": Column(pa.String, nullable=False),
        "value_prop_id": Column(pa.String, nullable=False),
        "amount": Column(pa.Float, nullable=False, checks=[Check.gt(0), Check.le(
            lambda: float(get_config().get_param("validation.business_rules.max_payment_amount", 1_000_000))
        )]),
    },
    coerce=True,
    strict=False,
    name="payments_raw",
)

# ---------- processed/final schemas: strict=True ----------
ProcessedPrintsSchema = DataFrameSchema(
    {
        **PrintsRawSchema.columns,
        "processed_at": Column(pa.DateTime, nullable=False),
        "source_file": Column(pa.String, nullable=False),
        "row_number": Column(pa.Int64, nullable=True),
        "duplicate_flag": Column(pa.Bool, nullable=False),
        "data_quality_score": Column(pa.Float, nullable=False, checks=[Check.ge(0), Check.le(1)]),
    },
    coerce=True,
    strict=True,
    name="processed_prints",
)

FinalDatasetSchema = DataFrameSchema(
    {
        "timestamp": Column(pa.DateTime, nullable=False, coerce=True),
        "user_id": Column(pa.String, nullable=False),
        "value_prop_id": Column(pa.String, nullable=False),
        "clicked": Column(pa.Bool, nullable=False),
        "prints_count_3w": Column(pa.Int64, nullable=False, checks=[Check.ge(0)]),
        "taps_count_3w": Column(pa.Int64, nullable=False, checks=[Check.ge(0)]),
        "payments_count_3w": Column(pa.Int64, nullable=False, checks=[Check.ge(0)]),
        "payments_amount_3w": Column(pa.Float, nullable=False, checks=[Check.ge(0)]),
    },
    coerce=True,
    strict=True,
    name="final_dataset",
)

# ---------- helper ----------
def get_schema_by_name(name: str) -> DataFrameSchema:
    m = {
        "prints_raw": PrintsRawSchema,
        "taps_raw": TapsRawSchema,
        "payments_raw": PaymentsRawSchema,
        "processed_prints": ProcessedPrintsSchema,
        "final_dataset": FinalDatasetSchema,
    }
    if name not in m:
        raise ValueError(f"Unknown schema: {name}")
    return m[name]

def validate_dataframe(df: pd.DataFrame, schema_name: str) -> None:
    schema = get_schema_by_name(schema_name)
    schema.validate(df, lazy=True)