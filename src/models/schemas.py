"""
MercadoPago ML Pipeline - Data Models Layer
Pydantic schemas for data validation and consistency
"""

#================================================================================

from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Optional
from pydantic import BaseModel, Field, field_validator

from src.carrusel_mp.config import get_config

#================================================================================

class BaseRecord(BaseModel):
    """Base record with common validation rules"""

    model_config = {
        "validate_assignment": True,
        "use_enum_values": True,
        "from_attributes": False,
    }
    
    def _bounds() -> tuple[datetime, datetime]:
        return get_config().get_validation_bounds()

#================================================================================

class PrintRecord(BaseRecord):
    """Schema for print events (value props shown to users)"""
    timestamp: datetime = Field(...)
    user_id: str = Field(..., min_length=1, max_length=100)
    value_prop_id: str = Field(..., min_length=1, max_length=100)
    position: Optional[int] = None
    
    @field_validator("user_id", "value_prop_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = str(v).strip()
        if not v:
            raise ValueError("blank id")
        return v

    #--------------------------------------------------------------------------------
    
    @field_validator("timestamp")
    @classmethod
    def _ts_bounds(cls, v: datetime) -> datetime:
        lo, hi = _bounds()
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        if not (lo <= v <= hi):
            raise ValueError(f"timestamp out of bounds [{lo}..{hi}]")
        return v    

#================================================================================

class TapRecord(PrintRecord):
    """Same shape as PrintRecord (we keep position if present)."""
    pass

#================================================================================

class PaymentRecord(BaseRecord):
    timestamp: datetime = Field(...)
    user_id: str = Field(..., min_length=1, max_length=100)
    value_prop_id: str = Field(..., min_length=1, max_length=100)
    amount: float = Field(..., gt=0)

    #--------------------------------------------------------------------------------

    @field_validator("user_id", "value_prop_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = str(v).strip()
        if not v:
            raise ValueError("blank id")
        return v
    
    #--------------------------------------------------------------------------------
    @field_validator("timestamp")
    @classmethod
    def _ts_bounds(cls, v: datetime) -> datetime:
        lo, hi = _bounds()
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        if not (lo <= v <= hi):
            raise ValueError(f"timestamp out of bounds [{lo}..{hi}]")
        return v
    
    #--------------------------------------------------------------------------------
    
    @field_validator("amount")
    @classmethod
    def _amt(cls, v: float) -> float:
        max_amt = float(get_config().get_param("validation.business_rules.max_payment_amount", 1_000_000))
        if v <= 0 or v > max_amt:
            raise ValueError(f"amount must be (0, {max_amt}]")
        return round(float(v), 2)

#================================================================================

class BatchProcessingResult(BaseModel):
    records_processed: int
    records_valid: int
    records_invalid: int
    processing_time_seconds: float
    data_quality_score: float
    errors: list[str] = []

    #================================================================================