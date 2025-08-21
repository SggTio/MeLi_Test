"""
MercadoPago ML Pipeline - Data Models Layer
Pydantic schemas for data validation and consistency
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator, root_validator
from decimal import Decimal
import re


class BaseRecord(BaseModel):
    """Base record with common validation rules"""
    
    class Config:
        # Enable validation on assignment
        validate_assignment = True
        # Use enum values instead of names
        use_enum_values = True
        # Allow population by field name and alias
        allow_population_by_field_name = True
        # Validate default values
        validate_all = True


class PrintRecord(BaseRecord):
    """Schema for print events (value props shown to users)"""
    
    timestamp: datetime = Field(
        ..., 
        description="When the value prop was shown to the user"
    )
    user_id: str = Field(
        ..., 
        min_length=1,
        max_length=100,
        description="Unique identifier for the user"
    )
    value_prop_id: str = Field(
        ..., 
        min_length=1,
        max_length=100,
        description="Unique identifier for the value proposition"
    )
    
    @validator('user_id', 'value_prop_id')
    def validate_ids(cls, v):
        """Validate ID format and content"""
        if not v or v.isspace():
            raise ValueError("ID cannot be empty or whitespace")
        return v.strip()
    
    @validator('timestamp')
    def validate_timestamp(cls, v):
        """Ensure timestamp is reasonable (not too far in future/past)"""
        now = datetime.utcnow()
        if v > now:
            raise ValueError("Timestamp cannot be in the future")
        # Allow up to 2 years of historical data
        if (now - v).days > 730:
            raise ValueError("Timestamp too old (> 2 years)")
        return v


class TapRecord(BaseRecord):
    """Schema for tap events (value props clicked by users)"""
    
    timestamp: datetime = Field(
        ..., 
        description="When the value prop was clicked by the user"
    )
    user_id: str = Field(
        ..., 
        min_length=1,
        max_length=100,
        description="Unique identifier for the user"
    )
    value_prop_id: str = Field(
        ..., 
        min_length=1,
        max_length=100,
        description="Unique identifier for the value proposition"
    )
    
    @validator('user_id', 'value_prop_id')
    def validate_ids(cls, v):
        """Validate ID format and content"""
        if not v or v.isspace():
            raise ValueError("ID cannot be empty or whitespace")
        return v.strip()
    
    @validator('timestamp')
    def validate_timestamp(cls, v):
        """Ensure timestamp is reasonable"""
        now = datetime.utcnow()
        if v > now:
            raise ValueError("Timestamp cannot be in the future")
        if (now - v).days > 730:
            raise ValueError("Timestamp too old (> 2 years)")
        return v


class PaymentRecord(BaseRecord):
    """Schema for payment events"""
    
    timestamp: datetime = Field(
        ..., 
        description="When the payment was made"
    )
    user_id: str = Field(
        ..., 
        min_length=1,
        max_length=100,
        description="Unique identifier for the user"
    )
    value_prop_id: str = Field(
        ..., 
        min_length=1,
        max_length=100,
        description="Unique identifier for the value proposition"
    )
    amount: float = Field(
        ..., 
        gt=0,
        description="Payment amount (must be positive)"
    )
    
    @validator('user_id', 'value_prop_id')
    def validate_ids(cls, v):
        """Validate ID format and content"""
        if not v or v.isspace():
            raise ValueError("ID cannot be empty or whitespace")
        return v.strip()
    
    @validator('timestamp')
    def validate_timestamp(cls, v):
        """Ensure timestamp is reasonable"""
        now = datetime.utcnow()
        if v > now:
            raise ValueError("Timestamp cannot be in the future")
        if (now - v).days > 730:
            raise ValueError("Timestamp too old (> 2 years)")
        return v
    
    @validator('amount')
    def validate_amount(cls, v):
        """Validate payment amount"""
        if v <= 0:
            raise ValueError("Payment amount must be positive")
        # Check for reasonable upper bound (adjust as needed)
        if v > 1000000:  # 1M currency units
            raise ValueError("Payment amount seems unreasonably high")
        return round(v, 2)  # Round to 2 decimal places


class ProcessingMetadata(BaseRecord):
    """Metadata for processed records"""
    
    processed_at: datetime = Field(default_factory=datetime.utcnow)
    source_file: str = Field(..., description="Original source file name")
    row_number: Optional[int] = Field(None, description="Row number in source file")
    data_quality_flags: Dict[str, Any] = Field(default_factory=dict)
    processing_version: str = Field(default="1.0.0")


class DatasetRecord(BaseRecord):
    """Schema for final ML dataset records"""
    
    # Core identifiers
    timestamp: datetime = Field(..., description="Print timestamp")
    user_id: str = Field(..., description="User identifier")
    value_prop_id: str = Field(..., description="Value proposition identifier")
    
    # Target variable
    clicked: bool = Field(..., description="Whether the value prop was clicked")
    
    # Historical features (3 weeks lookback)
    prints_count_3w: int = Field(ge=0, description="Number of prints in 3 weeks")
    taps_count_3w: int = Field(ge=0, description="Number of taps in 3 weeks")
    payments_count_3w: int = Field(ge=0, description="Number of payments in 3 weeks")
    payments_amount_3w: float = Field(ge=0, description="Total payment amount in 3 weeks")
    
    # Metadata
    metadata: Optional[ProcessingMetadata] = None
    
    @validator('prints_count_3w', 'taps_count_3w', 'payments_count_3w')
    def validate_counts(cls, v):
        """Ensure counts are non-negative"""
        if v < 0:
            raise ValueError("Counts cannot be negative")
        return v
    
    @validator('payments_amount_3w')
    def validate_amount(cls, v):
        """Ensure amount is non-negative"""
        if v < 0:
            raise ValueError("Payment amount cannot be negative")
        return round(v, 2)


class PipelineConfig(BaseModel):
    """Configuration schema for the pipeline"""
    
    # Time windows
    target_window_days: int = Field(default=7, description="Target window in days")
    lookback_window_days: int = Field(default=21, description="Lookback window in days")
    
    # Processing settings
    chunk_size: int = Field(default=10000, description="Chunk size for processing")
    max_memory_usage_mb: int = Field(default=1024, description="Max memory usage in MB")
    
    # Data quality thresholds
    min_data_quality_score: float = Field(default=0.8, description="Minimum data quality score")
    max_null_percentage: float = Field(default=0.1, description="Maximum null percentage")
    
    # File paths
    raw_data_path: str = Field(default="data/raw/")
    processed_data_path: str = Field(default="data/processed/")
    output_data_path: str = Field(default="data/output/")
    
    class Config:
        validate_assignment = True


# Batch processing schemas
class BatchProcessingResult(BaseModel):
    """Result of batch processing operation"""
    
    records_processed: int = Field(ge=0)
    records_valid: int = Field(ge=0)
    records_invalid: int = Field(ge=0)
    processing_time_seconds: float = Field(ge=0)
    data_quality_score: float = Field(ge=0, le=1)
    errors: List[str] = Field(default_factory=list)
    
    @validator('records_invalid')
    def validate_invalid_count(cls, v, values):
        """Ensure invalid count makes sense"""
        if 'records_processed' in values and v > values['records_processed']:
            raise ValueError("Invalid records cannot exceed total processed")
        return v