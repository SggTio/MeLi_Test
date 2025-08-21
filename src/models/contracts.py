"""
MercadoPago ML Pipeline - Data Contracts Layer
Pandera DataFrame schemas for data validation
"""

import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check, Index
from pandera.typing import Series
from datetime import datetime, timedelta
from typing import Optional


class DataFrameContracts:
    """Container for all DataFrame validation contracts"""
    
    @staticmethod
    def get_current_time_bounds():
        """Get reasonable time bounds for validation"""
        now = datetime.utcnow()
        min_date = now - timedelta(days=730)  # 2 years ago
        max_date = now + timedelta(hours=1)   # Allow 1 hour future for timezone issues
        return min_date, max_date

    @classmethod
    def prints_schema(cls) -> DataFrameSchema:
        """Schema for prints DataFrame"""
        min_date, max_date = cls.get_current_time_bounds()
        
        return DataFrameSchema(
            columns={
                "timestamp": Column(
                    pd.Timestamp,
                    checks=[
                        Check.greater_than_or_equal_to(min_date),
                        Check.less_than_or_equal_to(max_date),
                    ],
                    nullable=False,
                    description="Print timestamp in UTC"
                ),
                "user_id": Column(
                    str,
                    checks=[
                        Check.str_length(min_val=1, max_val=100),
                        Check(lambda x: ~x.str.isspace().any(), 
                              error="user_id cannot be whitespace"),
                        Check(lambda x: x.notna().all(), 
                              error="user_id cannot be null")
                    ],
                    nullable=False,
                    description="User identifier"
                ),
                "value_prop_id": Column(
                    str,
                    checks=[
                        Check.str_length(min_val=1, max_val=100),
                        Check(lambda x: ~x.str.isspace().any(), 
                              error="value_prop_id cannot be whitespace"),
                        Check(lambda x: x.notna().all(), 
                              error="value_prop_id cannot be null")
                    ],
                    nullable=False,
                    description="Value proposition identifier"
                )
            },
            checks=[
                Check(lambda df: len(df) > 0, error="DataFrame cannot be empty"),
                Check(lambda df: df.duplicated().sum() / len(df) < 0.1, 
                      error="Too many duplicate records (>10%)"),
            ],
            strict=True,
            name="prints_schema",
            description="Validation schema for prints data"
        )

    @classmethod
    def taps_schema(cls) -> DataFrameSchema:
        """Schema for taps DataFrame"""
        min_date, max_date = cls.get_current_time_bounds()
        
        return DataFrameSchema(
            columns={
                "timestamp": Column(
                    pd.Timestamp,
                    checks=[
                        Check.greater_than_or_equal_to(min_date),
                        Check.less_than_or_equal_to(max_date),
                    ],
                    nullable=False,
                    description="Tap timestamp in UTC"
                ),
                "user_id": Column(
                    str,
                    checks=[
                        Check.str_length(min_val=1, max_val=100),
                        Check(lambda x: ~x.str.isspace().any(), 
                              error="user_id cannot be whitespace"),
                        Check(lambda x: x.notna().all(), 
                              error="user_id cannot be null")
                    ],
                    nullable=False,
                    description="User identifier"
                ),
                "value_prop_id": Column(
                    str,
                    checks=[
                        Check.str_length(min_val=1, max_val=100),
                        Check(lambda x: ~x.str.isspace().any(), 
                              error="value_prop_id cannot be whitespace"),
                        Check(lambda x: x.notna().all(), 
                              error="value_prop_id cannot be null")
                    ],
                    nullable=False,
                    description="Value proposition identifier"
                )
            },
            checks=[
                Check(lambda df: len(df) > 0, error="DataFrame cannot be empty"),
                Check(lambda df: df.duplicated().sum() / len(df) < 0.1, 
                      error="Too many duplicate records (>10%)"),
            ],
            strict=True,
            name="taps_schema",
            description="Validation schema for taps data"
        )

    @classmethod
    def payments_schema(cls) -> DataFrameSchema:
        """Schema for payments DataFrame"""
        min_date, max_date = cls.get_current_time_bounds()
        
        return DataFrameSchema(
            columns={
                "timestamp": Column(
                    pd.Timestamp,
                    checks=[
                        Check.greater_than_or_equal_to(min_date),
                        Check.less_than_or_equal_to(max_date),
                    ],
                    nullable=False,
                    description="Payment timestamp in UTC"
                ),
                "user_id": Column(
                    str,
                    checks=[
                        Check.str_length(min_val=1, max_val=100),
                        Check(lambda x: ~x.str.isspace().any(), 
                              error="user_id cannot be whitespace"),
                        Check(lambda x: x.notna().all(), 
                              error="user_id cannot be null")
                    ],
                    nullable=False,
                    description="User identifier"
                ),
                "value_prop_id": Column(
                    str,
                    checks=[
                        Check.str_length(min_val=1, max_val=100),
                        Check(lambda x: ~x.str.isspace().any(), 
                              error="value_prop_id cannot be whitespace"),
                        Check(lambda x: x.notna().all(), 
                              error="value_prop_id cannot be null")
                    ],
                    nullable=False,
                    description="Value proposition identifier"
                ),
                "amount": Column(
                    float,
                    checks=[
                        Check.greater_than(0, error="Payment amount must be positive"),
                        Check.less_than_or_equal_to(1000000, 
                              error="Payment amount seems unreasonably high"),
                        Check(lambda x: x.notna().all(), 
                              error="Payment amount cannot be null")
                    ],
                    nullable=False,
                    description="Payment amount"
                )
            },
            checks=[
                Check(lambda df: len(df) > 0, error="DataFrame cannot be empty"),
                Check(lambda df: df.duplicated().sum() / len(df) < 0.1, 
                      error="Too many duplicate records (>10%)"),
                # Business logic: payments should have corresponding taps
                Check(lambda df: df['amount'].sum() > 0, 
                      error="Total payment amount should be positive"),
            ],
            strict=True,
            name="payments_schema",
            description="Validation schema for payments data"
        )

    @classmethod
    def processed_prints_schema(cls) -> DataFrameSchema:
        """Schema for processed prints data (Bronze -> Silver)"""
        base_schema = cls.prints_schema()
        
        # Add processing metadata columns
        additional_columns = {
            "processed_at": Column(
                pd.Timestamp,
                nullable=False,
                description="Processing timestamp"
            ),
            "source_file": Column(
                str,
                nullable=False,
                description="Source file name"
            ),
            "row_number": Column(
                int,
                checks=[Check.greater_than_or_equal_to(0)],
                nullable=True,
                description="Row number in source file"
            ),
            "duplicate_flag": Column(
                bool,
                nullable=False,
                description="Flag indicating if record is duplicate"
            ),
            "data_quality_score": Column(
                float,
                checks=[
                    Check.greater_than_or_equal_to(0),
                    Check.less_than_or_equal_to(1)
                ],
                nullable=False,
                description="Data quality score for this record"
            )
        }
        
        # Merge with base schema
        base_schema.columns.update(additional_columns)
        base_schema.name = "processed_prints_schema"
        base_schema.description = "Validation schema for processed prints data"
        
        return base_schema

    @classmethod
    def final_dataset_schema(cls) -> DataFrameSchema:
        """Schema for final ML dataset (Gold layer)"""
        min_date, max_date = cls.get_current_time_bounds()
        
        return DataFrameSchema(
            columns={
                # Core identifiers
                "timestamp": Column(
                    pd.Timestamp,
                    checks=[
                        Check.greater_than_or_equal_to(min_date),
                        Check.less_than_or_equal_to(max_date),
                    ],
                    nullable=False,
                    description="Print timestamp in UTC"
                ),
                "user_id": Column(
                    str,
                    checks=[
                        Check.str_length(min_val=1, max_val=100),
                        Check(lambda x: ~x.str.isspace().any()),
                        Check(lambda x: x.notna().all())
                    ],
                    nullable=False,
                    description="User identifier"
                ),
                "value_prop_id": Column(
                    str,
                    checks=[
                        Check.str_length(min_val=1, max_val=100),
                        Check(lambda x: ~x.str.isspace().any()),
                        Check(lambda x: x.notna().all())
                    ],
                    nullable=False,
                    description="Value proposition identifier"
                ),
                
                # Target variable
                "clicked": Column(
                    bool,
                    nullable=False,
                    description="Whether the value prop was clicked"
                ),
                
                # Historical features (3 weeks lookback)
                "prints_count_3w": Column(
                    int,
                    checks=[Check.greater_than_or_equal_to(0)],
                    nullable=False,
                    description="Number of prints in 3 weeks"
                ),
                "taps_count_3w": Column(
                    int,
                    checks=[Check.greater_than_or_equal_to(0)],
                    nullable=False,
                    description="Number of taps in 3 weeks"
                ),
                "payments_count_3w": Column(
                    int,
                    checks=[Check.greater_than_or_equal_to(0)],
                    nullable=False,
                    description="Number of payments in 3 weeks"
                ),
                "payments_amount_3w": Column(
                    float,
                    checks=[Check.greater_than_or_equal_to(0)],
                    nullable=False,
                    description="Total payment amount in 3 weeks"
                ),
            },
            checks=[
                Check(lambda df: len(df) > 0, error="Final dataset cannot be empty"),
                Check(lambda df: df.duplicated(['timestamp', 'user_id', 'value_prop_id']).sum() == 0,
                      error="No duplicate records allowed in final dataset"),
                # Business logic validation
                Check(lambda df: (df['taps_count_3w'] <= df['prints_count_3w']).all(),
                      error="Taps count cannot exceed prints count"),
                Check(lambda df: ((df['payments_count_3w'] == 0) | (df['payments_amount_3w'] > 0)).all(),
                      error="If payments_count > 0, amount should be > 0"),
                # Data quality checks
                Check(lambda df: df.isnull().sum().sum() == 0,
                      error="No null values allowed in final dataset"),
            ],
            strict=True,
            name="final_dataset_schema",
            description="Validation schema for final ML dataset"
        )

    @classmethod
    def get_schema_by_name(cls, schema_name: str) -> DataFrameSchema:
        """Get schema by name for dynamic validation"""
        schema_mapping = {
            "prints": cls.prints_schema(),
            "taps": cls.taps_schema(),
            "payments": cls.payments_schema(),
            "processed_prints": cls.processed_prints_schema(),
            "final_dataset": cls.final_dataset_schema(),
        }
        
        if schema_name not in schema_mapping:
            raise ValueError(f"Unknown schema: {schema_name}. "
                           f"Available schemas: {list(schema_mapping.keys())}")
        
        return schema_mapping[schema_name]

    @classmethod
    def validate_dataframe(cls, df: pd.DataFrame, schema_name: str, 
                          raise_on_error: bool = True) -> dict:
        """
        Validate DataFrame against specified schema
        
        Returns:
            dict: Validation result with success status and details
        """
        try:
            schema = cls.get_schema_by_name(schema_name)
            validated_df = schema.validate(df, lazy=True)
            
            return {
                "success": True,
                "message": f"DataFrame validation successful for {schema_name}",
                "records_validated": len(df),
                "schema_name": schema_name,
                "errors": []
            }
            
        except pa.errors.SchemaErrors as e:
            error_details = []
            for error in e.schema_errors:
                error_details.append({
                    "column": getattr(error, 'column', 'unknown'),
                    "check": getattr(error, 'check', 'unknown'),
                    "error_message": str(error)
                })
            
            result = {
                "success": False,
                "message": f"DataFrame validation failed for {schema_name}",
                "records_validated": len(df),
                "schema_name": schema_name,
                "errors": error_details,
                "total_errors": len(error_details)
            }
            
            if raise_on_error:
                raise ValueError(f"Schema validation failed: {result}")
            
            return result
            
        except Exception as e:
            result = {
                "success": False,
                "message": f"Unexpected error during validation: {str(e)}",
                "records_validated": len(df),
                "schema_name": schema_name,
                "errors": [{"error_message": str(e)}]
            }
            
            if raise_on_error:
                raise
                
            return result