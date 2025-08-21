"""
MercadoPago Pipeline - Capa de acceso a los datos
clase abstracta DataExtractor con validacion de tipos, schema enforcement, 
manejo de errores y manejo de memoria a traves de carga por chunks
"""

#================================================================================

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, Dict, Any, Optional, Type, Generic, TypeVar

import pandas as pd
from pydantic import BaseModel, ValidationError

from src.carrusel_mp.config import get_config
from src.data.loaders import jsonl_chunks, csv_chunks
from src.models.schemas import (
    PrintRecord,
    TapRecord,
    PaymentRecord,
    BatchProcessingResult,
)
from src.models.contracts import validate_dataframe
from mp_carousel.utils.time_utils import parse_day_date, ensure_datetime_utc

T = TypeVar("T", bound=BaseModel)

#================================================================================

class DataExtractionError(Exception):
    """excepciones para errores de extracción"""
    pass

#================================================================================

class MemoryManager:
    """Utilidades para el manejo de memoria"""
    
    @staticmethod
    def get_memory_usage_mb() -> float:
        """toma el uso de memoria en MB"""
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    
    #--------------------------------------------------------------------------------
    
    @staticmethod
    def get_available_memory_mb() -> float:
        """toma la memoria disponible en MB"""
        return psutil.virtual_memory().available / 1024 / 1024
    
    #--------------------------------------------------------------------------------
    
    @staticmethod
    def estimate_chunk_size(file_size_mb: float, max_memory_mb: float = 1024) -> int:
        """Estima el tamaño optimo del chunk basado en tamaño de archivo y restricciones
        de memoria"""
        # estimado conservador: usa 25% de la máxima memoria para chunking
        available_for_chunk = max_memory_mb * 0.25
        
        # estimado de filas por mb (en papel)
        estimated_rows_per_mb = 1000  # Ajustar basado en data real
        
        # Ccalcular chunk 
        chunk_size = int(available_for_chunk * estimated_rows_per_mb)
        
        # Aplicar parámetros
        min_chunk = 1000
        max_chunk = 50000
        
        return max(min_chunk, min(chunk_size, max_chunk))

#================================================================================

class DataExtractor(ABC):
    """
    Clase abstracta para extraccion de datos con:
    - Validacion de Tipos de dato con Pydantic
    - Revision de schema con Pandera
    - Manejo de errores y mecanismos de retry
    - Manejo de memoria por chunking
    - track de la linea de datos
    """
    
    def __init__(self, 
                 file_path: Union[str, Path],
                 chunk_size: Optional[int] = None,
                 max_memory_mb: float = 1024,
                 encoding: str = 'utf-8',
                 validate_schema: bool = True):
        """
        Inicializar extraccion
        
        Args:
            file_path: path a los datos
            chunk_size: tamaño del chunk
            max_memory_mb: uso mamixo de memoria en MB
            encoding: File encoding
            validate_schema:revisar si se usa pandera
        """
        self.file_path = Path(file_path)
        self.encoding = encoding
        self.validate_schema = validate_schema
        self.max_memory_mb = max_memory_mb
        
        # logging
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        
        # validar si archivo existe
        if not self.file_path.exists():
            raise FileNotFoundError(f"Archivo de datos no encontrado: {self.file_path}")
        
        # calcular tamaño del chunk si no fue dado
        if chunk_size is None:
            file_size_mb = self.file_path.stat().st_size / 1024 / 1024
            self.chunk_size = MemoryManager.estimate_chunk_size(file_size_mb, max_memory_mb)
            self.logger.info(f"Auto calculado el chunk: {self.chunk_size} (tamaño archivo: {file_size_mb:.2f} MB)")
        else:
            self.chunk_size = chunk_size
        
        # Preprocesamiento y metadata
        self.processing_metadata = {
            'start_time': None,
            'end_time': None,
            'total_records': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'errors': [],
            'file_path': str(self.file_path),
            'extractor_class': self.__class__.__name__
        }
        
        self.logger.info(f"Initialized {self.__class__.__name__} for file: {self.file_path}")

    #--------------------------------------------------------------------------------
    
    @abstractmethod
    def get_pydantic_model(self):
        """retorna el modelo de pydantic"""
        pass

    #--------------------------------------------------------------------------------
    
    @abstractmethod
    def get_schema_name(self) -> str:
        """Retorna el modelo de panderas"""
        pass

    #--------------------------------------------------------------------------------
    
    @abstractmethod
    def _read_raw_chunk(self, chunk_start: int, chunk_size: int) -> List[Dict[str, Any]]:
        """lee el chunk crudo del archivo de datos"""
        pass

    #--------------------------------------------------------------------------------
    
    def _validate_record(self, record_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Valida un registro usando pydantic
        
        Returns:
            registro validado en dict o None
        """
        try:
            pydantic_model = self.get_pydantic_model()
            validated_record = pydantic_model(**record_data)
            return validated_record.dict()
        except Exception as e:
            self.processing_metadata['errors'].append({
                'record': record_data,
                'error': str(e),
                'error_type': type(e).__name__
            })
            return None
        
    #--------------------------------------------------------------------------------
    
    def _validate_dataframe_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Valida DataFrame en esquema pandera
        
        Returns:
            DataFrame validado
        """
        if not self.validate_schema:
            return df
        
        try:
            schema_name = self.get_schema_name()
            validation_result = DataFrameContracts.validate_dataframe(
                df, schema_name, raise_on_error=True
            )
            self.logger.info(f"validación de esquema exitosa: {validation_result['message']}")
            return df
        except Exception as e:
            self.logger.error(f"Esquema de validación fallido: {str(e)}")
            raise DataExtractionError(f"Schema validation failed: {str(e)}")
        
    #--------------------------------------------------------------------------------
    
    def _process_chunk(self, raw_records: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Procesa un chunk de registros crudos en DataFrame validado
        
        Args:
            raw_records: Lista de diccionarios crudos
            
        Returns:
            DataFrame validado
        """
        if not raw_records:
            return pd.DataFrame()
        
        validated_records = []
        invalid_count = 0
        
        # Validate each record
        for record in raw_records:
            validated_record = self._validate_record(record)
            if validated_record:
                validated_records.append(validated_record)
            else:
                invalid_count += 1
        
        # Update processing metadata
        self.processing_metadata['total_records'] += len(raw_records)
        self.processing_metadata['valid_records'] += len(validated_records)
        self.processing_metadata['invalid_records'] += invalid_count
        
        if not validated_records:
            self.logger.warning("No valid records found in chunk")
            return pd.DataFrame()
        
        # Create DataFrame
        df = pd.DataFrame(validated_records)
        
        # Convert timestamp columns to proper datetime
        timestamp_cols = [col for col in df.columns if 'timestamp' in col.lower()]
        for col in timestamp_cols:
            df[col] = pd.to_datetime(df[col], utc=True)
        
        # Validate against Pandera schema
        df = self._validate_dataframe_schema(df)
        
        # Add processing metadata
        df = self._add_processing_metadata(df)
        
        return df
    
    #--------------------------------------------------------------------------------
    
    def _add_processing_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add processing metadata to DataFrame"""
        if df.empty:
            return df
        
        df = df.copy()
        df['processed_at'] = datetime.utcnow()
        df['source_file'] = self.file_path.name
        df['row_number'] = range(len(df))
        df['duplicate_flag'] = df.duplicated()
        df['data_quality_score'] = 1.0  # Base quality score, can be enhanced
        
        return df
    
    #--------------------------------------------------------------------------------
    
    @contextmanager
    def _memory_monitor(self):
        """Context manager for memory monitoring"""
        initial_memory = MemoryManager.get_memory_usage_mb()
        self.logger.debug(f"Initial memory usage: {initial_memory:.2f} MB")
        
        try:
            yield
        finally:
            final_memory = MemoryManager.get_memory_usage_mb()
            memory_diff = final_memory - initial_memory
            self.logger.debug(f"Final memory usage: {final_memory:.2f} MB "
                            f"(diff: {memory_diff:+.2f} MB)")
            
            if memory_diff > self.max_memory_mb * 0.8:
                self.logger.warning(f"High memory usage detected: {memory_diff:.2f} MB")

    #--------------------------------------------------------------------------------
    
    def extract_chunked(self) -> Iterator[pd.DataFrame]:
        """
        Extract data in chunks as an iterator
        
        Yields:
            pandas DataFrames containing validated data chunks
        """
        self.processing_metadata['start_time'] = datetime.utcnow()
        self.logger.info(f"Starting chunked extraction from {self.file_path}")
        
        try:
            chunk_start = 0
            chunk_number = 0
            
            while True:
                with self._memory_monitor():
                    # Read raw chunk
                    raw_records = self._read_raw_chunk(chunk_start, self.chunk_size)
                    
                    if not raw_records:
                        self.logger.info("No more records to process")
                        break
                    
                    chunk_number += 1
                    self.logger.info(f"Processing chunk {chunk_number}, "
                                   f"records {chunk_start}-{chunk_start + len(raw_records)}")
                    
                    # Process chunk
                    df_chunk = self._process_chunk(raw_records)
                    
                    if not df_chunk.empty:
                        yield df_chunk
                    
                    # Update position
                    chunk_start += len(raw_records)
                    
                    # Break if we got fewer records than requested (end of file)
                    if len(raw_records) < self.chunk_size:
                        break
        
        except Exception as e:
            self.logger.error(f"Error during chunked extraction: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            self.processing_metadata['errors'].append({
                'error': str(e),
                'error_type': type(e).__name__,
                'traceback': traceback.format_exc()
            })
            raise DataExtractionError(f"Extraction failed: {str(e)}")
        
        finally:
            self.processing_metadata['end_time'] = datetime.utcnow()
            self._log_processing_summary()

    #--------------------------------------------------------------------------------
    
    def extract_all(self) -> pd.DataFrame:
        """
        Extract all data at once (use with caution for large files)
        
        Returns:
            Complete pandas DataFrame with all validated data
        """
        self.logger.info("Starting full extraction (not recommended for large files)")
        
        all_chunks = []
        for chunk_df in self.extract_chunked():
            all_chunks.append(chunk_df)
        
        if not all_chunks:
            self.logger.warning("No valid data extracted")
            return pd.DataFrame()
        
        # Combine all chunks
        combined_df = pd.concat(all_chunks, ignore_index=True)
        
        self.logger.info(f"Full extraction complete. Total records: {len(combined_df)}")
        return combined_df
    
    def _log_processing_summary(self):
        """Log processing summary"""
        metadata = self.processing_metadata
        
        if metadata['start_time'] and metadata['end_time']:
            duration = (metadata['end_time'] - metadata['start_time']).total_seconds()
        else:
            duration = 0
        
        self.logger.info("=" * 60)
        self.logger.info(f"EXTRACTION SUMMARY for {self.__class__.__name__}")
        self.logger.info("=" * 60)
        self.logger.info(f"File: {metadata['file_path']}")
        self.logger.info(f"Total records processed: {metadata['total_records']}")
        self.logger.info(f"Valid records: {metadata['valid_records']}")
        self.logger.info(f"Invalid records: {metadata['invalid_records']}")
        self.logger.info(f"Processing duration: {duration:.2f} seconds")
        
        if metadata['total_records'] > 0:
            validity_rate = metadata['valid_records'] / metadata['total_records'] * 100
            self.logger.info(f"Data validity rate: {validity_rate:.2f}%")
        
        if metadata['errors']:
            self.logger.warning(f"Total errors encountered: {len(metadata['errors'])}")
            
        self.logger.info("=" * 60)

    #--------------------------------------------------------------------------------
    
    def get_processing_result(self) -> BatchProcessingResult:
        """Get processing result as Pydantic model"""
        metadata = self.processing_metadata
        
        if metadata['start_time'] and metadata['end_time']:
            duration = (metadata['end_time'] - metadata['start_time']).total_seconds()
        else:
            duration = 0
        
        # Calculate data quality score
        if metadata['total_records'] > 0:
            data_quality_score = metadata['valid_records'] / metadata['total_records']
        else:
            data_quality_score = 0.0
        
        return BatchProcessingResult(
            records_processed=metadata['total_records'],
            records_valid=metadata['valid_records'],
            records_invalid=metadata['invalid_records'],
            processing_time_seconds=duration,
            data_quality_score=data_quality_score,
            errors=[str(error) for error in metadata['errors'][:10]]  # Limit to first 10 errors
        )

#================================================================================

class PrintsExtractor(DataExtractor):
    """Extractor for prints data (JSON Lines format)"""
    
    def get_pydantic_model(self):
        return PrintRecord
    
    #--------------------------------------------------------------------------------
    
    def get_schema_name(self) -> str:
        return "prints"
    
    #--------------------------------------------------------------------------------
    
    def _read_raw_chunk(self, chunk_start: int, chunk_size: int) -> List[Dict[str, Any]]:
        """Read chunk from JSON Lines file"""
        records = []
        current_line = 0
        
        try:
            with open(self.file_path, 'r', encoding=self.encoding) as file:
                for line in file:
                    if current_line < chunk_start:
                        current_line += 1
                        continue
                    
                    if len(records) >= chunk_size:
                        break
                    
                    try:
                        record = json.loads(line.strip())
                        records.append(record)
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Invalid JSON at line {current_line + 1}: {e}")
                        self.processing_metadata['errors'].append({
                            'line_number': current_line + 1,
                            'error': f"JSON decode error: {str(e)}",
                            'raw_line': line.strip()[:100]  # First 100 chars for debugging
                        })
                    
                    current_line += 1
        
        except Exception as e:
            raise DataExtractionError(f"Error reading prints file: {str(e)}")
        
        return records

#================================================================================

class TapsExtractor(DataExtractor):
    """Extractor for taps data (JSON Lines format)"""
    
    def get_pydantic_model(self):
        return TapRecord
    
    #--------------------------------------------------------------------------------
    
    def get_schema_name(self) -> str:
        return "taps"
    
    #--------------------------------------------------------------------------------
    
    def _read_raw_chunk(self, chunk_start: int, chunk_size: int) -> List[Dict[str, Any]]:
        """Read chunk from JSON Lines file"""
        records = []
        current_line = 0
        
        try:
            with open(self.file_path, 'r', encoding=self.encoding) as file:
                for line in file:
                    if current_line < chunk_start:
                        current_line += 1
                        continue
                    
                    if len(records) >= chunk_size:
                        break
                    
                    try:
                        record = json.loads(line.strip())
                        records.append(record)
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Invalid JSON at line {current_line + 1}: {e}")
                        self.processing_metadata['errors'].append({
                            'line_number': current_line + 1,
                            'error': f"JSON decode error: {str(e)}",
                            'raw_line': line.strip()[:100]
                        })
                    
                    current_line += 1
        
        except Exception as e:
            raise DataExtractionError(f"Error reading taps file: {str(e)}")
        
        return records

#================================================================================

class PaymentsExtractor(DataExtractor):
    """Extractor for payments data (CSV format)"""
    
    def get_pydantic_model(self):
        return PaymentRecord
    
    #--------------------------------------------------------------------------------
    
    def get_schema_name(self) -> str:
        return "payments"
    
    #--------------------------------------------------------------------------------
    
    def _read_raw_chunk(self, chunk_start: int, chunk_size: int) -> List[Dict[str, Any]]:
        """Read chunk from CSV file"""
        try:
            # Read CSV chunk using pandas
            df_chunk = pd.read_csv(
                self.file_path,
                encoding=self.encoding,
                skiprows=chunk_start if chunk_start > 0 else 0,
                nrows=chunk_size,
                dtype=str  # Read as string first, then validate
            )
            
            if df_chunk.empty:
                return []
            
            # Convert to list of dictionaries
            records = df_chunk.to_dict('records')
            
            # Convert numeric fields
            for record in records:
                if 'amount' in record and record['amount'] is not None:
                    try:
                        record['amount'] = float(record['amount'])
                    except (ValueError, TypeError):
                        self.logger.warning(f"Invalid amount value: {record['amount']}")
                        record['amount'] = None
            
            return records
        
        except Exception as e:
            raise DataExtractionError(f"Error reading payments file: {str(e)}")


#================================================================================

# Factory function for creating extractors
def create_extractor(file_path: Union[str, Path], 
                    data_type: str,
                    **kwargs) -> DataExtractor:
    """
    Factory function to create appropriate extractor based on data type
    
    Args:
        file_path: Path to the data file
        data_type: Type of data ('prints', 'taps', 'payments')
        **kwargs: Additional arguments for extractor initialization
        
    Returns:
        Appropriate DataExtractor instance
    """
    extractors = {
        'prints': PrintsExtractor,
        'taps': TapsExtractor,
        'payments': PaymentsExtractor
    }
    
    if data_type not in extractors:
        raise ValueError(f"Unknown data type: {data_type}. "
                        f"Available types: {list(extractors.keys())}")
    
    return extractors[data_type](file_path, **kwargs)

#================================================================================