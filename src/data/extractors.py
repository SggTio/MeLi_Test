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
from src.utils.time_utils import parse_day_date, ensure_datetime_utc

T = TypeVar("T", bound=BaseModel)

#================================================================================

class DataExtractionError(Exception):
    """excepciones para errores de extracción"""
    pass


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    def _append_quarantine(rows: List[Dict[str, Any]], path: Path) -> None:
        if not rows:
            return
        _ensure_parent(path)
        pd.DataFrame(rows).to_csv(path, mode="a", index=False, header=not path.exists())
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
    
    def __init__(
        self,
        file_path: str | Path,
        model: Type[T],
        schema_name: str,
        source_name: str,
        chunksize: Optional[int] = None,
        max_reject_rate: float = 0.02,
        encoding: str = "utf-8",
    ) -> None:
        """
        Inicializar extraccion
        
        Args:
            file_path: path a los datos
            chunk_size: tamaño del chunk
            max_memory_mb: uso mamixo de memoria en MB
            encoding: File encoding
            validate_schema:revisar si se usa pandera
        """
        self.cfg = get_config()
        self.file_path = Path(file_path)
        self.model = model
        self.schema_name = schema_name
        self.source_name = source_name
        self.chunksize = chunksize or self.cfg.processing.chunk_size
        self.max_reject_rate = max_reject_rate
        self.encoding = encoding
        
        # logging
        if not self.file_path.exists():
            raise FileNotFoundError(f"data file not found: {self.file_path}")

        self.logger = logging.getLogger(f"mp_carousel.extractor.{self.source_name}")
        self.rejects_path = Path(self.cfg.data_paths.processed) / "_rejects" / f"{self.source_name}.csv"
        self.manifest_path = Path(self.cfg.data_paths.processed) / "_manifests" / f"{self.source_name}.json"

        self.metrics = {
            "rows_read": 0,
            "rows_valid": 0,
            "rows_rejected": 0,
            "chunks": 0,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }

    #--------------------------------------------------------------------------------
    
    @abstractmethod
    def _reader(self) -> Iterator[pd.DataFrame]:
        ...

    #--------------------------------------------------------------------------------
    
    @abstractmethod
    def _normalize_chunk(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        ...

    #--------------------------------------------------------------------------------
    
    def extract_chunked(self) -> Iterator[pd.DataFrame]:
        self.logger.info(f"start extracting {self.source_name} from {self.file_path}")
        for df_raw in self._reader():
            self.metrics["chunks"] += 1
            self.metrics["rows_read"] += int(len(df_raw))

            # Normalize to canonical fields
            normalized: List[Dict[str, Any]] = self._normalize_chunk(df_raw)

            ok, bad = [], []
            for rec in normalized:
                try:
                    v = self.model.model_validate(rec)
                    ok.append(v.model_dump())
                except ValidationError as e:
                    rec["_error"] = str(e)
                    bad.append(rec)

            if bad:
                _append_quarantine(bad, self.rejects_path)

            df_ok = pd.DataFrame(ok)
            if not df_ok.empty:
                # Pandera validation on RAW canonical schema
                validate_dataframe(df_ok, self.schema_name)

            self.metrics["rows_valid"] += int(len(df_ok))
            self.metrics["rows_rejected"] += int(len(bad))

            yield df_ok

        # end-of-stream checks
        self.metrics["ended_at"] = datetime.now(timezone.utc).isoformat()
        total = max(1, self.metrics["rows_read"])
        reject_rate = self.metrics["rows_rejected"] / total

        _ensure_parent(self.manifest_path)
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            json.dump(self.metrics, f, indent=2)

        if reject_rate > self.max_reject_rate:
            raise DataExtractionError(
                f"[{self.source_name}] reject_rate={reject_rate:.2%} > {self.max_reject_rate:.2%} "
                f"(read={self.metrics['rows_read']}, rejected={self.metrics['rows_rejected']})"
            )


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
        chunks = list(self.extract_chunked())
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


#================================================================================

class PrintsExtractor(DataExtractor[PrintRecord]):
    """Extractor for prints data (JSON Lines format)"""
    def __init__(self, file_path: str | Path, **kw: Any) -> None:
        super().__init__(file_path, PrintRecord, "prints_raw", "prints", **kw)
    
    #--------------------------------------------------------------------------------
    
    def _reader(self) -> Iterator[pd.DataFrame]:
        yield from jsonl_chunks(self.file_path, self.chunksize)
    
    #--------------------------------------------------------------------------------
    
    def _normalize_chunk(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        # raw: {"day":"YYYY-MM-DD", "event_data":{"position":int,"value_prop":str},"user_id":int}
        out: List[Dict[str, Any]] = []
        if df.empty:
            return out
        # guard nested columns
        ev = df["event_data"].apply(lambda x: x if isinstance(x, dict) else {})
        pos = ev.apply(lambda d: d.get("position"))
        vp  = ev.apply(lambda d: d.get("value_prop"))
        ts  = df["day"].apply(lambda s: parse_day_date(str(s)))

        for user, vpid, position, ts_ in zip(df["user_id"], vp, pos, ts):
            out.append({
                "timestamp": ts_.to_pydatetime() if pd.notna(ts_) else None,
                "user_id": str(user),
                "value_prop_id": None if vpid is None else str(vpid),
                "position": None if pd.isna(position) else int(position),
            })
        return out

#================================================================================

class TapsExtractor(DataExtractor[TapRecord]):
    """Extractor for taps data (JSON Lines format)"""
    
    def __init__(self, file_path: str | Path, **kw: Any) -> None:
        super().__init__(file_path, TapRecord, "taps_raw", "taps", **kw)
    
    #--------------------------------------------------------------------------------
    
    def _reader(self) -> Iterator[pd.DataFrame]:
        yield from jsonl_chunks(self.file_path, self.chunksize)
    #--------------------------------------------------------------------------------
    
  def _normalize_chunk(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        if df.empty:
            return out
        ev = df["event_data"].apply(lambda x: x if isinstance(x, dict) else {})
        pos = ev.apply(lambda d: d.get("position"))
        vp  = ev.apply(lambda d: d.get("value_prop"))
        ts  = df["day"].apply(lambda s: parse_day_date(str(s)))

        for user, vpid, position, ts_ in zip(df["user_id"], vp, pos, ts):
            out.append({
                "timestamp": ts_.to_pydatetime() if pd.notna(ts_) else None,
                "user_id": str(user),
                "value_prop_id": None if vpid is None else str(vpid),
                "position": None if pd.isna(position) else int(position),
            })
        return out

#================================================================================

class PaymentsExtractor(DataExtractor[PaymentRecord]):
    def __init__(self, file_path: str | Path, **kw: Any) -> None:
        super().__init__(file_path, PaymentRecord, "payments_raw", "payments", **kw)
    
    #--------------------------------------------------------------------------------
    
    def _reader(self) -> Iterator[pd.DataFrame]:
        yield from csv_chunks(self.file_path, self.chunksize, encoding=self.encoding)
    
    #--------------------------------------------------------------------------------
    
    def _normalize_chunk(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        # raw CSV cols: pay_date,total,user_id,value_prop
        out: List[Dict[str, Any]] = []
        if df.empty:
            return out
        # ensure columns exist
        for _, row in df.iterrows():
            pay_date = row.get("pay_date")
            total = row.get("total")
            user_id = row.get("user_id")
            vp = row.get("value_prop")

            ts = parse_day_date(str(pay_date))  # 'YYYY-MM-DD' -> UTC midnight
            amt = None
            try:
                amt = float(total) if total is not None and str(total) != "" else None
            except Exception:
                amt = None

            out.append({
                "timestamp": ts.to_pydatetime() if pd.notna(ts) else None,
                "user_id": str(user_id),
                "value_prop_id": None if vp is None else str(vp),
                "amount": amt,
            })
        return out

#================================================================================