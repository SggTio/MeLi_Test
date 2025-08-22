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

# ================================================================================
# Helpers
# ================================================================================

def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def _append_quarantine(rows: List[Dict[str, Any]], path: Path) -> None:
    """
    Acumula filas rechazadas en un CSV (append). Crea encabezado si el archivo no existe.
    """
    if not rows:
        return
    _ensure_parent(path)
    pd.DataFrame(rows).to_csv(path, mode="a", index=False, header=not path.exists())

#================================================================================

class DataExtractor(Generic[T], ABC):
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

        self.metrics: Dict[str, Any] = {
            "rows_read": 0,
            "rows_valid": 0,
            "rows_rejected": 0,
            "chunks": 0,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "source": self.source_name,
            "file": str(self.file_path),
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
        """
        Itera por chunks:
          - normaliza,
          - valida (Pydantic row-level, Pandera DF-level),
          - encola rechazos,
          - yieldea DF válido (canónico).
        """
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
                # Pandera: valida contra el esquema RAW canónico (strict=False en contratos)
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
    
    def extract_all(self) -> pd.DataFrame:
        """Conveniencia: concatena todos los chunks válidos."""
        chunks = list(self.extract_chunked())
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


#================================================================================

class PrintsExtractor(DataExtractor[PrintRecord]):
    """Extractor para prints (JSON Lines)"""
    def __init__(self, file_path: str | Path, **kw: Any) -> None:
        super().__init__(file_path, PrintRecord, "prints_raw", "prints", **kw)
    
    #--------------------------------------------------------------------------------
    
    def _reader(self) -> Iterator[pd.DataFrame]:
        yield from jsonl_chunks(self.file_path, self.chunksize)
    
    #--------------------------------------------------------------------------------
    
    def _normalize_chunk(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        raw JSONL:
          {"day":"YYYY-MM-DD","event_data":{"position":int,"value_prop":str},"user_id":int}
        canónico:
          timestamp(UTC), user_id(str), value_prop_id(str), position(int|None)
        """
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
    """Extractor para taps (JSON Lines)"""
    
    def __init__(self, file_path: str | Path, **kw: Any) -> None:
        super().__init__(file_path, TapRecord, "taps_raw", "taps", **kw)
    
    #--------------------------------------------------------------------------------
    
    def _reader(self) -> Iterator[pd.DataFrame]:
        yield from jsonl_chunks(self.file_path, self.chunksize)
    #--------------------------------------------------------------------------------
    
    def _normalize_chunk(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        raw JSONL:
          {"day":"YYYY-MM-DD","event_data":{"position":int,"value_prop":str},"user_id":int}
        """
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
    """Extractor para payments (CSV)"""
    def __init__(self, file_path: str | Path, **kw: Any) -> None:
        super().__init__(file_path, PaymentRecord, "payments_raw", "payments", **kw)
    
    #--------------------------------------------------------------------------------
    
    def _reader(self) -> Iterator[pd.DataFrame]:
        yield from csv_chunks(self.file_path, self.chunksize, encoding=self.encoding)
    
    #--------------------------------------------------------------------------------
    
    def _normalize_chunk(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        raw CSV:
          pay_date,total,user_id,value_prop
        canónico:
          timestamp(UTC), user_id(str), value_prop_id(str), amount(float>0)
        """
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