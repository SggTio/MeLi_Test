#================================================================================
from __future__ import annotations

import os
import yaml
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from datetime import datetime, timedelta, timezone

#================================================================================

#--------------------------------------------------------------------------------

@dataclass
class DataPaths:
    raw: str = "data/raw"
    processed: str = "data/processed"
    output: str = "data/output"

    def ensure_dirs(self) -> None:
        for p in [self.raw, self.processed, self.output]:
            Path(p).mkdir(parents=True, exist_ok=True)

#--------------------------------------------------------------------------------

@dataclass
class ProcessingConfig:
    chunk_size: int = 10000
    max_memory_mb: int = 1024
    target_window_days: int = 7
    lookback_window_days: int = 21
    encoding: str = "utf-8"
    validate_schema: bool = True
    parallel_processing: bool = False
    n_workers: int = 4

#--------------------------------------------------------------------------------

@dataclass
class QualityConfig:
    min_data_quality_score: float = 0.8
    max_null_percentage: float = 0.1
    max_duplicate_percentage: float = 0.1
    enable_quality_checks: bool = True
    quality_report_path: str = "data/quality_reports"

#--------------------------------------------------------------------------------

@dataclass
class LoggingCfg:
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_dir: str = "logs"
    log_file: str = "pipeline.log"
    max_file_size_mb: int = 10
    backup_count: int = 5
    enable_console: bool = True

#================================================================================

class Config:
    def __init__(self, config_path: Optional[str] = None) -> None:
        self.base_dir = Path(__file__).resolve().parents[2]
        self.config_path = config_path or str(self.base_dir / "config" / "params.yaml")
        self._raw: Dict[str, Any] = self._load_yaml(self.config_path)

        self.data_paths = DataPaths(**self._raw.get("data_paths", {}))
        self.processing = ProcessingConfig(**self._raw.get("processing", {}))
        self.quality = QualityConfig(**self._raw.get("quality", {}))
        self.logging = LoggingCfg(**self._raw.get("logging", {}))

        self._apply_env_overrides()
        self.data_paths.ensure_dirs()

    #--------------------------------------------------------------------------------

    def _load_yaml(self, fp: str) -> Dict[str, Any]:
        if Path(fp).exists():
            with open(fp, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        return {}
    
    #--------------------------------------------------------------------------------

    def _apply_env_overrides(self) -> None:
        env = os.environ

        # Paths
        self.data_paths.raw = env.get("MP_RAW_DATA_PATH", self.data_paths.raw)
        self.data_paths.processed = env.get("MP_PROCESSED_DATA_PATH", self.data_paths.processed)
        self.data_paths.output = env.get("MP_OUTPUT_DATA_PATH", self.data_paths.output)

        # Processing
        self.processing.chunk_size = int(env.get("MP_CHUNK_SIZE", self.processing.chunk_size))
        self.processing.max_memory_mb = int(env.get("MP_MAX_MEMORY_MB", self.processing.max_memory_mb))
        self.processing.target_window_days = int(env.get("MP_TARGET_WINDOW_DAYS", self.processing.target_window_days))
        self.processing.lookback_window_days = int(env.get("MP_LOOKBACK_WINDOW_DAYS", self.processing.lookback_window_days))
        vs = env.get("MP_VALIDATE_SCHEMA")
        if vs is not None:
            self.processing.validate_schema = vs.lower() == "true"

        # Quality
        self.quality.min_data_quality_score = float(env.get("MP_MIN_QUALITY_SCORE", self.quality.min_data_quality_score))
        self.quality.max_null_percentage = float(env.get("MP_MAX_NULL_PCT", self.quality.max_null_percentage))

        # Logging
        self.logging.level = env.get("MP_LOG_LEVEL", self.logging.level)
        self.logging.log_dir = env.get("MP_LOG_DIR", self.logging.log_dir)

    #--------------------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "data_paths": asdict(self.data_paths),
            "processing": asdict(self.processing),
            "quality": asdict(self.quality),
            "logging": asdict(self.logging),
        }
    
    #--------------------------------------------------------------------------------

    # Helpers for validators
    def get_param(self, key: str, default: Any = None) -> Any:
        """
        Access loaded YAML by dotted key, e.g. 'validation.business_rules.min_timestamp_days_ago'
        """
        cur = self._raw
        for part in key.split("."):
            if not isinstance(cur, dict):
                return default
            cur = cur.get(part, {})
        return default if cur == {} else cur
    
    #--------------------------------------------------------------------------------

    def get_validation_bounds(self) -> Tuple[datetime, datetime]:
        days = int(self.get_param("validation.business_rules.min_timestamp_days_ago", 1825))
        now = datetime.now(timezone.utc)
        min_date = now - timedelta(days=days)
        max_date = now + timedelta(hours=1)
        return (min_date, max_date)

#--------------------------------------------------------------------------------

_CONFIG: Optional[Config] = None

def get_config() -> Config:
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = Config()
    return _CONFIG

def get_params() -> Dict[str, Any]:
    return get_config().to_dict()

def get_validation_bounds() -> Tuple[datetime, datetime]:
    return get_config().get_validation_bounds()

#================================================================================
