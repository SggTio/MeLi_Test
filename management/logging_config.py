"""
MercadoPago Pipeline - Configuracion y Logging
manejo de ambiente y de configuraciones con logging estructurado
"""
#================================================================================

from __future__ import annotations

import logging
import logging.config
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone

from src.carrusel_mp.config import get_config

def setup_logging() -> None:
    cfg = get_config().logging
    log_dir = Path(cfg.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": "%(asctime)s | %(name)s | %(levelname)s | %(funcName)s:%(lineno)d | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "simple": {"format": "%(levelname)s | %(name)s | %(message)s"},
        },
        "handlers": {
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": cfg.level,
                "formatter": "detailed",
                "filename": str(log_dir / cfg.log_file),
                "maxBytes": cfg.max_file_size_mb * 1024 * 1024,
                "backupCount": cfg.backup_count,
                "encoding": "utf-8",
            }
        },
        "loggers": {
            "mp_carousel": {"level": cfg.level, "handlers": ["file"], "propagate": False},
            "root": {"level": cfg.level, "handlers": ["file"]},
        },
    }

    if cfg.enable_console:
        logging_config["handlers"]["console"] = {
            "class": "logging.StreamHandler",
            "level": cfg.level,
            "formatter": "simple",
            "stream": "ext://sys.stdout",
        }
        logging_config["loggers"]["mp_carousel"]["handlers"].append("console")
        logging_config["loggers"]["root"]["handlers"].append("console")

    logging.config.dictConfig(logging_config)
    logger = logging.getLogger("mp_carousel.bootstrap")
    logger.info("Logging initialized")

class ProgressTracker:
    def __init__(self, name: str, total_steps: Optional[int] = None) -> None:
        self.name = name
        self.total_steps = total_steps
        self.current_step = 0
        self.start_time = datetime.now(timezone.utc)   # ← fixed
        self.logger = logging.getLogger(f"mp_carousel.progress.{name}")
        self.logger.info(f"🚀 start: {self.name}")
        if total_steps:
            self.logger.info(f"📊 total steps: {total_steps}")

    def update(self, step_name: str, details: Optional[str] = None) -> None:
        self.current_step += 1
        pct = f"({(self.current_step / self.total_steps) * 100:.1f}%)" if self.total_steps else ""
        msg = f"📈 [{self.current_step}{'/' + str(self.total_steps) if self.total_steps else ''}] {pct} {step_name}"
        if details:
            msg += f" - {details}"
        self.logger.info(msg)

    def finish(self, final_message: Optional[str] = None) -> None:
        duration = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        msg = f"✅ done: {self.name}"
        if final_message:
            msg += f" - {final_message}"
        msg += f" (duration: {duration:.2f}s)"
        self.logger.info(msg)

    def error(self, error_message: str) -> None:
        duration = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        self.logger.error(f"❌ failed: {self.name} - {error_message} (duration: {duration:.2f}s)")
