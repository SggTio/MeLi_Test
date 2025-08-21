"""
MercadoPago Pipeline - Configuracion y Logging
manejo de ambiente y de configuraciones con logging estructurado
"""
#================================================================================
import os
import yaml
import logging
import logging.config
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime

#================================================================================

@dataclass
class DataPaths:

    #--------------------------------------------------------------------------------

    """Configuracion de los paths de los datos"""
    raw: str = "data/raw"
    processed: str = "data/processed"
    output: str = "data/output"

    #--------------------------------------------------------------------------------
    
    def __post_init__(self):
        """Crea los directorios si no existen"""
        for path in [self.raw, self.processed, self.output]:
            Path(path).mkdir(parents=True, exist_ok=True)
    
#================================================================================

@dataclass
class ProcessingConfig:
    """configuracion de procesamiento"""
    chunk_size: int = 10000
    max_memory_mb: int = 1024
    target_window_days: int = 7
    lookback_window_days: int = 21
    encoding: str = 'utf-8'
    validate_schema: bool = True
    parallel_processing: bool = False
    n_workers: int = 4

#================================================================================

@dataclass
class QualityConfig:
    """configuracion de la data de alidad"""
    min_data_quality_score: float = 0.8
    max_null_percentage: float = 0.1
    max_duplicate_percentage: float = 0.1
    enable_quality_checks: bool = True
    quality_report_path: str = "data/quality_reports"

#================================================================================

@dataclass
class LoggingConfig:
    """configuracion de los logs"""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_dir: str = "logs"
    log_file: str = "pipeline.log"
    max_file_size_mb: int = 10
    backup_count: int = 5
    enable_console: bool = True

#================================================================================

class Config:
    """Clase principal de cinfiguracion con el soporte de las variables de entorno"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Iniciar configuracion
        
        Args:
            config_path: path al YAML
        """
        self.config_path = config_path or "config/params.yaml"
        self.config_data = self._load_config()
        
        # inicializar las secciones de configuracion
        self.data_paths = DataPaths(**self.config_data.get('data_paths', {}))
        self.processing = ProcessingConfig(**self.config_data.get('processing', {}))
        self.quality = QualityConfig(**self.config_data.get('quality', {}))
        self.logging_config = LoggingConfig(**self.config_data.get('logging', {}))
        
        # sobreescribir con variables de configuracion
        self._apply_env_overrides()
        
        # setup del logging
        self._setup_logging()

    #--------------------------------------------------------------------------------
    
    def _load_config(self) -> Dict[str, Any]:
        """cargar configuraciones del YAML"""
        try:
            if Path(self.config_path).exists():
                with open(self.config_path, 'r') as f:
                    config = yaml.safe_load(f) or {}
                print(f"✓ Configuracion cargada de {self.config_path}")
                return config
            else:
                print(f"⚠ archivo de Configuracion no encontrado en {self.config_path}, usando defaults")
                return {}
        except Exception as e:
            print(f"X Error cargando configuracion: {e}")
            return {}
        
    #--------------------------------------------------------------------------------
    
    def _apply_env_overrides(self):
        """aplicar las variables de configuracion"""
        env_mappings = {
            # paths de los datos
            'MP_RAW_DATA_PATH': ('data_paths', 'raw'),
            'MP_PROCESSED_DATA_PATH': ('data_paths', 'processed'),
            'MP_OUTPUT_DATA_PATH': ('data_paths', 'output'),
            
            # procesamiento
            'MP_CHUNK_SIZE': ('processing', 'chunk_size', int),
            'MP_MAX_MEMORY_MB': ('processing', 'max_memory_mb', int),
            'MP_TARGET_WINDOW_DAYS': ('processing', 'target_window_days', int),
            'MP_LOOKBACK_WINDOW_DAYS': ('processing', 'lookback_window_days', int),
            'MP_VALIDATE_SCHEMA': ('processing', 'validate_schema', lambda x: x.lower() == 'true'),
            
            # calidad
            'MP_MIN_QUALITY_SCORE': ('quality', 'min_data_quality_score', float),
            'MP_MAX_NULL_PCT': ('quality', 'max_null_percentage', float),
            
            # Logging
            'MP_LOG_LEVEL': ('logging', 'level'),
            'MP_LOG_DIR': ('logging', 'log_dir'),
        }
        
        overrides_applied = 0
        
        for env_var, config_path in env_mappings.items():
            if env_var in os.environ:
                value = os.environ[env_var]
                
                # aplicar conversion de tipo si aplica
                if len(config_path) > 2 and callable(config_path[2]):
                    try:
                        value = config_path[2](value)
                    except (ValueError, TypeError) as e:
                        print(f"⚠ valor invalido para {env_var}: {value} ({e})")
                        continue
                
                # definir el valor de config
                section = config_path[0]
                key = config_path[1]
                
                if hasattr(self, section.replace('_', '')):
                    section_obj = getattr(self, section.replace('_', ''))
                    if hasattr(section_obj, key):
                        setattr(section_obj, key, value)
                        overrides_applied += 1
                        print(f"✓ sobre escritura de ambiente aplicada: {env_var} = {value}")
        
        if overrides_applied > 0:
            print(f"✓ se aplicaron {overrides_applied} las sobre escrituras de ambiente")

    #--------------------------------------------------------------------------------
    
    def _setup_logging(self):
        """definir el logging estructurado y configurarlo"""
        # crear directorio de logs
        log_dir = Path(self.logging_config.log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # diccionario de configuracion de logs
        logging_config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'detailed': {
                    'format': '%(asctime)s | %(name)s | %(levelname)s | %(funcName)s:%(lineno)d | %(message)s',
                    'datefmt': '%Y-%m-%d %H:%M:%S'
                },
                'simple': {
                    'format': '%(levelname)s | %(name)s | %(message)s'
                },
                'json': {
                    'format': '{"timestamp": "%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": "%(message)s", "function": "%(funcName)s", "line": %(lineno)d}'
                }
            },
            'handlers': {
                'file': {
                    'class': 'logging.handlers.RotatingFileHandler',
                    'level': self.logging_config.level,
                    'formatter': 'detailed',
                    'filename': str(log_dir / self.logging_config.log_file),
                    'maxBytes': self.logging_config.max_file_size_mb * 1024 * 1024,
                    'backupCount': self.logging_config.backup_count,
                    'encoding': 'utf-8'
                }
            },
            'loggers': {
                'mp_carousel': {
                    'level': self.logging_config.level,
                    'handlers': ['file'],
                    'propagate': False
                },
                'root': {
                    'level': self.logging_config.level,
                    'handlers': ['file']
                }
            }
        }
        
        # anadir el manejo de consola si aplica
        if self.logging_config.enable_console:
            logging_config['handlers']['console'] = {
                'class': 'logging.StreamHandler',
                'level': self.logging_config.level,
                'formatter': 'simple',
                'stream': 'ext://sys.stdout'
            }
            logging_config['loggers']['mp_carousel']['handlers'].append('console')
            logging_config['loggers']['root']['handlers'].append('console')
        
        # Aplicar ocnfiguracion de logging
        logging.config.dictConfig(logging_config)
        
        # cargar archivo de inicio
        logger = logging.getLogger('mp_carousel.config')
        logger.info("=" * 80)
        logger.info("MercadoPago Pipeline - configuracion inicializada")
        logger.info("=" * 80)
        logger.info(f"Log level: {self.logging_config.level}")
        logger.info(f"Log directory: {log_dir}")
        logger.info(f"Console logging: {'Enabled' if self.logging_config.enable_console else 'Disabled'}")
        logger.info("Configuration summary:")
        logger.info(f"  - Chunk size: {self.processing.chunk_size:,}")
        logger.info(f"  - Max memory: {self.processing.max_memory_mb} MB")
        logger.info(f"  - Target window: {self.processing.target_window_days} days")
        logger.info(f"  - Lookback window: {self.processing.lookback_window_days} days")
        logger.info(f"  - Schema validation: {'Enabled' if self.processing.validate_schema else 'Disabled'}")
        logger.info(f"  - Quality checks: {'Enabled' if self.quality.enable_quality_checks else 'Disabled'}")
        logger.info("=" * 80)

    #--------------------------------------------------------------------------------
    
    def get_file_paths(self) -> Dict[str, str]:
        """cargar los directorios de los archivos de datos"""
        return {
            'prints': str(Path(self.data_paths.raw) / "prints.json"),
            'taps': str(Path(self.data_paths.raw) / "taps.json"),
            'payments': str(Path(self.data_paths.raw) / "pays.csv")
        }
    
    #--------------------------------------------------------------------------------
    
    def to_dict(self) -> Dict[str, Any]:
        """convertir configuracion a diccionario"""
        return {
            'data_paths': asdict(self.data_paths),
            'processing': asdict(self.processing),
            'quality': asdict(self.quality),
            'logging': asdict(self.logging_config)
        }
    
    #--------------------------------------------------------------------------------
    
    def save_config(self, output_path: Optional[str] = None):
        """almacenar la confgiruacion en el archivo YAML"""
        output_path = output_path or self.config_path
        
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False, indent=2)
        
        logger = logging.getLogger('mp_carousel.config')
        logger.info(f"configuracion guardada en {output_path}")

#======================================================================

class ProgressTracker:
    """Clase de tracking de progreso con integraccion en logs"""
    
    def __init__(self, name: str, total_steps: Optional[int] = None):
        """
        Inicializa el tracker de progreso
        
        Args:
            name: nombre del proceso que se supervisa
            total_steps: cantidad de pasos (opcional)
        """
        self.name = name
        self.total_steps = total_steps
        self.current_step = 0
        self.start_time = datetime.timezone.utc()
        self.logger = logging.getLogger(f'mp_carousel.progress.{name}')
        
        self.logger.info(f"🚀 inicio: {self.name}")
        if total_steps:
            self.logger.info(f"📊 cantidad de pasos: {total_steps}")

    #--------------------------------------------------------------------------------
    
    def update(self, step_name: str, details: Optional[str] = None):
        """Acutalizacion del progreso con el avance de los pasos"""
        self.current_step += 1
        
        if self.total_steps:
            percentage = (self.current_step / self.total_steps) * 100
            progress_info = f"[{self.current_step}/{self.total_steps}] ({percentage:.1f}%)"
        else:
            progress_info = f"[{self.current_step}]"
        
        message = f"📈 {progress_info} {step_name}"
        if details:
            message += f" - {details}"
        
        self.logger.info(message)

    #--------------------------------------------------------------------------------
    
    def finish(self, final_message: Optional[str] = None):
        """marcar un proceso como finalizado"""
        duration = (datetime.timezone.utc() - self.start_time).total_seconds()
        
        message = f"✅ completado: {self.name}"
        if final_message:
            message += f" - {final_message}"
        message += f" (Duracion: {duration:.2f}s)"
        
        self.logger.info(message)

    #--------------------------------------------------------------------------------
    
    def error(self, error_message: str):
        """Log dev error y marca como fallido"""
        duration = (datetime.timezone.utc() - self.start_time).total_seconds()
        self.logger.error(f"❌ Fallo: {self.name} - {error_message} (Duracion: {duration:.2f}s)")


#--------------------------------------------------------------------------------
# Global configuration instance

_config_instance = None

#--------------------------------------------------------------------------------

def get_config() -> Config:
    """Obtener configuracion global (patron singleton)"""
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance

#--------------------------------------------------------------------------------

def setup_config(config_path: Optional[str] = None) -> Config:
    """montar instanci de configuracion"""
    global _config_instance
    _config_instance = Config(config_path)
    return _config_instance