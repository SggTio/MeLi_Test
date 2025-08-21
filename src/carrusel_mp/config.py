import os
from pathlib import Path
from typing import Dict, Any
import yaml
from dotenv import load_dotenv

# cargar variables de ambiente
load_dotenv()

class Config:
    """manejo de configuracion del pipeline."""
    
    def __init__(self):
        self.BASE_DIR = Path(__file__).resolve().parent.parent.parent
        self.CONFIG_DIR = self.BASE_DIR / "config"
        
        # Carga de parametros
        self.params = self._load_yaml(self.CONFIG_DIR / "params.yaml")
        
        # Definir los paths
        self.RAW_DATA_DIR = self.BASE_DIR / "data" / "raw"
        self.PROCESSED_DATA_DIR = self.BASE_DIR / "data" / "processed"
        self.OUTPUT_DIR = self.BASE_DIR / "data" / "output"
        
        # configuraciones de seguridad
        self.PII_SALT = os.getenv("PII_SALT", "default_salt_change_in_production")
        
    def _load_yaml(self, file_path: Path) -> Dict[str, Any]:
        """cargar el YAML  de configuracion."""
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    
    def get_param(self, key: str, default: Any = None) -> Any:
        """obtener un parametro desde la configuracion."""
        keys = key.split('.')
        value = self.params
        for k in keys:
            value = value.get(k, {})
        return value if value != {} else default

# instancia global de configuracion
config = Config()