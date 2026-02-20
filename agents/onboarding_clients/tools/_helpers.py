"""
Utilidades compartidas para todos los tools del proyecto.
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# Paths del proyecto
PROJECT_ROOT = Path(__file__).parent.parent
TOOLS_DIR = PROJECT_ROOT / "tools"
WORKFLOWS_DIR = PROJECT_ROOT / "workflows"
TMP_DIR = PROJECT_ROOT / ".tmp"


def setup_env():
    """Carga las variables de entorno desde .env"""
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        # In container/CI environments (Coolify), env vars are provided by the platform.
        # Only warn if it looks like we're missing configuration entirely.
        expected = [
            "HUBSPOT_ACCESS_TOKEN",
            "SUPABASE_URL",
            "SUPABASE_SERVICE_ROLE_KEY",
            "GMAIL_ADDRESS",
            "GMAIL_APP_PASSWORD",
        ]
        if any(os.getenv(k) for k in expected):
            return
        logging.warning(
            "No se encontró .env — copiá .env.template como .env y completá los valores."
        )


def setup_logging(name: str, level=logging.INFO) -> logging.Logger:
    """Configura logging estándar para un tool."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def tmp_path(filename: str) -> Path:
    """Retorna un path dentro de .tmp/, creando el directorio si no existe."""
    TMP_DIR.mkdir(exist_ok=True)
    return TMP_DIR / filename
