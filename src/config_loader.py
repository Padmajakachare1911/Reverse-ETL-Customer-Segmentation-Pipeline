"""
src/config_loader.py
--------------------
Loads and validates the YAML configuration file.
Provides a singleton Config object used across the pipeline.
"""

import yaml
from pathlib import Path
from src.logger import get_logger

logger = get_logger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "config.yaml"


def load_config(config_path: Path = _CONFIG_PATH) -> dict:
    """
    Load YAML config from disk and return as a dict.

    Args:
        config_path: Path to the config.yaml file.

    Returns:
        Parsed configuration dictionary.

    Raises:
        FileNotFoundError: If config file doesn't exist.
        yaml.YAMLError: If config file is malformed.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    logger.info(f"Config loaded from: {config_path}")
    return config
