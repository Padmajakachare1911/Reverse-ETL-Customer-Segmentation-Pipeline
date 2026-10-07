"""
src/logger.py
-------------
Centralized logging configuration for the pipeline.
All modules import `get_logger()` from here.
"""

import logging
import sys
from pathlib import Path


def get_logger(name: str, log_file: str = "logs/pipeline.log", level: str = "INFO") -> logging.Logger:
    """
    Returns a configured logger with both console and file handlers.

    Args:
        name: Module name (use __name__).
        log_file: Path to log file.
        level: Logging level string.

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # already configured

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler
    try:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except Exception as e:
        logger.warning(f"Could not create log file handler: {e}")

    return logger
