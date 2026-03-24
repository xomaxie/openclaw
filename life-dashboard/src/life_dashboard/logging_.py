"""Structured logging setup — JSON lines to file, human-readable to console."""

from __future__ import annotations

import logging as stdlib_logging
import sys
from pathlib import Path

import structlog

log = structlog.get_logger(__name__)


def setup_logging(level: str = "INFO", log_path: str | Path | None = None) -> None:
    """Configure structlog. If log_path is given, writes JSON lines to file; else prints to stdout."""
    numeric_level = getattr(stdlib_logging, level.upper(), stdlib_logging.INFO)

    shared_processors = [
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    if log_path:
        log_path = Path(log_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_proc = structlog.processors.JSONRenderer()
        factory = structlog.PrintLoggerFactory(file=open(log_path, "a"))
        processors = shared_processors + [file_proc]
    else:
        # Console output — use ConsoleRenderer with colors, suppress traceback for cleaner output
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(colors=True, exception_formatter=structlog.dev.plain_traceback)
        ]
        factory = structlog.PrintLoggerFactory(file=sys.stdout)

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        context_class=dict,
        logger_factory=factory,
        cache_logger_on_first_use=False,
    )
    # Use the root logger for the startup message
    structlog.get_logger("life_dashboard").info("logging.configured", level=level, path=str(log_path) if log_path else "stdout")
