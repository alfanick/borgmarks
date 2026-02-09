from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass

try:
    from rich.logging import RichHandler
    _HAS_RICH = True
except Exception:
    RichHandler = None  # type: ignore
    _HAS_RICH = False


@dataclass(frozen=True)
class LogConfig:
    level: str = "INFO"
    no_color: bool = False


def setup_logging(cfg: LogConfig) -> None:
    level = getattr(logging, cfg.level.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)

    for h in list(root.handlers):
        root.removeHandler(h)

    force_no_color = cfg.no_color or os.getenv("NO_COLOR") is not None
    is_tty = sys.stderr.isatty()

    if _HAS_RICH and (not force_no_color) and is_tty:
        handler = RichHandler(rich_tracebacks=True, show_time=False, show_level=True, show_path=False)
        fmt = "%(message)s"
    else:
        handler = logging.StreamHandler()
        fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"

    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(fmt))
    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
