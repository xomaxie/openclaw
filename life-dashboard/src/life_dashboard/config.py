"""Config loading — environment variables + config file."""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any

import structlog
from dotenv import load_dotenv

log = structlog.get_logger(__name__)

DEFAULT_CONFIG_PATH = Path("~/.config/life-dashboard/config.toml").expanduser()


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Load config from file + environment. Env vars override file values."""
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    config: dict[str, Any] = {}

    env_path = path.with_name("environment")
    if env_path.exists():
        load_dotenv(env_path, override=False)
        log.info("config.environment_loaded", path=str(env_path))

    if path.exists():
        with open(path, "rb") as f:
            config = tomllib.load(f)
        log.info("config.loaded", path=str(path))
    else:
        log.warn("config.file_missing", path=str(path))

    # Env var substitution for secret references
    # e.g. token_env = "GITHUB_TOKEN" → look up os.environ["GITHUB_TOKEN"]
    config = _resolve_env_refs(config)

    return config


def _resolve_env_refs(obj: Any) -> Any:
    """Recursively resolve *_env keys to their os.environ values."""
    if isinstance(obj, dict):
        result: dict[str, Any] = {}
        for k, v in obj.items():
            if k.endswith("_env") and isinstance(v, str):
                result[k.removesuffix("_env")] = os.environ.get(v, "")
            elif k == "imap_password_env":
                # Special: IMAP passwords stored as IMAP_PASSWORD_<KEY>
                result[k] = os.environ.get(v, "")
            else:
                result[k] = _resolve_env_refs(v)
        return result
    elif isinstance(obj, list):
        return [_resolve_env_refs(item) for item in obj]
    return obj
