"""Environment configuration: config/<env>.toml, selected by CITIBIKE_ENV (default: dev)."""
import os
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "config"


def load(env: str | None = None) -> dict:
    env = env or os.environ.get("CITIBIKE_ENV", "dev")
    path = CONFIG_DIR / f"{env}.toml"
    if not path.exists():
        raise FileNotFoundError(f"no config for environment {env!r} ({path})")
    with open(path, "rb") as f:
        cfg = tomllib.load(f)
    cfg["paths"] = {k: os.path.expanduser(v) for k, v in cfg["paths"].items()}
    return cfg
