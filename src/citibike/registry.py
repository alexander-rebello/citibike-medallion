"""Schema registry: maps an exact raw CSV header to a logical schema."""
import hashlib
import re
import tomllib
from functools import lru_cache

from citibike.config import CONFIG_DIR


class UnknownHeaderError(Exception):
    """A source file has a header nobody has reviewed yet: stop instead of guessing."""


def fingerprint(header: str | bytes) -> str:
    if isinstance(header, str):
        header = header.encode()
    return hashlib.sha256(header.strip().lower()).hexdigest()[:16]


def normalize_columns(header: str) -> list[str]:
    """'Start Station ID' / '"start station id"' / 'start_station_id' -> 'startstationid'."""
    return [re.sub(r"[^a-z0-9]", "", c.lower()) for c in header.strip().split(",")]


@lru_cache
def registry() -> dict:
    with open(CONFIG_DIR / "schema_registry.toml", "rb") as f:
        return tomllib.load(f)


def lookup(fp: str) -> dict:
    """Return {'schema': ..., 'variant': ..., 'columns': [...], 'timestamp_patterns': [...]}."""
    reg = registry()
    for h in reg["headers"]:
        if h["fingerprint"] == fp:
            return {**reg["schemas"][h["schema"]], "schema": h["schema"], "variant": h["variant"]}
    raise UnknownHeaderError(
        f"header fingerprint {fp} is not in config/schema_registry.toml; "
        "inspect the file and register its header before ingesting it")
