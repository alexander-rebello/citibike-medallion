import importlib.util
from pathlib import Path

import pytest

from citibike import registry

REPO = Path(__file__).resolve().parents[1]


def load_downloader():
    spec = importlib.util.spec_from_file_location("download_raw", REPO / "scripts" / "download_raw.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize("entry", registry.registry()["headers"], ids=lambda h: h["variant"])
def test_registered_header_matches_its_fingerprint(entry):
    assert registry.fingerprint(entry["header"]) == entry["fingerprint"]


@pytest.mark.parametrize("entry", registry.registry()["headers"], ids=lambda h: h["variant"])
def test_registered_header_normalizes_to_schema_columns(entry):
    schema = registry.registry()["schemas"][entry["schema"]]
    assert registry.normalize_columns(entry["header"]) == schema["columns"]


def test_downloader_and_registry_use_the_same_fingerprint():
    dl = load_downloader()
    for h in registry.registry()["headers"]:
        assert dl.header_fingerprint(h["header"].encode()) == h["fingerprint"]


def test_fingerprint_ignores_case_and_surrounding_whitespace():
    assert registry.fingerprint("A,B\r\n") == registry.fingerprint("a,b")


def test_unknown_header_stops_the_pipeline():
    with pytest.raises(registry.UnknownHeaderError):
        registry.lookup(registry.fingerprint("trip_id,some_new_column"))


def test_lookup_returns_logical_schema():
    info = registry.lookup("51579225cd4c70fb")
    assert info["schema"] == "legacy_v1"
    assert "M/d/yyyy H:mm" in info["timestamp_patterns"]
