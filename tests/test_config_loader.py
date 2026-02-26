import os
from unittest.mock import Mock

import pytest

from src.utilities.ConfigLoader import ConfigLoader


@pytest.fixture(autouse=True)
def reset_config_loader_state(monkeypatch):
    """Reset loader state and isolate OFTL_* environment between tests."""
    # Avoid reading developer/local .env files during tests.
    monkeypatch.setattr(ConfigLoader._env, "read_env", lambda *args, **kwargs: None)

    # Remove existing OFTL_ keys that may be present in runner environment.
    existing_oftl_keys = [key for key in os.environ if key.startswith("OFTL_")]
    for key in existing_oftl_keys:
        monkeypatch.delenv(key, raising=False)

    ConfigLoader.configurations = {}
    yield
    ConfigLoader.configurations = {}


@pytest.mark.parametrize(
    "key",
    [
        "OFTL_A_B",  # minimal valid shape
        "OFTL_SERVICE_TIMEOUT",
        "OFTL_SERVICE_TIMEOUT_1",
        "OFTL_1_2",  # numeric boundary segments
        "OFTL_API_PRIVATE_KEY_SECRET",  # valid secret suffix
        "OFTL_ABCD_EFGH_IJKL",
    ],
)
def test_is_valid_key_positive_cases(key):
    assert ConfigLoader._is_valid_key(key) is True


@pytest.mark.parametrize(
    "key",
    [
        "",  # empty
        "OFTL_",  # missing segments
        "OFTL_A_",  # missing value segment
        "OFTL__B",  # missing first segment
        "oftl_A_B",  # wrong prefix case
        "OFTL_a_B",  # lowercase first segment
        "OFTL_A_b",  # lowercase second segment
        "OFTL-A-B",  # wrong separators
        "NOTOFTL_A_B",  # wrong prefix
        "OFTL_A_B_SECRET-EXTRA",  # invalid character in key
    ],
)
def test_is_valid_key_negative_cases(key):
    assert ConfigLoader._is_valid_key(key) is False


def test_load_configurations_loads_only_valid_oftl_keys(monkeypatch):
    monkeypatch.setenv("OFTL_SERVICE_TIMEOUT", "30")
    monkeypatch.setenv("OFTL_API_KEY_SECRET", "encrypted-value")
    monkeypatch.setenv("OFTL_A_", "invalid")
    monkeypatch.setenv("OFTL_A-B", "invalid")
    monkeypatch.setenv("OTHER_PREFIX_VALUE", "ignored")

    decrypt_mock = Mock(return_value="decrypted-value")
    monkeypatch.setattr(ConfigLoader, "decrypt", decrypt_mock)

    loaded = ConfigLoader.load_configurations()

    assert loaded == {
        "OFTL_SERVICE_TIMEOUT": "30",
        "OFTL_API_KEY_SECRET": "decrypted-value",
    }
    decrypt_mock.assert_called_once_with("encrypted-value")


def test_load_configurations_boundary_empty_values(monkeypatch):
    monkeypatch.setenv("OFTL_FEATURE_FLAG", "")
    monkeypatch.setenv("OFTL_TOKEN_SECRET", "")

    decrypt_mock = Mock(return_value="")
    monkeypatch.setattr(ConfigLoader, "decrypt", decrypt_mock)

    loaded = ConfigLoader.load_configurations()

    assert loaded["OFTL_FEATURE_FLAG"] == ""
    assert loaded["OFTL_TOKEN_SECRET"] == ""
    decrypt_mock.assert_called_once_with("")


def test_load_configurations_boundary_large_value(monkeypatch):
    large_value = "x" * 10000
    monkeypatch.setenv("OFTL_PAYLOAD_DATA", large_value)

    loaded = ConfigLoader.load_configurations()

    assert loaded["OFTL_PAYLOAD_DATA"] == large_value


def test_get_returns_existing_value_without_reload(monkeypatch):
    ConfigLoader.configurations = {"OFTL_CACHE_TTL": "300"}

    load_mock = Mock(side_effect=AssertionError("load_configurations should not be called"))
    monkeypatch.setattr(ConfigLoader, "load_configurations", load_mock)

    assert ConfigLoader.get("OFTL_CACHE_TTL") == "300"
    load_mock.assert_not_called()


def test_get_triggers_lazy_load_and_uses_default(monkeypatch):
    def _fake_load():
        ConfigLoader.configurations = {"OFTL_REGION": "us-east-1"}
        return ConfigLoader.configurations

    load_mock = Mock(side_effect=_fake_load)
    monkeypatch.setattr(ConfigLoader, "load_configurations", load_mock)

    assert ConfigLoader.get("OFTL_REGION") == "us-east-1"
    assert ConfigLoader.get("OFTL_UNKNOWN", default="fallback") == "fallback"
    load_mock.assert_called_once()
