import importlib

import pytest

from src.utilities.ConfigLoader import ConfigLoader
from src.utilities.RabbitMQHelper import RabbitMQConnectionError, RabbitMQHelper

rabbitmq_helper_module = importlib.import_module("src.utilities.RabbitMQHelper")


@pytest.fixture(autouse=True)
def reset_rabbitmq_helper_state(monkeypatch):
    monkeypatch.setattr(ConfigLoader._env, "read_env", lambda *args, **kwargs: None)
    ConfigLoader.configurations = {}
    RabbitMQHelper._connection = None
    RabbitMQHelper._channel = None
    RabbitMQHelper._delivery_confirm_enabled = False
    yield
    ConfigLoader.configurations = {}
    RabbitMQHelper._connection = None
    RabbitMQHelper._channel = None
    RabbitMQHelper._delivery_confirm_enabled = False


def test_connection_retry_count_uses_default():
    assert RabbitMQHelper._connection_retry_count() == 3


def test_build_connection_parameters_uses_single_pika_attempt(monkeypatch):
    class FakePikaModule:
        class PlainCredentials:
            def __init__(self, username, password):
                self.username = username
                self.password = password

        class ConnectionParameters:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

    monkeypatch.setattr(rabbitmq_helper_module, "pika", FakePikaModule)

    params = RabbitMQHelper._build_connection_parameters()

    assert params.kwargs["connection_attempts"] == 1
    assert params.kwargs["retry_delay"] == 0
    assert params.kwargs["socket_timeout"] == 5.0
    assert params.kwargs["stack_timeout"] == 10.0


def test_connect_with_retry_raises_after_configured_attempts(monkeypatch):
    monkeypatch.setenv("OFTL_RABITMQ_CONN_RETRYCOUNT", "2")
    attempts: list[int] = []

    def failing_open():
        attempts.append(1)
        raise RuntimeError("auth failed")

    monkeypatch.setattr(RabbitMQHelper, "_open_connection", classmethod(lambda cls: failing_open()))
    monkeypatch.setattr(RabbitMQHelper, "close", classmethod(lambda cls: None))
    monkeypatch.setattr(rabbitmq_helper_module.time, "sleep", lambda _: None)

    with pytest.raises(RabbitMQConnectionError):
        RabbitMQHelper._connect_with_retry()

    assert len(attempts) == 2


def test_initialize_connection_normalizes_connection_failures(monkeypatch):
    monkeypatch.setattr(
        RabbitMQHelper,
        "_ensure_channel",
        classmethod(lambda cls: (_ for _ in ()).throw(RuntimeError("dns failure"))),
    )
    monkeypatch.setattr(RabbitMQHelper, "close", classmethod(lambda cls: None))

    with pytest.raises(RabbitMQConnectionError):
        RabbitMQHelper.initialize_connection()


def test_ensure_channel_uses_retry_connection(monkeypatch):
    sentinel_channel = object()

    def connect_successfully():
        RabbitMQHelper._connection = type("Conn", (), {"is_closed": False})()
        RabbitMQHelper._channel = sentinel_channel

    monkeypatch.setattr(RabbitMQHelper, "_connect_with_retry", classmethod(lambda cls: connect_successfully()))

    assert RabbitMQHelper._ensure_channel() is sentinel_channel
