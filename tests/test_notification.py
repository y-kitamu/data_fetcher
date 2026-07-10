"""Tests for notify_to_line: must never raise, must never leak the LINE token."""

from typing import Any

import pytest
import requests
from loguru import logger

from data_fetcher.core import notification


@pytest.fixture
def log_sink() -> list:
    """Loguru writes to a sink installed at import time, which pytest's
    stdout/stderr capture fixtures don't reliably intercept — capture
    directly via a dedicated sink instead."""
    messages: list = []
    sink_id = logger.add(messages.append, level="DEBUG")
    yield messages
    logger.remove(sink_id)


class _FakeResponse:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text


@pytest.fixture(autouse=True)
def _token_file(tmp_path, monkeypatch: Any) -> None:
    token_file = tmp_path / "line_message_api.txt"
    token_file.write_text("secret-token-value")
    monkeypatch.setattr(notification, "LINE_ACCESS_TOKEN_FILE", token_file)


def test_success_does_not_raise(mocker: Any) -> None:
    mocker.patch(
        "data_fetcher.core.notification.requests.post",
        return_value=_FakeResponse(200),
    )
    notification.notify_to_line("hello")


def test_http_failure_does_not_leak_token(mocker: Any, log_sink: list) -> None:
    mocker.patch(
        "data_fetcher.core.notification.requests.post",
        return_value=_FakeResponse(400, text="bad request"),
    )
    notification.notify_to_line("hello")
    log_text = "".join(str(record) for record in log_sink)
    assert "secret-token-value" not in log_text
    assert "Bearer ***" in log_text


def test_http_failure_does_not_raise(mocker: Any) -> None:
    mocker.patch(
        "data_fetcher.core.notification.requests.post",
        return_value=_FakeResponse(500, text="server error"),
    )
    notification.notify_to_line("hello")


def test_request_exception_does_not_raise(mocker: Any) -> None:
    mocker.patch(
        "data_fetcher.core.notification.requests.post",
        side_effect=requests.exceptions.ConnectionError("down"),
    )
    notification.notify_to_line("hello")


def test_missing_token_file_does_not_raise(monkeypatch: Any, tmp_path) -> None:
    monkeypatch.setattr(
        notification, "LINE_ACCESS_TOKEN_FILE", tmp_path / "does-not-exist.txt"
    )
    notification.notify_to_line("hello")
