"""Tests for the notifiers: must never raise, must never leak credentials."""

import base64
from email import message_from_bytes
from email.header import decode_header, make_header
from pathlib import Path
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


def _patch_post(monkeypatch: Any, response: _FakeResponse) -> None:
    monkeypatch.setattr(
        notification.requests, "post", lambda *args, **kwargs: response
    )


def test_success_does_not_raise(monkeypatch: Any) -> None:
    _patch_post(monkeypatch, _FakeResponse(200))
    notification.notify_to_line("hello")


def test_http_failure_does_not_leak_token(monkeypatch: Any, log_sink: list) -> None:
    _patch_post(monkeypatch, _FakeResponse(400, text="bad request"))
    notification.notify_to_line("hello")
    log_text = "".join(str(record) for record in log_sink)
    assert "secret-token-value" not in log_text
    assert "Bearer ***" in log_text


def test_http_failure_does_not_raise(monkeypatch: Any) -> None:
    _patch_post(monkeypatch, _FakeResponse(500, text="server error"))
    notification.notify_to_line("hello")


def test_request_exception_does_not_raise(monkeypatch: Any) -> None:
    def _raise(*args: Any, **kwargs: Any) -> None:
        raise requests.exceptions.ConnectionError("down")

    monkeypatch.setattr(notification.requests, "post", _raise)
    notification.notify_to_line("hello")


def test_missing_token_file_does_not_raise(monkeypatch: Any, tmp_path) -> None:
    monkeypatch.setattr(
        notification, "LINE_ACCESS_TOKEN_FILE", tmp_path / "does-not-exist.txt"
    )
    notification.notify_to_line("hello")


# --- notify_to_gmail ---------------------------------------------------------


class _FakeMessages:
    def __init__(self, capture: dict, error: Exception | None = None) -> None:
        self._capture = capture
        self._error = error

    def send(self, userId: str, body: dict) -> "_FakeMessages":  # noqa: N803
        self._capture["userId"] = userId
        self._capture["body"] = body
        return self

    def execute(self) -> dict:
        if self._error is not None:
            raise self._error
        return {"id": "fake-message-id"}


class _FakeUsers:
    def __init__(self, capture: dict, error: Exception | None = None) -> None:
        self._capture = capture
        self._error = error

    def messages(self) -> _FakeMessages:
        return _FakeMessages(self._capture, self._error)


class _FakeService:
    def __init__(self, capture: dict, error: Exception | None = None) -> None:
        self._capture = capture
        self._error = error

    def users(self) -> _FakeUsers:
        return _FakeUsers(self._capture, self._error)


class _FakeCredentials:
    """Stands in for google.oauth2.credentials.Credentials."""

    def __init__(self, expired: bool = False) -> None:
        self.expired = expired
        self.refresh_token = "fake-refresh-token"
        self.refresh_calls = 0

    def refresh(self, request: Any) -> None:
        self.refresh_calls += 1
        self.expired = False

    def to_json(self) -> str:
        return '{"refreshed": true}'


@pytest.fixture
def gmail_files(tmp_path, monkeypatch: Any) -> Path:
    """Point the module at throwaway token/recipient files and hand back the
    token path so tests can assert on what gets written to it."""
    token_file = tmp_path / "gmail_send_token.json"
    token_file.write_text("{}")  # contents never matter — Credentials is faked
    recipient_file = tmp_path / "gmail_notify_address.txt"
    recipient_file.write_text("user@example.com\n")
    monkeypatch.setattr(notification, "GMAIL_SEND_TOKEN_FILE", token_file)
    monkeypatch.setattr(notification, "GMAIL_RECIPIENT_FILE", recipient_file)
    return token_file


def _patch_gmail(
    monkeypatch: Any,
    *,
    expired: bool = False,
    error: Exception | None = None,
) -> tuple[_FakeCredentials, dict]:
    creds = _FakeCredentials(expired=expired)
    capture: dict = {}
    monkeypatch.setattr(
        notification.Credentials,
        "from_authorized_user_file",
        staticmethod(lambda *args, **kwargs: creds),
    )
    monkeypatch.setattr(
        notification, "build", lambda *args, **kwargs: _FakeService(capture, error)
    )
    return creds, capture


def test_gmail_success_sends_via_api(monkeypatch: Any, gmail_files: Path) -> None:
    _, capture = _patch_gmail(monkeypatch)

    notification.notify_to_gmail("date: 20260811\nnews/kabutan : 20260811, 42\n")

    assert capture["userId"] == "me"
    mime = message_from_bytes(base64.urlsafe_b64decode(capture["body"]["raw"]))
    assert mime["To"] == "user@example.com"
    body = mime.get_payload(decode=True).decode("utf-8")
    # Real newlines, not the literal "\n" the LINE JSON payload used to need.
    assert body.splitlines() == ["date: 20260811", "news/kabutan : 20260811, 42"]


def test_gmail_japanese_subject_and_body_survive_encoding(
    monkeypatch: Any, gmail_files: Path
) -> None:
    _, capture = _patch_gmail(monkeypatch)

    notification.notify_to_gmail("データ収集状況", subject="[data_fetcher] 日次通知")

    mime = message_from_bytes(base64.urlsafe_b64decode(capture["body"]["raw"]))
    assert str(make_header(decode_header(mime["Subject"]))) == "[data_fetcher] 日次通知"
    assert mime.get_payload(decode=True).decode("utf-8") == "データ収集状況"


def test_gmail_explicit_recipient_overrides_file(
    monkeypatch: Any, gmail_files: Path
) -> None:
    _, capture = _patch_gmail(monkeypatch)

    notification.notify_to_gmail("hello", to="other@example.com")

    mime = message_from_bytes(base64.urlsafe_b64decode(capture["body"]["raw"]))
    assert mime["To"] == "other@example.com"


def test_gmail_expired_token_is_refreshed(monkeypatch: Any, gmail_files: Path) -> None:
    creds, _ = _patch_gmail(monkeypatch, expired=True)

    notification.notify_to_gmail("hello")

    assert creds.refresh_calls == 1
    assert gmail_files.read_text() == '{"refreshed": true}'


def test_gmail_missing_token_file_does_not_raise(
    monkeypatch: Any, gmail_files: Path, tmp_path
) -> None:
    # Credentials deliberately left unpatched: the real loader must be the one
    # that fails on the missing file.
    monkeypatch.setattr(
        notification, "GMAIL_SEND_TOKEN_FILE", tmp_path / "does-not-exist.json"
    )
    notification.notify_to_gmail("hello")


def test_gmail_missing_recipient_file_does_not_raise(
    monkeypatch: Any, gmail_files: Path, tmp_path
) -> None:
    monkeypatch.setattr(
        notification, "GMAIL_RECIPIENT_FILE", tmp_path / "does-not-exist.txt"
    )
    notification.notify_to_gmail("hello")


def test_gmail_api_error_does_not_raise_and_logs_warning(
    monkeypatch: Any, gmail_files: Path, log_sink: list
) -> None:
    _patch_gmail(monkeypatch, error=RuntimeError("gmail api down"))

    notification.notify_to_gmail("hello")

    log_text = "".join(str(record) for record in log_sink)
    assert "Gmail notification failed" in log_text


def test_gmail_token_file_contents_never_appear_in_logs(
    monkeypatch: Any, gmail_files: Path, log_sink: list
) -> None:
    gmail_files.write_text('{"refresh_token": "super-secret-value"}')
    _patch_gmail(monkeypatch, error=RuntimeError("gmail api down"))

    notification.notify_to_gmail("hello")

    log_text = "".join(str(record) for record in log_sink)
    assert "super-secret-value" not in log_text
