"""api.py - FRED (Federal Reserve Economic Data) APIクライアント

無料のAPIキー登録のみで利用可能: https://fred.stlouisfed.org/docs/api/fred/
"""

from pathlib import Path
from typing import Any

import requests

from ...core.constants import PROJECT_ROOT
from ...core.retry import retry_with_backoff

BASE_URL = "https://api.stlouisfed.org/fred"
API_KEY_PATH = PROJECT_ROOT / "cert/fred_api_key.txt"
TIMEOUT = 15.0


class FredApiError(RuntimeError):
    """FRED APIがエラーを返した場合に送出される。"""


def load_api_key(path: Path = API_KEY_PATH) -> str:
    """cert/fred_api_key.txt からAPIキーを読み込む。"""
    if not path.exists():
        raise FileNotFoundError(
            f"FRED APIキーが見つかりません: {path}. "
            "https://fred.stlouisfed.org/docs/api/api_key.html で無料登録後、"
            "発行されたキーをこのファイルに保存してください。"
        )
    return path.read_text().strip()


@retry_with_backoff(
    max_retries=4, base_delay=3.0, exceptions=(requests.exceptions.RequestException,)
)
def get_series_observations(
    session: requests.Session,
    api_key: str,
    series_id: str,
    observation_start: str | None = None,
    observation_end: str | None = None,
) -> list[dict[str, Any]]:
    """`series_id`の観測値を取得する（`/series/observations`）。

    `observation_start`/`observation_end`を省略した場合は全履歴が返る。
    """
    params: dict[str, str] = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
    }
    if observation_start is not None:
        params["observation_start"] = observation_start
    if observation_end is not None:
        params["observation_end"] = observation_end

    res = session.get(f"{BASE_URL}/series/observations", params=params, timeout=TIMEOUT)
    if res.status_code != 200:
        raise FredApiError(
            f"FRED API error for series_id={series_id}: HTTP {res.status_code} {res.text}"
        )
    payload = res.json()
    if "observations" not in payload:
        raise FredApiError(f"FRED API error for series_id={series_id}: {payload}")
    return payload["observations"]


def clean_observations(observations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """`value == "."`（FRED独自の欠損値表記）を`None`に変換する。"""
    return [
        {"date": obs["date"], "value": None if obs.get("value") == "." else obs.get("value")}
        for obs in observations
    ]
