"""api.py - J-Quants API (V2, APIキー認証) クライアント

無料プランの制約:
- 直近12週間分のデータは取得できない（サーバー側で日付範囲エラーが返る）
- そこから遡って約2年分のみ取得可能
- => バックテスト用の過去データ収集専用であり、当日〜直近のライブ運用には使えない
- 利用可能なエンドポイントは 上場銘柄一覧・株価四本値・財務情報・決算発表予定日 の4種のみ。
  投資部門別売買状況・信用取引週末残高・空売り関連データは有料プラン（Light/Standard以上）が必要。
  有料プラン移行時は _request()/_fetch_all_pages() をそのまま再利用し、
  get_* 関数を追加するだけで拡張できる設計にしている。

V1（リフレッシュトークン→IDトークン方式）は2025年12月22日以降の新規登録者では廃止されており、
本モジュールはV2のAPIキー認証（x-api-key ヘッダー）のみを実装する。
"""

from pathlib import Path
from typing import Any

import requests

from ...core.constants import PROJECT_ROOT
from ...core.retry import retry_with_backoff

BASE_URL = "https://api.jquants.com/v2"
API_KEY_PATH = PROJECT_ROOT / "cert/jquants_api_key.txt"
TIMEOUT = 10.0


class JQuantsRateLimitError(RuntimeError):
    """レート制限 (HTTP 429) に達した場合に送出される。"""


class JQuantsApiError(RuntimeError):
    """J-Quants APIがエラーメッセージを返した場合に送出される（データ範囲外、認証エラー等）。"""


def load_api_key(path: Path = API_KEY_PATH) -> str:
    """cert/jquants_api_key.txt からAPIキーを読み込む。"""
    if not path.exists():
        raise FileNotFoundError(
            f"J-Quants APIキーが見つかりません: {path}. "
            "https://jpx-jquants.com/ で登録後、ダッシュボードでAPIキーを発行してください。"
        )
    return path.read_text().strip()


@retry_with_backoff(max_retries=5, base_delay=5.0, exceptions=(JQuantsRateLimitError,))
def _request(
    session: requests.Session, path: str, params: dict[str, Any], api_key: str
) -> dict[str, Any]:
    res = session.get(
        f"{BASE_URL}{path}",
        headers={"x-api-key": api_key},
        params=params,
        timeout=TIMEOUT,
    )
    if res.status_code == 429:
        raise JQuantsRateLimitError(f"Rate limit exceeded for {path} (params={params})")
    res.raise_for_status()
    return res.json()


def _fetch_all_pages(
    session: requests.Session, path: str, params: dict[str, Any], api_key: str
) -> list[dict[str, Any]]:
    """pagination_key を使って全ページを取得する。"""
    rows: list[dict[str, Any]] = []
    query = dict(params)
    while True:
        payload = _request(session, path, query, api_key)
        if "data" not in payload:
            raise JQuantsApiError(
                f"J-Quants API error for {path} (params={query}): {payload.get('message', payload)}"
            )
        rows.extend(payload["data"])
        pagination_key = payload.get("pagination_key")
        if not pagination_key:
            break
        query = {**params, "pagination_key": pagination_key}
    return rows


def _clean_params(**kwargs: str | None) -> dict[str, str]:
    return {k: v for k, v in kwargs.items() if v is not None}


def get_listed_info(
    session: requests.Session,
    api_key: str,
    code: str | None = None,
    date: str | None = None,
) -> list[dict[str, Any]]:
    """上場銘柄一覧 (/equities/master) を取得する。"""
    params = _clean_params(code=code, date=date)
    return _fetch_all_pages(session, "/equities/master", params, api_key)


def get_daily_quotes(
    session: requests.Session,
    api_key: str,
    code: str | None = None,
    date: str | None = None,
) -> list[dict[str, Any]]:
    """株価四本値 (/equities/bars/daily) を取得する。

    code のみ指定した場合、無料プランで契約している期間内の全履歴が返る。
    """
    params = _clean_params(code=code, date=date)
    return _fetch_all_pages(session, "/equities/bars/daily", params, api_key)


def get_statements(
    session: requests.Session,
    api_key: str,
    code: str | None = None,
    date: str | None = None,
) -> list[dict[str, Any]]:
    """財務情報 (/fins/summary) を取得する。"""
    params = _clean_params(code=code, date=date)
    return _fetch_all_pages(session, "/fins/summary", params, api_key)


def get_earnings_dates(
    session: requests.Session,
    api_key: str,
    code: str | None = None,
    date: str | None = None,
    scheduled_date: str | None = None,
) -> list[dict[str, Any]]:
    """決算発表予定日 (/fins/earnings-date) を取得する。"""
    params = _clean_params(code=code, date=date, scheduled_date=scheduled_date)
    return _fetch_all_pages(session, "/fins/earnings-date", params, api_key)
