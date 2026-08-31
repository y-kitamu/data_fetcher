"""data.py - 日本証券金融（taisyaku.jp）の信用残高データ取得"""

import re
from datetime import date, timedelta

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.taisyaku.jp"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# 銘柄詳細検索フォーム（/app/stock/detail/{code}/search）の「市場区分」の値
TRJO_KBN_BY_MARKET = {
    "東証": "01",
    "名証": "03",
    "福証": "06",
    "札証": "07",
}


class CsrfExpiredError(RuntimeError):
    """銘柄検索フォームのCSRFトークンがサーバーに拒否された場合に送出される。"""


def fetch_zandaka_csv(session: requests.Session) -> str:
    """銘柄別信用残高一覧（直近営業日分、全銘柄）をCSVテキストとして取得する。"""
    res = session.get(f"{BASE_URL}/data/zandaka.csv", headers=HEADERS)
    res.raise_for_status()
    return res.content.decode("cp932")


def _extract_csrf_token(html: str) -> str:
    soup = BeautifulSoup(html, features="lxml")
    token_input = soup.find("input", attrs={"name": "csrf_test_name"})
    if token_input is None or not token_input.get("value"):
        raise RuntimeError("CSRF token not found in stock search page.")
    return token_input["value"]


def get_stock_search_csrf_token(session: requests.Session) -> str:
    """銘柄検索フォームのCSRFトークンを取得する（新規セッション開始時に呼ぶ）。"""
    res = session.get(f"{BASE_URL}/app/stock", headers=HEADERS)
    res.raise_for_status()
    return _extract_csrf_token(res.text)


def _parse_result_count(html: str) -> int:
    match = re.search(r"検索結果\s*(?:&nbsp;)?\s*([\d,]+)件", html)
    if match is None:
        raise RuntimeError("Could not find search result count in response.")
    return int(match.group(1).replace(",", ""))


def search_stocks_by_date(
    session: requests.Session, csrf_token: str, target_date: date
) -> tuple[int, str]:
    """指定した申込日（取引日）で全銘柄を検索する。

    Returns:
        (該当件数, レスポンスに含まれる最新のCSRFトークン)
    """
    data = {
        "csrf_test_name": csrf_token,
        "mgrCd[]": "",
        "mgrMei": "",
        "mkYmd": target_date.strftime("%Y / %m / %d"),
        "favorites": "",
        "submit": "検索する",
    }
    res = session.post(
        f"{BASE_URL}/app/stock/search",
        data=data,
        headers={**HEADERS, "Referer": f"{BASE_URL}/app/stock"},
    )
    res.raise_for_status()

    if "要求されたアクションは許可されていません" in res.text:
        raise CsrfExpiredError("CSRF token was rejected by the server.")

    count = _parse_result_count(res.text)
    new_token = _extract_csrf_token(res.text)
    return count, new_token


def download_search_result_csv(session: requests.Session) -> str:
    """直前の search_stocks_by_date() の検索結果（全銘柄分）をCSVテキストとして取得する。

    このエンドポイントはURLではなくセッション状態（直前の検索条件）に依存するため、
    呼び出し側は必ずキャッシュを無効化したセッション（get_session(cache_file=None)）を使うこと。
    """
    res = session.get(f"{BASE_URL}/app/stock/csv", headers=HEADERS)
    res.raise_for_status()
    return res.content.decode("cp932")


def fetch_ticker_history_csv(
    session: requests.Session,
    csrf_token: str,
    code: str,
    trjo_kbn: str,
    start_date: date,
    end_date: date,
    name: str = "",
) -> tuple[str, str]:
    """指定銘柄・市場の信用残高履歴（融資新規/返済、貸株新規/返済を含む）を取得する。

    銘柄詳細ページの「抽出条件」検索（/app/stock/detail/{code}/search）は
    日付範囲に上限がなく、保有期間全体を一度のリクエストで取得できる
    （データがない範囲は自動的に切り詰められる）。

    Returns:
        (CSVテキスト, 更新後のCSRFトークン)
    """
    data = {
        "csrf_test_name": csrf_token,
        "orgMgrCd": code,
        "orgMgrMei": name,
        "sort": "",
        "page": "",
        "fsort": "",
        "fpage": "",
        "mkYmdFrom": start_date.strftime("%Y / %m / %d"),
        "mkYmdTo": end_date.strftime("%Y / %m / %d"),
        "kjnYmdDays": "",
        "trjoKbn": trjo_kbn,
    }
    res = session.post(
        f"{BASE_URL}/app/stock/detail/{code}/search",
        data=data,
        headers={**HEADERS, "Referer": f"{BASE_URL}/app/stock"},
    )
    res.raise_for_status()

    if "要求されたアクションは許可されていません" in res.text:
        raise CsrfExpiredError("CSRF token was rejected by the server.")

    new_token = _extract_csrf_token(res.text)

    csv_res = session.get(f"{BASE_URL}/app/stock/detail/{code}/csv", headers=HEADERS)
    csv_res.raise_for_status()
    return csv_res.content.decode("cp932"), new_token


def fetch_ticker_history_csv_with_retry(
    session: requests.Session,
    csrf_token: str,
    code: str,
    trjo_kbn: str,
    start_date: date,
    end_date: date,
    name: str = "",
) -> tuple[str, str]:
    try:
        return fetch_ticker_history_csv(
            session, csrf_token, code, trjo_kbn, start_date, end_date, name
        )
    except CsrfExpiredError:
        csrf_token = get_stock_search_csrf_token(session)
        return fetch_ticker_history_csv(
            session, csrf_token, code, trjo_kbn, start_date, end_date, name
        )


def _search_with_retry(
    session: requests.Session, csrf_token: str, target_date: date
) -> tuple[int, str]:
    try:
        return search_stocks_by_date(session, csrf_token, target_date)
    except CsrfExpiredError:
        csrf_token = get_stock_search_csrf_token(session)
        return search_stocks_by_date(session, csrf_token, target_date)


def fetch_history_for_date(
    session: requests.Session, csrf_token: str, target_date: date
) -> tuple[str | None, str]:
    """指定日の全銘柄分の信用残高履歴CSVを取得する。

    Returns:
        (CSVテキスト または 該当日にデータがない場合はNone, 更新後のCSRFトークン)
    """
    count, csrf_token = _search_with_retry(session, csrf_token, target_date)
    if count == 0:
        return None, csrf_token
    return download_search_result_csv(session), csrf_token


def find_earliest_available_date(
    session: requests.Session,
    csrf_token: str,
    end_date: date,
    min_date: date = date(2015, 1, 1),
) -> tuple[date, str]:
    """サイト側のデータ保有期間の開始日を自動検出する。

    月単位で後方に探索してデータが存在しなくなる月を特定し、
    その月内（〜直近の確認済みデータ日）を日単位で前進探索して
    最初にデータが存在する日を返す。
    """
    def probe_week(anchor: date) -> date | None:
        """anchor から最大7日分探索し、最初にデータがある日を返す（土日祝対策）。"""
        nonlocal csrf_token
        for offset in range(7):
            probe = anchor + timedelta(days=offset)
            if probe > end_date:
                return None
            count, csrf_token = _search_with_retry(session, csrf_token, probe)
            if count > 0:
                return probe
        return None

    latest_known_good = end_date
    cursor = end_date.replace(day=1)

    while cursor > min_date:
        probe_anchors = [cursor, min(cursor + timedelta(days=14), end_date)]
        found_date = None
        for anchor in probe_anchors:
            found_date = probe_week(anchor)
            if found_date is not None:
                break
        if found_date is None:
            break
        latest_known_good = found_date
        cursor = (cursor - timedelta(days=1)).replace(day=1)

    boundary_start = max(cursor, min_date)
    probe = boundary_start
    while probe <= latest_known_good:
        count, csrf_token = _search_with_retry(session, csrf_token, probe)
        if count > 0:
            return probe, csrf_token
        probe += timedelta(days=1)

    return latest_known_good, csrf_token
