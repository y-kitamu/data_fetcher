"""api.py - e-Stat（政府統計の総合窓口）API クライアント

無料のappId登録のみで利用可能: https://www.e-stat.go.jp/api/

1テーブルが数千〜数十万行になりうる（実測: 機械受注統計調査は絞り込み前
TOTAL_NUMBER=278520）ため、`cdTab`/`cdCat01`等のcd_filtersで単一系列まで
絞り込んでから取得する。絞り込みが不十分だと複数系列が混在したまま返るため、
`assert_single_series`で検知する。

時間軸は`@time`が`"2026000707"`のような特殊コード（階層コード、`@parentCode`で
月次/四半期/年次が混在管理されている）で、そのままでは日付として使えない。
`CLASS_INF`の`time`次元の`@code -> @name`マップ（例: `"2026000707" -> "2026年7月"`）
を経由して`@name`をパースする必要がある（実測確認済み）。
"""

import re
from pathlib import Path
from typing import Any

import requests

from ...core.constants import PROJECT_ROOT
from ...core.retry import retry_with_backoff

BASE_URL = "https://api.e-stat.go.jp/rest/3.0/app/json"
API_KEY_PATH = PROJECT_ROOT / "cert/estat_appid.txt"
TIMEOUT = 20.0

_YEAR_MONTH_RE = re.compile(r"(\d{4})年(\d{1,2})月")
_YEAR_ONLY_RE = re.compile(r"^(\d{4})年$")


class EstatApiError(RuntimeError):
    """e-Stat APIがエラーを返した場合（STATUS != 0）に送出される。"""


def load_app_id(path: Path = API_KEY_PATH) -> str:
    """cert/estat_appid.txt からappIdを読み込む。"""
    if not path.exists():
        raise FileNotFoundError(
            f"e-Stat appIdが見つかりません: {path}. "
            "https://www.e-stat.go.jp/api/ で無料登録後、発行されたappIdを"
            "このファイルに保存してください。"
        )
    return path.read_text().strip()


@retry_with_backoff(
    max_retries=4, base_delay=3.0, exceptions=(requests.exceptions.RequestException,)
)
def search_stats_list(
    session: requests.Session,
    app_id: str,
    search_word: str,
    stats_code: str | None = None,
) -> list[dict[str, Any]]:
    """テーブルを検索する（`getStatsList`、実装時のテーブルID探索用。cronからは呼ばない）。

    日本語の`search_word`は`requests`のparams経由で渡すこと（自前でパーセント
    エンコードすると`STATUS:1 該当データなし`という誤った結果になることを確認済み）。
    """
    params: dict[str, str] = {"appId": app_id, "searchWord": search_word}
    if stats_code is not None:
        params["statsCode"] = stats_code

    res = session.get(f"{BASE_URL}/getStatsList", params=params, timeout=TIMEOUT)
    res.raise_for_status()
    payload = res.json()["GET_STATS_LIST"]
    result = payload["RESULT"]
    if result["STATUS"] != 0:
        raise EstatApiError(f"e-Stat getStatsList error: {result.get('ERROR_MSG')}")

    tables = payload.get("DATALIST_INF", {}).get("TABLE_INF", [])
    if isinstance(tables, dict):
        tables = [tables]
    return tables


@retry_with_backoff(
    max_retries=4, base_delay=3.0, exceptions=(requests.exceptions.RequestException,)
)
def get_stats_data(
    session: requests.Session,
    app_id: str,
    stats_data_id: str,
    **cd_filters: str,
) -> dict[str, Any]:
    """統計データを取得する（`getStatsData`）。

    戻り値: `{"class_inf": [...], "values": [...]}`。
    `class_inf`は次元(`tab`/`cat01`/`area`/`time`等)ごとの`@code -> @name`情報、
    `values`は`@tab`,`@cat01`,...,`@time`,`$`（値）属性を持つ行の一覧。
    """
    params: dict[str, str] = {"appId": app_id, "statsDataId": stats_data_id, **cd_filters}
    res = session.get(f"{BASE_URL}/getStatsData", params=params, timeout=TIMEOUT)
    res.raise_for_status()
    payload = res.json()["GET_STATS_DATA"]
    result = payload["RESULT"]
    if result["STATUS"] != 0:
        raise EstatApiError(
            f"e-Stat getStatsData error for statsDataId={stats_data_id}: "
            f"{result.get('ERROR_MSG')}"
        )

    stat_data = payload["STATISTICAL_DATA"]
    class_obj = stat_data["CLASS_INF"]["CLASS_OBJ"]
    if isinstance(class_obj, dict):
        class_obj = [class_obj]

    values = stat_data["DATA_INF"]["VALUE"]
    if isinstance(values, dict):
        values = [values]

    return {"class_inf": class_obj, "values": values}


def assert_single_series(values: list[dict[str, Any]], dimension_keys: list[str]) -> None:
    """`values`が単一系列（時間軸以外の全次元が固定）になっているか検証する。

    `cd_filters`の絞り込みが不十分だと複数系列が混在したまま返る（実測で
    `cdTab`+`cdCat01`だけでは絞り切れないケースを確認済み）ため、そのまま
    保存すると異なる系列の値が1つのCSVに混ざってしまう。絞り込み漏れがあれば
    `ValueError`で即座に検知する。
    """
    if not values:
        return
    seen = {tuple(v.get(k) for k in dimension_keys) for v in values}
    if len(seen) > 1:
        raise ValueError(
            f"複数系列が混在しています(dimension_keys={dimension_keys}): "
            f"{sorted(seen)[:5]} など{len(seen)}種類。cd_filtersでの絞り込みを見直してください。"
        )


def build_time_label_map(class_inf: list[dict[str, Any]]) -> dict[str, str]:
    """`CLASS_INF`の`time`次元から`@code -> @name`のマップを作る。"""
    for obj in class_inf:
        if obj.get("@id") == "time":
            classes = obj["CLASS"]
            if isinstance(classes, dict):
                classes = [classes]
            return {c["@code"]: c["@name"] for c in classes}
    return {}


def time_label_to_date(label: str) -> str:
    """`"2026年7月"`または`"2024年"`のようなラベルをISO日付文字列に変換する。"""
    match = _YEAR_MONTH_RE.search(label)
    if match is not None:
        year, month = match.groups()
        return f"{year}-{int(month):02d}-01"
    match = _YEAR_ONLY_RE.match(label)
    if match is not None:
        return f"{match.group(1)}-01-01"
    raise ValueError(f"Could not parse e-Stat time label: {label!r}")
