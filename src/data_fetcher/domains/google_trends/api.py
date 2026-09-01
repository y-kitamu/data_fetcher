"""api.py - Google Trends (pytrends, 非公式) クライアント

pytrendsはGoogle Trendsの非公式スクレイピングベースのライブラリであり、公式APIではない。
リクエスト頻度が高いと一時的にブロックされることがあるため、呼び出し側で
リクエスト間隔を十分に空け（数秒〜数十秒単位）、失敗時は `core.retry_with_backoff` 等で
時間を置いてリトライすること。

1リクエストにつき複数キーワードを渡すとGoogle Trends側で相対値に正規化されてしまうため
（絶対値としての比較ができなくなる）、銘柄ごとの検索関心度を独立に取得するには
1キーワードずつ問い合わせる。
"""

import polars as pl
from pytrends.request import TrendReq


def get_trend_request(hl: str = "ja-JP", tz: int = 540) -> TrendReq:
    return TrendReq(hl=hl, tz=tz)


def fetch_interest_over_time(
    pytrends: TrendReq,
    keyword: str,
    timeframe: str = "today 3-m",
    geo: str = "JP",
) -> pl.DataFrame:
    """指定キーワード単独の検索関心度の推移を取得する（0〜100の相対値）。"""
    pytrends.build_payload([keyword], timeframe=timeframe, geo=geo)
    df = pytrends.interest_over_time()
    if df.empty:
        return pl.DataFrame(schema={"date": pl.Date, "keyword": pl.Utf8, "interest": pl.Int64, "is_partial": pl.Boolean})

    df = df.reset_index().rename(columns={keyword: "interest", "isPartial": "is_partial"})
    df["keyword"] = keyword
    return pl.from_pandas(df[["date", "keyword", "interest", "is_partial"]])
