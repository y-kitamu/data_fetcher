"""api.py - Google Trends (pytrends, 非公式) クライアント

pytrendsはGoogle Trendsの非公式スクレイピングベースのライブラリであり、公式APIではない。
リクエスト頻度が高いと一時的にブロックされることがあるため、呼び出し側で
リクエスト間隔を十分に空け（数秒〜数十秒単位）、失敗時は `core.retry_with_backoff` 等で
時間を置いてリトライすること。

1リクエストにつき複数キーワードを渡すとGoogle Trends側で相対値に正規化されてしまうため
（絶対値としての比較ができなくなる）、銘柄ごとの検索関心度を独立に取得するには
1キーワードずつ問い合わせる。

重要: `interest` は0〜100の相対値で、正規化の基準は「そのリクエストで指定した期間
（timeframe）内の最大値」。そのため同じ `date` でも取得タイミングがずれると異なる値が返る。
戻り値には `fetched_at`（取得日）・`window_start`/`window_end`（実際に返ってきた期間）を
含めているので、呼び出し側はこれらを保持したまま追記保存すること（上書きしない）。
"""

import datetime

import polars as pl
from dateutil.relativedelta import relativedelta
from pytrends.request import TrendReq

# Google Trendsは1回のクエリで指定する期間の長さで粒度が決まる（実測で確認済み）:
#   ~9ヶ月(270日)以内: 日次、9ヶ月~5年: 週次、5年超: 月次。
# 過去の期間を指定した場合でも、この幅を超えなければ日次粒度が返る。
MAX_DAILY_GRANULARITY_DAYS = 270


def get_trend_request(hl: str = "ja-JP", tz: int = 540) -> TrendReq:
    return TrendReq(hl=hl, tz=tz)


def generate_daily_windows(
    end_date: datetime.date,
    years_back: int,
    window_months: int = 6,
    overlap_months: int = 1,
) -> list[tuple[datetime.date, datetime.date]]:
    """日次粒度を保ったまま過去に遡るための (start, end) ウィンドウ列を新しい順に生成する。

    `window_months` は `MAX_DAILY_GRANULARITY_DAYS` 以内に収める必要がある。
    `overlap_months` はウィンドウ同士を意図的に重複させる幅で、後日リスケールして
    1本の連続系列に繋ぎ合わせる際のアンカーに使う想定（リスケール自体は行わない）。
    """
    if window_months * 30 > MAX_DAILY_GRANULARITY_DAYS:
        raise ValueError(
            f"window_months={window_months} は日次粒度の上限"
            f"（約{MAX_DAILY_GRANULARITY_DAYS}日）を超えています。"
        )
    step_months = window_months - overlap_months
    earliest = end_date - relativedelta(years=years_back)

    windows: list[tuple[datetime.date, datetime.date]] = []
    cur_end = end_date
    while True:
        cur_start = max(cur_end - relativedelta(months=window_months), earliest)
        windows.append((cur_start, cur_end))
        if cur_start <= earliest:
            break
        cur_end = cur_end - relativedelta(months=step_months)
    return windows


def fetch_interest_over_time_for_range(
    pytrends: TrendReq,
    keyword: str,
    start_date: datetime.date,
    end_date: datetime.date,
    geo: str = "JP",
) -> pl.DataFrame:
    """指定した日付範囲（日次粒度を保つには270日以内を推奨）の検索関心度を取得する。"""
    timeframe = f"{start_date.isoformat()} {end_date.isoformat()}"
    return fetch_interest_over_time(pytrends, keyword, timeframe=timeframe, geo=geo)


def fetch_interest_over_time(
    pytrends: TrendReq,
    keyword: str,
    timeframe: str = "today 3-m",
    geo: str = "JP",
) -> pl.DataFrame:
    """指定キーワード単独の検索関心度の推移を取得する（0〜100の相対値）。

    戻り値の列:
        date, keyword, interest, is_partial: pytrendsの生レスポンス
        fetched_at: このリクエストを実行した日（正規化の基準がいつのウィンドウかを示す）
        window_start, window_end: 実際にレスポンスに含まれていた date の最小・最大値
    """
    fetched_at = datetime.date.today().isoformat()
    empty_schema = {
        "date": pl.Date,
        "keyword": pl.Utf8,
        "interest": pl.Int64,
        "is_partial": pl.Boolean,
        "fetched_at": pl.Utf8,
        "window_start": pl.Utf8,
        "window_end": pl.Utf8,
    }

    pytrends.build_payload([keyword], timeframe=timeframe, geo=geo)
    df = pytrends.interest_over_time()
    if df.empty:
        return pl.DataFrame(schema=empty_schema)

    df = df.reset_index().rename(
        columns={keyword: "interest", "isPartial": "is_partial"}
    )
    df["keyword"] = keyword
    result = pl.from_pandas(df[["date", "keyword", "interest", "is_partial"]])

    window_start = str(result["date"].min())
    window_end = str(result["date"].max())
    return result.with_columns(
        pl.lit(fetched_at).alias("fetched_at"),
        pl.lit(window_start).alias("window_start"),
        pl.lit(window_end).alias("window_end"),
    )
