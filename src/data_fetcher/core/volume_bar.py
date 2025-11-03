"""volume_bar.py
"""

from pathlib import Path
from typing import Any

import polars as pl


def convert_ticker_to_volume_bar(
    ticker_df: pl.DataFrame,
    volume_size: float,
    last_row: list[Any] | None = None,
    volume_key: str = "size",
    price_key: str = "price",
    date_key: str = "datetime",
) -> pl.DataFrame:
    """
    tickerデータをvolume barデータに変換する
    Args:
        ticker_df (pl.DataFrame): tickerデータ (symbol, type, volume, price, date)
        volume_size (float): volume barのサイズ
    Return:
        pl.DataFrame: volume barデータ (open, high, low, close, volume, start_date, end_date)
    """
    volume_bar: dict[str, list[Any]] = {
        "open": [],
        "high": [],
        "low": [],
        "close": [],
        "volume": [],
        "start_date": [],
        "end_date": [],
    }
    cum_volume = 0
    high, low = 0, 0

    if last_row is not None:
        volume_bar["open"].append(last_row[0])
        volume_bar["start_date"].append(last_row[5])
        high = last_row[1]
        low = last_row[2]
        cum_volume = last_row[4]

    # volume barの作成
    for idx in range(len(ticker_df)):
        volume, price, date = (
            ticker_df[volume_key][idx],
            ticker_df[price_key][idx],
            ticker_df[date_key][idx],
        )
        if cum_volume == 0:  # cum_volume == 0のときは新しいbarを作成
            volume_bar["open"].append(price)
            volume_bar["start_date"].append(date)
            high, low = price, price

        cum_volume += volume
        high = max(high, price)
        low = min(low, price)
        if volume_size - cum_volume < volume_size * 1e-5:  # volume_sizeを超えたらbarを閉じる
            volume_bar["high"].append(high)
            volume_bar["low"].append(low)
            volume_bar["close"].append(price)
            volume_bar["volume"].append(cum_volume)
            volume_bar["end_date"].append(date)
            cum_volume = 0

    # 最後のbarが閉じられていない場合
    if len(volume_bar["open"]) != len(volume_bar["close"]):
        volume_bar["high"].append(high)
        volume_bar["low"].append(low)
        volume_bar["close"].append(price)
        volume_bar["volume"].append(cum_volume)
        volume_bar["end_date"].append(date)

    return pl.DataFrame(volume_bar)


def create_volume_bar_csv(input_csv_lists: list[Path], volume_size: float, output_dir: Path):
    """
    tickerデータをvolume barデータに変換してcsvファイルに保存する
    Args:
        input_csv_lists (list[Path]): tickerデータのcsvファイルのリスト
        volume_size (float): volume barのサイズ
        output_dir (Path): 出力先のディレクトリ
    """
    last_row = None
    for i in range(len(input_csv_lists)):
        csv_path = input_csv_lists[i]
        df = pl.read_csv(csv_path)
        volume_df = convert_ticker_to_volume_bar(df, volume_size, last_row)

        if len(volume_df) == 0:
            continue

        if i < len(input_csv_lists) - 1:
            if volume_df["volume"][-1] < volume_size:
                last_row = volume_df[-1].to_numpy()[0]
                volume_df = volume_df[:-1]
            else:
                last_row = None

        output_path = output_dir / csv_path.name.replace(
            ".csv.gz", "_{}.csv".format(str(volume_size).replace(".", "_"))
        )
        volume_df.write_csv(output_path)
