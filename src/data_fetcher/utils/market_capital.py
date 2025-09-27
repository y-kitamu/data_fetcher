"""market_capital.py"""

# import datetime

# import polars as pl

# from ..constants import PROJECT_ROOT
# from ..kabutan.kabutan_fetcher import KabutanFetcher

# from ..tdnet.preprocess import preprocess_csv


# def get_market_capital(
#     ticker: str, current_date: datetime.datetime = datetime.datetime.now()
# ) -> float | None:
#     """Get the market capitalization of a given ticker on a specific date.
#     Args:
#         ticker (str): The ticker symbol of the stock.
#         current_date (datetime.datetime): The date for which to get the market capitalization.
#     Returns:
#         float | None: The market capitalization of the stock, or None if not available.
#     """
#     csv_file = PROJECT_ROOT / f"data/tdnet/csv/{ticker}.csv"
#     if not csv_file.exists():
#         print(f"TDNet csv file for {ticker} does not exist.")
#         return None
#     dfs = preprocess_csv(csv_file)
#     filtered_df = (
#         pl.concat(dfs[:-1])
#         .filter(pl.col("number_of_shares").is_not_null())
#         .sort(pl.col("filing_date"))
#     )
#     if len(filtered_df) == 0:
#         print("No valid data in", csv_file)
#         return None
#     number_of_shares = filtered_df["number_of_shares"][-1]

#     symbol = csv_file.stem
#     try:
#         df = KabutanFetcher().fetch_ohlc(
#             symbol=symbol,
#             interval=datetime.timedelta(days=1),
#             start_date=current_date - datetime.timedelta(days=30),
#         )
#     except FileNotFoundError:
#         print(f"Data for {symbol} not found.")
#         return None

#     if len(df) == 0:
#         print(f"No OHLC data for {symbol}.")
#         return None

#     return df["close"][-1] * number_of_shares
