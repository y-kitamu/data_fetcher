"""CFTC (Commodity Futures Trading Commission) data utilities.

This module provides utilities for reading and processing CFTC Commitments of
Traders (COT) reports, which provide weekly data on futures market positions.

The module includes column mappings for converting CFTC report format to a
more concise internal format, and functions for reading CSV files.
"""

import csv
import datetime
from pathlib import Path

import polars as pl

int_columns = [
    ["Open_Interest_All", "OpenInterest"],
    ["Dealer_Positions_Long_All", "DealerLong"],
    ["Dealer_Positions_Short_All", "DealerShort"],
    ["Dealer_Positions_Spread_All", "DealerSpread"],
    ["Asset_Mgr_Positions_Long_All", "AssetMgrLong"],
    ["Asset_Mgr_Positions_Short_All", "AssetMgrShort"],
    ["Asset_Mgr_Positions_Spread_All", "AssetMgrSpread"],
    ["Lev_Money_Positions_Long_All", "LevMoneyLong"],
    ["Lev_Money_Positions_Short_All", "LevMoneyShort"],
    ["Lev_Money_Positions_Spread_All", "LevMoneySpread"],
    ["Other_Rept_Positions_Long_All", "OtherReptLong"],
    ["Other_Rept_Positions_Short_All", "OtherReptShort"],
    ["Other_Rept_Positions_Spread_All", "OtherReptSpread"],
    ["NonRept_Positions_Long_All", "NonReptLong"],
    ["NonRept_Positions_Short_All", "NonReptShort"],
    ["Change_in_Open_Interest_All", "ChangeInOpenInterest"],
    ["Change_in_Dealer_Long_All", "ChangeInDealerLong"],
    ["Change_in_Dealer_Short_All", "ChangeInDealerShort"],
    ["Change_in_Dealer_Spread_All", "ChangeInDealerSpread"],
    ["Change_in_Asset_Mgr_Long_All", "ChangeInAssetMgrLong"],
    ["Change_in_Asset_Mgr_Short_All", "ChangeInAssetMgrShort"],
    ["Change_in_Asset_Mgr_Spread_All", "ChangeInAssetMgrSpread"],
    ["Change_in_Lev_Money_Long_All", "ChangeInLevMoneyLong"],
    ["Change_in_Lev_Money_Short_All", "ChangeInLevMoneyShort"],
    ["Change_in_Lev_Money_Spread_All", "ChangeInLevMoneySpread"],
    ["Change_in_Other_Rept_Long_All", "ChangeInOtherReptLong"],
    ["Change_in_Other_Rept_Short_All", "ChangeInOtherReptShort"],
    ["Change_in_Other_Rept_Spread_All", "ChangeInOtherReptSpread"],
    ["Change_in_NonRept_Long_All", "ChangeInNonReptLong"],
    ["Change_in_NonRept_Short_All", "ChangeInNonReptShort"],
    ["Traders_Tot_All", "TradersTot"],
    ["Traders_Dealer_Long_All", "TradersDealerLong"],
    ["Traders_Dealer_Short_All", "TradersDealerShort"],
    ["Traders_Dealer_Spread_All", "TradersDealerSpread"],
    ["Traders_Asset_Mgr_Long_All", "TradersAssetMgrLong"],
    ["Traders_Asset_Mgr_Short_All", "TradersAssetMgrShort"],
    ["Traders_Asset_Mgr_Spread_All", "TradersAssetMgrSpread"],
    ["Traders_Lev_Money_Long_All", "TradersLevMoneyLong"],
    ["Traders_Lev_Money_Short_All", "TradersLevMoneyShort"],
    ["Traders_Lev_Money_Spread_All", "TradersLevMoneySpread"],
    ["Traders_Other_Rept_Long_All", "TradersOtherReptLong"],
    ["Traders_Other_Rept_Short_All", "TradersOtherReptShort"],
    ["Traders_Other_Rept_Spread_All", "TradersOtherReptSpread"],
    ["Traders_Tot_Rept_Long_All", "TradersTotReptLong"],
    ["Traders_Tot_Rept_Short_All", "TradersTotReptShort"],
]

float_columns = [
    ["Conc_Gross_LE_4_TDR_Long_All", "ConcGross4Long"],
    ["Conc_Gross_LE_4_TDR_Short_All", "ConcGross4Short"],
    ["Conc_Gross_LE_8_TDR_Long_All", "ConcGross8Long"],
    ["Conc_Gross_LE_8_TDR_Short_All", "ConcGross8Short"],
    ["Conc_Net_LE_4_TDR_Long_All", "ConcNet4Long"],
    ["Conc_Net_LE_4_TDR_Short_All", "ConcNet4Short"],
    ["Conc_Net_LE_8_TDR_Long_All", "ConcNet8Long"],
    ["Conc_Net_LE_8_TDR_Short_All", "ConcNet8Short"],
]


def read_csv(
    filepath: Path, target_column: str = "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE"
) -> pl.DataFrame:
    with open(filepath) as f:
        csv_reader = csv.reader(f)
        lines = list(csv_reader)

    header = lines[0]
    df = pl.DataFrame(lines[1:], schema=header, orient="row")

    converted_df = (
        df.filter(pl.col("Market_and_Exchange_Names").str.contains(target_column))
        .select(
            pl.col(header[2]).str.strptime(pl.Date, "%Y-%m-%d").alias("ReportDate"),
            *[
                pl.col(name).str.strip_chars().cast(pl.Int64, strict=False).alias(alias)
                for name, alias in int_columns
            ],
            *[
                pl.col(name)
                .str.strip_chars()
                .cast(pl.Float64, strict=False)
                .alias(alias)
                for name, alias in float_columns
            ],
        )
        .with_columns(
            (pl.col("ReportDate") + datetime.timedelta(days=4)).alias("ReleaseDate")
        )
    )
    return converted_df
