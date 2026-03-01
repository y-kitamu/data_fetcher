import math
from typing import Dict, List


def calculate_book_stats(
    bids: List[Dict[str, float]], asks: List[Dict[str, float]], mid_price: float
) -> Dict[str, float]:
    """
    板全体のデータから統計量を計算する。
    bids, asks は {'price': float, 'size': float} のリストを想定し、
    bids は価格の高い順、asks は価格の低い順にソートされていること。

    返される統計量には以下が含まれる:
    - cum_bid_vol_xxpct, cum_ask_vol_xxpct: ミッド価格から±0.xx%以内の累積ボリューム
    - obi_xxpct: 上記の買いと売りのボリュームのオーダーブックインデックス
    - max_bid_vol_wxx, max_ask_vol_wxx: xx levelsのローリングウィンドウの最大ボリュームとその基準価格
    - microprice: (Bid1_Price * Ask1_Size + Ask1_Price * Bid1_Size) / (Bid1_Size + Ask1_Size)
    - bid_vwap, ask_vwap: VWAP (Volume Weighted Average Price) for bids and asks
    - bid_dispersion, ask_dispersion: VWAPからの価格分散（標準偏差）
    """
    stats = {}

    # O(1)アクセスと累積和のための配列準備
    n_bids = len(bids)
    n_asks = len(asks)

    bid_cum_sizes = [0.0] * (n_bids + 1)
    ask_cum_sizes = [0.0] * (n_asks + 1)

    bid_vol_1, bid_vol_5, bid_vol_10 = 0.0, 0.0, 0.0
    ask_vol_1, ask_vol_5, ask_vol_10 = 0.0, 0.0, 0.0

    total_bid_size = 0.0
    total_bid_price_size = 0.0
    total_bid_price2_size = 0.0

    total_ask_size = 0.0
    total_ask_price_size = 0.0
    total_ask_price2_size = 0.0

    inv_mid = 1.0 / mid_price if mid_price > 0 else 0.0

    # Bid 側のループ処理 (スライス生成を避けて O(N))
    for i in range(n_bids):
        p = bids[i]["price"]
        s = bids[i]["size"]

        dist = (mid_price - p) * inv_mid

        total_bid_size += s
        total_bid_price_size += p * s
        total_bid_price2_size += p * p * s

        bid_cum_sizes[i + 1] = bid_cum_sizes[i] + s

        if dist <= 0.001:
            bid_vol_1 += s
        if dist <= 0.005:
            bid_vol_5 += s
        if dist <= 0.010:
            bid_vol_10 += s  # ensure 0.01 threshold

    # Ask 側のループ処理
    for i in range(n_asks):
        p = asks[i]["price"]
        s = asks[i]["size"]

        dist = (p - mid_price) * inv_mid

        total_ask_size += s
        total_ask_price_size += p * s
        total_ask_price2_size += p * p * s

        ask_cum_sizes[i + 1] = ask_cum_sizes[i] + s

        if dist <= 0.001:
            ask_vol_1 += s
        if dist <= 0.005:
            ask_vol_5 += s
        if dist <= 0.010:
            ask_vol_10 += s

    # ローリングウィンドウで壁(最大ボリューム)とその基準価格を取得
    # スライスを使わずに累積和の差分で O(1) でボリュームを計算する
    # w=1
    mb_vol_w1, ma_vol_w1 = 0.0, 0.0
    mb_price_w1, ma_price_w1 = 0.0, 0.0
    for i in range(n_bids):
        v = bid_cum_sizes[i + 1] - bid_cum_sizes[i]
        if v > mb_vol_w1 + 1e-9:
            mb_vol_w1 = v
            mb_price_w1 = bids[i]["price"]
    for i in range(n_asks):
        v = ask_cum_sizes[i + 1] - ask_cum_sizes[i]
        if v > ma_vol_w1 + 1e-9:
            ma_vol_w1 = v
            ma_price_w1 = asks[i]["price"]

    # w=5
    mb_vol_w5, ma_vol_w5 = 0.0, 0.0
    mb_price_w5, ma_price_w5 = 0.0, 0.0
    for i in range(n_bids - 4):
        v = bid_cum_sizes[i + 5] - bid_cum_sizes[i]
        if v > mb_vol_w5 + 1e-9:
            mb_vol_w5 = v
            mb_price_w5 = bids[i]["price"]
    for i in range(n_asks - 4):
        v = ask_cum_sizes[i + 5] - ask_cum_sizes[i]
        if v > ma_vol_w5 + 1e-9:
            ma_vol_w5 = v
            ma_price_w5 = asks[i]["price"]

    # w=10
    mb_vol_w10, ma_vol_w10 = 0.0, 0.0
    mb_price_w10, ma_price_w10 = 0.0, 0.0
    for i in range(n_bids - 9):
        v = bid_cum_sizes[i + 10] - bid_cum_sizes[i]
        if v > mb_vol_w10 + 1e-9:
            mb_vol_w10 = v
            mb_price_w10 = bids[i]["price"]
    for i in range(n_asks - 9):
        v = ask_cum_sizes[i + 10] - ask_cum_sizes[i]
        if v > ma_vol_w10 + 1e-9:
            ma_vol_w10 = v
            ma_price_w10 = asks[i]["price"]

    # VWAP・分散の計算
    # 浮動小数点演算の誤差により負になる可能性を max(0.0, var) で回避
    bid_vwap = (
        total_bid_price_size / total_bid_size if total_bid_size > 0 else mid_price
    )
    ask_vwap = (
        total_ask_price_size / total_ask_size if total_ask_size > 0 else mid_price
    )

    bid_dispersion = 0.0
    if total_bid_size > 0:
        var = (
            total_bid_price2_size
            - 2 * bid_vwap * total_bid_price_size
            + (bid_vwap**2) * total_bid_size
        ) / total_bid_size
        bid_dispersion = math.sqrt(max(0.0, var))

    ask_dispersion = 0.0
    if total_ask_size > 0:
        var = (
            total_ask_price2_size
            - 2 * ask_vwap * total_ask_price_size
            + (ask_vwap**2) * total_ask_size
        ) / total_ask_size
        ask_dispersion = math.sqrt(max(0.0, var))

    # microprice
    # (Bid1_Price * Ask1_Size + Ask1_Price * Bid1_Size) / (Bid1_Size + Ask1_Size)
    if bids and asks:
        b1_price = bids[0]["price"]
        b1_size = bids[0]["size"]
        a1_price = asks[0]["price"]
        a1_size = asks[0]["size"]
        if (b1_size + a1_size) > 0:
            microprice = (b1_price * a1_size + a1_price * b1_size) / (b1_size + a1_size)
        else:
            microprice = mid_price
    else:
        microprice = mid_price

    # 辞書への結果の格納
    stats["cum_bid_vol_01pct"] = round(bid_vol_1, 6)
    stats["cum_ask_vol_01pct"] = round(ask_vol_1, 6)
    stats["obi_01pct"] = (
        round((bid_vol_1 - ask_vol_1) / (bid_vol_1 + ask_vol_1), 6)
        if (bid_vol_1 + ask_vol_1) > 0
        else 0.0
    )

    stats["cum_bid_vol_05pct"] = round(bid_vol_5, 6)
    stats["cum_ask_vol_05pct"] = round(ask_vol_5, 6)
    stats["obi_05pct"] = (
        round((bid_vol_5 - ask_vol_5) / (bid_vol_5 + ask_vol_5), 6)
        if (bid_vol_5 + ask_vol_5) > 0
        else 0.0
    )

    stats["cum_bid_vol_10pct"] = round(bid_vol_10, 6)
    stats["cum_ask_vol_10pct"] = round(ask_vol_10, 6)
    stats["obi_10pct"] = (
        round((bid_vol_10 - ask_vol_10) / (bid_vol_10 + ask_vol_10), 6)
        if (bid_vol_10 + ask_vol_10) > 0
        else 0.0
    )

    stats["max_bid_vol_w1"] = round(mb_vol_w1, 6)
    stats["max_bid_dist_w1"] = round((mid_price - mb_price_w1) * inv_mid, 6)
    stats["max_ask_vol_w1"] = round(ma_vol_w1, 6)
    stats["max_ask_dist_w1"] = round((ma_price_w1 - mid_price) * inv_mid, 6)

    stats["max_bid_vol_w5"] = round(mb_vol_w5, 6)
    stats["max_bid_dist_w5"] = round((mid_price - mb_price_w5) * inv_mid, 6)
    stats["max_ask_vol_w5"] = round(ma_vol_w5, 6)
    stats["max_ask_dist_w5"] = round((ma_price_w5 - mid_price) * inv_mid, 6)

    stats["max_bid_vol_w10"] = round(mb_vol_w10, 6)
    stats["max_bid_dist_w10"] = round((mid_price - mb_price_w10) * inv_mid, 6)
    stats["max_ask_vol_w10"] = round(ma_vol_w10, 6)
    stats["max_ask_dist_w10"] = round((ma_price_w10 - mid_price) * inv_mid, 6)

    stats["microprice"] = round(microprice, 6)
    stats["bid_vwap"] = round(bid_vwap, 6)
    stats["ask_vwap"] = round(ask_vwap, 6)
    stats["bid_dispersion"] = round(bid_dispersion, 6)
    stats["ask_dispersion"] = round(ask_dispersion, 6)

    return stats
