def is_limit_high(prev_close: float, target: float) -> bool:
    """ """
    if is_limit(prev_close, target):
        return prev_close < target
    return False


def is_limit_low(prev_close: float, target: float) -> bool:
    """ """
    if is_limit(prev_close, target):
        return prev_close > target
    return False


def is_limit(prev_close: float, target: float) -> bool:
    prev_close, target = int(prev_close), int(target)
    diff = abs(prev_close - target)
    return diff == int(get_limit_range(prev_close))


def get_limit_range(prev_close: float) -> float:
    if prev_close < 100:
        return 30
    elif prev_close < 200:
        return 50
    elif prev_close < 500:
        return 80
    elif prev_close < 700:
        return 100
    elif prev_close < 1000:
        return 150
    elif prev_close < 1500:
        return 300
    elif prev_close < 2000:
        return 400
    elif prev_close < 3000:
        return 500
    elif prev_close < 5000:
        return 700
    elif prev_close < 7000:
        return 1000
    elif prev_close < 10000:
        return 1500
    elif prev_close < 15000:
        return 3000
    elif prev_close < 20000:
        return 4000
    elif prev_close < 30000:
        return 5000
    elif prev_close < 50000:
        return 7000
    elif prev_close < 70000:
        return 10000
    elif prev_close < 100000:
        return 15000
    elif prev_close < 150000:
        return 30000
    elif prev_close < 200000:
        return 40000
    elif prev_close < 300000:
        return 50000
    elif prev_close < 500000:
        return 70000
    elif prev_close < 700000:
        return 100000
    elif prev_close < 1000000:
        return 150000
    elif prev_close < 1500000:
        return 300000
    elif prev_close < 2000000:
        return 400000
    elif prev_close < 3000000:
        return 500000
    elif prev_close < 5000000:
        return 700000
    elif prev_close < 7000000:
        return 1000000
    elif prev_close < 10000000:
        return 1500000
    elif prev_close < 15000000:
        return 3000000
    elif prev_close < 20000000:
        return 4000000
    elif prev_close < 30000000:
        return 5000000
    elif prev_close < 50000000:
        return 7000000
    else:
        return 10000000
