"""util.py"""

import re


def convert_to_number(val_str: str) -> int | float | None:
    val_str = val_str.replace(",", "")
    res = re.search("-*\d+\.*\d*", val_str)
    if res is None:
        return None
    if "." in val_str:
        return float(res.group(0))
    return int(res.group(0))
