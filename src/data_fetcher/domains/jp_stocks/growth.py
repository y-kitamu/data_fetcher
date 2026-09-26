"""Peak-to-peak revenue CAGR :
the annualized growth rate between the most recent local revenue peak and
the most recent local peak that occurred before it, over the last 15 years.
A "local peak" is a year whose revenue is strictly greater than every other
year within 2 years on either side.
"""

from __future__ import annotations

_WINDOW_YEARS = 15
_LOCAL_PEAK_SPAN = 2
_MIN_DATA_POINTS = 5


def peak_to_peak_revenue_cagr(revenue: list[float | None]) -> float | None:
    windowed = revenue[-_WINDOW_YEARS:]
    indices = [i for i, v in enumerate(windowed) if v is not None]
    if len(indices) < _MIN_DATA_POINTS:
        return None

    global_peak_index = max(indices, key=lambda i: windowed[i])
    candidates = [
        i for i in indices if i < global_peak_index and _is_local_peak(windowed, i)
    ]
    if not candidates:
        return None
    prior_peak_index = candidates[-1]

    years = global_peak_index - prior_peak_index
    prior_value = windowed[prior_peak_index]
    latest_value = windowed[global_peak_index]
    if years <= 0 or prior_value is None or prior_value <= 0 or latest_value is None:
        return None
    return (latest_value / prior_value) ** (1 / years) - 1


def _is_local_peak(
    windowed: list[float | None], index: int, span: int = _LOCAL_PEAK_SPAN
) -> bool:
    value = windowed[index]
    if value is None:
        return False
    lo, hi = max(0, index - span), min(len(windowed), index + span + 1)
    return all(
        windowed[j] is None or windowed[j] < value for j in range(lo, hi) if j != index
    )
