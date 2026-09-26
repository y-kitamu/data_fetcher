"""Two-scenario DCF :

bear:  net_cash + fcf_base / R
bull:  net_cash + sum_{k=1..N} fcf_base*(1+g)^k / (1+R)^k
              + (fcf_base*(1+g)^N / R) / (1+R)^N   (terminal value at year N)

Both scenarios are null (with a NEGATIVE_FCF warning left to the caller) when
fcf_base <= 0 - normalized_value.py's normalized/peak scenarios should be
used instead in that case.
"""

from __future__ import annotations

from pydantic import BaseModel


class DcfResult(BaseModel):
    discount_rate: float
    net_cash: float | None
    fcf_base: float | None
    bear_total: float | None
    bear_per_share: float | None
    bull_growth_rate: float
    bull_growth_years: int
    bull_total: float | None
    bull_per_share: float | None


def compute_dcf(
    net_cash: float | None,
    fcf_base: float | None,
    shares_outstanding: float | None,
    discount_rate: float = 0.10,
    bull_growth_rate: float = 0.20,
    bull_growth_years: int = 5,
) -> DcfResult:
    if net_cash is None or fcf_base is None or fcf_base <= 0:
        return DcfResult(
            discount_rate=discount_rate,
            net_cash=net_cash,
            fcf_base=fcf_base,
            bear_total=None,
            bear_per_share=None,
            bull_growth_rate=bull_growth_rate,
            bull_growth_years=bull_growth_years,
            bull_total=None,
            bull_per_share=None,
        )

    bear_total = net_cash + fcf_base / discount_rate

    growth_phase = sum(
        fcf_base * (1 + bull_growth_rate) ** k / (1 + discount_rate) ** k
        for k in range(1, bull_growth_years + 1)
    )
    terminal_value = (
        fcf_base * (1 + bull_growth_rate) ** bull_growth_years / discount_rate
    ) / (1 + discount_rate) ** bull_growth_years
    bull_total = net_cash + growth_phase + terminal_value

    def per_share(total: float) -> float | None:
        return total * 1e6 / shares_outstanding if shares_outstanding else None

    return DcfResult(
        discount_rate=discount_rate,
        net_cash=net_cash,
        fcf_base=fcf_base,
        bear_total=bear_total,
        bear_per_share=per_share(bear_total),
        bull_growth_rate=bull_growth_rate,
        bull_growth_years=bull_growth_years,
        bull_total=bull_total,
        bull_per_share=per_share(bull_total),
    )
