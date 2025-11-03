"""__init__.py

This module provides backward compatibility for yfinance utilities.
The actual implementation has been moved to utils.converters.
"""

# Import from new location for backward compatibility
from ..utils.converters import pd_to_pl

__all__ = ["pd_to_pl"]
