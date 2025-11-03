# Data Fetcher Architecture

## Module Organization

The `data_fetcher` package is organized into the following categories:

### Base Classes

- **`base_fetcher.py`**: Base class for all data fetchers. Provides common interface for fetching financial data from various sources.
- **`base_reader.py`**: Base class for all data readers. Provides common interface for reading stored financial data.

### Fetcher Modules

Modules that implement the `BaseFetcher` interface for fetching data from various sources:

- **`binance/`**: Binance cryptocurrency exchange fetcher
- **`bitflyer/`**: Bitflyer cryptocurrency exchange fetcher  
- **`gmo/`**: GMO cryptocurrency exchange fetcher
- **`histdata/`**: Historical forex data fetcher
- **`kabutan/`**: Japanese stock data fetcher (Kabutan)
- **`rakuten/`**: Rakuten securities data fetcher

Each fetcher module contains:
- `<module>_fetcher.py`: Implementation of the fetcher class (e.g., `BinanceFetcher`)
- `__init__.py`: Exports the fetcher class

### Reader Modules

Modules that implement the `BaseReader` interface for reading stored data:

- **`readers/`**: Directory containing reader implementations
  - `histdata.py`: Reader for HistData stored data
  - `yfinance.py`: Reader for yfinance stored data

### Utility and API Modules

Modules that provide utilities, APIs, or helper functions:

- **`utils/`**: General utility functions
  - `converters.py`: Data format conversion utilities (pandas to polars, etc.)
  - `market.py`: Market-related utilities
  - `market_capital.py`: Market capitalization utilities
  
- **`edinet/`**: EDINET API wrapper for Japanese financial statements
  - `api.py`: API client implementation
  
- **`tdnet/`**: TDnet (Timely Disclosure Network) scraper for Japanese market disclosures
  - `fetcher.py`: Web scraping utilities for TDnet
  - `document.py`: Document processing
  - `numeric_data.py`: Numeric data extraction
  - `taxonomy_element.py`: XBRL taxonomy handling
  - `constants/`: TDnet-specific constants
  
- **`sbi/`**: SBI Securities utility functions
  - Utilities for processing SBI data files
  
- **`yfinance/`**: yfinance utility functions
  - Re-exports `pd_to_pl` from `utils.converters` for backward compatibility

### Core Modules

- **`fetcher.py`**: Factory function (`get_fetcher()`) for creating fetcher instances
- **`session.py`**: HTTP session management with rate limiting and caching
- **`constants.py`**: Project-wide constants (PROJECT_ROOT, ticker paths, etc.)
- **`ticker_list.py`**: Ticker list management utilities
- **`volume_bar.py`**: Volume bar conversion utilities
- **`notification.py`**: Notification utilities
- **`debug.py`**: Debug utilities

## Design Patterns

### Fetcher Pattern

All data fetchers inherit from `BaseFetcher` and implement:
- `available_tickers`: Property returning list of available symbols
- `get_latest_date(symbol)`: Get latest available date for a symbol
- `get_earliest_date(symbol)`: Get earliest available date for a symbol
- `fetch_ticker(symbol, ...)`: Fetch tick data
- `fetch_ohlc(symbol, interval, ...)`: Fetch OHLC data
- `fetch_volume_bar(symbol, volume_size, ...)`: Fetch volume bar data

### Reader Pattern

All data readers inherit from `BaseReader` and implement:
- `available_tickers`: Property returning list of available symbols
- `get_latest_date(symbol)`: Get latest available date for stored data
- `get_earliest_date(symbol)`: Get earliest available date for stored data
- `read_ohlc(symbol, interval, ...)`: Read OHLC data from storage
- `read_ticker(symbol, ...)`: Read tick data from storage

### Factory Pattern

The `fetcher.py` module provides a factory function:
```python
from data_fetcher import fetcher

fetcher_instance = fetcher.get_fetcher("binance")
```

## Import Structure

### Recommended imports:

```python
# Import the package
import data_fetcher

# Use the factory to get a fetcher
fetcher = data_fetcher.fetcher.get_fetcher("binance")

# Use readers
reader = data_fetcher.readers.YFinanceReader()

# Use utilities
df = data_fetcher.utils.converters.pd_to_pl(pandas_df)

# Use constants
project_root = data_fetcher.constants.PROJECT_ROOT

# Use logger
data_fetcher.logger.info("Message")
```

### Backward Compatibility

The `yfinance` module maintains backward compatibility by re-exporting utilities:
```python
# This still works for backward compatibility
df = data_fetcher.yfinance.pd_to_pl(pandas_df)
```

## Module Dependencies

```
base_fetcher.py
    ├── volume_bar.py (convert_ticker_to_volume_bar)
    └── Used by: binance, bitflyer, gmo, histdata, kabutan, rakuten

base_reader.py
    ├── base_fetcher.py (convert_timedelta_to_str)
    └── Used by: readers/histdata.py, readers/yfinance.py

fetcher.py (Factory)
    └── Imports: binance, gmo, histdata, kabutan

utils/converters.py
    └── Re-exported by: yfinance/__init__.py

session.py
    └── Used by: most fetcher modules for HTTP requests
```

## Notes

- **Python Version**: Requires Python >= 3.13
- **Data Storage**: Each module stores data in `PROJECT_ROOT/data/<source>/`
- **Caching**: HTTP requests are cached via `session.py` in `PROJECT_ROOT/cache/`
- **Logging**: Uses `loguru` configured in `__init__.py`
