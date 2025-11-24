# Data Fetcher - Copilot Instructions

## Repository Overview

Python 3.13+ package for collecting financial data from multiple sources (US/Japanese stocks, crypto exchanges, forex, disclosure databases). ~1.9GB repo, modular architecture with fetchers, readers, processors, and domain models.

**Stack:** Python 3.13+, Polars, Pandas, FastAPI, Selenium, yfinance, requests  
**Package Manager:** pip + pyproject.toml (NO requirements.txt, setup.py, or poetry.lock)

## Critical: Python 3.13+ Required

**MUST use Python >=3.13** (specified in pyproject.toml). Installation fails otherwise. Do not modify version requirement.

## Build & Validation

### Installation (Required First Step)

```bash
pip install -e .  # Takes 2-5 minutes
```

**Common issue:** Python version error means you need 3.13+

### Linting

```bash
ruff check .  # Primary linter configured (in dev dependencies)
```

Do not add other linters unless requested.

### Testing

**Test infrastructure exists** using pytest. Test files in `tests/` directory include:

- `test_base_fetcher.py` - BaseFetcher class tests
- `test_fetcher.py` - Fetcher factory tests
- `test_session.py` - HTTP session tests
- `test_volume_bar.py` - Volume bar tests

Run tests with:

```bash
pytest tests/
```

## Project Structure

### Key Directories

- `src/data_fetcher/` - Main package with modular architecture:
  - **`core/`** - Core infrastructure:
    - `base_fetcher.py` - Base class for all fetchers
    - `base_reader.py` - Base class for all readers
    - `constants.py` - PROJECT_ROOT, ticker paths
    - `session.py` - Rate-limited HTTP sessions (requests_ratelimiter + requests_cache)
    - `debug.py` - Debugging utilities
    - `notification.py` - Notification functionality
    - `ticker_list.py` - Ticker list management
    - `volume_bar.py` - Volume bar calculations
  - **`fetchers/`** - Data fetchers organized by category:
    - `crypto/` - Cryptocurrency: `binance.py`, `bitflyer.py`, `gmo.py`
    - `stocks/` - Stock markets: `kabutan.py`, `rakuten.py`
    - `forex/` - Foreign exchange: `histdata.py`
    - `disclosure/` - Disclosure databases: `edinet.py`
    - `other/` - Other data sources
    - `__init__.py` - Factory: `get_fetcher(source)`, `get_available_sources()`
  - **`readers/`** - Data readers for saved data:
    - `histdata.py`, `yfinance.py`, `kabutan.py`
    - `__init__.py` - Factory: `get_reader(source)`
  - **`processors/`** - Data processors:
    - `cftc.py`, `sbi.py`, `yfinance.py`
  - **`domains/`** - Domain models:
    - `edinet/`, `jp_stocks/`, `kabutan/`, `tdnet/`
  - `__init__.py` - Logger setup (loguru), exports core modules and functions
- `scripts/` - Data update scripts: `update_financial_data*.py`, `fetch_data_from_*.py`, `cron_*.sh` (note: cron scripts use `uv` and hardcoded paths)
- `tests/` - pytest-based tests: `test_base_fetcher.py`, `test_fetcher.py`, `test_session.py`, `test_volume_bar.py`
- `data/` - Fetched data (mostly gitignored: various source directories)
- `docker/` - docker-compose.yml for Selenium Chrome (ports 4444, 7900)

### GitHub Workflows (.github/workflows/)

- `ci.yml` - Daily 5:00 UTC: runs `update_financial_data.py` (US stocks)
- `ci_jp.yml` - Daily 8:00 UTC: runs 5 scripts for Japanese stocks:
  - `fetch_data_from_edinet.py`
  - `update_jp_tickers_list.py`
  - `update_financial_data_jp.py`
  - `fetch_data_from_kabutan.py`
  - `divide_stocks_jp.py`
- Both use `.github/actions/install_deps` which: sets up Python 3.13, installs Chrome, runs `pip install -e .`

## CI Validation Steps (from .github/actions/install_deps)

1. Setup Python 3.13
2. Install Chrome: `sudo apt-get install google-chrome-stable`
3. Run: `pip install -e .`

Changes must be compatible with this CI setup.

## Key Patterns & Architecture

**Fetcher Pattern:** All fetchers inherit `BaseFetcher` (in `core/base_fetcher.py`). Factory in `fetchers/__init__.py`: `get_fetcher(source)` returns fetchers for binance, gmo, bitflyer, histdata, kabutan, rakuten. Available sources: `get_available_sources()`.

**Reader Pattern:** All readers inherit `BaseReader` (in `core/base_reader.py`). Factory in `readers/__init__.py`: `get_reader(source)` returns readers for histdata, yfinance, kabutan.

**Sessions:** `core/session.py` provides rate-limited cached HTTP sessions via `get_session(max_requests_per_second=10)`. Cache at `PROJECT_ROOT/cache/requests.cache`.

**Logging:** Use `data_fetcher.logger` (loguru, configured in `__init__.py`). Do not use print().

**Constants:** `core/constants.py` has PROJECT_ROOT (computed from package path), JP_TICKERS_PATH, US_TICKERS_PATH.

## Common Operations

**Add Fetcher:** Create file in appropriate category dir (crypto/, stocks/, forex/, disclosure/, other/) in src/data_fetcher/fetchers/, subclass BaseFetcher, update fetchers/**init**.py factory, optionally add script.

**Add Reader:** Create file in src/data_fetcher/readers/, subclass BaseReader, update readers/**init**.py factory.

**Change Dependencies:** Edit pyproject.toml only, then `pip install -e .`. Never create requirements.txt.

**Run Scripts:** `python scripts/script_name.py` or `uv run python scripts/script_name.py`

**Data Files:** Stored in data/{source}/. Check .gitignore before committing (most subdirs ignored).

## Common Issues

**Python version error:** Must use 3.13+. Cannot workaround.

**Selenium/Chrome:** Some fetchers need Chrome (installed by CI). May need docker-compose selenium service (ports 4444, 7900).

**Rate limiting:** Adjust max_requests_per_second in get_session() calls. Check cache exists. Note API-specific limits (e.g., EDINET).

**Import errors:** Reinstall `pip install -e .`, check **init**.py imports updated.

## Workflow for Changes

**Before:** Check Python 3.13+, run `pip install -e .`, `ruff check .`

**During:** Use data_fetcher.logger (not print), relative imports, polars/pandas style. Update **init**.py for new modules. Follow existing patterns in core/, fetchers/, readers/.

**After:** Run `ruff check .`, `pytest tests/` for validation, manually test scripts, consider CI impact (daily runs that commit results).

**Never:**

- Add tests/linters unless requested
- Create requirements.txt, setup.py, poetry.lock
- Modify Python >=3.13 requirement
- Hardcode paths like /home/kitamura/ in production code

## Quick Reference Commands

```bash
# Install (required first step)
pip install -e .

# Lint with ruff
ruff check .
ruff check src/data_fetcher/

# Run tests
pytest tests/

# Run a data update script
python scripts/update_financial_data.py
python scripts/fetch_data_from_edinet.py

# Check Python version
python --version  # Must be 3.13+

# Start Selenium container (if needed)
cd docker && docker-compose up -d

# Check project structure
ls -la src/data_fetcher/
ls -la scripts/
```

## Trust These Instructions

These instructions are based on thorough repository analysis. Only search for additional information if:

- These instructions are incomplete for your specific task
- You encounter errors not covered here
- The repository structure has changed significantly

For routine coding tasks, trust this document and minimize exploration time.
