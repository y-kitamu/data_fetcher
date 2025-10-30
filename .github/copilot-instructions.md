# Data Fetcher - Copilot Instructions

## Repository Overview
Python 3.13+ package for collecting financial data from multiple sources (US/Japanese stocks, crypto exchanges, APIs). ~1.9GB repo, 63 Python files, modular fetcher architecture.

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
ruff check .  # Only linter configured (in dev dependencies)
```
Do not add other linters unless requested.

### Testing
**NO test infrastructure exists.** No test files, no pytest config, no test directory. Do not create tests unless explicitly requested.

## Project Structure

### Key Directories
- `src/data_fetcher/` - Main package with fetcher modules:
  - **Fetcher modules:**
    - `binance`
    - `gmo`
    - `histdata`
    - `kabutan`
    - `tdnet`
    - `edinet`
    - `sbi`
    - `rakuten`
    - `yfinance`
    - *(etc. - ensure this matches the actual codebase)*
  - `__init__.py` - Logger setup (uses loguru with custom format)
  - `constants.py` - PROJECT_ROOT, ticker paths
  - `fetcher.py` - Factory: get_fetcher(source)
  - `base_fetcher.py` - Base class for all fetchers
  - `session.py` - Rate-limited HTTP sessions (requests_ratelimiter + requests_cache)
- `scripts/` - 29+ scripts: update_financial_data*.py, fetch_data_from_*.py, cron_*.sh (note: cron scripts use `uv` and hardcoded paths)
- `data/` - Fetched data (mostly gitignored: minutes/, tick/, raw/, download/)
- `docker/` - docker-compose.yml for Selenium Chrome (ports 4444, 7900)

### GitHub Workflows (.github/workflows/)
- `ci.yml` - Daily 5:00 UTC: runs update_financial_data.py (US stocks)
- `ci_jp.yml` - Daily 8:00 UTC: runs 5 scripts for Japanese stocks (edinet, kabutan, tdnet, etc.)
- Both use `.github/actions/install_deps` which: sets up Python 3.13, installs Chrome, runs `pip install -e .`

## CI Validation Steps (from .github/actions/install_deps)
1. Setup Python 3.13
2. Install Chrome: `sudo apt-get install google-chrome-stable`
3. Run: `pip install -e .`

Changes must be compatible with this CI setup.

## Key Patterns & Architecture

**Fetcher Pattern:** All fetchers inherit `BaseFetcher`. Factory in `fetcher.py`: `get_fetcher(source)` returns fetchers for gmo, binance, histdata, kabutan.

**Sessions:** `session.py` provides rate-limited cached HTTP sessions via `get_session(max_requests_per_second=10)`. Cache at `PROJECT_ROOT/cache/requests.cache`.

**Logging:** Use `data_fetcher.logger` (loguru, configured in `__init__.py`). Do not use print().

**Constants:** `constants.py` has PROJECT_ROOT (computed from package path), JP_TICKERS_PATH, US_TICKERS_PATH.

## Common Operations

**Add Fetcher:** Create dir in src/data_fetcher/, subclass BaseFetcher, update fetcher.py + __init__.py, optionally add script.

**Change Dependencies:** Edit pyproject.toml only, then `pip install -e .`. Never create requirements.txt.

**Run Scripts:** `python scripts/script_name.py` or `uv run python scripts/script_name.py`

**Data Files:** Stored in data/{source}/. Check .gitignore before committing (most subdirs ignored).

## Common Issues

**Python version error:** Must use 3.13+. Cannot workaround.

**Selenium/Chrome:** Some fetchers need Chrome (installed by CI). May need docker-compose selenium service (ports 4444, 7900).

**Rate limiting:** Adjust max_requests_per_second in get_session() calls. Check cache exists. Note API-specific limits (e.g., EDINET).

**Import errors:** Reinstall `pip install -e .`, check __init__.py imports updated.

## Workflow for Changes

**Before:** Check Python 3.13+, run `pip install -e .`, `ruff check .`

**During:** Use data_fetcher.logger (not print), relative imports, polars/pandas style. Update __init__.py for new modules.

**After:** Run `ruff check .`, manually test scripts, consider CI impact (daily runs that commit results).

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
