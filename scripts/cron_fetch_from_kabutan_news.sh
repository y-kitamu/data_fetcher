#!/bin/bash
PATH=/home/kitamura/.local/bin${PATH:+:${PATH}}

cd /home/kitamura/work/data_fetcher
echo "Start fetch_jp_news.py"
uv run python scripts/fetch_jp_news.py --days 30
uv run python scripts/fetch_data_from_kabutan.py
uv run python scripts/update_financial_data_jp.py
echo "Finish fetch_jp_news.py"
