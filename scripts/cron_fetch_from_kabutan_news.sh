#!/bin/bash
PATH=/home/kitamura/.local/bin${PATH:+:${PATH}}

cd /home/kitamura/work/data_fetcher
echo "Start fetch_jp_news.py"
uv run python scripts/fetch_jp_news.py --days 30
echo "Finish fetch_jp_news.py"
