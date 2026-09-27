#!/bin/bash
PATH=/home/kitamura/.local/bin${PATH:+:${PATH}}

cd /home/kitamura/work/data_fetcher
echo "Start fetch_data_from_oecd.py"
uv run python scripts/fetch_data_from_oecd.py
echo "Finish fetch_data_from_oecd.py"
