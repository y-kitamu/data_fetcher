#!/bin/bash
PATH=/home/kitamura/.local/bin${PATH:+:${PATH}}

cd /home/kitamura/work/data_fetcher
echo "Start fetch_data_from_histdata.py"
uv run python scripts/fetch_data_from_histdata.py
echo "Finish fetch_data_from_histdata.py"
