#!/bin/bash
PATH=/home/kitamura/.local/bin${PATH:+:${PATH}}

cd /home/kitamura/work/data_fetcher
echo "Start fetch_data_from_tdnet.py"
poetry run python scripts/fetch_data_from_tdnet.py
echo "Finish fetch_data_from_tdnet.py"
