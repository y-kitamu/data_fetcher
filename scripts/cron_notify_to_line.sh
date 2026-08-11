#!/bin/bash
# Filename kept as-is on purpose: crontab points at this path, so renaming it
# to match notify_data_status.py would require editing the crontab entry too.
PATH=/home/kitamura/.local/bin${PATH:+:${PATH}}
cd /home/kitamura/work/data_fetcher
echo "Start notify_data_status.py"
uv run python scripts/notify_data_status.py
echo "Finish notify_data_status.py"
