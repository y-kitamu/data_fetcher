#!/bin/bash
PATH=/home/kitamura/.local/bin${PATH:+:${PATH}}
cd /home/kitamura/work/data_fetcher
echo "Start notifiy_to_line.py"
poetry run python scripts/notify_to_line.py
echo "Finish notifiy_to_line.py"
