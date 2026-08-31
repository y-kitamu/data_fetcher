#!/bin/bash
# 信用残高一覧（速報/確報）は cron_fetch_from_taisyaku_zandaka.sh 側で
# 1日2回（11時頃・18時半頃）実行すること。本スクリプトは残高履歴の日次更新のみ行う。
PATH=/home/kitamura/.local/bin${PATH:+:${PATH}}

cd /home/kitamura/work/data_fetcher
echo "Start fetch_data_from_taisyaku (history)"
uv run python scripts/fetch_taisyaku_history.py
echo "Finish fetch_data_from_taisyaku (history)"
