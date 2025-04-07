#!/bin/bash

# GCPからbitflyerのデータをダウンロード
echo "Start rsync from gcp"

# 2日前のデータを取得
day=$(date -d '2 days ago' '+%Y%m%d')
rsync -auv gcp:/home/ymyk6602/work/data_fetcher/data/bitflyer/tick/$day /home/kitamura/work/data_fetcher/data/bitflyer/tick/
unset day
echo "Finish rsync from gcp"

#30 6 * * * /home/kitamura/work/stock/scripts/cron_fetch_from_gmo.sh >> /home/kitamura/log.txt 2>&1
#0 7 * * * /home/kitamura/work/stock/scripts/cron_fetch_from_yf.sh >> /home/kitamura/log.txt 2>&1
#0 0 * * * /home/kitamura/work/stock/scripts/cron_fetch_from_rss.sh >> /home/kitamura/log.txt 2>&1
