#!/bin/bash
# PATH=/home/kitamura/.local/bin${PATH:+:${PATH}}
echo "Start rsync from wsl"
cd /home/kitamura/work/data_fetcher
# 一ヶ月前からのデータを取得
# month=$(date -d '2 month ago' '+%Y%m')
# rsync -auv wsl:/mnt/d/stock/data/minutes/$month* /home/kitamura/work/data_fetcher/data/rakuten/minutes/
month=$(date -d '1 month ago' '+%Y%m')
rsync -auv wsl:/mnt/d/stock/data/minutes/$month* /home/kitamura/work/data_fetcher/data/rakuten/minutes/
month=$(date '+%Y%m')
rsync -auv wsl:/mnt/d/stock/data/minutes/$month* /home/kitamura/work/data_fetcher/data/rakuten/minutes/
unset month
echo "Finish rsync from wsl"

#30 6 * * * /home/kitamura/work/stock/scripts/cron_fetch_from_gmo.sh >> /home/kitamura/log.txt 2>&1
#0 7 * * * /home/kitamura/work/stock/scripts/cron_fetch_from_yf.sh >> /home/kitamura/log.txt 2>&1
#0 0 * * * /home/kitamura/work/stock/scripts/cron_fetch_from_rss.sh >> /home/kitamura/log.txt 2>&1
