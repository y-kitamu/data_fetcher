#!/bin/bash
# PATH=/home/kitamura/.local/bin${PATH:+:${PATH}}
echo "Start rsync from wsl"
cd /home/kitamura/work/stock
rsync -auv wsl:/mnt/d/stock/data/minutes /home/kitamura/work/data_fetcher/data/rakuten/
echo "Finish rsync from wsl"

#30 6 * * * /home/kitamura/work/stock/scripts/cron_fetch_from_gmo.sh >> /home/kitamura/log.txt 2>&1
#0 7 * * * /home/kitamura/work/stock/scripts/cron_fetch_from_yf.sh >> /home/kitamura/log.txt 2>&1
#0 0 * * * /home/kitamura/work/stock/scripts/cron_fetch_from_rss.sh >> /home/kitamura/log.txt 2>&1
