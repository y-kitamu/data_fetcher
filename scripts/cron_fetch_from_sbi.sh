#!/bin/bash
PATH=/home/kitamura/.local/bin${PATH:+:${PATH}}
cd /home/kitamura/work/data_fetcher
echo "Start fetch_data_from_sbi.py"

# clean selenium docker
rm selenium_profile/SingletonLock
rm selenium_profile/SingletonSocket
rm selenium_profile/SingletonCookie

# cd docker
# docker compose restart
# cd ..
# sleep 10

# execute fetch_data_from_sbi.py
docker exec -it data_fetcher-selenium-hub-1 sudo chmod -R 777 /home/seluser/work/selenium_profile/
uv run python scripts/fetch_data_from_sbi.py
echo "Finish fetch_data_from_sbi.py"
