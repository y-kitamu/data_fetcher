nohup uv run python ./scripts/fetch_data_from_gmo.py --fx >> logs/gmo.log 2>&1 & 
nohup uv run python ./scripts/fetch_data_from_gmo.py --crypto >> logs/gmo.log 2>&1 & 

nohup uv run python ./scripts/fetch_data_from_bitflyer.py >> logs/bitflyer.log 2>&1 &
nohup uv run python ./scripts/fetch_data_from_bitflyer_book.py >> logs/bitflyer.log 2>&1 &
