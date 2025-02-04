"""notify_to_line.py"""

import datetime
import uuid
from pathlib import Path

import requests

import data_fetcher

ACCESS_TOKEN_FILE = (
    data_fetcher.constants.PROJECT_ROOT / "cert" / "line_message_api.txt"
)
ENDPOINT = "https://api.line.me/v2/bot/message/broadcast"


def get_latest_dates_data_number() -> dict[str, str]:
    # dirnames = ["minutes", "minutes_gmo", "minutes_yf"]
    data_root = data_root = data_fetcher.constants.PROJECT_ROOT / "data"
    results: dict[str, str] = {}
    for data_src_dir in data_root.glob("*"):
        if not data_src_dir.is_dir():
            continue

        for data_dir in data_src_dir.glob("*"):
            if not data_dir.is_dir():
                continue
            dirname = f"{data_src_dir.name}/{data_dir.name}"
            dirs = sorted([p for p in data_dir.glob("20*") if p.is_dir()])
            if len(dirs) == 0:
                # results[dirname] = "No data directory found."
                continue
            latest_path = dirs[-1]
            if not latest_path.is_dir():
                # results[dirname] = "csv"
                continue
            num_data = len(list(latest_path.glob("*.csv*")))
            results[dirname] = f"{latest_path.name}, {num_data}"

    return results


if __name__ == "__main__":
    with open(ACCESS_TOKEN_FILE, "r") as f:
        access_token = f.read().strip()

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {}".format(access_token),
        "X-Line-Retry-Key": str(uuid.uuid1()),
    }
    data_nums = get_latest_dates_data_number()
    text = "date: {}\\n".format(datetime.date.today().strftime("%Y%m%d"))
    for key, val in data_nums.items():
        text += f"{key} : {val}\\n"
    data = '{{"messages": [{{"type": "text", "text": "{}"}}]}}'.format(text)
    res = requests.post(ENDPOINT, headers=headers, data=data)

    if res.status_code == 200:
        print("Message sent successfully!")
    else:
        print("header: {}".format(headers))
        print("data : {}".format(data))
        print("{}: {}".format(res.status_code, res.text))
