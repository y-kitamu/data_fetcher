"""notification.py"""

import uuid

import requests

from .constants import PROJECT_ROOT
from .logging import logger

LINE_ACCESS_TOKEN_FILE = PROJECT_ROOT / "cert" / "line_message_api.txt"
LINE_ENDPOINT = "https://api.line.me/v2/bot/message/broadcast"


def notify_to_line(message):
    with open(LINE_ACCESS_TOKEN_FILE, "r") as f:
        access_token = f.read().strip()

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {}".format(access_token),
        "X-Line-Retry-Key": str(uuid.uuid1()),
    }
    data = '{{"messages": [{{"type": "text", "text": "{}"}}]}}'.format(message)
    res = requests.post(LINE_ENDPOINT, headers=headers, data=data)

    if res.status_code == 200:
        ("Message sent successfully!")
    else:
        logger.debug("header: {}".format(headers))
        logger.debugprint("data : {}".format(data))
        logger.debug("{}: {}".format(res.status_code, res.text))
