"""notification.py"""

import uuid

import requests
from loguru import logger

from .constants import PROJECT_ROOT

LINE_ACCESS_TOKEN_FILE = PROJECT_ROOT / "cert" / "line_message_api.txt"
LINE_ENDPOINT = "https://api.line.me/v2/bot/message/broadcast"


def notify_to_line(message):
    """Send a LINE broadcast message.

    Best-effort: never raises. Callers invoke this synchronously from live
    trading control flow, so a token-file/network failure here must not be
    able to crash or hang them.
    """
    try:
        with open(LINE_ACCESS_TOKEN_FILE, "r") as f:
            access_token = f.read().strip()

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(access_token),
            "X-Line-Retry-Key": str(uuid.uuid1()),
        }
        data = '{{"messages": [{{"type": "text", "text": "{}"}}]}}'.format(message)
        res = requests.post(LINE_ENDPOINT, headers=headers, data=data, timeout=10)

        if res.status_code == 200:
            logger.debug("Message sent successfully!. message = {}".format(message))
        else:
            safe_headers = {**headers, "Authorization": "Bearer ***"}
            logger.warning(
                "LINE notification failed: {} {}. header: {}".format(
                    res.status_code, res.text, safe_headers
                )
            )
    except (OSError, requests.RequestException) as e:
        logger.warning("LINE notification failed: {}".format(e))
