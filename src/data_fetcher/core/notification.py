"""notification.py"""

import base64
import uuid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from loguru import logger

from .constants import PROJECT_ROOT

LINE_ACCESS_TOKEN_FILE = PROJECT_ROOT / "cert" / "line_message_api.txt"
LINE_ENDPOINT = "https://api.line.me/v2/bot/message/broadcast"

GMAIL_SEND_TOKEN_FILE = PROJECT_ROOT / "cert" / "gmail_send_token.json"
GMAIL_RECIPIENT_FILE = PROJECT_ROOT / "cert" / "gmail_notify_address.txt"
GMAIL_SEND_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


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


def notify_to_gmail(
    message, subject="data_fetcher notification", to=None, is_html=False
):
    """Send `message` as a plain-text email via the Gmail API.

    `to` defaults to the single address stored in `GMAIL_RECIPIENT_FILE`.
    Authentication uses an OAuth2 token carrying the `gmail.send` scope (not
    SMTP / an app password); run `scripts/generate_gmail_send_token.py` once
    to create `GMAIL_SEND_TOKEN_FILE`.

    Best-effort: never raises, mirroring `notify_to_line`. The exception net
    is intentionally broad (`Exception`, not a narrow tuple): the failure
    modes here span a missing token/recipient file (`OSError`), an expired
    refresh token (`RefreshError`) and `googleapiclient`'s `HttpError`, none
    of which share a useful common base.
    """
    try:
        recipient = to if to is not None else GMAIL_RECIPIENT_FILE.read_text().strip()

        creds = Credentials.from_authorized_user_file(
            str(GMAIL_SEND_TOKEN_FILE), GMAIL_SEND_SCOPES
        )
        if creds.expired and creds.refresh_token:
            logger.info("Refreshing Gmail send OAuth token")
            creds.refresh(Request())
            GMAIL_SEND_TOKEN_FILE.write_text(creds.to_json())

        if is_html:
            mime = MIMEMultipart("related")
            mime["To"] = recipient
            mime["Subject"] = subject

            alternative = MIMEMultipart("alternative")
            mime.attach(alternative)
            alternative.attach(MIMEText(message, "html", "utf-8"))

        else:
            mime = MIMEText(message, _charset="utf-8")
            mime["To"] = recipient
            mime["Subject"] = subject

        raw = base64.urlsafe_b64encode(mime.as_bytes()).decode("ascii")
        service = build("gmail", "v1", credentials=creds)
        service.users().messages().send(userId="me", body={"raw": raw}).execute()
        logger.debug("Message sent successfully!. message = {}".format(message))
    except Exception as e:
        logger.warning("Gmail notification failed: {}".format(e))
