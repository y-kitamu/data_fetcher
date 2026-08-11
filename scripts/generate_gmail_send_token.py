"""generate_gmail_send_token.py - One-time interactive setup for the Gmail
notification sender (`data_fetcher.notify_to_gmail`).

Produces `cert/gmail_send_token.json` carrying the `gmail.send` scope. This is
a *different* token from `token.json` (used by `fetch_data_from_sbi.py` to read
the SBI 2FA code, `gmail.readonly` scope only) — Google requires a fresh
interactive consent per scope, there is no way to add a scope to an existing
token headlessly, and coupling the SBI login path's token lifecycle to this
unrelated notification job would only widen the blast radius of either one
breaking. Run this once; re-run only if the token file is deleted or the scope
needs to change.

Usage over SSH (no browser/display on the remote box):
    1. From your LOCAL machine, open a tunnel BEFORE running the script:
           ssh -L 8090:127.0.0.1:8090 uname@hostname
       (Forward to the literal IPv4 address `127.0.0.1`, not the hostname
       `localhost` — on hosts where `localhost` resolves to the IPv6 loopback
       `::1` first, ssh would forward the tunnel to `::1:8090` while the local
       server below only listens on IPv4, so the browser redirect would
       silently fail to connect. Pinning both sides to `127.0.0.1` sidesteps
       that mismatch entirely.)
    2. In that same SSH session:
           uv run python scripts/generate_gmail_send_token.py
    3. The script prints an authorization URL — open it in a browser on your
       LOCAL machine (not on the remote host, which has none) and sign in.
       Google's redirect back to `127.0.0.1:8090` travels through the tunnel
       from step 1 to the server this script started on the remote host, which
       captures it and completes the flow automatically.

Already-issued tokens are refreshed silently (no browser/tunnel needed) as long
as the refresh token itself is still valid — see the note below on why that
isn't always the case.

Note: gmail.send is a Google-restricted scope. If the OAuth client's consent
screen is still in "Testing" publishing status, refresh tokens expire 7 days
after being issued regardless of use, so this script's interactive (tunnel)
path will need to be re-run periodically until the app is published (or that
cadence is accepted).
"""

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from loguru import logger

import data_fetcher

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
CREDENTIALS_PATH = (
    data_fetcher.core.PROJECT_ROOT
    / "cert"
    / "client_secret_940362520303-ml8op0bgm8d22p23c5ghhaljr2ggkmpb.apps.googleusercontent.com.json"
)
TOKEN_PATH = data_fetcher.core.PROJECT_ROOT / "cert" / "gmail_send_token.json"

# Pin the loopback interface explicitly (see the SSH usage note above) — bind to
# the same 127.0.0.1 the ssh -L tunnel targets, not "localhost", so the
# forwarded connection can never land on IPv6 ::1 while this listens on IPv4.
_LOCAL_SERVER_HOST = "127.0.0.1"
_LOCAL_SERVER_PORT = 8090


def main() -> None:
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)

    if creds and not creds.valid and creds.expired and creds.refresh_token:
        # A silent refresh needs neither a browser nor an SSH tunnel — try it
        # before falling back to the interactive flow below.
        try:
            logger.info("Refreshing Gmail send OAuth token")
            creds.refresh(Request())
            TOKEN_PATH.write_text(creds.to_json())
        except Exception as e:  # refresh token itself expired/revoked
            logger.warning(
                f"Silent refresh failed ({e}); falling back to interactive consent"
            )
            creds = None

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
        logger.info(
            "No usable token — starting interactive consent. "
            "If you are on a headless/SSH host, open a tunnel from your LOCAL "
            "machine first:\n"
            f"    ssh -L {_LOCAL_SERVER_PORT}:{_LOCAL_SERVER_HOST}:{_LOCAL_SERVER_PORT} <this host>\n"
            "then open the URL printed below in a browser on your LOCAL machine."
        )
        creds = flow.run_local_server(
            host=_LOCAL_SERVER_HOST,
            bind_addr=_LOCAL_SERVER_HOST,
            port=_LOCAL_SERVER_PORT,
            open_browser=False,
        )
        TOKEN_PATH.write_text(creds.to_json())

    logger.info(f"Gmail send token ready: {TOKEN_PATH}")


if __name__ == "__main__":
    main()
