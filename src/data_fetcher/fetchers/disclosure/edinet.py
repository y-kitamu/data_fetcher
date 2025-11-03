"""EDINET API wrapper for fetching disclosure documents.

EDINET (Electronic Disclosure for Investors' NETwork) is a Japanese financial
disclosure system operated by the Financial Services Agency.
"""

import datetime
import time
import zipfile
from io import BytesIO

import requests
from loguru import logger
from requests.exceptions import Timeout

# EDINET API configuration
API_KEY = "c528ad6f91db40468bf86c3f080daaff"
ENDPOINT = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
TIMEOUT = 5.0


def get_document_list(
    target_date: datetime.date, session: requests.Session, max_retries: int = 3
):
    """Get document list from EDINET API with retry logic.

    Args:
        target_date: Date to fetch documents for
        session: Requests session to use
        max_retries: Maximum number of retry attempts (default: 3)

    Returns:
        JSON response containing document list

    Raises:
        Timeout: If all retry attempts fail
    """
    params = {
        "date": target_date.strftime("%Y-%m-%d"),
        "type": "2",
        "Subscription-Key": API_KEY,
    }
    params_txt = "&".join([f"{key}={value}" for key, value in params.items()])
    url = f"{ENDPOINT}?{params_txt}"

    for attempt in range(max_retries):
        try:
            res = session.get(url, timeout=TIMEOUT)
            return res.json()
        except Timeout:
            if attempt < max_retries - 1:
                wait_time = 2**attempt  # Exponential backoff: 1s, 2s, 4s
                logger.warning(
                    f"Failed to get document list from the Edinet. "
                    f"Retry attempt {attempt + 1}/{max_retries} after {wait_time}s..."
                )
                time.sleep(wait_time)
            else:
                logger.error(
                    f"Failed to get document list after {max_retries} attempts"
                )
                raise


def download_documents(doc_id: str, session: requests.Session, doc_type: int = 1):
    """Download document from EDINET.

    Args:
        doc_id: Document ID to download
        session: Requests session to use
        doc_type: Document type (1=PDF, 2=CSV, etc.)

    Returns:
        BytesIO: Downloaded document content

    Raises:
        requests.RequestException: If download fails
    """
    url = f"https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
    params = {"type": str(doc_type), "Subscription-Key": API_KEY}

    res = session.get(url, params=params, timeout=TIMEOUT)
    res.raise_for_status()

    return BytesIO(res.content)


def extract_xbrl_from_zip(zip_content: BytesIO) -> dict[str, bytes]:
    """Extract XBRL files from downloaded ZIP content.

    Args:
        zip_content: ZIP file content as BytesIO

    Returns:
        dict: Mapping of filename to file content for XBRL files
    """
    xbrl_files = {}

    with zipfile.ZipFile(zip_content) as zf:
        for filename in zf.namelist():
            if filename.endswith(".xbrl") or filename.endswith(".xml"):
                xbrl_files[filename] = zf.read(filename)

    return xbrl_files


__all__ = [
    "get_document_list",
    "download_documents",
    "extract_xbrl_from_zip",
    "API_KEY",
    "ENDPOINT",
]
