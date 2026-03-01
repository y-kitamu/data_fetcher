from pathlib import Path
from ..core.constants import PROJECT_ROOT
from .crypto_book_base import BaseCryptoBookReader

BITFLYER_BOOK_DATA_DIR = PROJECT_ROOT / "data" / "bitflyer" / "book"

class BitflyerBookReader(BaseCryptoBookReader):
    def __init__(self, data_dir: Path = BITFLYER_BOOK_DATA_DIR):
        super().__init__(data_dir=data_dir, timestamp_col="received_timestamp", is_utc=False)
