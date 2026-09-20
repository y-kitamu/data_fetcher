"""iXBRLファイルのオープンとパースエラーのロギングを共通化する。"""

from pathlib import Path

from ixbrlparse import IXBRL
from loguru import logger


def open_ixbrl(path: Path) -> IXBRL:
    with open(path, "r") as f:
        ixbrl = IXBRL(f, raise_on_error=False)
    if len(ixbrl.errors) > 0:
        logger.warning(f"IXBRL parsing errors in {path.name}")
    return ixbrl
