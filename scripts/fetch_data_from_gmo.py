"""fetch_data_from_gmo.py"""

import pdb
import platform
import sys
import traceback

import data_fetcher


def run_debug(func, *args, **kwargs):
    """エラーが発生したときにpdbを起動する"""
    try:
        return func(*args, **kwargs)
    except:
        extype, value, tb = sys.exc_info()
        traceback.print_exc()
        pdb.post_mortem(tb)


def debug(func):
    """エラーが発生したときにpdbを起動するデコレータ"""

    def wrapper(*args, **kwargs):
        return run_debug(func, *args, **kwargs)

    return wrapper


if __name__ == "__main__":
    fetcher = data_fetcher.gmo.GMOFetcher()
    run_debug(fetcher.download_all)
