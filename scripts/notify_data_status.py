"""notify_data_status.py - Email a daily digest of how much data landed under
`data/` for each source.

Previously broadcast to LINE (hence the still-named `cron_notify_to_line.sh`
wrapper); now delivered by email through `data_fetcher.notify_to_gmail`.
"""

import datetime

import data_fetcher
from data_fetcher.core import data_health

data_type = {
    "yfinance/minutes": "US stock",
    "yfinance/financial": "US stock",
    "binance": "Crypto",
    "rakuten": "Japan stock",
    "sbi": "Japan stock",
    "histdata": "Forex",
    "gmo/tick": "Forex",
    "gmo/book": "Crypto",
    "bitflyer": "Crypto",
    "news": "News",
    "fxea": "Forex",
    "kabus": "Japan stock",
    "kabutan": "Japan stock",
    "tdnet": "Japan stock",
    "taisyaku": "Japan stock",
    "jpx_stats": "Japan stock",
    "jp_ticker_themes": "Japan stock",
    "edinet": "Japan stock",
    "google_trends": "Japan stock",
}

_DATA_FILE_PATTERNS = ("*.csv*", "*.json*")


def _describe_dir(dir_path) -> tuple[str, int] | None:
    """latest date (or mtime) and file count for a directory of data files,
    covering both per-day files (YYYYMMDD*.csv) and per-ticker files (CODE.csv)."""
    files = [
        p
        for pattern in _DATA_FILE_PATTERNS
        for p in dir_path.glob(pattern)
        if p.is_file()
    ]
    if not files:
        return None
    if all(p.stem[:8].isdigit() for p in files):
        latest = sorted(files)[-1]
        date_str = latest.stem[:8]
    else:
        latest = max(files, key=lambda p: p.stat().st_mtime)
        date_str = datetime.datetime.fromtimestamp(latest.stat().st_mtime).strftime(
            "%Y%m%d"
        )
    return (date_str, len(files))


def get_latest_dates_data_number() -> dict[str, str]:
    data_root = data_fetcher.constants.PROJECT_ROOT / "data"
    results: dict[str, str] = {}
    for data_src_dir in data_root.glob("*"):
        if not data_src_dir.is_dir():
            continue

        sub_dirs = [p for p in data_src_dir.glob("*") if p.is_dir()]
        if not sub_dirs:
            # flat layout: data files sit directly under data/<src>/
            described = _describe_dir(data_src_dir)
            if described is not None:
                results[data_src_dir.name] = described
            continue

        for data_dir in sub_dirs:
            dirname = f"{data_src_dir.name}/{data_dir.name}"
            dated_dirs = sorted(
                [p for p in data_dir.glob("20*") if p.is_dir() and len(p.name) > 4]
            )
            if dated_dirs:
                latest_path = dated_dirs[-1]
                num_data = len(list(latest_path.glob("*.csv*")))
                results[dirname] = (latest_path.name, num_data)
                continue

            # flat layout: data files sit directly under data/<src>/<subdir>/
            described = _describe_dir(data_dir)
            if described is not None:
                results[dirname] = described

    return results


def get_fxea_data_number() -> tuple[str, int]:
    data_dir = data_fetcher.constants.PROJECT_ROOT / "../fxea/data"
    text_files = sorted(data_dir.glob("*.txt"))
    if len(text_files) == 0:
        return ("No data", 0)

    latest_file = text_files[-1]
    latest_date = latest_file.name.split("_")[0]
    num_files = len(list(data_dir.glob(f"{latest_date}_*.txt")))
    return (latest_date, num_files)


def get_kabus_data_number() -> tuple[str, int]:
    kabus_dir = data_fetcher.constants.PROJECT_ROOT.parent / "stock/logs/ticks"
    latest_date = None
    for jsonl_file in sorted(kabus_dir.glob("*.jsonl*")):
        stem = jsonl_file.name.split(".jsonl")[0]
        date = datetime.date.fromisoformat(stem.split("_")[1])
        if latest_date is None or date > latest_date:
            latest_date = date
    if latest_date is None:
        return ("No data", 0)

    return (
        latest_date.isoformat().replace("-", ""),
        len(list(kabus_dir.glob(f"*_{latest_date.isoformat()}*.jsonl*"))),
    )


def get_news_number() -> dict[str, str]:
    news_dir = data_fetcher.constants.PROJECT_ROOT / "data" / "news"
    results: dict[str, str] = {}
    for subdir in news_dir.glob("*"):
        if not subdir.is_dir():
            continue
        latest_csv_path = sorted(subdir.glob("*.csv"))
        if len(latest_csv_path) == 0:
            continue
        latest_csv_path = latest_csv_path[-1]
        num_rows = len(latest_csv_path.read_text().splitlines())
        date_str = (latest_csv_path.stem).replace("-", "")
        results[f"news/{subdir.name}"] = (date_str, num_rows)
    return results


def build_status_text() -> str:
    data_nums = get_latest_dates_data_number()
    data_nums.update(get_news_number())
    data_nums["fxea/data"] = get_fxea_data_number()
    data_nums["kabus"] = get_kabus_data_number()
    # grouped by source, sorted by source name
    grouped_data_nums: dict[str, list[tuple[str, str]]] = {}
    for key in sorted(data_nums.keys()):
        for src_key in data_type.keys():
            if key.startswith(src_key):
                grouped_data_nums.setdefault(data_type[src_key], []).append(
                    (key, data_nums[key])
                )
                break

    # html でtext作成する
    rows = []
    for group, items in grouped_data_nums.items():
        rows.append(f"<tr><td colspan='3'><b>{group}</b></td></tr>")
        for key, (date, val) in items:
            rows.append(f"<tr><td>{key}</td><td>{date}</td><td>{val}</td></tr>")

    rows_html = "\n".join(rows)
    anomaly_items = data_health.build_anomaly_items(data_nums, datetime.date.today())
    anomaly_html = data_health.render_anomaly_table_html(anomaly_items)
    text = f"""<html>
<head>
<style>
table {{
  border-collapse: collapse;
  border: 1px solid black;
}}
th, td {{
  border: 1px solid black;
  padding: 4px;
}}
.alert-critical {{ background: #fdd; }}
.alert-warning {{ background: #ffd; }}
.alert-info {{ background: #eef; }}
</style>
</head>
<body>
<table>
  <thead>
    <tr><th>Source</th><th>Date</th><th>Data</th></tr>
  </thead>
  <tbody>
{rows_html}
  </tbody>
</table>
{anomaly_html}
</body>
</html>
"""
    # text = "date: {}\n".format(datetime.date.today().strftime("%Y%m%d"))
    # for key, val in data_nums.items():
    #     text += f"{key:20} : {val}\n"
    return text


if __name__ == "__main__":
    today = datetime.date.today().strftime("%Y%m%d")
    data_fetcher.notify_to_gmail(
        build_status_text(),
        subject=f"[data_fetcher] データ収集状況 {today}",
        is_html=True,
    )
