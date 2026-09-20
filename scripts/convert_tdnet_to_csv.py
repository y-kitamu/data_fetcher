"""convert_tdnet_to_csv.py
data/tdnet/raw/{code}/*.zip に保存済みのTDnet決算短信iXBRLアーカイブを解析し、
data/tdnet/csv/{code}.csv に1事実=1行のロング形式で書き出す。

取得直後の変換は scripts/fetch_data_from_tdnet.py が行うため、本スクリプトは主に
過去に貯まった未変換分のバックフィル用。変換の実体は
data_fetcher.domains.tdnet.csv_export.append_zip_to_csv を両スクリプトで共有している。

提出日時の解決 (resolve_filing_datetime) がzipファイルごとにkabutan.jpへアクセスする
ため、全銘柄(data/tdnet/raw配下 約4000銘柄・約19万zip)をまとめて変換すると数時間規模の
実行時間になる。--codes で対象銘柄を絞れるようにし、また既に変換済みのzip
(CSVのsource_file列に記録)はスキップして、中断後の再実行でも続きから処理できるように
している。
"""

import argparse
from pathlib import Path

import tqdm
from requests import Session

import data_fetcher
from data_fetcher.domains.tdnet.constants import zip_root_dir
from data_fetcher.domains.tdnet.csv_export import OUTPUT_DIR, append_zip_to_csv
from data_fetcher.domains.tdnet.taxonomy_element import collect_all_taxonomies
from data_fetcher.domains.tdnet.taxonomy_index import TaxonomyIndex

WORK_DIR = data_fetcher.constants.PROJECT_ROOT / "data/tdnet/tmp_convert"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--codes",
        nargs="*",
        default=None,
        help="対象の証券コード（省略時はdata/tdnet/raw配下の全銘柄）",
    )
    return parser.parse_args()


def convert_code(
    code: str,
    session: Session,
    taxonomy_index: TaxonomyIndex,
    work_dir: Path = WORK_DIR,
    output_dir: Path = OUTPUT_DIR,
) -> None:
    zip_files = sorted((zip_root_dir / code).glob("*.zip"))
    if not zip_files:
        data_fetcher.logger.warning(f"No zip files found for {code}")
        return

    for zip_file in zip_files:
        append_zip_to_csv(zip_file, session, taxonomy_index, work_dir, output_dir)


def main():
    args = parse_args()
    codes = args.codes or sorted(p.name for p in zip_root_dir.iterdir() if p.is_dir())

    session = data_fetcher.get_session()
    taxonomy_index = TaxonomyIndex.from_elements(collect_all_taxonomies())

    for code in tqdm.tqdm(codes):
        convert_code(code, session, taxonomy_index)


if __name__ == "__main__":
    data_fetcher.debug.run_debug(main)
