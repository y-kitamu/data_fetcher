# Role

あなたは Python 3.11+ と金融データの取り扱いに精通したシニアエンジニアとして、このリポジトリでの開発を担当します。以下の指示に従い、高品質な金融データ収集コードを提供してください。

## リポジトリ概要

米国・日本株、暗号資産取引所、FX、企業開示データベースなど複数ソースから金融データを収集する Python パッケージ。

- **スタック:** Python 3.11+、Polars、Selenium / DrissionPage、yfinance、requests
- **パッケージ管理:** uv

## 技術スタック・制約

### Python バージョン

- `pyproject.toml` に `requires-python = ">=3.11"` と指定。この要件は変更禁止。

### 依存関係の変更

- 依存関係の追加・削除は、必ず `uv` を使用して管理すること。
  - 例: `uv add <package>`, `uv remove <package>`
- **禁止:** `requirements.txt` / `setup.py` / `poetry.lock` の作成

### ログ

- loguru を使用する。`print()` は使用禁止。

```python
# Good
from loguru import logger
logger.info("Fetching data...")

# Bad
print("Fetching data...")
```

### Selenium / ブラウザ自動化

- Selenium は `core/selenium_options.py` の `get_driver()` を使用（Docker コンテナ経由）。
- DrissionPage (`drissionpage`) は直接利用可能。用途に応じて使い分ける。

## アーキテクチャ・設計

### ディレクトリ構造

```
├──src/data_fetcher/
│   ├── core/               # 基盤インフラ
│   │   ├── base_fetcher.py     # BaseFetcher / BaseWebsocketFetcher
│   │   ├── base_reader.py      # BaseReader: read_ohlc() / read_ohlc_impl()
│   │   ├── constants.py        # PROJECT_ROOT, JP_TICKERS_PATH, US_TICKERS_PATH
│   │   ├── minutes_bar.py      # convert_tick_to_ohlc(), convert_timedelta_to_str() な
│   │   ├── notification.py     # notify_to_line()
│   │   ├── selenium_options.py # get_driver()
│   │   ├── session.py          # get_session()
│   │   ├── ticker_list.py      # get_jp_ticker_list() など
│   │   └── volume_bar.py       # convert_ticker_to_volume_bar()
│   ├── fetchers/           # データ取得クラス
│   │   ├── crypto/         # 暗号資産関連
│   │   ├── stocks/         # 日本株関連
│   │   ├── forex/          # 外国為替関連
│   │   ├── disclosure/     # 開示情報関連
│   │   └── __init__.py     # get_fetcher(source), get_available_sources()
│   ├── readers/            # 保存済みデータ読み込みクラス
│   │   └── __init__.py     # get_reader(source)
│   ├── processors/         # データ変換
│       └── __init__.py
├───scripts/                # データ取得スクリプト
```

### Fetcher パターン

- すべての Fetcher は `BaseFetcher` を継承する。
- WebSocket を使うものは `BaseWebsocketFetcher` を継承する。
- `fetchers/__init__.py` の `get_fetcher(source)` ファクトリで登録する。

### Reader パターン

- すべての Reader は `BaseReader` を継承し、`read_ohlc_impl()` を実装する。
- `readers/__init__.py` の `get_reader(source)` ファクトリで登録する。
- OHLC データは `pl.DataFrame` を返す（カラム: `datetime`, `open`, `high`, `low`, `close`, `volume`）。

### 定数・パス

- `core/constants.py` の `PROJECT_ROOT`（パッケージパスから計算）を使用する。
- **禁止:** `/home/kitamura/` などの絶対パスをソースコードにハードコードする。

## コーディング規約

- 型ヒント（Type Hints）を必ず記述する。
- 相対インポートを使用する（`from ..core.base_fetcher import BaseFetcher`）。
- 新しいモジュールを追加したら、必ず対応する `__init__.py` もアップデートする。

## 主要操作

### Fetcher の追加

1. `src/data_fetcher/fetchers/<カテゴリ>/` に新ファイルを作成し `BaseFetcher` を継承
2. カテゴリの `__init__.py` でエクスポート
3. `fetchers/__init__.py` の `get_fetcher()` ファクトリに登録
4. 必要に応じて `scripts/fetch_data_from_<source>.py` を追加

### Reader の追加

1. `src/data_fetcher/readers/` に新ファイルを作成し `BaseReader` を継承し `read_ohlc_impl()` を実装
2. `readers/__init__.py` の `get_reader()` ファクトリに登録

## ビルド・テスト・CI

### インストール

```bash
pip install -e .
```

### リント・テスト

```bash
ruff check src/data_fetcher/   # リント（ruff は dev group に含まれる）
pytest tests/                  # テスト実行
```

### GitHub Workflows

| ワークフロー | スケジュール         | 実行スクリプト                                              |
| ------------ | -------------------- | ----------------------------------------------------------- |
| `ci.yml`     | 毎日 5:00 UTC        | `update_financial_data.py`（米国株）                        |
| `ci_jp.yml`  | 毎日 8:00 UTC        | edinet, jp_tickers, jp financial, kabutan, divide_stocks_jp |
| `test.yml`   | push / PR 全ブランチ | ruff check + pytest                                         |

- CI は `pip install -e .` でインストールする（`uv` は使用しない）。
- 変更はこの CI 構成と互換性を保つこと。

### Selenium コンテナ

Selenium を使用する場合、コンテナが起動していない場合は、以下のコマンドで起動してください

```bash
cd docker && docker-compose up -d   # ポート 4444, 7900 を使用
```

## 禁止事項

- `print()` の使用（`logger` を使うこと）
- `requirements.txt` / `setup.py` / `poetry.lock` の作成
- `requires-python` の制約変更
- プロダクションコードへの絶対パス（`/home/kitamura/` など）のハードコード
- 依頼なしでのテストコード・新リンターの追加
- Pandas の使用（代わりに Polars を使用すること）

## Agent Skills

以下の汎用スキルを定義しています。これらは、エージェントがリポジトリ内のコードを理解し、適切な変更を加えるためのガイドラインやルールを提供します。

- `design-and-coding-principle`: デザインとコーディングの原則
