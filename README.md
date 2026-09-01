# data_fetcher

日本株・米国株・暗号資産・FX・企業開示情報など、複数ソースから金融データを収集するPythonパッケージ。

## セットアップ

```bash
uv sync
```

認証情報（APIキー・OAuthトークン等）は `cert/` ディレクトリに配置する（`.gitignore` 済み）。各ファイルの用途は [`cert/README.md`](cert/README.md) を参照。

## アーキテクチャ

```
src/data_fetcher/
├── core/        # 基盤インフラ (get_session, retry_with_backoff, get_driver 等)
├── domains/     # ソース固有の低レベルAPIクライアント (edinet, tdnet, taisyaku, jquants, jpx_stats, ...)
├── fetchers/    # BaseFetcher を継承した取得クラス (crypto, forex, stocks, disclosure)
├── readers/     # 保存済みデータの読み込みクラス (BaseReader)
├── processors/  # データ変換
└── db/          # DuckDB スキーマ (XBRL財務データ・ニュース等)
scripts/         # 実行スクリプト (fetch_data_from_*.py)
data/            # 取得データ本体。ソース別ディレクトリ、日次CIで自動コミットされるものが大半
```

詳細な開発規約は [`.github/copilot-instructions.md`](.github/copilot-instructions.md) を参照。

