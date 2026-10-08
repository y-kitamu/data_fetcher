# 企業分析レポート

銘柄ごとの企業分析レポートを Markdown（人の判断）と JSON スナップショット（自動計算）の組で保存するディレクトリです。生成の仕様は
[`docs/20260925_claude_code_instructions/claude_code_instructions_A_report_generator.md`](../docs/20260925_claude_code_instructions/claude_code_instructions_A_report_generator.md)
を参照してください（スナップショット JSON スキーマの正はその §6）。

## 使い方

```bash
# 初回レポート（財務データ・企業価値・自動評価を計算して雛形を書き出す）
uv run python scripts/new_report.py initial 7014 [--as-of YYYY-MM-DD] [--peers 7003,7014,...] [--dry-run]

# 経過記録（決算発表・重要開示のたびに作成。initial が必要）
uv run python scripts/new_report.py review 7014 [--as-of YYYY-MM-DD] [--trigger earnings|disclosure|price_move|scheduled] [--dry-run]

# 売却時の振り返り（initial と trades.csv が必要。全株売却していること）
uv run python scripts/new_report.py exit 7014 [--as-of YYYY-MM-DD] [--dry-run]

# JSON を標準出力に出すだけ（ファイルは書かない。動作確認用）
uv run python scripts/new_report.py snapshot 7014 [--as-of YYYY-MM-DD] [--peers ...]

# 既存の snapshot.json から表の Markdown（*_tables.md）を再生成する（--as-of 省略時はその銘柄の全スナップショット）
uv run python scripts/new_report.py tables 7014 [--as-of YYYY-MM-DD]
```

- `--as-of` を省略すると当日を使う。ただし当日の株価終値がまだなければ直近の営業日を使い、その旨を表示する。
- 保存先はデフォルトで `reports/`。環境変数 `REPORTS_DIR` で変更できる（レポートを別リポジトリで管理する場合）。
- 同名の `snapshot.json` / `*.md` が既に存在する場合はどちらも書かずにエラーで終了する（上書き禁止）。
- `initial` / `review` / `exit` は、スナップショットの表を Markdown にした `YYYY-MM-DD_tables.md` も書き出す。stock-viewer を開かずにエディタで数値を確認するためのファイルで、`snapshot.json` から導出するだけなので手で編集しない（`tables` で再生成すると上書きされる）。人が後から書き換える frontmatter に依存する表（予測・最終評価・売買計画など）とグラフは含まない。

## ディレクトリ構成

```
reports/
  README.md
  rubric.md                 # 評価基準（人向けの説明）
  _templates/                # 雛形（initial.md, review.md, exit.md, snapshot.example.json）。本文は変更しない
  _config/
    rubric_v1.yaml            # 評価基準の閾値（機械向け）
    leading_indicators.yaml   # 業種ごとの先行指標の登録（値は未実装、名前のみ）
    watchlist.yaml            # ウォッチリスト（active な銘柄のみ。エディタで編集）
    watchlist.schema.json     # 上記の JSON Schema（new_watch.py schema で生成。手で編集しない）
    watchlist_archive/
      YYYY.yaml               # 外した銘柄の履歴（外した年ごと。new_watch.py remove が追記）
  <ticker>/
    YYYY-MM-DD_initial.md
    YYYY-MM-DD_snapshot.json
    YYYY-MM-DD_tables.md      # snapshot.json の表（自動生成。手で編集しない）
    YYYY-MM-DD_review.md
    YYYY-MM-DD_exit.md
    trades.csv                # 売買記録（人が記入）: date,side,qty,price,fee
```

`_` で始まるディレクトリ・ファイルは一覧の対象外です。

## kill_criteria で使える指標（review の自動判定、§9.4）

`kill_criteria` をオブジェクト形式 `{text, metric, op, value}` で書くと、`new_report review` 実行時に `metric` の値と `value` を `op`（`<`, `<=`, `>`, `>=`, `==`, `!=`）で比較し、結果を `kill_criteria_check` に自動記録します。`metric` にはスナップショット JSON のドット区切りパスを指定します。よく使うものの例:

| metric | 意味 |
|---|---|
| `survival.runway_months` | 持ちこたえられる月数 |
| `survival.debt_due_to_cash` | 1年以内返済の借入 ÷ 現預金 |
| `market.drawdown_from_peak` | ピークからの下落率 |
| `valuation_metrics.pbr` | PBR |
| `valuation_metrics.per_normalized` | 正常化PER |
| `ratios.cycle_stats.operating_margin.latest` | 営業利益率の最新値 |
| `ratios.cycle_stats.operating_margin.percentile_latest` | 営業利益率の直近15年分布内でのパーセンタイル |
| `ratios.consecutive_loss_years.operating` | 連続営業赤字期数 |
| `ratios.consecutive_loss_years.net` | 連続純損失期数 |
| `cycle.peer_margin_deterioration_share` | 同業で同時に悪化している割合 |
| `value_range.risk_reward` | リスクリワード比（`"price_below_liquidation"` の場合は数値比較できず `hit: null` になる） |

値が取得できない（`null`）指標を条件にした場合は `hit: null` になります。文字列だけの `kill_criteria`（オブジェクト形式でないもの）は自動判定されず、人が判断してください。

## ウォッチリスト

設計は [`docs/20261004_watchlist.md`](../docs/20261004_watchlist.md)。`_config/watchlist.yaml` を直接エディタで編集し、追加・削除は次のコマンドで行う。

```bash
uv run python scripts/new_watch.py add 7014                      # 雛形を末尾に追記（過去のウォッチ回数も表示）
uv run python scripts/new_watch.py remove 7014 --reason "..."    # removed / removed_reason を付けて archive へ移す
uv run python scripts/new_watch.py sync [--dry-run]              # status=watch で未登録なら追加、sold/rejected なら archive へ移す
uv run python scripts/new_watch.py check [--list-metrics]        # 書式検証・status との不整合の警告・metric 一覧
uv run python scripts/new_watch.py schema                        # watchlist.schema.json を再生成（metric を追加したとき）
```

- status はウォッチリストに持たず、各銘柄の最新レポート（initial / review / exit をファイル名の日付順に並べた最後）の frontmatter から導出する。レポートが無い銘柄は「候補」として `buy_conditions` を評価する。
- 条件は `{text, metric, op, value}`（kill_criteria と同じ形式）で、リスト内は AND。`sell_conditions` は hold のときだけ評価する。
- ticker は文字列で書く（`"7014"`, `"278A"`）。エントリは行頭の `- ticker:` で始める（remove はこれを区切りにテキストを切り取るため、コメントは保持される）。
- `watchlist.yaml` 先頭の `# yaml-language-server: $schema=...` により、VS Code の YAML 拡張でキー・`op`・`metric` の補完と検証が効く。

## 既知の制約

- TDnet の T9〜T34（資本政策・財務リスク・株主異動などの開示）は、開示日時・タイトル・カテゴリ・PDF URLのみメタデータとして保存しています。金額や株数など本文にしかない値は原則 `null` です（例外: 株式分割・併合の比率はタイトルから正規表現で抽出）。
- この開示メタデータは `scripts/fetch_data_from_tdnet.py` の日次実行分から蓄積されます。過去分の一括バックフィルは行っていません（TDnetの過去の日次一覧ページを再取得する必要があり、本タスクの範囲外としました）。
- 大株主上位10（`shareholders.top10`）、配当性向・1株配当の推移、業績予想の修正履歴（`forecast.revisions`）、次回決算発表日（`next_earnings_date`）は未実装です。
