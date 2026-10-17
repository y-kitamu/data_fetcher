# 企業分析レポート

銘柄ごとの企業分析レポートを Markdown（人の判断）と JSON スナップショット（自動計算）の組で保存するディレクトリです。生成の仕様は
[`docs/20260925_claude_code_instructions/claude_code_instructions_A_report_generator.md`](../docs/20260925_claude_code_instructions/claude_code_instructions_A_report_generator.md)
を参照してください（スナップショット JSON スキーマの正はその §6）。

## レポート作成の流れ

```
initial 生成 → 記入・コミット → ウォッチリスト登録 → (決算・開示のたびに review 生成 → 記入・コミット) … → exit 生成 → archive へ移す
```

各コマンドのオプションは次節「使い方」を参照。

### 1. 初回レポート（initial）

1. 対象銘柄の株価・財務・TDnet のデータが `as_of` まで DB に取り込まれていることを確認する。`new_report.py snapshot <ticker>` を実行すると、ファイルを書かずに計算結果の JSON を確認できる。
2. `new_report.py initial <ticker> [--peers ...]` を実行する（先に `--dry-run` を付けると、ファイルを書かずに作成予定のファイル名と要約を表示する）。`reports/<ticker>/` に次の3ファイルができる。
   - `YYYY-MM-DD_<ticker>_snapshot.json`: 自動計算の結果。手で編集しない
   - `YYYY-MM-DD_<ticker>_initial.md`: 記入用の雛形
   - `YYYY-MM-DD_<ticker>_tables.md`: 数値の確認用の表。手で編集しない
3. `initial.md` を記入する。数値は `tables.md`（エディタ）か stock-viewer で確認する。
   - 自動で記入済みの項目: frontmatter の `ticker`, `company_name`, `as_of`, `snapshot`, `algo_flagged`, `ratings.*.auto`, `valuation.*`, `next_review` と、本文冒頭の `snapshot-summary` ブロック。
   - 人が書く項目: frontmatter の `status`, `strategy`, `thesis`, `decision`, `conviction`, `ratings.*.final`, `predictions`, `kill_criteria`, `plan`, `tags` と、本文の「（記入）」欄。
   - `<!-- auto:xxx -->` は stock-viewer が表やグラフを差し込む位置なので、残しておく。`snapshot-summary` ブロックの中身も編集しない。
   - `kill_criteria` は `{text, metric, op, value}` の形式で書くと、review のときに自動判定される（「kill_criteria で使える指標」を参照）。
   - `ratings` の `final` を `auto` と違う評価にしたときは、本文の「評価の補足」に理由を書く。

### 2. ウォッチリストへの登録

1. `new_watch.py sync` を実行し、`status: watch` の銘柄を `_config/watchlist.yaml` に追加する（手動で追加するなら `new_watch.py add <ticker>`）。
2. 追加されたエントリに `scenario` と `buy_conditions` を書き、`new_watch.py check` で書式を検証する。

### 3. 売買の記録

売買したら `reports/<ticker>/trades.csv`（`date,side,qty,price,fee`。`side` は `buy` / `sell`）に追記する。取得単価・保有数・exit の損益はこのファイルから計算する。買った後は、次の review で `status: hold` にし、ウォッチリストのエントリに `sell_conditions` を書く。

### 4. 経過記録（review）

決算発表、撤退条件や予測に関わる重要な開示、大きな値動き、`next_review` の到来のいずれかがあったら作成する。

1. `new_report.py review <ticker> --trigger earnings|disclosure|price_move|scheduled` を実行する。最新の initial を親とし、新しい `snapshot.json` / `review.md` / `tables.md` を作る。
2. `review.md` を記入する。
   - 自動で記入済みの項目: `parent`, `period`, `prediction_updates` の `id`, `kill_criteria_hit`, `kill_criteria_check`（オブジェクト形式の撤退条件を判定した結果）, `next_review` と、前回の値を併記した要約ブロック。
   - 人が書く項目: `thesis_status`, `status`（この記録の後の状態）, `prediction_updates` の `progress` / `outcome` / `note`, `rating_changes`, `action`, `action_detail` と、本文の「（記入）」欄。期限を過ぎた予測は `outcome` に `true` / `false` を入れる。
3. コミットする。

### 5. 売却・見送り

- 全株を売却したら、`trades.csv` に売りを記入してから `new_report.py exit <ticker>` を実行する。`result.*` は自動で計算される。人が書くのは `exit_reason`, `prediction_outcomes`, `decision_quality`, `outcome_quality`, `lesson_tags` と本文。保有株数が0でない場合、または `trades.csv` がない場合はエラーになる。
- 買わずに見送る場合は、initial（記入時）か review の `status` を `rejected` にする。一度も買っていない銘柄では、`trades.csv` が必要なため exit を作れない。
- どちらの場合も、`new_watch.py remove <ticker> --reason "..."`（または `sync`）でウォッチリストの archive へ移す。

### AIによる下書き（initial / review）

人が記入する前に、Claude Code に事実を調査させて記入欄を下書きさせることができる（`claude` コマンドが必要。数十分かかる）。

```bash
uv run python scripts/draft_report.py 7014 [--as-of YYYY-MM-DD] [--print-prompt]
```

- `--as-of` を省略すると、その銘柄の initial / review のうち最新のものを対象にする。`--print-prompt` は claude を呼ばずに指示文を表示する。
- 本文の「（記入）」が「（AI下書き・要確認）」で始まる文章に置き換わる。事実には脚注が付き、末尾の「出典（AI下書き）」に URL・公表日・参照日が並ぶ。`as_of` より後に公表された情報は使わない。
- thesis・ratings.final・predictions・kill_criteria・plan（review では thesis_status・prediction_updates・action など）は frontmatter には書かれず、末尾の「AI案（要確認・frontmatter未反映）」に YAML 片として提案される。
- frontmatter・`snapshot-summary` ブロック・`<!-- auto:xxx -->` が変更された場合は元に戻してエラーになる。コミット済みのレポートは対象外（エラーになる）。
- 実行後は `git diff` で差分を確認し、「AI下書き」で検索して内容と出典を検証・修正する。採用する AI案 は frontmatter に転記し、「（AI下書き・要確認）」の表記と不要な AI案 の節を整理してからコミットする。

### やり直したいとき

同名のファイルが既にあると、何も書かずにエラーで終了する（上書きは禁止）。コミット前に作り直す場合は、生成された3ファイル（`snapshot.json` / `*.md` / `tables.md`）を手で削除してから再実行する。同じ銘柄の initial を別の日付で作ると、警告を表示したうえで再分析として作成する。

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
- `initial` / `review` / `exit` は、スナップショットの表を Markdown にした `YYYY-MM-DD_<ticker>_tables.md` も書き出す。stock-viewer を開かずにエディタで数値を確認するためのファイルで、`snapshot.json` から導出するだけなので手で編集しない（`tables` で再生成すると上書きされる）。人が後から書き換える frontmatter に依存する表（予測・最終評価・売買計画など）とグラフは含まない。

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
    YYYY-MM-DD_<ticker>_initial.md
    YYYY-MM-DD_<ticker>_snapshot.json
    YYYY-MM-DD_<ticker>_tables.md   # snapshot.json の表（自動生成。手で編集しない）
    YYYY-MM-DD_<ticker>_review.md
    YYYY-MM-DD_<ticker>_exit.md
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
