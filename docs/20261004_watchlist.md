# ウォッチリスト設計

## 1. 目的と要件

- 日本株のみを対象に、株価・指標のモニタリングと、決算・開示の通知を行う。
- ウォッチリストの銘柄が条件を満たしたら通知し、人が売買を判断する（自動売買はしない）。
- レポート未作成の銘柄も載せられる。レポートの `status`（`watch / hold / sold / rejected`）と連動する。
- 保有中（`hold`）も監視する。
- 記録項目: ticker、追加日、追加理由（想定シナリオ・買い条件）、削除日と理由。
- 編集はエディタで直接行う。履歴を残す。
- 通知はメール。将来 GitHub などに拡張できるようにする。

## 2. 責務分担

| 担当 | 内容 |
|---|---|
| data_fetcher | ウォッチリストの読み込み・検証、株価指標の計算、条件判定、アラート生成、通知 |
| stock-viewer | 一覧・条件の充足状況・アラート履歴の表示（読み取り専用）。別途指示書を作成する |

## 3. 保存場所と形式

- `reports/_config/watchlist.yaml`: **active な銘柄のみ**。日常的にエディタで編集する（`REPORTS_DIR` 配下。別リポジトリ管理に対応）。
- `reports/_config/watchlist_archive/YYYY.yaml`: 外した銘柄の履歴（外した年ごと）。普段は開かない。
- 外すときは `new_watch.py remove <ticker> --reason "..."` が `removed` / `removed_reason` を付けて該当エントリを archive へ移す。手で移さない。
- 再ウォッチは `add` で新エントリを作る。過去にウォッチ済みの ticker は `add` 時に回数を表示する。
- 日次の判定・通知は active のみを読む。履歴が必要なときだけ archive も読む。
- `scenario` は2〜3行の要約に留める。詳しい根拠は `reports/<ticker>/` のレポートに書く。
- ticker は文字列で書く（`"7014"`, `"278A"`）。

```yaml
- ticker: "7014"
  added: 2026-10-04
  scenario: |
    受注残の増加で来期増益を想定。調整局面での押し目を狙う。
  buy_conditions:
    - {text: 75日線を下回る, metric: price.vs_ma75, op: "<", value: 0}
  sell_conditions:            # hold のときだけ評価
    - {text: 取得単価から+30%, metric: position.return_from_cost, op: ">=", value: 0.30}
    - {text: ピークから-15%,   metric: price.drawdown_from_peak, op: "<=", value: -0.15}
```

archive に移すエントリには、上記に加えて `removed: YYYY-MM-DD` と `removed_reason: ...` が付く。

- 条件の書式は `kill_criteria` と同じ `{text, metric, op, value}`。`op` は `<, <=, >, >=, ==, !=`。条件リストは全て満たしたときに成立（AND）。
- 取得単価・保有数は `reports/<ticker>/trades.csv` から算出し、ウォッチリストには持たない。

### 3.1 書式を覚えなくて済む仕組み

- `scripts/new_watch.py add <ticker>`: コメント付きの雛形を末尾に追記する（`added` は当日、`scenario` と条件は記入欄）。テキスト追記のため既存コメントを壊さない。
- `scripts/new_watch.py sync`: report ディレクトリの内容とウォッチリストの整合性を確認し、status が watch だが active に含まれないものを追加、status が sold / rejected だが archive に含まれないものを archive に移す。
- `scripts/new_watch.py remove <ticker> --reason "..."`: 該当エントリの `removed` / `removed_reason` だけを書き換える。
- `scripts/new_watch.py check [--list-metrics]`: 書式の検証、レポート status との不整合の警告、使える `metric` 一覧の表示。
- JSON Schema（`reports/_config/watchlist.schema.json`）を用意し、VS Code の YAML 拡張でキー・`op`・`metric` の補完と検証を効かせる。`metric` の列挙は `check --list-metrics` と同じ定義から生成する。
- ruamel.yaml など YAML を読み書きし直すライブラリは、依存関係に未導入なら追加しない。

## 4. レポート status との連動

status はウォッチリストに持たず、レポートから導出する（正はレポート）。

| レポート status | ウォッチリスト | 評価する条件 | 備考 |
|---|---|---|---|
| なし / `watch` | active | `buy_conditions` | レポートなしは「候補」 |
| `hold` | active | `sell_conditions` | `kill_criteria` は既存の review が判定 |
| `sold` | archive へ移す必要あり | なし | 売却時に `remove` を実行 |
| `rejected` | archive へ移す必要あり | なし | 見送り時に `remove` を実行 |

不整合（例: `rejected` なのに active（watchlist.yaml に残っている）、`hold` なのに `sell_conditions` が空）は検証コマンドが警告する。自動書き換えはしない。

## 5. 株価指標（最初のセット）

日次の終値データから計算する。

| metric | 内容 |
|---|---|
| `price.close` | 終値 |
| `price.vs_ma25` / `vs_ma75` / `vs_ma200` | 移動平均乖離率 |
| `price.return_1w` / `return_1m` / `return_3m` | 騰落率 |
| `price.drawdown_from_peak` | 直近高値からの下落率 |
| `price.drawdown_from_52w_high` | 52週高値からの下落率 |
| `position.return_from_cost` | 取得単価からの損益率（hold のみ） |

バリュエーション・需給系の指標は後続の拡張とする。

## 6. 通知

- 種類: 買い条件の充足、売り条件の充足、新規開示（TDnet・EDINET）、決算発表予定日の接近。
- `Notifier` インターフェースを切り出し、最初の実装はメール（SMTP）。認証情報は `cert/` に置く。GitHub 通知は実装クラスの追加で対応する。
- 重複送信を避けるため、通知済みの状態を `alerts/` に記録する（同時に履歴になる）。
- 日次CI（`.github/workflows/ci_jp.yml`）の後段で実行する。

## 7. 実装の順序

1. スキーマ確定と読み込み（`readers/`。active と archive）、`new_watch.py`（add / remove〔archive へ移動〕/ check）、JSON Schema
2. 株価指標の計算
3. 条件判定（`kill_criteria` の判定ロジックを再利用）
4. アラート生成と重複抑止
5. メール通知
6. 日次CIへの組み込み
7. stock-viewer 向け指示書

## 8. 未決事項

- 決算発表予定日のデータ取得元と、何営業日前に通知するか。
- 通知メールの宛先・送信元の管理方法（`cert/` のファイル形式）。
- アラートの置き場所（`reports/` 配下か `data/` 配下か）。
