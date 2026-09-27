---
# ============================================================
# 企業分析レポート（経過記録）  schema_version: 1
# ファイル名: reports/<ticker>/<YYYY-MM-DD>_review.md
# 決算発表ごと、または撤退条件・予測に関わる重要な開示があったときに作成する。
# ============================================================
schema_version: 1
type: review
ticker: ""
as_of: YYYY-MM-DD
snapshot: YYYY-MM-DD_snapshot.json
parent: YYYY-MM-DD_initial.md  # 初回レポート
trigger: earnings              # earnings / disclosure / price_move / scheduled
period: ""                     # 対象の決算期（例: 2027年3月期 Q2）

thesis_status: intact          # intact（維持）/ weakened（弱まった）/ broken（崩れた）
status: hold                   # watch / hold / sold / rejected（この記録の後の状態）

# 予測の進捗。初回レポートの predictions の id に対応させる。
# 期限を過ぎたものは outcome に true / false を入れる（ブライアースコアの計算に使う）。
prediction_updates:
  - id: p1
    progress: on_track         # on_track / behind / failed / achieved
    outcome: null              # 期限到来後に true / false
    note: ""
  - id: p2
    progress: on_track
    outcome: null
    note: ""

kill_criteria_hit: []          # 該当した撤退条件（初回レポートの文言をそのまま）

# 評価が変わった項目だけ書く
rating_changes: {}             # 例: { reversal: { from: C, to: B } }

action: none                   # none / buy_more / trim / sell_all
action_detail: ""              # 例: 第2トランシェ 2% 買い増し @1,150円
next_review: YYYY-MM-DD
tags: []
---

# {{ticker}} 経過記録 {{as_of}}（{{period}}）

> 仮説の状態: {{thesis_status}}　行動: {{action}}

## 1. 前回からの変化

<!-- snapshot-summary:start 生成スクリプトが書き込む。手で編集しない -->
<!-- snapshot-summary:end -->

<!-- auto:diff_since_last -->
<!-- 主要指標の前回比: 売上 / 営業利益 / 営業利益率 / 現預金 / 有利子負債 / 持ちこたえられる月数 /
     株価 / PBR / 企業価値レンジ（清算・正常化・ピーク）の更新 -->

## 2. 予測の進捗

<!-- auto:prediction_progress -->
<!-- 初回の predictions と、この記録の prediction_updates を並べて表示 -->

（記入）遅れ・外れの理由。一時的か構造的か。

## 3. サイクル指標の確認

（記入）循環性・生存力・反転兆候のそれぞれに変化はあったか。業界の市況・同業他社の決算。

## 4. 撤退条件の確認

<!-- auto:kill_criteria_check -->
<!-- 数値で判定できる撤退条件は自動判定して表示 -->

（記入）該当なし／該当あり（該当した場合は action に反映）。

## 5. 判断

（記入）仮説の状態をそう判断した理由と、取った行動の理由。
「今この株を初めて知ったとして、買うと言えるか」に答える。
