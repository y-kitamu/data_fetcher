---
# ============================================================
# 企業分析レポート（売却時の振り返り）  schema_version: 1
# ファイル名: reports/<ticker>/<YYYY-MM-DD>_exit.md
# 全株売却時、または見送り（rejected）銘柄の追跡を終えるときに作成する。
# ============================================================
schema_version: 1
type: exit
ticker: ""
as_of: YYYY-MM-DD
snapshot: YYYY-MM-DD_snapshot.json
parent: YYYY-MM-DD_initial.md

exit_reason: target            # target（目標到達）/ thesis_broken / kill_criteria /
                               # better_idea（より良い銘柄）/ overheated（短期急騰）/ time_stop
status: sold

# --- 結果（auto で埋まる。手で書かない） ----------------------
result:
  avg_buy_price: null
  avg_sell_price: null
  return_pct: null
  holding_days: null
  benchmark_return_pct: null   # 同期間の TOPIX
  excess_return_pct: null
  max_drawdown_pct: null       # 保有中の最大含み損

# --- 予測の最終判定（期限前に売却した予測は outcome: null のまま） ---
prediction_outcomes:
  - { id: p1, outcome: null }
  - { id: p2, outcome: null }

# --- 判断と結果の4象限 ---------------------------------------
decision_quality: null         # good / bad（当時の情報で見て判断は妥当だったか）
outcome_quality: null          # good / bad（結果）
# good×good: 実力 / good×bad: 不運 / bad×good: 幸運 / bad×bad: 失敗から学ぶ

# 教訓タグ（語彙は固定。増やすときは rubric.md の一覧に追加する）
lesson_tags: []
# 例: [timing_early, timing_late, cycle_longer_than_expected, dilution,
#      sold_too_early, sold_too_late, ignored_kill_criteria, thesis_right_price_wrong]
tags: []
---

# {{ticker}} 売却時の振り返り {{as_of}}

> 理由: {{exit_reason}}　リターン: {{result.return_pct}}%（超過 {{result.excess_return_pct}}%）　保有 {{result.holding_days}}日

## 1. 経過のまとめ

<!-- auto:timeline -->
<!-- 株価チャート上に 初回 / 経過記録 / 売買 のマーカー。仮説の状態の推移 -->

## 2. 予測の答え合わせ

<!-- auto:prediction_scorecard -->
<!-- 予測ごとの 確率 / 結果 / ブライアースコア。全レポート通算のスコアへのリンク -->

## 3. 何が当たり、何が外れたか

（記入）シナリオのどこが当たり、どこが外れたか。
初回レポートの弱気シナリオ・プレモーテムに書いたことは実際に起きたか。

## 4. 判断と結果の評価

（記入）decision_quality / outcome_quality をそう判断した理由。
当時の情報だけで見て、判断は妥当だったか（後知恵で評価しない）。

## 5. 教訓

（記入）次に同じような局面が来たら何を変えるか。1〜3点に絞る。

## 6. 売却後の追跡メモ（任意・追記可）

<!-- 売却後も半年〜1年は株価を追い、売却判断の良し悪しを確認する。
     このセクションだけは日付付きで追記してよい。 -->

- YYYY-MM-DD:
