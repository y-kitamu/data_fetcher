# Claude Code 指示書 B：企業分析レポートの表示（stock-viewer の画面）

対象リポジトリ: stock-viewer
対になる指示書: `claude_code_instructions_A_report_generator.md`（生成側）。
本書だけで実装できるように書いている。スナップショット JSON のスキーマは、**指示書 A の §6 を正とする**。本書 §4 には、表示に使うキーだけを抜き出して載せている。指示書 A の実装がまだでも、§9 の開発用データで先に実装できる。

---

## 0. 背景と目的

銘柄ごとの企業分析レポートを Markdown で残している。レポートは次の3つの層でできている。

- **自動計算の層**：生成スクリプト（指示書 A）が JSON（スナップショット）に固定して保存する。
- **人の判断の層**：人が Markdown に記入する。
- **表示の層**：本書の担当。

Markdown には、表やグラフを置きたい位置に目印だけが書いてある。

```markdown
## 3. 要約財務諸表

<!-- auto:balance_sheet years=15 -->

（記入）2025年度に棚卸資産が急増している点に注意。
```

stock-viewer は Markdown を画面に表示するとき、この `auto:xxx` の目印を、同じ日付のスナップショット JSON から作った表・グラフの部品に置き換える。本書の目的はこの表示の仕組みと、レポートを横断して振り返る一覧画面を作ること。

**原則**
1. **表示専用**：画面からファイルを書き換えない。レポートの編集はエディタで行う。
2. **再計算しない**：財務指標や企業価値は JSON の値をそのまま表示する。画面側で計算するのは「前回との差分」と「予測の採点（ブライアースコア）」だけ。
3. **欠損に強く**：キーがない・null のときは「—」または「データなし」と表示し、画面全体を落とさない。

---

## 1. 最初にやること（既存コードの調査）

以下を調査し、**調査結果と実装方針を短く報告してから**着手すること。

- フロントエンドの構成（Vite 上のフレームワーク、ルーティング、状態管理、UI とグラフのライブラリ、ダークモードの有無）。既存のライブラリとスタイルに合わせること。
- 銘柄ページの構成。現状は、上部にローソク足チャート、その下に「歩み値／板ヒートマップ／適時開示／ニュース／バックテスト」のタブ、右側に「企業概要／信用残高推移／大量保有報告書」がある。
- チャートの「ニュース/開示マーカー」の実装方法（レポートマーカーで流用する）。
- バックエンドの構成と API の書き方（ファイルを読む API を追加する場所）。

---

## 2. ファイルの置き場所と形式

```
reports/                      # 環境変数 REPORTS_DIR で変更可能（バックエンドで読む）
  rubric.md
  _templates/  _config/       # 表示対象外
  <ticker>/
    YYYY-MM-DD_initial.md     + YYYY-MM-DD_snapshot.json
    YYYY-MM-DD_review.md      + YYYY-MM-DD_snapshot.json
    YYYY-MM-DD_exit.md        + YYYY-MM-DD_snapshot.json
    trades.csv                # 売買記録: date,side,qty,price,fee
```

- `_` で始まるディレクトリは読まない。
- Markdown は YAML の frontmatter（`---` で囲んだ先頭部分）と本文でできている。frontmatter の `snapshot` キーに、対応する JSON のファイル名が入っている。
- frontmatter の主なキー（雛形 `initial.md` / `review.md` / `exit.md` を参照）:
  - 共通: `schema_version`, `type`（initial / review / exit）, `ticker`, `as_of`, `snapshot`, `status`, `tags`
  - initial: `company_name`, `strategy`, `algo_flagged`, `thesis`, `decision`, `conviction`, `ratings.<項目>.{auto,final}`, `valuation.*`, `predictions[]`（id, claim, prob, due, metric）, `kill_criteria[]`（文字列、または `{text, metric, op, value}`）, `plan.*`, `next_review`
  - review: `parent`, `trigger`, `period`, `thesis_status`, `prediction_updates[]`（id, progress, outcome, note）, `kill_criteria_hit[]`, `rating_changes`, `action`, `action_detail`, `next_review`
  - exit: `parent`, `exit_reason`, `result.*`, `prediction_outcomes[]`（id, outcome）, `decision_quality`, `outcome_quality`, `lesson_tags[]`

---

## 3. バックエンド API（読み取り専用）

| メソッド・パス | 返す内容 |
|---|---|
| `GET /api/reports` | 全銘柄の「最新状態」の一覧（§6.1 の列に必要な値）。クエリ `status`, `strategy`, `tag` で絞り込み |
| `GET /api/reports/{ticker}` | その銘柄のレポートのファイル一覧（日付順）: `[{file, type, as_of, status, thesis_status}]` と、`trades.csv` の中身 |
| `GET /api/reports/{ticker}/{file}` | `{ frontmatter, body, snapshot, previous_snapshot, parent_frontmatter }` |
| `GET /api/reports/scoreboard` | §6.2 の集計に必要な、全レポートの予測・結果の生データ |

- `previous_snapshot` は review / exit のとき、その銘柄の直前のレポートのスナップショット。`parent_frontmatter` は review / exit のとき、親の initial の frontmatter。
- frontmatter はバックエンドでパースして JSON で返す。本文（`body`）は Markdown 文字列のまま返す。
- **パストラバーサル対策**：`ticker` は英数字のみ、`file` は `^\d{4}-\d{2}-\d{2}_(initial|review|exit)\.md$` のみ受け付ける。
- ファイルの更新時刻でキャッシュする（エディタで保存したら再読み込みで反映されればよい）。
- パースに失敗したファイルは一覧から外さず、`error` フィールドを付けて返す。

---

## 4. 本文の描画

### 4.1 処理の順序

1. `body` を Markdown としてパースする（GFM の表に対応させる。例: react-markdown + remark-gfm）。
2. **自作の remark プラグイン**で、HTML コメントのノードを次のように処理する。
   - `<!-- auto:NAME key=value key=value ... -->` → カスタムノード `{type: "autoBlock", name, args}` に変換する。
   - `<!-- snapshot-summary:start ... -->` から `<!-- snapshot-summary:end -->` までは、**非表示にする**。summary_card と内容が重複するため。この部分は他のビューア向けに生成スクリプトが書き込んだもの。
   - それ以外のコメント（書き方のガイド）は表示しない。
3. `{{path}}` のプレースホルダを frontmatter の値で置き換える。ドット区切りで入れ子を参照する（例: `{{valuation.price}}`, `{{result.return_pct}}`）。値がない場合は「—」にする。テキストノードに対してだけ置き換え、コードブロックの中は置き換えない。
4. `autoBlock` ノードを §5 の部品で描画する。未登録の NAME なら、灰色の枠で「未対応の自動ブロック: NAME」と表示する。
5. 本文中の「（記入）」がそのまま残っている段落は、薄い色で表示する（まだ書いていない欄が一目で分かるように）。

### 4.2 部品の登録

`NAME → React 部品` の対応表を1か所にまとめる（例: `autoBlocks/registry.ts`）。各部品は `(props: {snapshot, frontmatter, previousSnapshot, parentFrontmatter, args})` を受け取る。部品を足すときは対応表に1行足すだけで済むようにする。

### 4.3 共通の表示ルール

- 金額の単位は百万円。表では「百万円」と明記し、3桁区切りで表示する。**1株あたりの値は円**。
- 比率は小数で入っているので、% に変換して表示する（0.061 → 6.1%）。
- 負の値は赤。前年比は括弧で併記する。
- 年次の表は横スクロールにし、**最新年の列を右端に固定**して表示する。左端の科目名の列も固定する。
- A〜E の評価は色分けした小さなバッジで表示する（A が最も良い）。
- `snapshot.warnings` に関係するキーがある部品は、右上に注意アイコンを出し、ホバーで警告の内容を表示する。

---

## 5. 自動ブロックの部品仕様（全24種）

雛形に出てくる目印は以下のとおりで、これですべて。スナップショットのキーは指示書 A §6 のもの。

### 5.1 initial.md で使うもの

| NAME | 使うデータ | 表示 |
|---|---|---|
| `summary_card` | frontmatter: `ratings`, `valuation`, `status`, `decision`, `conviction`, `next_review`, `thesis` | ①10項目のレーダーチャート（A=5〜E=1。`final` を実線、`auto` を点線、null の軸は欠ける）②企業価値レンジの帯（§`value_range` と同じ部品を小さく）③状態・判定・確信度・次の見直し日のバッジ。見直し日を過ぎていたら赤 |
| `segment_history` | `segments`, `fiscal_years`（args: `years`） | セグメント別の売上の積み上げ棒と、営業利益の折れ線。下に数値の表 |
| `cost_structure` | `cost_structure`, `income_statement.revenue` | 横軸を売上高、縦軸を営業費用にした散布図（年ごとの点に年のラベル）と、回帰直線 `F + v×売上`。損益分岐点の売上に縦線、最新年の点を強調。F・v・R²・損益分岐点・最新売上との差を数値で表示 |
| `balance_sheet` | `balance_sheet`, `fiscal_years`（args: `years`） | たーちゃん氏の雛形の行の順で表示: 流動資産（現金及び預金、受取手形及び売掛金、棚卸資産、流動資産合計）→ 固定資産（有形固定資産、無形固定資産、投資有価証券、資産合計）→ 流動負債（支払手形及び買掛金、短期借入金、その他、流動負債合計）→ 固定負債（長期借入金、その他、固定負債合計）→ 負債合計、純資産合計、負債・純資産合計。スキーマにある他の科目（社債、リースなど）は「その他」の内訳として展開できるようにする |
| `income_statement` | `income_statement`（args: `years`） | 売上高、原価率、販管費率、営業利益、経常利益、当期利益。原価率・販管費率以外は前年比を括弧で併記 |
| `cash_flow` | `cash_flow`（args: `years`） | 営業CF、投資CF（うち設備投資）、財務CF、FCF、減価償却費 |
| `financial_ratios` | `ratios`（args: `years`） | 表は3区分（(1)健全性: 株主資本比率、流動比率、ネットキャッシュ、有利子負債/EBITDA、(2)収益性: 営業利益率、ROE、ROA、ROIC、(3)成長性: 売上成長、営業利益成長）。右端に「10年中央値」「現在の位置（パーセンタイル）」の列を追加（`cycle_stats` があるものだけ）。表の下に、営業利益率・ROE・ROIC の小さな折れ線を並べ、10年中央値を横線で表示 |
| `valuation_metrics` | `valuation_metrics`, `market` | 2列の表（たーちゃん氏の雛形の順: 近似PER、PCFR、PSR、PBR、予想収益率、PER×PBR、EV/EBITDA、ROIC、アクルーアル/総資産、時価総額）＋ 追加行（正常化PER、実績PER、ピークからの下落率）。表の上に「株価 ○○円（YYYY-MM-DD）で計算」と表示 |
| `peer_comparison` | `cycle.peers`, `cycle.industry_median_operating_margin`, 自社の `ratios.operating_margin` | 自社と同業の比較表（PBR、PSR、最新の営業利益率、前期からの変化）と、営業利益率の推移の折れ線（自社を太線、同業を細線、業界中央値を点線）。`peer_selection` が auto なら「同業は自動選定」と注記 |
| `shareholders` | `shareholders` | ①大株主上位10の表 ②大量保有報告書の表（提出者、保有比率、増減、日付）③配当性向・1株配当の推移 ④資本政策の年表（`capital_actions`。増資・MSCB・転換社債など希薄化するものは赤、自社株買い・消却は緑）⑤株主還元方針、資本コスト開示へのリンク |
| `liquidation_value` | `liquidation_value`, `market.price` | 科目ごとの「簿価 × 掛け目 = 修正後」の表、修正資産合計、負債合計、清算価値（総額と1株あたり）、株価に対する比率 |
| `dcf` | `dcf` | 弱気・強気それぞれの式に数値を入れた形で表示（例: `ネットキャッシュ 12,000 + FCF 3,000 ÷ 10% = 42,000百万円 → 1株 1,424円`）。`bear_per_share` が null なら「直近 FCF がマイナスのため算出せず（7.3 正常化収益バリューを参照）」と表示 |
| `normalized_value` | `normalized_value`, `ratios.operating_margin` | 計算の過程（売上 × 中央値の利益率 → 正常化営業利益 → 正常化FCF → 1株価値）を数値で表示。営業利益率の推移グラフに、中央値とピークの横線を重ねる |
| `value_range` | `value_range` | 横1本の帯グラフ: 清算価値・正常化価値・ピーク価値を目盛りにし、株価をマーカーで表示。リスクリワード比を大きく表示。`risk_reward` が `"price_below_liquidation"` なら「株価が清算価値を下回る」と表示 |
| `cycle_position` | `cycle`, `ratios`, `market.drawdown_from_peak` | ①自社と業界中央値の営業利益率の長期推移 ②数値カード: ピークからの下落率、連続赤字期数（営業・純）、同業で同時に悪化している割合、業界の利益率の半減期 ③先行指標の一覧（空なら「未登録」） |
| `survival_metrics` | `survival` | 数値カード: 持ちこたえられる月数（営業CFが黒字なら「営業CF黒字」）、1年以内返済の借入、その現預金比。フラグ（継続企業の前提の注記、重要事象、財務制限条項への抵触、監理・整理銘柄の指定）は、該当したら赤で大きく表示 |
| `predictions_table` | frontmatter: `predictions`。加えて同じ銘柄の後続の review / exit | 列: id、内容、確率、期限、判定指標、**最新の進捗**。最新の進捗は後続レポートの `prediction_updates` / `prediction_outcomes` から取る。見出しに「（最新の進捗は後から追記された記録）」と明記し、当時の記載と区別する。期限を過ぎたのに outcome がないものは黄色 |
| `ratings_table` | frontmatter: `ratings`、`snapshot.auto_ratings.details` | 10項目 ×（自動評価、最終評価、根拠）の表。自動と最終が違う行を強調。根拠は `details` の指標値と点数 |
| `plan_table` | frontmatter: `plan`, `kill_criteria`, `next_review` | 分割買いの計画（割合・条件）、上限比率、撤退条件（オブジェクト形式は「指標 演算子 値」も併記）、目標、期限、次の見直し日 |

### 5.2 review.md で使うもの

| NAME | 使うデータ | 表示 |
|---|---|---|
| `diff_since_last` | `snapshot` と `previous_snapshot` | 主要指標の前回比の表: 売上、営業利益、営業利益率（直近四半期と年次）、現金及び預金、有利子負債、持ちこたえられる月数、株価、PBR、PSR、正常化PER、清算価値・正常化価値・ピーク価値、リスクリワード、自動評価。改善を緑、悪化を赤 |
| `prediction_progress` | `parent_frontmatter.predictions` と frontmatter の `prediction_updates` | 予測ごとに、当初の内容・確率・期限と、今回の progress・outcome・note を並べる |
| `kill_criteria_check` | `snapshot.kill_criteria_check`、`parent_frontmatter.kill_criteria` | 数値で判定できる条件は「実際の値 vs 閾値」と該当・非該当（該当は赤）。文字列だけの条件は「人が判定」として一覧に表示 |

### 5.3 exit.md で使うもの

| NAME | 使うデータ | 表示 |
|---|---|---|
| `timeline` | 既存の株価 API、その銘柄の全レポートの frontmatter、`trades.csv` | 株価チャートに、レポートのマーカー（初回・経過記録・売却）と売買のマーカー（買い▲・売り▼）を重ねる。背景を `thesis_status` で色分けする（維持=無色、弱まった=薄黄、崩れた=薄赤）。§7 のチャート部品を流用してよい |
| `prediction_scorecard` | `parent_frontmatter.predictions`、frontmatter の `prediction_outcomes` | 予測ごとの 確率・結果・ブライアースコア `(prob − outcome)²` と、この銘柄の平均。§6.2 のスコアボードへのリンク |

---

## 6. レポート一覧・振り返り画面

ルーティングに `/reports` を追加する。ヘッダーからリンクする。

### 6.1 一覧（タブ1）

各銘柄の最新のレポートを1行で表示する。

- **列**：銘柄コード、会社名、状態、戦略、最新レポートの日付と種類、仮説の状態、次の見直し日、30日以内に期限が来る予測の数、最新のリスクリワード、タグ。
- **見直し日を過ぎた行**：赤で強調し、一覧の先頭に並べる。
- **絞り込み**：状態・戦略・タグで絞り込める。
- **行をクリックしたとき**：その銘柄ページの「企業分析」タブへ移動する。

### 6.2 スコアボード（タブ2）

以下を集計して表示する。

| 集計 | 定義 |
|---|---|
| 予測の精度 | outcome が確定した全予測のブライアースコアの平均 `mean((prob − outcome)²)`。件数も表示 |
| キャリブレーション図 | 確率を 0–0.2 / 0.2–0.4 / … / 0.8–1.0 に区切り、区間ごとに「平均確率」と「実際に当たった割合」を散布図で表示。対角線を基準線にする。件数が5未満の区間は薄く表示 |
| アルゴリズム候補とそれ以外の比較 | `algo_flagged` が true / false の銘柄ごとに、exit の件数、平均リターン、平均超過リターン、ブライアースコア |
| 判断と結果の4象限 | exit の `decision_quality` × `outcome_quality` の件数表（実力／不運／幸運／失敗） |
| 教訓タグの頻度 | `lesson_tags` の出現回数を棒グラフで |
| 評価と結果の関係 | initial の最終評価（項目ごと）別の、exit のリターンの平均（件数が少ないうちは件数も表示） |

件数が少ないうちは統計的に意味がないので、各集計に件数を必ず表示し、10件未満なら「参考値」と表示する。

---

## 7. 銘柄ページへの組み込み

### 7.1 「企業分析」タブ
既存の下段タブ（歩み値〜バックテスト）の末尾に「企業分析」を追加する。

- **上部**：その銘柄のレポートの履歴を横並びのタイムラインで表示する（日付と種類。クリックで選択）。初期表示は最新のレポート。
- **下部**：選択したレポートの本文を描画する（§4）。
- **レポートがない場合**：「レポートがありません。`new_report initial <ticker>` で作成できます」と表示する。
- **本文が長い場合**：右端に目次（見出しから自動生成）を出す。
- **全画面表示**：`/reports/{ticker}/{file}` の単独ページも用意し、タブから「全画面で開く」でリンクする。

### 7.2 チャートのレポートマーカー
チャートのツールバーの「ニュース/開示マーカー」と同じ形で、トグル「レポートマーカー」を追加する。

- **表示するもの**：レポートの日付にマーカー（初回・経過記録・売却で形か色を変える）、`trades.csv` の売買にマーカー。
- **ホバー**：レポートの種類、仮説の状態、行動を表示する。
- **クリック**：「企業分析」タブでそのレポートを開く。

### 7.3 ヘッダーのバッジ
銘柄名の横のバッジ（市場区分・業種）の並びに、レポートがある銘柄だけ状態バッジ（監視中／保有中／売却済／見送り）を追加する。

---

## 8. やらないこと

- 画面からのレポートの作成・編集・削除（すべてエディタと生成スクリプトで行う）。
- 財務指標や企業価値の再計算（JSON の値を表示するだけ）。
- スナップショットの生成（指示書 A の担当）。

---

## 9. 開発用データとテスト

### 9.1 開発用データ
`reports/_fixtures/9999/` に、架空の会社「サンプル造船（9999）」のレポートを作成する。

- **ファイル構成**：initial・review 2件・exit のひと揃いと、それぞれのスナップショット、`trades.csv`。
- **スナップショット**：雛形セットの `snapshot.example.json` を指示書 A §6 のスキーマに合わせて埋め、15年分の架空の数値を入れる。
- **わざと入れておくケース**：次の境界ケースを含め、表示が崩れないことを確認できるようにする。
  - null の値、`warnings`
  - FCF がマイナス
  - `risk_reward` が `"price_below_liquidation"`
  - 文字列だけの撤退条件
  - 期限切れの予測
- **読み込み方**：環境変数 `REPORTS_DIR=reports/_fixtures` で開発用データを読み込めるようにする。通常の一覧には `_fixtures` を出さない。

### 9.2 テスト
- **remark プラグイン**：`auto:NAME` と引数のパース、`snapshot-summary` ブロックの非表示、ガイドコメントの除去。
- **`{{path}}` の置き換え**：入れ子のキー、値がない場合、コードブロックの中は置き換えないこと。
- **ブライアースコア・キャリブレーションの集計**：手計算の例と一致すること。
- **API**：パストラバーサルの拒否（`../`、不正なファイル名）、パースに失敗したファイルの扱い。
- **部品**：24種の部品すべてについて、開発用データで描画し、キーが欠けていても例外にならないこと。

---

## 10. 完了時に報告すること

1. 実装したファイルの一覧と、既存コードへの変更の要約。
2. 24種の部品それぞれの実装状況（完成／簡易版／未実装）。
3. 開発用データで描画した「企業分析」タブと `/reports` のスクリーンショット。
4. 雛形やスキーマに対して、表示の都合で変更したほうがよいと判断した点（実装はせずに提案として）。
