# Claude Code 指示書 A：企業分析レポートの生成（スナップショット作成＋雛形の書き出し）

対象リポジトリ: stock-viewer
対になる指示書: `claude_code_instructions_B_report_viewer.md`（表示側）。本書だけで実装できるように書いているが、スナップショットの JSON スキーマ（§6）は B でも参照する。**§6 がスキーマの正**。

---

## 0. 背景と目的

個人投資家たーちゃん氏の「企業分析レポート」の形式を拡張したレポートを、銘柄ごとに Markdown で残していく。レポートは3つの層で構成する。

| 層 | 中身 | 担当 |
|---|---|---|
| ① 自動計算 | 財務諸表、財務指標、株価指標、清算価値、DCF、サイクル指標など | **本書（生成スクリプト）**が JSON に固定して保存 |
| ② 人の判断 | 投資仮説、予測と確率、撤退条件、最終評価、総括 | 人が Markdown に記入 |
| ③ 表示 | Markdown と JSON を読み、表・グラフを差し込んで表示 | 指示書 B（stock-viewer の画面） |

本書の担当は ① と、② のための雛形ファイルを用意するところまで。

処理の流れ:

```
new_report initial 7014
  → DB（既存データ＋TDnet）から as_of 時点で公表済みのデータだけを読む
  → 計算して reports/7014/2026-10-01_snapshot.json を保存
  → 雛形をコピーして reports/7014/2026-10-01_initial.md を作成
     （frontmatter の自動項目と、本文冒頭の要約ブロックを記入済みにする）
```

**最重要原則**
1. **ポイント・イン・タイム**：スナップショットには `as_of` 時点で公表済みだったデータだけを使う。開示日時が `as_of` の日付の終わり（23:59:59 JST）より後のデータは使わない。訂正開示も、訂正の開示日時で判定する。
2. **上書き禁止**：既存のレポート・スナップショットは絶対に上書きしない。同名ファイルがあればエラーで止まる。
3. **欠損は null**：取れない値は推測で埋めず `null` にし、`warnings` に理由を残す。

---

## 1. 最初にやること（既存コードの調査）

実装前に以下を調査し、**調査結果と実装方針を短く報告してから**着手すること。

- 既存のデータ取り込み処理とテーブル構成。現在の画面の更新時刻表示から、少なくとも次のソースがあることが分かっている:
  `kabutan_ohlc`, `yfinance_ohlc`, `kabu_tick`, `sbi_tick`, `kabutan_financial`, `edinet_financial`, `taisyaku_zandaka`, `taisyaku_history`, `jpx_margin`, `edinet_large_shareholding`, `news`, `google_trends`
- 画面に「適時開示」タブがあるので、**TDnet の取り込み処理が既にある可能性が高い**。あればそれを拡張し、二重実装しない。
- `edinet_financial` がどの科目を、何年分、連結・単体のどちらで持っているか。XBRL 由来か（§5.1 で TDnet に値がない場合の補完に使う）。
- 既存の TDnet データ（過去分は収集済み）: テーブル構成、保存されている期間、XBRL（サマリー・財務諸表）と PDF の原本が保存されているか、§5.2 の T1〜T34 のうちどの開示種別・値が既に構造化されているか。
- 株式分割・併合の調整済み株価があるか。
- 銘柄マスタ（33業種コード、市場区分、発行済株式数、自己株式数）の有無。
- バックエンドの言語・構成（本書は Python を想定して書いているが、既存に合わせてよい）。

---

## 2. 成果物

1. CLI `new_report`（§3）
2. スナップショット作成モジュール（データ取得層・計算層・書き出し層に分ける）
3. TDnet 取り込みの拡張（§5）
4. 設定ファイル（§7 評価基準、§8 業界の先行指標）
5. 単体テスト（§11）
6. `reports/README.md`（使い方を短く）

---

## 3. CLI 仕様

```
new_report initial <ticker> [--as-of YYYY-MM-DD] [--peers 7003,7014,...] [--dry-run]
new_report review  <ticker> [--as-of YYYY-MM-DD] [--trigger earnings|disclosure|price_move|scheduled] [--dry-run]
new_report exit    <ticker> [--as-of YYYY-MM-DD] [--dry-run]
new_report snapshot <ticker> [--as-of YYYY-MM-DD] [--peers ...]   # JSON を標準出力に出すだけ。ファイルは書かない
```

- `--as-of` 省略時は当日。ただし当日の株価終値がまだなければ直近の営業日にし、その旨を表示する。
- `--peers` 省略時は、同じ33業種コードで時価総額が近い順に5社を自動選定する。選ばれた銘柄をスナップショットに記録する。
- `--dry-run` は書き出す予定のファイル名と要約ブロックを表示するだけ。
- 保存先はリポジトリ直下の `reports/`。環境変数 `REPORTS_DIR` で変更できる（レポートを別の git リポジトリで管理する可能性があるため）。
- 同名ファイルがあれば**エラーで終了**（上書きオプションは作らない）。
- 終了時に、作成したファイルのパスと `warnings` の件数を表示する。

### 3.1 各サブコマンドの動き

| サブコマンド | 作るファイル | 前提 |
|---|---|---|
| initial | `YYYY-MM-DD_initial.md`, `YYYY-MM-DD_snapshot.json` | なし。既に initial があっても作れる（再分析）。その場合は警告を出す |
| review | `YYYY-MM-DD_review.md`, `YYYY-MM-DD_snapshot.json` | 同銘柄に initial が必要。なければエラー |
| exit | `YYYY-MM-DD_exit.md`, `YYYY-MM-DD_snapshot.json` | initial が必要。売買記録 `trades.csv`（§9.3）が必要 |

---

## 4. ディレクトリ構成

```
reports/
  README.md
  rubric.md                     # 評価基準（人向けの説明。雛形セットに同梱）
  _templates/
    initial.md  review.md  exit.md
  _config/
    rubric_v1.yaml              # 評価基準の閾値（機械向け。§7）
    leading_indicators.yaml     # 業界の先行指標の登録（§8。任意）
  <ticker>/
    YYYY-MM-DD_initial.md
    YYYY-MM-DD_snapshot.json
    YYYY-MM-DD_review.md
    YYYY-MM-DD_snapshot.json
    YYYY-MM-DD_exit.md
    trades.csv                  # 売買記録（人が記入。§9.3）
```

`_templates/` には、別途渡す雛形セット（`initial.md`, `review.md`, `exit.md`, `rubric.md`, `snapshot.example.json`）を配置する。雛形の本文は変更しないこと。変更が必要だと判断した場合は、実装せずに提案として報告する。

同じ日に同じ銘柄の initial と review を作ることは想定しない。スナップショットのファイル名が衝突した場合はエラーにする。

---

## 5. データソースと取得項目

### 5.1 ソースの役割分担

**方針：TDnet で取得できるものは TDnet から取得する。** EDINET は、TDnet に存在しないデータと、既存の TDnet データに値がない場合の補完にだけ使う。

| データ | 第1ソース | 補完ソース（第1ソースで取れない場合のみ） | 補足 |
|---|---|---|---|
| 年次の BS / PL / CF | **TDnet（決算短信の財務諸表 XBRL）** | EDINET（有価証券報告書 XBRL、既存の `edinet_financial`） | 15年分を目標。TDnet の既存データに値がない期間だけ補完ソースで埋める |
| 半期・四半期（Q1〜Q3）の BS / PL / CF | **TDnet（四半期決算短信の財務諸表 XBRL）** | EDINET（半期報告書。Q2のみ） | 2024年4月以降、四半期報告書が廃止され Q1・Q3 は短信のみ |
| セグメント別の売上・利益 | **TDnet（決算短信のセグメント情報注記。XBRL でタグ付けされていれば）** | EDINET（有価証券報告書） | 短信で XBRL 化されていない会社は補完ソースを使う |
| 会社予想・配当予想・予想の修正 | **TDnet** | なし | |
| 発行済株式数・自己株式数 | **TDnet（決算短信サマリー、自己株式関連の開示）** | EDINET | 1株あたりの値の計算に使う |
| 資本政策・継続企業の前提・業績修正など | **TDnet** | なし | §5.2 |
| 大株主上位10 | EDINET（有価証券報告書） | なし | TDnet には存在しない |
| 大量保有報告書 | EDINET（既存の `edinet_large_shareholding`） | なし | TDnet には存在しない |
| 主要な販売先（売上の10%以上の顧客） | EDINET（有価証券報告書のセグメント情報注記） | なし | TDnet には存在しない。取れなければ null |
| 株価（日足、分割調整済み） | 既存の `kabutan_ohlc` / `yfinance_ohlc` | | |

**値が食い違う場合の優先順位**
1. as_of 時点で開示済みの値だけを候補にする（§0 のポイント・イン・タイム）。
2. 同じ期間・同じ項目の値が複数ある場合は **TDnet（訂正開示があれば訂正後の値）を優先**し、TDnet に値がない場合に限り EDINET を使う。
3. TDnet と EDINET の両方に値があって食い違う場合は、TDnet の値を採用したうえで、差異（項目、期間、両方の値）を `warnings` に `SOURCE_MISMATCH` として記録する。
4. スナップショットの `sources` には、系列ごとにどちらのソースを使ったか（期間別に混在する場合はその内訳）を記録する。例: `"balance_sheet": {"tdnet": ["2021-03", …], "edinet": ["2012-03", …]}`。

**TDnet の過去データ**：TDnet の過去分は**収集済みで、既存の DB に保存されている**。新たな過去分の収集処理は作らず、既存の保存データを第1ソースとして読む。新しい開示の取り込みも既存の処理に任せる（T1〜T34 で既存の処理が保存していない種別・値があれば、既存の処理を拡張する）。EDINET で補完するのは、既存の TDnet データに該当する開示や値（XBRL の科目など）がない場合だけ。

### 5.2 TDnet から読み込む必要のあるデータ（全列挙）

以下は本機能で使う TDnet 開示のすべて。各項目について、**開示日時（秒まで）・開示種別・タイトル・PDF の URL・XBRL があれば XBRL**を保存すること。「抽出する値」は構造化して DB に持つ値。表の後に書いた共通の要件も満たすこと。

#### (1) 決算関連（必須）

| # | 開示 | 抽出する値 | 使う場所 |
|---|---|---|---|
| T1 | 決算短信（通期） | サマリー XBRL の全項目: 売上高、営業利益、経常利益、当期純利益（親会社株主帰属）、EPS、総資産、純資産、自己資本比率、BPS、営業CF・投資CF・財務CF・期末現金同等物、配当（実績・予想）、**翌期の会社予想**（売上・営業・経常・純利益・EPS）、期末発行済株式数（自己株式を含む）、期末自己株式数、期中平均株式数。**加えて財務諸表 XBRL（添付の連結財務諸表）の全科目**: §6 の `balance_sheet`・`income_statement`・`cash_flow` の全キー（減価償却費を含む）。セグメント情報注記が XBRL でタグ付けされていればセグメント別の売上・利益 | **年次の財務諸表の第1ソース**（§5.1）、会社予想、近似PER、株式数、セグメント |
| T2 | 四半期決算短信（Q1・Q2・Q3） | T1 と同じサマリー項目（累計値）と、財務諸表 XBRL の全科目（四半期 BS、累計 PL、Q2 は CF も）。セグメント情報（タグ付けされていれば） | **四半期の財務諸表の第1ソース**、**四半期 PL の系列**（累計の差分で単独四半期を計算）、赤字幅の四半期比較 |
| T3 | 決算短信・四半期決算短信の本文 | 「継続企業の前提に関する注記」の有無と本文。「継続企業の前提に関する重要事象等」の記載の有無。受注高・受注残高（記載があれば） | 生存力、反転兆候 |
| T4 | 業績予想の修正に関するお知らせ | 修正前・修正後の売上・営業・経常・純利益・EPS、対象期間、修正理由の本文 | 反転兆候（上方修正）、近似PER の分母 |
| T5 | 配当予想の修正／剰余金の配当に関するお知らせ | 修正前後の1株配当、基準日 | 配当性向、株主重視姿勢 |
| T6 | 決算期変更に関するお知らせ | 新旧の決算期 | 年度の揃え（変則決算の扱い） |
| T7 | 決算発表予定日の通知 | 発表予定日。TDnet になければ JPX の決算発表予定日一覧で補完 | `next_review` の自動設定 |
| T8 | 訂正開示（上記 T1〜T6 の訂正） | 訂正後の値と訂正の開示日時 | ポイント・イン・タイムでの値の差し替え |

#### (2) 資本政策（必須）

| # | 開示 | 抽出する値 | 使う場所 |
|---|---|---|---|
| T9 | 自己株式の取得に係る事項の決定 | 取得上限株数・上限金額・取得期間 | 資本政策の履歴 |
| T10 | 自己株式の取得状況／取得終了 | 期間中の取得株数・金額 | 同上 |
| T11 | 自己株式の消却 | 消却株数・消却日 | 同上、株式数 |
| T12 | 自己株式の処分（第三者割当等） | 処分株数・価格・割当先 | 希薄化 |
| T13 | 新株式発行（公募・第三者割当）、株式の売出し | 発行株数・発行価格・調達額・資金使途・割当先 | 希薄化、生存力 |
| T14 | 新株予約権の発行（**行使価額修正条項付＝MSCB を含む**） | 潜在株式数、行使価額、修正条項の有無、割当先 | 希薄化 |
| T15 | 転換社債型新株予約権付社債の発行 | 発行額、転換価額、潜在株式数、償還日 | 希薄化、有利子負債 |
| T16 | 新株予約権の月間行使状況 | 月間の行使数・残数 | 希薄化の進み具合 |
| T17 | 株式分割・株式併合 | 比率、効力発生日 | **1株あたりの値の調整**（必須） |
| T18 | 株主還元方針・配当方針の変更 | 方針の本文（配当性向目標、DOE、累進配当など） | 株主重視姿勢 |
| T19 | 資本コストや株価を意識した経営の実現に向けた対応 | 開示の有無と本文 | 株主重視姿勢 |

#### (3) 財務・事業リスク（必須）

| # | 開示 | 抽出する値 | 使う場所 |
|---|---|---|---|
| T20 | 特別損失（減損損失等）の計上 | 金額、内容 | 財務諸表の注記、一時要因の判別 |
| T21 | 繰延税金資産の取崩し | 金額 | 同上 |
| T22 | 資金の借入、コミットメントライン契約の締結 | 金額、期間、借入先 | 生存力 |
| T23 | 財務制限条項への抵触 | 抵触の有無、対象借入 | 生存力（重大） |
| T24 | 社債の発行 | 金額、償還日、利率 | 有利子負債、返済期限 |
| T25 | 事業再編（事業譲渡・撤退・工場閉鎖・生産能力の削減・希望退職） | 種別、金額・規模、実施時期 | 反転兆候（供給側の引き締め）、一時要因 |
| T26 | 合併・子会社化・株式交換などの M&A | 相手先、金額 | 事業構造の変化 |
| T27 | 中期経営計画 | 開示の有無、PDF | 事業理解（人が読む） |
| T28 | 月次の売上・受注（開示している会社のみ） | 月次の売上・受注の値または前年比 | 反転兆候 |

#### (4) 株主・上場区分（必須）

| # | 開示 | 抽出する値 | 使う場所 |
|---|---|---|---|
| T29 | 主要株主・親会社・その他の関係会社の異動 | 異動前後の保有比率 | 株主動向 |
| T30 | 公開買付け（TOB）の開始・結果、MBO | 買付価格、期間、結果 | 株主動向、売却理由 |
| T31 | 代表取締役の異動 | 氏名、就任日 | 事業理解 |
| T32 | 監理銘柄・整理銘柄・特別注意銘柄の指定、上場廃止 | 指定の種別と日付 | 生存力（重大）、上場廃止銘柄の扱い |

#### (5) 任意（取れれば取る）

| # | 開示 | 使う場所 |
|---|---|---|
| T33 | 決算説明資料（PDF） | 事業理解の下書きに LLM で使う（人が読む） |
| T34 | 受注・大型契約の獲得に関するお知らせ | 反転兆候 |

#### TDnet 取り込みの共通要件

- 開示種別の判定は、タイトルのキーワードと、XBRL がある場合はその書類種別で行う。キーワードの辞書は設定ファイルに出し、テストを付ける。
- **上場廃止銘柄の開示も保持する**（生存者バイアス対策）。
- 過去分は収集済みの既存データを使う（§5.1）。既存データに値がなく EDINET でも補完できなかった項目は、スナップショットの `warnings` に `TDNET_VALUE_MISSING` として記録する。
- XBRL の勘定科目は会社ごとに独自の拡張科目があるため、§6 の各キーへの対応表（標準タクソノミの要素名 → キー）を設定ファイルに出す。対応できない科目は `warnings` に記録する。
- 取り込みは冪等にする（同じ開示を二重に登録しない）。キーは開示日時＋銘柄コード＋タイトル＋URL。

---

## 6. スナップショット JSON スキーマ（正）

- 金額の単位は**百万円**、1株あたりの値は**円**、比率は**小数**（12% → 0.12）。
- 年次の系列は `fiscal_years` と同じ長さの配列にし、取れない年は `null`。**配列の順は古い年から新しい年**。
- 年度ラベルは決算期末の年月 `"YYYY-MM"`（決算期変更に対応するため）。
- 雛形セットの `snapshot.example.json` は形の例。以下の定義との違いは本節を優先する。

```jsonc
{
  "schema_version": 1,
  "ticker": "7014",
  "company_name": "…",
  "as_of": "2026-10-01",
  "generated_at": "ISO8601 +09:00",
  "report_type": "initial | review | exit",
  "rubric_version": 1,
  "sources": {
    "last_ingested": { "<source名>": "そのソースの最終取り込み日時" },
    "by_series": {                                  // 系列ごとに使ったソースと期間（§5.1）
      "balance_sheet": { "tdnet": ["2021-03", "…"], "edinet": ["2012-03", "…"] },
      "income_statement": { "…": "同形式" }, "cash_flow": { "…": "同形式" },
      "quarterly": { "…": "同形式" }, "segments": { "…": "同形式" }
    }
  },
  "warnings": [ { "code": "MISSING_SEGMENT", "field": "segments", "message": "…" } ],

  "company": {
    "sector33_code": "…", "sector33_name": "…", "market": "プライム",
    "fiscal_year_end_month": 3, "consolidated": true
  },

  "market": {
    "price": 1234, "price_date": "2026-10-01",
    "shares_issued": 30000000, "treasury_shares": 500000,
    "shares_outstanding": 29500000,            // 発行済 − 自己株式
    "market_cap": 36403,                        // price × shares_outstanding / 1e6
    "peak_price": 4600, "peak_date": "2021-11-08",   // as_of 以前10年の最高終値（分割調整済み）
    "drawdown_from_peak": -0.732,
    "price_history_10y_ref": "DB参照のため保存しない"  // 表示側は既存APIから取得
  },

  "fiscal_years": ["2012-03", "…", "2026-03"],     // 最大15年
  "balance_sheet": {
    "cash": [], "receivables": [], "securities_current": [], "inventory": [],
    "current_assets_other": [], "current_assets_total": [],
    "ppe": [], "intangibles": [], "investment_securities": [], "investments_other": [],
    "noncurrent_assets_total": [], "total_assets": [],
    "payables": [], "short_term_debt": [], "current_portion_long_term_debt": [],
    "current_portion_bonds": [], "current_liabilities_other": [], "current_liabilities_total": [],
    "long_term_debt": [], "bonds": [], "lease_obligations": [],
    "noncurrent_liabilities_other": [], "noncurrent_liabilities_total": [],
    "total_liabilities": [], "shareholders_equity": [], "net_assets": []
  },
  "income_statement": {
    "revenue": [], "cost_of_sales": [], "sga": [],
    "operating_income": [], "ordinary_income": [], "net_income": [],
    "cost_of_sales_ratio": [], "sga_ratio": [],
    "revenue_yoy": [], "operating_income_yoy": [], "ordinary_income_yoy": [], "net_income_yoy": []
  },
  "cash_flow": {
    "operating_cf": [], "investing_cf": [], "capex": [], "depreciation": [],
    "financing_cf": [], "fcf": []                   // fcf = operating_cf − capex
  },
  "quarterly": {                                    // 直近12四半期（単独四半期）
    "periods": ["2023-Q3", "…"],
    "revenue": [], "operating_income": [], "net_income": [],
    "operating_margin": []
  },
  "segments": [ { "name": "…", "revenue": [], "operating_income": [] } ],
  "forecast": {                                     // as_of 時点で最新の会社予想
    "fiscal_year": "2027-03", "disclosed_at": "…",
    "revenue": null, "operating_income": null, "ordinary_income": null,
    "net_income": null, "eps": null, "dividend_per_share": null,
    "revisions": [ { "disclosed_at": "…", "field": "operating_income", "before": 0, "after": 0 } ]
  },
  "ratios": {
    "equity_ratio": [], "current_ratio": [], "net_cash": [], "debt_to_ebitda": [],
    "operating_margin": [], "roe": [], "roa": [], "roic": [],
    "revenue_growth": [], "operating_income_growth": [],
    "cycle_stats": {                                 // 指標ごとの 中央値・現在値・パーセンタイル
      "operating_margin": { "median_10y": 0.061, "latest": -0.02, "percentile_latest": 0.05 },
      "roe": { "…": "同形式" }, "roic": { "…": "同形式" }, "equity_ratio": { "…": "同形式" }
    },
    "consecutive_loss_years": { "operating": 2, "net": 2 }
  },
  "valuation_metrics": {
    "per_forecast": null, "per_trailing": null, "per_normalized": 7.4,
    "pcfr": null, "psr": 0.30, "pbr": 0.45, "earnings_yield": null,
    "per_x_pbr": null, "ev": null, "ev_ebitda": null, "roic": null,
    "accruals_to_assets": -0.021, "market_cap": 36403, "drawdown_from_peak": -0.732
  },
  "cost_structure": {
    "fixed_cost": null, "variable_cost_ratio": null, "r2": null, "n_years": 10,
    "breakeven_revenue": null, "latest_revenue": null, "gap_to_breakeven_pct": null
  },
  "liquidation_value": {
    "haircuts": { "cash": 1.0, "receivables": 0.85, "securities_current": 1.0, "inventory": 0.5,
                  "current_assets_other": 0.0, "ppe": 0.5, "intangibles": 0.0, "investments": 0.5 },
    "components": { "<科目>": { "book": 0, "haircut": 0.5, "adjusted": 0 } },
    "adjusted_assets": 0, "total_liabilities": 0, "value_total": 0, "value_per_share": 0
  },
  "dcf": {
    "discount_rate": 0.10, "net_cash": 0, "fcf_base": 0, "fcf_base_year": "2026-03",
    "bear_total": null, "bear_per_share": null,
    "bull_growth_rate": 0.20, "bull_growth_years": 5, "bull_total": null, "bull_per_share": null
  },
  "normalized_value": {
    "method": "latest_revenue_x_median_margin_10y",
    "median_operating_margin": 0.061, "normalized_operating_income": 0,
    "tax_rate": 0.30, "normalized_fcf": 0, "value_per_share": 0,
    "peak_operating_margin": 0.118, "peak_fcf": 0, "peak_value_per_share": 0
  },
  "value_range": { "liquidation": 0, "price": 0, "normalized": 0, "peak": 0, "risk_reward": null },
  "cycle": {
    "peer_tickers": [], "peer_selection": "auto | manual",
    "peers": [ { "ticker": "…", "name": "…", "operating_margin": [], "pbr": null, "psr": null,
                 "margin_change_latest": null } ],
    "industry_median_operating_margin": [],        // 自社＋同業の年次中央値
    "peer_margin_deterioration_share": null,
    "industry_margin_ar1_phi": null, "industry_margin_half_life_years": null,
    "leading_indicators": [ { "name": "…", "unit": "…", "latest": null, "yoy": null, "series_ref": "…" } ]
  },
  "survival": {
    "runway_months": null, "runway_basis": "annual_ocf | ttm_ocf", "ocf_positive": false,
    "debt_due_within_1y": 0, "debt_due_to_cash": null,
    "going_concern_note": false, "going_concern_events": false,
    "covenant_breach": false, "commitment_line": null,
    "exchange_designation": null                     // 監理・整理・特別注意など
  },
  "reversal_signals": {
    "loss_narrowing_qoq": null,                      // 直近四半期の営業損益が前四半期より改善
    "loss_narrowing_yoy": null,
    "upward_revision_last_6m": false,
    "industry_capex_to_depreciation": null,          // 同業合算の 設備投資 ÷ 減価償却費
    "restructuring_last_12m": []                     // T25 の開示の要約
  },
  "shareholders": {
    "top10": [ { "name": "…", "ratio": 0.0, "as_of": "…" } ],
    "large_shareholding_reports": [ { "filer": "…", "ratio": 0.0, "change_pt": 0.0, "date": "…" } ],
    "payout_ratio": [], "dividend_per_share": [],
    "capital_actions": [ { "date": "…", "type": "buyback_decision | buyback_done | cancellation | disposal | equity_issue | msc_warrant | cb | split | reverse_split", "detail": "…", "dilution_ratio": null, "url": "…" } ],
    "shareholder_return_policy": { "disclosed_at": null, "text": null },
    "capital_cost_disclosure": { "disclosed_at": null, "url": null }
  },
  "disclosures": [                                   // as_of 前24か月の関連開示（T1〜T34）
    { "code": "T4", "disclosed_at": "…", "title": "…", "url": "…" }
  ],
  "auto_ratings": {
    "asset_value": "B", "earnings_value": "B", "financial_health": "C",
    "profitability": "C", "growth": "D",
    "cyclicality": null, "survival": "B", "reversal": null,
    "business_quality": null, "shareholder_policy": null,
    "details": { "<項目>": { "metric": 0.0, "points": null, "note": "…" } }
  },
  "next_earnings_date": null,

  // review / exit のときだけ
  "previous_snapshot": "YYYY-MM-DD_snapshot.json",
  "kill_criteria_check": [ { "text": "…", "metric": "…", "op": "<", "value": 12, "actual": 28, "hit": false } ],
  // exit のときだけ
  "trades": [ { "date": "…", "side": "buy|sell", "qty": 0, "price": 0, "fee": 0 } ],
  "result": { "avg_buy_price": null, "avg_sell_price": null, "return_pct": null, "holding_days": null,
              "benchmark": "TOPIX", "benchmark_return_pct": null, "excess_return_pct": null,
              "max_drawdown_pct": null }
}
```

---

## 7. 計算仕様

以下、`t` は最新の決算期、`R` は割引率 10%、`税率` は 30%。分母が0以下、または入力が null のときは結果も null にし、`warnings` に記録する。

### 7.1 株式数・時価総額
- `shares_outstanding` = as_of 時点で最新の開示の（発行済株式数 − 自己株式数）。as_of 以降に効力が発生する分割・併合は使わない。
- 1株あたりの値 = 総額（百万円）× 1e6 ÷ `shares_outstanding`。

### 7.2 財務指標（年次）
| 指標 | 定義 |
|---|---|
| equity_ratio（株主資本比率） | 株主資本 ÷ 総資産 |
| current_ratio | 流動資産 ÷ 流動負債 |
| 有利子負債 | 短期借入金 + 1年内返済長期借入金 + 1年内償還社債 + 長期借入金 + 社債 + リース債務 |
| net_cash | 現金及び預金 + 流動資産の有価証券 − 有利子負債 |
| EBITDA | 営業利益 + 減価償却費 |
| debt_to_ebitda | 有利子負債 ÷ EBITDA（EBITDA ≤ 0 なら null） |
| operating_margin | 営業利益 ÷ 売上高 |
| roe | 当期純利益 ÷ 期首期末平均の株主資本 |
| roa | 当期純利益 ÷ 期首期末平均の総資産 |
| roic | 営業利益 × (1 − 税率) ÷ (有利子負債 + 純資産) |
| revenue_growth / operating_income_growth | 前年比。前年が負の場合の営業利益成長は null |
| cycle_stats | 直近10年の中央値、最新値、最新値の直近15年分布内でのパーセンタイル |
| consecutive_loss_years | 最新期から遡って営業損失・純損失が続いた期数 |

### 7.3 株価指標
| 指標 | 定義 |
|---|---|
| per_forecast（近似PER） | 時価総額 ÷ 会社予想の純利益（T1・T4 の最新値） |
| per_trailing | 時価総額 ÷ 直近4四半期の純利益合計（TTM） |
| per_normalized | 時価総額 ÷ (正常化営業利益 × (1 − 税率)) |
| pcfr | 時価総額 ÷ (純利益 + 減価償却費)（直近年度） |
| psr | 時価総額 ÷ 売上高（直近年度） |
| pbr | 時価総額 ÷ 純資産（直近の開示。四半期でもよい） |
| earnings_yield（予想収益率） | 1 ÷ per_forecast |
| per_x_pbr | per_forecast × pbr |
| ev | 時価総額 + 有利子負債 − 現金及び預金 − 流動資産の有価証券 |
| ev_ebitda | ev ÷ EBITDA（直近年度） |
| accruals_to_assets | (純利益 − 営業CF) ÷ 総資産（直近年度） |

### 7.4 損益分岐点（cost_structure）
- 直近10年（最低6年、足りなければ null）で `営業費用 = 売上高 − 営業利益` を売上高に回帰する: `営業費用 = F + v × 売上高`。
- `fixed_cost = F`、`variable_cost_ratio = v`、`breakeven_revenue = F ÷ (1 − v)`（v ≥ 1 なら null）、決定係数 `r2` も保存する。
- `gap_to_breakeven_pct = (breakeven_revenue − latest_revenue) ÷ latest_revenue`。

### 7.5 清算価値（たーちゃん氏の雛形準拠）
修正資産 = Σ(簿価 × 掛け目) で、掛け目は次のとおり。

| 科目 | 掛け目 |
|---|---|
| 現金及び預金 | 100% |
| 受取手形及び売掛金 | 85% |
| 有価証券（流動資産） | 100% |
| 棚卸資産 | 50% |
| その他の流動資産 | 0% |
| 有形固定資産 | 50% |
| 無形固定資産 | 0% |
| 投資等（投資有価証券を含む投資その他の資産） | 50% |

`value_total` = 修正資産 − 負債合計。`components` に科目ごとの 簿価・掛け目・修正後 を残す。使う貸借対照表は as_of 時点で最新のもの（四半期でもよい）。どの期の BS を使ったかも記録する。

### 7.6 DCF（たーちゃん氏の雛形準拠）
- `fcf_base` = 直近年度の FCF（営業CF − 設備投資）。
- 弱気: `bear_total = net_cash + fcf_base ÷ R`
- 強気: 1〜5年目は年20%成長、6年目以降は成長なしとする。`bull_total = net_cash + Σ_{k=1..5} fcf_base×1.2^k ÷ 1.1^k + (fcf_base×1.2^5 ÷ R) ÷ 1.1^5`
- `fcf_base ≤ 0` のとき、bear・bull とも null にし、`warnings` に `NEGATIVE_FCF`（正常化収益バリューを参照）と記録する。

### 7.7 正常化収益バリュー（シクリカル株用）
- `median_operating_margin` = 直近10年の営業利益率の中央値。
- `normalized_operating_income` = 直近年度の売上高 × median_operating_margin。
- `normalized_fcf` = normalized_operating_income × (1 − 税率) + (減価償却費 ÷ 売上高 の10年中央値 − 設備投資 ÷ 売上高 の10年中央値) × 直近年度の売上高。
- `value_per_share` = (net_cash + normalized_fcf ÷ R) の1株あたり。
- ピーク: 直近15年で最大の営業利益率を使い、同じ式で `peak_fcf`・`peak_value_per_share` を出す。
- `value_range.risk_reward` = (normalized − price) ÷ (price − liquidation)。price ≤ liquidation のときは `"price_below_liquidation"` という文字列にする。

### 7.8 サイクル指標
- **同業の選定**：§3 のとおり。同業の財務も同じ処理で取得する（系列は operating_margin のみでよい）。
- `industry_median_operating_margin`：自社と同業の年次営業利益率の中央値（各年）。
- `peer_margin_deterioration_share`：同業のうち「最新期の営業利益率 < 前期」かつ「最新期 < その社の10年中央値」を満たす割合。
- `industry_margin_ar1_phi`：industry_median_operating_margin について `(m_t − μ) = φ(m_{t−1} − μ) + ε` を最小二乗で推定。`half_life = ln(0.5) ÷ ln(φ)`（0 < φ < 1 のときのみ。それ以外は null）。年数が10未満なら null。
- `leading_indicators`：§8 の設定に登録された業種だけ。値の取得処理は今回は**枠だけ**作り、データがなければ空配列でよい。

### 7.9 生存力
- `runway_months`：営業CF（TTM が取れれば TTM、なければ直近年度）が負のとき、`(現金及び預金 + 流動資産の有価証券) ÷ (−営業CF ÷ 12)`。営業CF ≥ 0 なら null とし、`ocf_positive: true`。
- `debt_due_within_1y` = 短期借入金 + 1年内返済長期借入金 + 1年内償還社債。`debt_due_to_cash` = それ ÷ 現金及び預金。
- `going_concern_note` / `going_concern_events`：T3、`covenant_breach`：T23、`commitment_line`：T22、`exchange_designation`：T32。いずれも as_of 前18か月の開示で判定する。

### 7.10 反転兆候
- `loss_narrowing_qoq`：直近の単独四半期の営業利益 > その前の四半期の営業利益（直近が赤字の場合にのみ true/false。黒字なら null）。`loss_narrowing_yoy` は前年同期比で同様。
- `upward_revision_last_6m`：as_of 前6か月以内に T4 で営業利益または純利益の予想が上方修正されたか。
- `industry_capex_to_depreciation`：同業＋自社の直近年度の 設備投資合計 ÷ 減価償却費合計。
- `restructuring_last_12m`：T25 の開示のタイトル・日付・URL。

### 7.11 自動評価
- 閾値は `reports/_config/rubric_v1.yaml` から読む（コードに直書きしない）。内容は雛形セットの `rubric.md` の表と一致させる。YAML の例:

```yaml
rubric_version: 1
asset_value:    { metric: market_cap_to_liquidation, bins: [0.7, 1.0, 1.5, 2.5], order: asc }
earnings_value: { metric: per_normalized,            bins: [6, 9, 12, 18],       order: asc }
profitability:  { metric: median_operating_margin,   bins: [0.12, 0.08, 0.05, 0.02], order: desc }
growth:         { metric: peak_to_peak_revenue_cagr, bins: [0.10, 0.05, 0.02, 0.0],  order: desc }
financial_health:
  components:
    equity_ratio:        { points: [[0.60, 2], [0.40, 1]], else: 0 }
    net_cash_to_mcap:    { points: [[0.50, 2], [0.0, 1]],  else: 0 }
    runway_months:       { points: [[36, 2], [18, 1]], else: 0, null_if_ocf_positive: 2 }
  grade_by_total: { A: [6], B: [5], C: [3, 4], D: [2], E: [0, 1] }
survival:
  rules:   # 上から順に最初に当てはまったもの
    - { if: "going_concern_note or exchange_designation or covenant_breach", grade: E }
    - { if: "ocf_positive and debt_due_to_cash < 0.5", grade: A }
    - { if: "runway_months >= 36", grade: A }
    - { if: "runway_months >= 24", grade: B }
    - { if: "runway_months >= 12", grade: C }
    - { if: "runway_months >= 6",  grade: D }
    - { else: E }
cyclicality:
  rules:
    - { if: "peer_margin_deterioration_share >= 0.6 and industry_margin_half_life_years <= 3", grade: A }
    - { if: "peer_margin_deterioration_share >= 0.6", grade: B }
    - { if: "peer_margin_deterioration_share >= 0.3", grade: C }
    - { else: D }       # 自動では E をつけない（構造的な衰退の判断は人が行う）
reversal:
  score_items: [loss_narrowing_qoq, loss_narrowing_yoy, upward_revision_last_6m, "industry_capex_to_depreciation < 1.0"]
  grade_by_count: { A: [4], B: [3], C: [2], D: [1], E: [0] }
```

- `peak_to_peak_revenue_cagr`：直近15年の売上高で、最大の年とそれ以前の局所最大の年（前後2年より大きい年）の間の年平均成長率。見つからなければ null。
- `market_cap_to_liquidation`：清算価値が0以下なら E。
- `business_quality`・`shareholder_policy` は人が評価するので常に null。
- `details` に各評価の根拠（指標値・点数）を残す。
- 条件式は安全に評価すること（`eval` を使わない。小さな式パーサか、YAML 構造を固定する）。

---

## 8. 業界の先行指標の設定（任意・枠のみ）

`reports/_config/leading_indicators.yaml` に、33業種コードごとの先行指標の名前・単位・データの取得元を登録できるようにする。今回はデータ取得の実装は不要。スナップショットには、登録されている指標の名前だけを載せ、値は null にする。

```yaml
"3450":   # 例: 鉄鋼
  - { name: "鋼材価格 − 鉄鉱石・原料炭のスプレッド", unit: "円/t", source: "未実装" }
```

---

## 9. 雛形の書き出し

### 9.1 frontmatter の自動記入
雛形の frontmatter を YAML として読み、以下のキーだけを書き換える。**キーの順序と、雛形のコメントは残す**（ruamel.yaml などコメントを保持できるライブラリを使う）。

| 対象 | キー | 値 |
|---|---|---|
| 共通 | `ticker`, `as_of`, `snapshot` | |
| initial | `company_name`, `algo_flagged` | `algo_flagged` は既存のスクリーニング結果テーブルがあれば、as_of 時点で候補だったかを入れる。なければ false のままにし、報告する |
| initial | `ratings.*.auto` | `auto_ratings` の値 |
| initial | `valuation.*` | price, liquidation, dcf_bear, dcf_bull, normalized, peak, risk_reward（1株あたり） |
| initial | `next_review` | `next_earnings_date`（なければ as_of + 90日） |
| review | `parent` | 最新の initial のファイル名 |
| review | `period` | as_of 時点で最新の決算期（例: "2027年3月期 Q2"） |
| review | `prediction_updates` | 親レポートの `predictions` の id を列挙（progress は on_track、outcome は null のまま） |
| review | `kill_criteria_hit` | オブジェクト形式の撤退条件のうち、自動判定で該当したものの `text` |
| review | `next_review` | 同上 |
| exit | `parent`, `result.*` | §9.3 |
| exit | `prediction_outcomes` | 親レポートの id を列挙。直近の review の outcome があれば引き継ぐ |

### 9.2 本文の要約ブロック
雛形本文の次の2行の間に、Markdown の表を書き込む（これ以外の本文は変更しない）。

```
<!-- snapshot-summary:start 生成スクリプトが書き込む。手で編集しない -->
<!-- snapshot-summary:end -->
```

内容は下の例のとおり（stock-viewer 以外のビューアで開いても主要な数値が読めるようにするため）。

```markdown
| 株価 | 時価総額 | PBR | PSR | 正常化PER | ピーク比 |
|---|---|---|---|---|---|
| 1,234円 | 364億円 | 0.45 | 0.30 | 7.4 | −73% |

| 清算価値 | 正常化価値 | ピーク価値 | リスクリワード | 持ちこたえ月数 | 連続赤字 |
|---|---|---|---|---|---|
| 900円 | 2,400円 | 3,800円 | 3.5 | 28か月 | 2期 |

自動評価: 資産B / 収益B / 健全性C / 収益性C / 成長性D / 循環性A / 生存力B / 反転— （rubric v1）
警告: 2件（詳細は snapshot.json の warnings）
```

review では、1行目の表の各値に前回スナップショットからの変化を括弧で併記する（例: `0.45（前回0.52）`）。

### 9.3 売買記録と exit の結果
- `reports/<ticker>/trades.csv`（人が記入）: `date,side,qty,price,fee`（side は buy / sell）。
- `avg_buy_price`・`avg_sell_price` は数量で加重した平均値。`return_pct` = (売却総額 − 手数料 − 購入総額) ÷ 購入総額。
- `holding_days` = 最初の購入日から最後の売却日までの日数。
- `benchmark_return_pct`：同じ期間の TOPIX の騰落率。TOPIX のデータがなければ 1306 の終値で代用し、その旨を記録する。
- `max_drawdown_pct`：保有中（買付日以降、売却日まで）の、平均取得単価に対する最大含み損率。
- 保有株数が0でない（全部売っていない）のに exit を作ろうとした場合はエラー。

### 9.4 kill_criteria の自動判定（review）
親レポートの `kill_criteria` のうちオブジェクト形式 `{text, metric, op, value}` のものを、今回のスナップショットの値で判定し、結果を `kill_criteria_check` に保存する。`metric` はスナップショットのドット区切りのパス（例: `survival.runway_months`、`ratios.cycle_stats.operating_margin.latest`）。使える演算子は `<`, `<=`, `>`, `>=`, `==`, `!=` のみ。値が null の条件は `hit: null` にする。使える metric の一覧を `reports/README.md` に「kill_criteria で使える指標」として載せる。

---

## 10. やらないこと

- 既存のレポート・スナップショットの上書き、削除。
- 雛形の本文（要約ブロック以外）の編集。
- 人が書く欄（thesis、predictions、ratings の final など）への自動記入。
- LLM による文章生成（将来の拡張。今回は不要）。
- 表示側（stock-viewer の画面）の実装。これは指示書 B で行う。

---

## 11. テストと受け入れ条件

- **計算の単体テスト**：§7 の各式について、手計算した小さな架空データで一致を確認する（清算価値、DCF の弱気・強気、正常化価値、損益分岐点の回帰、AR(1) の半減期、runway、peak-to-peak CAGR、自動評価の境界値）。
- **ポイント・イン・タイムのテスト**：as_of の翌日に開示された決算短信・業績修正・訂正開示・株式分割が、スナップショットに混入しないこと。
- **ソース優先順位のテスト**：同じ期間の値が TDnet と EDINET の両方にあれば TDnet（訂正後）が採用され、食い違いが `SOURCE_MISMATCH` として記録されること。TDnet にない期間だけ EDINET で埋まり、`sources.by_series` に内訳が残ること。
- **上書き防止のテスト**：同名ファイルがあるときにエラーで止まること。
- **frontmatter のテスト**：書き出した initial / review / exit を YAML として読み直すと、キーの順序とコメントが保たれ、自動記入したキー以外は雛形と同じであること。
- **TDnet 取り込みのテスト**：§5.2 の開示種別の判定辞書について、代表的なタイトルの例で分類が合うこと。同じ開示を二度取り込んでも1件になること。
- **実データでの確認**：実在する3銘柄（安定黒字の大型株1、赤字が続いた景気敏感株1、増資・MSCB の履歴がある小型株1）で `new_report initial` を実行し、次の3つを報告する。
  - 生成された要約ブロック
  - `warnings` の一覧
  - 目視で確認した数値のうち、有価証券報告書の値と食い違ったもの

---

## 12. 完了時に報告すること

1. 実装したファイルの一覧と、既存コードに加えた変更の要約。
2. TDnet の各項目（T1〜T34）について: 既存データで構造化済み／今回拡張した／値が取れない／未対応、の一覧。あわせて、年次・四半期の財務諸表とセグメントについて、TDnet で賄えた期間と EDINET で補完した期間の内訳（実データ確認の3銘柄分）。
3. 仕様どおりに実装できなかった箇所と、その理由・代替案。
4. §11 の実データ確認の結果。
