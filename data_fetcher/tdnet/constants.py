"""constants.py"""

from ..constants import PROJECT_ROOT

excel_path = PROJECT_ROOT / "data/tdnet/項目リスト_事業会社.xlsx"


target_english_labels = dict(
    filing_date="Filing date",
    code="Securities code",
    fiscal_year_end="Fiscal Year End",  # 決算期
    quarterly_period="Quarterly period",  # 四半期
    # 業績情報
    net_sales="Net sales",  # 売上高
    net_sales_change="% change in net sales",  # 売上高変化率
    revenue="Revenue",  # 売上収益
    revenue_change="% change in revenue",  # 売上収益変化率
    operating_revenue="Operating revenues",  # 営業収益
    operating_revenue_change="% change in operating revenues",  # 営業収益変化率
    operating_profit="Operating profit",  # 営業利益
    operating_profit_change="% change in operating profit",  # 営業利益変化率
    ordinary_profit="Ordinary profit",  # 経常利益
    ordinary_profit_change="% change in ordinary profit",  # 経常利益変化率
    net_income="Net income",  # 当期純利益
    net_income_of_parent="Profit attributable to owners of parent",  # 親会社株主に帰属する当期純利益
    net_income_change="% change in net income",  # 当期純利益変化率
    eps="Basic earnings per share (Yen)",  # 基本的な1株当たり利益
    diluted_eps="Diluted earnings per share (Yen)",  # 希薄化後の1株当たり利益
    roe="Rate of return on equity (%)",  # ROE（自己資本利益率）
    roa="Ordinary profit to total assets ratio (%)",  # ROA（総資産利益率）
    operating_profit_to_net_sales="Operating profit to net sales ratio (%)",  # 営業利益率
    operating_profit_to_revenue="Operating profit to revenue ratio (%)",  # 営業収益率
    # 財務情報
    total_assets="Total assets",  # 総資産
    net_assets="Net assets",  # 純資産
    equity_ratio="Capital adequacy ratio (%)",  # 自己資本比率
    bps="Net assets per share (Yen)",  # 1株当たり純資産
    owner_equity="Owner's equity",  # 自己資本
    # キャッシュフロー情報
    operating_cash_flow="Cash flows from operating activities",  # 営業活動によるキャッシュフロー
    investing_cash_flow="Cash flows from investing activities",  # 投資活動によるキャッシュフロー
    financing_cash_flow="Cash flows from financing activities",  # 財務活動によるキャッシュフロー
    cash="Cash and equivalents, end of period",  # 期末現金及び現金同等物
    # 配当
    dividend_per_share="Dividend per share (Yen)",  # 1株当たり配当金
    payout_ratio="Payout ratio (%)",  # 配当性向
    commemorative_dividend="Commemorative dividend",  # 記念配当
    extra_dividend="Extra dividend",  # 特別配当
    # 株式
    number_of_shares="Number of issued and outstanding shares at the end of fiscal year (including treasury stock)",  # 期末発行済株式数（自己株式を含む）
    number_of_treqsury_shares="Number of treasury stock at the end of fiscal year",  # 期末自己株式数
    avg_number_of_shares="Average number of shares",  # 平均発行済株式数
)
forecast_labels = dict(filing_date="Reporting date of forecast correction")
forecast_dividend = dict(filing_date="Reporting date of dividend forecast correction")
