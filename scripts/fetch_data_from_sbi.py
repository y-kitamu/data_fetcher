import base64
import json
import re
import time
from datetime import datetime, timezone

import polars as pl
import tqdm
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from loguru import logger
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

import data_fetcher

command_executor = "http://localhost:4444/wd/hub"
download_dir = data_fetcher.core.PROJECT_ROOT / "data" / "Downloads"
dst_dir = data_fetcher.core.PROJECT_ROOT / "data" / "sbi" / "tick"

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
TOKEN_PATH = data_fetcher.core.PROJECT_ROOT / "token.json"
CREDENTIALS_PATH = (
    data_fetcher.core.PROJECT_ROOT
    / "cert"
    / "client_secret_658880078384-d3ld50eet0pc8qpr4mvjno891mkqlp98.apps.googleusercontent.com.json"
)


def get_gmail_service():
    """Gmail API serviceオブジェクトを返す。token.jsonからトークンを読み込み、期限切れならrefreshする。"""
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), GMAIL_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            from google_auth_oauthlib.flow import InstalledAppFlow

            flow = InstalledAppFlow.from_client_secrets_file(
                str(CREDENTIALS_PATH), GMAIL_SCOPES
            )
            creds = flow.run_local_server(
                port=8090, open_browser=False, bind_addr="0.0.0.0"
            )
        with open(TOKEN_PATH, "w") as token:
            token.write(creds.to_json())
    return build("gmail", "v1", credentials=creds)


def fetch_sbi_auth_email(service, after_timestamp):
    """SBIデバイス認証メールを待機取得し、本文中のリンクURLを返す。

    Args:
        service: Gmail API serviceオブジェクト
        after_timestamp: この時刻(UTC datetime)以降のメールのみ対象

    Returns:
        認証リンクURL文字列

    Raises:
        TimeoutError: 60秒以内にメールが届かなかった場合
    """
    query = "from:info@sbisec.co.jp subject:認証コード入力画面のお知らせ"
    after_epoch = int(after_timestamp.timestamp())

    for attempt in range(30):  # 最大60秒 (2秒 × 30回)
        time.sleep(2)
        results = (
            service.users()
            .messages()
            .list(userId="me", q=f"{query} after:{after_epoch}", maxResults=1)
            .execute()
        )
        messages = results.get("messages", [])
        if not messages:
            logger.debug(f"メール未着 (attempt {attempt + 1}/30)")
            continue

        msg = (
            service.users()
            .messages()
            .get(userId="me", id=messages[0]["id"], format="full")
            .execute()
        )
        # メール本文を取得
        payload = msg["payload"]
        body_data = ""
        if "parts" in payload:
            for part in payload["parts"]:
                if part["mimeType"] in ("text/plain", "text/html"):
                    body_data = part["body"].get("data", "")
                    break
        else:
            body_data = payload["body"].get("data", "")

        body = base64.urlsafe_b64decode(body_data).decode("utf-8", errors="replace")
        # deviceAuthentication リンクURL抽出
        url_match = re.search(
            r"https://m\.sbisec\.co\.jp/deviceAuthentication/input\?[^\s\"<>']+",
            body,
        )
        if url_match:
            # HTMLエンティティ &amp; → & に変換
            auth_url = url_match.group().replace("&amp;", "&")
            logger.info(f"認証メールのリンクを取得: {auth_url}")
            return auth_url

        logger.warning("メール本文からURLを抽出できませんでした")

    raise TimeoutError("SBI認証メールが60秒以内に届きませんでした")


def handle_device_auth(driver):
    """SBIデバイス認証フローを自動処理する。

    1. 「Eメールを送信する」ボタンをクリック
    2. 画面上の認証コード(6桁)を取得
    3. Gmail APIでメール受信を待機 → リンクURLを取得
    4. 新しいタブでリンク先を開き認証コードを入力 → 認証する
    5. タブを閉じて元ページに戻る
    6. 「確認しました」チェック → 「デバイスを登録する」クリック
    """
    wait = WebDriverWait(driver, 15)

    # Gmail APIサービスを事前に準備
    gmail_service = get_gmail_service()
    timestamp_before = datetime.now(timezone.utc)

    # 1. 「Eメールを送信する」ボタンをクリック
    logger.info("デバイス認証: Eメール送信ボタンをクリック")
    send_email_btn = wait.until(EC.element_to_be_clickable((By.ID, "sendEmailButton")))
    send_email_btn.click()

    # 2. Gmail APIでメール受信待機 → リンクURL取得
    logger.info("デバイス認証: 認証メール受信を待機中")
    auth_url = fetch_sbi_auth_email(gmail_service, timestamp_before)

    # 3. 認証コード(6桁数字)を画面から取得
    logger.info("デバイス認証: 認証コードを取得中")
    page_text = driver.find_element(By.TAG_NAME, "body").text
    code_match = re.search(r"\b(\d{6})\b", page_text)
    if not code_match:
        # テキストボックスから取得を試みる
        code_inputs = driver.find_elements(
            By.XPATH,
            "//input[@type='text' and string-length(@value)=6]",
        )
        for inp in code_inputs:
            val = inp.get_attribute("value")
            if val and re.match(r"^\d{6}$", val):
                code_match = re.match(r"(\d{6})", val)
                break
    if not code_match:
        logger.error(f"認証コードが見つかりません。ページテキスト: {page_text[:500]}")
        raise RuntimeError("デバイス認証コード(6桁)がページ上に見つかりません")

    auth_code = code_match.group(1)
    logger.info(f"デバイス認証: 認証コード = {auth_code}")

    # 4. 新しいタブでリンク先を開き、認証コードを入力
    original_window = driver.current_window_handle
    driver.execute_script("window.open('');")
    new_tab = [h for h in driver.window_handles if h != original_window][-1]
    driver.switch_to.window(new_tab)
    driver.get(auth_url)
    time.sleep(3)

    # セキュリティ確認ポップアップを閉じる（「認証コード入力画面に進む」）
    try:
        proceed_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//*[contains(text(), '認証コード入力画面に進む')]")
            )
        )
        proceed_btn.click()
        time.sleep(2)
    except Exception:
        logger.debug("セキュリティ確認ポップアップは表示されませんでした")

    logger.info("デバイス認証: 認証コードを入力中")
    code_input = wait.until(EC.presence_of_element_located((By.ID, "verifyCode")))
    code_input.clear()
    code_input.send_keys(auth_code)
    time.sleep(1)

    # 「認証する」ボタンをクリック（codeChange()で有効化される）
    auth_btn = driver.find_element(By.ID, "verification")
    # send_keysでoninputが発火しない場合に備え、JSで有効化
    driver.execute_script("arguments[0].removeAttribute('disabled');", auth_btn)
    wait.until(EC.element_to_be_clickable((By.ID, "verification")))
    auth_btn.click()
    time.sleep(3)

    # 5. タブを閉じて元ページに戻る
    driver.close()
    driver.switch_to.window(original_window)
    # time.sleep(2)

    # 6. 「確認しました」チェックボックスにチェック
    logger.info("デバイス認証: 確認チェックボックスにチェック")
    confirm_checkbox = wait.until(EC.element_to_be_clickable((By.ID, "authCheckBox")))
    if not confirm_checkbox.is_selected():
        confirm_checkbox.click()
    time.sleep(1)

    # 「デバイスを登録する」ボタンをクリック
    logger.info("デバイス認証: デバイスを登録する")
    register_btn = wait.until(EC.element_to_be_clickable((By.ID, "otpRegisterButton")))
    register_btn.click()
    time.sleep(10)
    logger.info("デバイス認証: 完了")


def get_chrome_options():
    # Chrome のオプションを設定する
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')
    # 省メモリ化のための設定
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Cookieの暗号化キーをOSに依存させない（Docker再起動時のCookie喪失を防ぐキーリング無効化）
    options.add_argument("--password-store=basic")
    # Selenium/自動操作の検知を回避する
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    # User-Agentを一般的なブラウザに固定し、Selenium特有の痕跡を消す
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    # https://note.com/tarakobababa/n/n746f77f0549e
    profile_dir = "/home/seluser/work/selenium_profile"
    options.add_argument(f"--user-data-dir={str(profile_dir)}")
    options.add_argument(
        "--profile-directory=Default"
    )  # プロファイルフォルダを明示的に指定

    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": "/home/seluser/Downloads",
            "download.prompt_for_download": False,  # Disable download prompt
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )
    return options


def _parse_data_row(
    row: BeautifulSoup,
) -> tuple[float, int, float, bool] | None:
    cells = row.find_all("div", class_="vg-grid-cell", recursive=False)
    if len(cells) < 4:
        return None
    price_div = cells[0].find("div", class_="qr-grid-cell")
    vol_div = cells[1].find("div", class_="qr-grid-cell")
    amt_div = cells[2].find("div", class_="qr-grid-cell")
    time_div = cells[3].find("div", class_="qr-grid-cell")
    if price_div is None or vol_div is None or amt_div is None:
        return None
    price_text = price_div.get_text(strip=True)
    vol_text = vol_div.get_text(strip=True)
    amt_text = amt_div.get_text(strip=True)
    time_text = time_div.get_text(strip=True)
    if not price_text or not vol_text or not amt_text:
        return None
    try:
        return (
            float(price_text.replace(",", "")),
            int(vol_text.replace(",", "")),
            float(amt_text.replace(",", "")),
            "qr-row-ask-bg" in price_div.get("class", []),
            time_text,
        )
    except ValueError:
        logger.warning(
            f"Failed to parse row: price={price_text!r}, vol={vol_text!r}, amt={amt_text!r}"
        )
        return None


def parse_table(table_element: BeautifulSoup) -> pl.DataFrame:
    """グリッドHTMLをparseして値段・株数・金額・uptick/downtickの配列を返す。

    現在DOMにレンダリングされている行のみ対象（テスト・デバッグ用）。
    全行を取得するには collect_all_tick_data() を使用すること。

    Args:
        table_element: #qr要素のinnerHTMLをBeautifulSoupでパースしたオブジェクト

    Returns:
        pl.DataFrame: columns=[price, volume, amount, is_uptick]
            is_uptick=True  → uptick (qr-row-ask-bg)
            is_uptick=False → downtick (qr-row-bid-bg)
    """
    rows = table_element.find_all("div", attrs={"draggable": "true"})
    parsed = [_parse_data_row(row) for row in rows]
    parsed = [r for r in parsed if r is not None]
    if not parsed:
        return pl.DataFrame(
            {"price": [], "volume": [], "amount": [], "is_uptick": [], "time": []}
        )
    prices, volumes, amounts, is_uptick, times = zip(*parsed)
    return pl.DataFrame(
        {
            "price": list(prices),
            "volume": list(volumes),
            "amount": list(amounts),
            "is_uptick": list(is_uptick),
            "time": list(times),
        }
    )


def collect_all_tick_data(driver, grid_element) -> pl.DataFrame:
    """仮想スクロールグリッドをスクロールしながら全行のtickデータを収集する。

    CDK Virtual Scroll は画面内の行しかDOMに保持しないため、順次スクロールして
    translateY + row.offsetTop の絶対ピクセル位置をキーに重複排除することで全行を取得する。
    行の高さを定数ではなく DOM の実測値から求めるため、デザイン変更に対して堅牢。

    Args:
        driver: Selenium WebDriver
        grid_element: #qr 要素の Selenium WebElement

    Returns:
        pl.DataFrame: columns=[price, volume, amount, is_uptick, time]（全行・行順ソート済み）
    """
    # CDK virtual scroll の実際のスクロール対象は cdk-virtual-scroll-viewport ではなく
    # cdkVirtualScrollingElement ディレクティブが付与された親要素
    scrollable = grid_element.find_element(
        By.CSS_SELECTOR, "div.cdk-virtual-scrollable"
    )
    content_viewport = grid_element.find_element(
        By.CSS_SELECTOR, "cdk-virtual-scroll-viewport"
    )

    # JS で translateY + row.offsetTop から absPos を計算し行データをまとめて返す。
    # 行高さ定数に一切依存しないため、デザイン変更があっても正常動作する。
    _JS_GET_ROWS = """
        var viewport = arguments[0];
        var wrapper = viewport.querySelector('.cdk-virtual-scroll-content-wrapper');
        if (!wrapper) return null;
        var translateY = new DOMMatrix(wrapper.style.transform).m42;
        var rows = Array.from(wrapper.querySelectorAll('div[draggable="true"]'));
        var result = [];
        rows.forEach(function(row) {
            var cells = row.querySelectorAll(':scope > .vg-grid-cell');
            if (cells.length < 4) return;
            var priceInner = cells[0].querySelector('.qr-grid-cell');
            var volInner   = cells[1].querySelector('.qr-grid-cell');
            var amtInner   = cells[2].querySelector('.qr-grid-cell');
            var timeInner  = cells[3].querySelector('.qr-grid-cell');
            if (!priceInner || !volInner || !amtInner) return;
            var priceText = priceInner.textContent.trim();
            var volText   = volInner.textContent.trim();
            var amtText   = amtInner.textContent.trim();
            if (!priceText || !volText || !amtText) return;
            result.push({
                absPos:   translateY + row.offsetTop,
                price:    priceText,
                volume:   volText,
                amount:   amtText,
                isUptick: priceInner.classList.contains('qr-row-ask-bg'),
                time:     timeInner ? timeInner.textContent.trim() : ''
            });
        });
        return {translateY: translateY, rows: result};
    """

    driver.execute_script("arguments[0].scrollTop = 0", scrollable)
    time.sleep(0.5)

    total_height = int(
        driver.execute_script("return arguments[0].scrollHeight", scrollable)
    )
    viewport_height = (
        int(driver.execute_script("return arguments[0].clientHeight", scrollable))
        or 400
    )
    step = int(viewport_height * 0.8)

    scroll_positions = list(range(0, total_height, step))
    if not scroll_positions or scroll_positions[-1] < total_height:
        scroll_positions.append(total_height)

    all_rows = {}

    prev_translate_y = -1.0
    for scroll_pos in scroll_positions:
        driver.execute_script(
            "arguments[0].scrollTop = arguments[1]", scrollable, scroll_pos
        )

        # translateY が更新されるまで待機（最大 2 秒）
        deadline = time.time() + 0.1
        data = None
        while time.time() < deadline:
            time.sleep(0.01)
            data = driver.execute_script(_JS_GET_ROWS, content_viewport)
            if data is None:
                continue
            if scroll_pos == 0 or float(data["translateY"]) != prev_translate_y:
                break
        else:
            data = driver.execute_script(_JS_GET_ROWS, content_viewport)
            # logger.debug("translateY did not update after scrolling, using last retrieved data")

        if data is None:
            continue

        prev_translate_y = float(data["translateY"])
        for item in data["rows"]:
            abs_pos = round(item["absPos"])
            try:
                all_rows[abs_pos] = {
                    "price": float(item["price"].replace(",", "")),
                    "volume": int(item["volume"].replace(",", "")),
                    "amount": float(item["amount"].replace(",", "")),
                    "is_uptick": bool(item["isUptick"]),
                    "time": item["time"],
                }
            except (ValueError, KeyError):
                logger.warning(f"Failed to parse JS row: {item!r}")

    # logger.info(
    #     f"collect_all_tick_data: {len(all_rows)} rows collected (total_height={total_height}px)"
    # )

    if not all_rows:
        return pl.DataFrame(
            {"price": [], "volume": [], "amount": [], "is_uptick": [], "time": []}
        )

    return pl.from_dicts(list(all_rows.values())).sort("time", descending=True)


def download(ticker_list):
    date = None  # "20250912"
    options = get_chrome_options()
    # ログイン情報のjsonファイルを読み込んで辞書に変換する
    with open(
        data_fetcher.core.PROJECT_ROOT / "cert" / "sbi_login_info.json", "r"
    ) as f:
        login_info = json.load(f)

    with webdriver.Remote(options=options, command_executor=command_executor) as driver:
        # sbiにログインする
        driver.get("https://login.sbisec.co.jp/login/entry")
        login_box = driver.find_element(By.ID, "idpw")
        login_box.find_element(By.NAME, "username").send_keys(login_info["user_id"])
        login_box.find_element(By.NAME, "password").send_keys(login_info["password"])
        login_box.find_element(By.ID, "pw-btn").click()
        time.sleep(5)
        handle_device_auth(driver)

        # search boxに適当な銘柄コードを入力して株価ページに遷移する
        search_box = driver.find_element(By.ID, "brand-search-text")
        search_box.send_keys("8473")
        search_box.send_keys(Keys.ENTER)
        time.sleep(3)
        # 全板windowを立ち上げる
        original_window = driver.current_window_handle
        driver.find_element(By.LINK_TEXT, "全板").click()

        # ウィンドウが2つになるのを待つ
        WebDriverWait(driver, 10).until(EC.number_of_windows_to_be(2))
        # windowの切り替え
        for window_handle in driver.window_handles:
            if window_handle != original_window:
                driver.switch_to.window(window_handle)
                break

        time.sleep(3)

        for ticker in tqdm.tqdm(ticker_list):
            if date is not None:
                dst_file = dst_dir / date / f"qr-{ticker}-{date}.csv"
                download_file = download_dir / f"qr-{ticker}-{date}.csv"
                parse_file = dst_dir / date / f"qr-{ticker}-{date}_html.csv"
                if (
                    dst_file.exists() or download_file.exists()
                ) and parse_file.exists():
                    continue

            elem = driver.find_element(By.ID, "header").find_element(
                By.TAG_NAME, "input"
            )
            # source = elem.get_attribute("innerHTML")
            elem.clear()
            elem.send_keys(ticker)
            elem.send_keys(Keys.ENTER)

            time.sleep(2)
            # 銘柄コードが存在しない場合はモーダルダイアログが出る
            modal_dialog = driver.find_elements(By.ID, "modalDialog")
            close_buttons = []
            if len(modal_dialog) > 0:
                close_buttons = modal_dialog[0].find_elements(By.TAG_NAME, "button")

            if len(close_buttons) > 0:
                # 銘柄コードが存在しない場合はモーダルダイアログを閉じて次の銘柄コードへ
                print(f"ticker {ticker} not found")
                close_buttons[0].click()
                time.sleep(1)
            else:
                # 銘柄コードが存在する場合は板情報をCSVでダウンロードする
                actions = ActionChains(driver)
                grid_cell = driver.find_element(By.ID, "grGrid").find_elements(
                    By.CLASS_NAME, "qr-grid-cell"
                )
                if len(grid_cell) == 0:
                    # 板情報が存在しない場合は次の銘柄コードへ
                    print(f"No data found for {ticker}")
                else:
                    actions.context_click(grid_cell[0]).perform()
                    driver.find_element(By.LINK_TEXT, "CSVエクスポート").click()

                    start_time = time.time()

                    table_el = driver.find_element(By.ID, "qr")
                    df = collect_all_tick_data(driver, table_el)

                    time.sleep(max(0.001, 1 - (time.time() - start_time)))
                    if date is None:
                        downloaded = sorted(download_dir.glob(f"qr-{ticker}-*.csv"))
                        if len(downloaded) == 0:
                            print(f"Download failed for {ticker}")
                        else:
                            match = re.search(
                                r"qr-(.*)-(\d+).*\.csv", downloaded[0].name
                            )
                            if match:
                                date = match.group(2)
                                print(f"date = {date}")

                    if date is None:
                        print(f"Date not determined yet, skipping save for {ticker}")
                    else:
                        dst_path = dst_dir / date / f"qr-{ticker}-{date}_html.csv"
                        dst_path.parent.mkdir(exist_ok=True)
                        df.write_csv(dst_path)
                        # time.sleep(2)


def move_from_download_dir():
    for csv_file in download_dir.glob("qr-*.csv"):
        match = re.search(r"qr-(.*)-(\d+).*\.csv", csv_file.name)
        if not match:
            print(
                f"Filename {csv_file.name} does not match expected pattern. Skipping."
            )
            continue
        ticker, date = match.group(1), match.group(2)
        dst_path = dst_dir / date / f"qr-{ticker}-{date}.csv"
        if dst_path.exists():
            print(f"{dst_path} already exists. Skipping.")
            csv_file.unlink()
        else:
            dst_path.parent.mkdir(exist_ok=True)
            csv_file.rename(dst_path)


if __name__ == "__main__":
    all_tickers = data_fetcher.core.get_jp_ticker_list(include_etf=True)

    max_retry = 3
    for i in range(max_retry):
        try:
            download(all_tickers)
            move_from_download_dir()
            break
        except Exception:
            logger.exception("Error during SBI data fetching")
            continue
    else:
        logger.error("Failed to fetch SBI data after multiple attempts")
        data_fetcher.core.notify_to_line(
            "Failed to fetch SBI data after multiple attempts"
        )
