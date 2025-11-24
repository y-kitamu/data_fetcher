import json
import re
import time

import tqdm
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


def get_chrome_options():
    # Chrome のオプションを設定する
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')
    # 省メモリ化のための設定
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # https://note.com/tarakobababa/n/n746f77f0549e
    profile_dir = "/home/seluser/work/selenium_profile"
    options.add_argument(f"--user-data-dir={str(profile_dir)}")
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
        time.sleep(3)

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
                if dst_file.exists() or download_file.exists():
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
                    time.sleep(1)
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
        time.sleep(2)


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

    # data_fetcher.debug.run_debug(download, all_tickers)
    download(all_tickers)
    move_from_download_dir()
