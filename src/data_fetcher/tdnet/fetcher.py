# tdnetからデータ取得する
import re
import time
from pathlib import Path

from loguru import logger
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By

from ..constants import PROJECT_ROOT

options = ChromeOptions()
options.set_capability("se:name", "test_visit_basic_auth_secured_page (ChromeTests)")
download_dir = PROJECT_ROOT / "data" / "Downloads"
save_root_dir = PROJECT_ROOT / "data" / "tdnet" / "raw"
executor_url = "http://localhost:4444"


def wait_until_element_available(driver, by, value, timeout=10):
    """指定した要素が見つかるまで待機する"""
    for i in range(timeout):
        if len(driver.find_elements(by, value)) > 0:
            break
        time.sleep(0.2)
    else:
        raise TimeoutError("Failed to load page.")


def get_tdnet_xbrl(
    code,
    download_dir=download_dir,
    save_root_dir=save_root_dir,
    options=options,
    executor_url=executor_url,
):
    save_dir = save_root_dir / code
    save_dir.mkdir(parents=True, exist_ok=True)
    with webdriver.Remote(options=options, command_executor=executor_url) as driver:
        driver.get("https://www2.jpx.co.jp/tseHpFront/JJK010010Action.do?Show=Show")
        # 指定した銘柄を検索
        form = driver.find_element(By.NAME, "JJK010010Form")
        form.find_element(By.NAME, "eqMgrCd").send_keys(code)
        form.find_element(By.NAME, "searchButton").click()
        time.sleep(0.2)
        # 詳細情報を表示
        wait_until_element_available(driver, By.NAME, "JJK010030Form", timeout=50)
        if (
            len(
                driver.find_element(By.NAME, "JJK010030Form").find_elements(
                    By.NAME, "detail_button"
                )
            )
            == 0
        ):
            logger.warning(f"Detail button not found for code: {code}")
            return
        driver.find_element(By.NAME, "JJK010030Form").find_element(
            By.NAME, "detail_button"
        ).click()
        time.sleep(0.1)

        # 適時開示情報を表示
        wait_until_element_available(driver, By.NAME, "JJK010040Form", timeout=50)
        driver.find_element(By.NAME, "JJK010040Form").find_element(
            By.LINK_TEXT, "適時開示情報"
        ).click()
        time.sleep(0.1)

        # 情報閲覧ボタンをクリック
        driver.find_element(By.ID, "closeUpKaiJi0_open").find_element(
            By.TAG_NAME, "input"
        ).click()
        time.sleep(0.1)
        # さらに表示をクリック
        if len(driver.find_elements(By.ID, "1101")) == 0:
            logger.warning(f"No data available for code: {code}")
            return
        more_button = driver.find_element(By.ID, "1101")
        if "display: none" not in more_button.get_attribute("style"):
            more_button.click()
        time.sleep(0.1)

        # 順番にxbrlを取得する
        table_body = driver.find_element(By.ID, "closeUpKaiJi0").find_element(
            By.CLASS_NAME, "NormalBody"
        )
        file_dict = {}
        for trow in table_body.find_elements(By.TAG_NAME, "tr"):
            # xbrlのリンクを取得
            cols = trow.find_elements(By.TAG_NAME, "td")
            if len(cols) < 3:
                continue

            res = re.search(r"(\d{4}/\d{2}/\d{2})", cols[0].text)
            if res is None:
                continue
            date = res.group(1).replace("/", "")

            link_img = cols[2].find_elements(By.TAG_NAME, "img")
            if len(link_img) > 0:
                zip_filename = re.search(
                    r"(\d*\.zip)",
                    cols[2].find_element(By.TAG_NAME, "img").get_attribute("onclick"),
                ).group(1)
                uid = Path(zip_filename).name.replace(".zip", "")
                save_file = save_dir / f"{code}_{date}_{uid}.zip"
                if not save_file.exists():
                    link_img[0].click()
                    file_dict[zip_filename] = date

        if len(file_dict) == 0:
            return

        time.sleep(0.5)

        # ダウンロードしたzipファイルを移動
        save_dir.mkdir(parents=True, exist_ok=True)
        for filename, date in file_dict.items():
            date_str = date.replace("/", "")
            download_file = download_dir / filename
            uid = Path(filename).name.replace(".zip", "")
            save_file = save_dir / f"{code}_{date_str}_{uid}.zip"
            if not download_file.exists():
                logger.warning(f"File not found: {download_file}")
                continue
            download_file.rename(save_file)
