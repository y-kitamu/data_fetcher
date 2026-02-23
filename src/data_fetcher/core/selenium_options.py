"""
Usage:

options = get_chrome_options()

with webdriver.Remote(options=options, command_executor=command_executor) as driver:
    driver.get(url)
"""

from selenium import webdriver

from .constants import PROJECT_ROOT

command_executor = "http://localhost:4444/wd/hub"
download_dir = PROJECT_ROOT / "data" / "Downloads"


def get_driver():
    """
    Get a Selenium WebDriver instance with predefined options.

    Usage:

    with get_driver() as driver:
        driver.get(url)
    """
    options = get_chrome_options()
    driver = webdriver.Remote(options=options, command_executor=command_executor)
    return driver


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
