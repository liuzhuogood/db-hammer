import os

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.webdriver import WebDriver
from webdriver_manager.chrome import ChromeDriverManager

from db_hammer.auto.base_driver import BaseDriver


class ChromeDriver(BaseDriver, WebDriver):

    def __init__(self, headless=True,
                 rec_log=False,
                 user_data_dir=None,
                 default_directory=None,
                 disable_images=False,
                 auto_tip_close=False,
                 no_sandbox=True,
                 proxy=None):
        self.udata = {}
        self.screenshot_list = []
        caps = {
            'browserName': 'chrome',
            'loggingPrefs': {
                'browser': 'ALL',
                'driver': 'ALL',
                'performance': 'ALL',
            },
            'goog:chromeOptions': {
                'perfLoggingPrefs': {
                    'enableNetwork': True,
                },
                'w3c': False,
            },
        }
        prefs = {}

        if os.path.exists(".webdriver"):
            executable_path = open(".webdriver", "r").read()
        else:
            executable_path = ChromeDriverManager(
                url="https://npm.taobao.org/mirrors/chromedriver/",
                latest_release_url="https://cdn.npm.taobao.org/dist/chromedriver/LATEST_RELEASE"
            ).install()
            open(".webdriver", "w+").write(executable_path)

        # d = DesiredCapabilities.CHROME
        # d['loggingPrefs'] = {'performance': 'ALL'}

        chrome_options = Options()
        if auto_tip_close:
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option("useAutomationExtension", 'False')
        chrome_options.accept_insecure_certs = True
        if user_data_dir:
            chrome_options.add_argument(f"user-data-dir={user_data_dir}")
        if headless:
            chrome_options.add_argument("--headless")
        if no_sandbox:
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument('--hide-scrollbars')
        if rec_log:
            chrome_options.add_experimental_option('w3c', False)
        if proxy:
            chrome_options.add_argument(f"--proxy-server={proxy}")
        else:
            chrome_options.add_argument('--no-proxy-server')
        if default_directory:
            os.makedirs(default_directory, exist_ok=True)

            prefs.update({"profile.default_content_settings.popups": 0,
                          "download.default_directory": default_directory,
                          'safebrowsing.enabled': False,
                          'safebrowsing.disable_download_protection': True,
                          'download.prompt_for_download': False,
                          "directory_upgrade": True})
        if disable_images:
            prefs.update({"profile.managed_default_content_settings.images": 2})

        chrome_options.add_experimental_option('prefs', prefs)
        # super().__init__(executable_path=executable_path, desired_capabilities=d, options=chrome_options)
        super().__init__(executable_path=executable_path, options=chrome_options)
        self.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
              get: () => undefined
            })
          """
        })
        self.execute_cdp_cmd("Emulation.setUserAgentOverride", {
            "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36"
            # "userAgent": "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 micromessenger/8.0.15(0x18000f27) NetType/4G Language/zh_CN"
        })
        self.set_window_position(0, 0)
        self.set_window_size(1200, 1000)
