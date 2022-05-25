import os

from selenium.webdriver.firefox import webdriver
from selenium.webdriver.firefox.webdriver import WebDriver
from webdriver_manager.firefox import GeckoDriverManager
from db_hammer.auto.base_driver import BaseDriver


class FirefoxDriver(BaseDriver, WebDriver):

    def __init__(self, headless=True, user_data_dir=None, default_directory=None, disable_images=False,
                 auto_tip_close=False, proxy=None, user_agent=None, fast=True):
        self.udata = {}
        if user_data_dir:
            profile = webdriver.FirefoxProfile(user_data_dir)
        else:
            profile = webdriver.FirefoxProfile()
        if proxy:
            proxy_ip, proxy_port = proxy.split(":")
            proxy_ip = proxy_ip.replace("http://", "")
            proxy_port = int(proxy_port)
            profile.set_preference('network.proxy.type', 1)
            profile.set_preference('network.proxy.http', proxy_ip)
            profile.set_preference('network.proxy.http_port', proxy_port)
            profile.set_preference('network.proxy.ssl', proxy_ip)
            profile.set_preference('network.proxy.ssl_port', proxy_port)
        if default_directory:
            profile.set_preference("browser.download.folderList", 2)
            profile.set_preference("browser.download.dir", default_directory)
            profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "binary/octet-stream")

        profile.set_preference("browser.privatebrowsing.autostart", True)
        if disable_images:
            profile.set_preference('permissions.default.stylesheet', 2)
            # Disable images
            profile.set_preference('permissions.default.image', 2)
            # profile.set_preference("network.http.use-cache", False)
            # profile.set_preference("browser.cache.memory.enable", False)
            profile.set_preference("browser.cache.disk.enable", False)
            profile.set_preference("browser.sessionhistory.max_total_viewers", 3)
            # profile.set_preference("network.dns.disableIPv6", True)
            profile.set_preference("Content.notify.interval", 750000)
            profile.set_preference("content.notify.backoffcount", 3)
        options = webdriver.Options()
        options.add_argument("--ignore-certificate-errors")
        if auto_tip_close:
            options.add_argument("--disable-infobars")
        # options.set_preference("dom.webdriver.enabled", False)
        if headless:
            options.add_argument('-headless')
            options.add_argument('--disable-gpu')
        if fast:
            profile.set_preference("network.http.proxy.pipelining", True)
            profile.set_preference("network.http.pipelining.maxrequests", 8)
            profile.set_preference("content.notify.interval", 500000)
            profile.set_preference("content.notify.ontimer", True)
            profile.set_preference("content.switch.threshold", 250000)
            profile.set_preference("browser.cache.memory.capacity", 65536)  # Increase the cache capacity.
            profile.set_preference("browser.startup.homepage", "about:blank")
            profile.set_preference("reader.parse-on-load.enabled", False)  # Disable reader, we won't need that.
            profile.set_preference("browser.pocket.enabled", False)  # Duck pocket too!
            profile.set_preference("loop.enabled", False)
            profile.set_preference("browser.chrome.toolbar_style", 1)  # Text on Toolbar instead of icons
            profile.set_preference("browser.display.show_image_placeholders",
                                   False)  # Don't show thumbnails on not loaded images.
            profile.set_preference("browser.display.use_document_colors", False)  # Don't show document colors.
            profile.set_preference("browser.display.use_document_fonts", 0)  # Don't load document fonts.
            profile.set_preference("browser.display.use_system_colors", True)  # Use system colors.
            profile.set_preference("browser.formfill.enable", False)  # Autofill on forms disabled.
            profile.set_preference("browser.helperApps.deleteTempFileOnExit", True)  # Delete temprorary files.
            profile.set_preference("browser.shell.checkDefaultBrowser", False)
            profile.set_preference("browser.startup.homepage", "about:blank")
            profile.set_preference("browser.startup.page", 0)  # blank
            profile.set_preference("browser.tabs.forceHide", True)  # Disable tabs, We won't need that.
            profile.set_preference("browser.urlbar.autoFill", False)  # Disable autofill on URL bar.
            profile.set_preference("browser.urlbar.autocomplete.enabled", False)  # Disable autocomplete on URL bar.
            profile.set_preference("browser.urlbar.showPopup", False)  # Disable list of URLs when typing on URL bar.
            profile.set_preference("browser.urlbar.showSearch", False)  # Disable search bar.
            profile.set_preference("extensions.checkCompatibility", False)  # Addon update disabled
            profile.set_preference("extensions.checkUpdateSecurity", False)
            profile.set_preference("extensions.update.autoUpdateEnabled", False)
            profile.set_preference("extensions.update.enabled", False)
            profile.set_preference("general.startup.browser", False)
            profile.set_preference("plugin.default_plugin_disabled", False)
            profile.set_preference("permissions.default.image", 2)  # Image load disabled again

        if user_agent:
            if user_agent == "random":
                user_agent = get_random_user_agent()
            options.add_argument('user-agent=' + user_agent)

        if os.path.exists(".firefoxdriver"):
            executable_path = open(".firefoxdriver", "r").read()
        else:
            executable_path = GeckoDriverManager(cache_valid_range=20).install()
            open(".firefoxdriver", "w+").write(executable_path)

        super().__init__(executable_path=executable_path,
                         options=options,
                         firefox_profile=profile)
