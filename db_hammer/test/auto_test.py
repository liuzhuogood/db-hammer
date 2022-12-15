# -*- coding: utf-8 -*-
from db_hammer.auto.chrome_driver import ChromeDriver

chrome = ChromeDriver(headless=False, auto_tip_close=True)

chrome.get("https://www.google.com")