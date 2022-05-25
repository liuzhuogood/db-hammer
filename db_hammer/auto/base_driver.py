import datetime
import json
import os
import re
import time
import logging
from retry import retry
from selenium.common.exceptions import TimeoutException, InvalidArgumentException
from selenium.webdriver import ActionChains
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By


def date_to_str(date=None, format_str="%Y-%m-%d %H:%M:%S"):
    """时期格式化成字符
        :param date: 时间
        :param format_str: %Y-%m-%d %H:%M:%S
    """
    if date is None:
        date = datetime.datetime.now()
    return date.strftime(format_str)


class BaseDriver(WebDriver):
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pwd = os.path.dirname(__file__)
        self.quit()

    def open(self, tab_index=0, url=None, timeout=0):
        try:
            handles = self.window_handles  # 获取当前窗口句柄集合（列表类型）
            if len(handles) < tab_index + 1:
                self.execute_script('window.open("","_blank");')
                while True:
                    time.sleep(0.1)
                    handles = self.window_handles  # 获取当前窗
                    if len(handles) > tab_index:
                        break
            if self.current_window_handle != handles[tab_index]:
                self.switch_to.window(handles[tab_index])
        except Exception:
            pass
        if url is not None:
            logging.info(f"chrome open {url}")
            try:
                if timeout > 0:
                    self.set_page_load_timeout(timeout)
                    self.set_script_timeout(timeout)
                self.get(url)
            except InvalidArgumentException:
                pass

    def get_timeout(self, url=None, timeout=10):
        if url is not None:
            logging.info(f"chrome open {url}")
            try:
                if timeout > 0:
                    self.set_page_load_timeout(timeout)
                    self.set_script_timeout(timeout)
                self.get(url)
            except Exception:
                pass

    def close_tab(self, tab_index):
        handles = self.window_handles  # 获取当前窗口句柄集合（列表类型）
        if len(handles) < tab_index + 1:
            return

        self.switch_to.window(handles[tab_index])
        self.close()

    def get_headers(self):
        try:
            headers = {}
            cookie = [item["name"] + "=" + item["value"] for item in self.get_cookies()]
            cookiestr = ';'.join(item for item in cookie)
            headers["cookie"] = cookiestr
            # token = re.search(r'"token": ".*?"', str(info))[0][10:-1]
            # s.headers["token"] = token
            # s.headers["Authorization"] = token
            return headers
        except:
            return None

    def scroll_end_page(self):
        elements = self.find_elements(by=By.XPATH, value="//*")

        for element in elements:
            try:
                self.execute_script("arguments[0].scrollIntoView();", element)
            except:
                pass
            # self.execute_script("document.body.scrollTop=document.body.scrollHeight")

    def switch_tab(self, tab_index):
        if len(self.window_handles) < tab_index + 1:
            return
        self.switch_to.window(self.window_handles[tab_index])

    @retry(Exception, tries=10, delay=2)
    def element_click(self, element):
        self.execute_script("arguments[0].click();", element)

    @retry(Exception, tries=10, delay=2)
    def element_action_click(self, element):
        ActionChains(self).move_to_element(element).click(element).perform()

    def wait_id_element(self, id_, timeout=10, displayed=None):
        if displayed:
            WebDriverWait(self, timeout).until(EC.presence_of_element_located((By.ID, id_)) and
                                               EC.visibility_of_element_located((By.ID, id_)))
        else:
            WebDriverWait(self, timeout).until(EC.presence_of_element_located((By.ID, id_)))
        element = self.find_element(by=By.ID, value=id_)
        element.jclick = lambda: self.element_click(element)
        element.aclick = lambda: self.element_action_click(element)
        return element

    def wait_css_element(self, cls, timeout=10, displayed=None):
        if displayed:
            WebDriverWait(self, timeout).until(EC.presence_of_element_located((By.CSS_SELECTOR, cls)) and
                                               EC.visibility_of_element_located((By.CSS_SELECTOR, cls)))
        else:
            WebDriverWait(self, timeout).until(EC.presence_of_element_located((By.CSS_SELECTOR, cls)))
        return self.find_element(by=By.CSS_SELECTOR, value=cls)

    @retry(Exception, tries=2, delay=1)
    def wait_xpath_element(self, path, timeout=10, delay=0, next_delay=0, displayed=None):
        time.sleep(delay)
        if displayed:
            WebDriverWait(self, timeout).until(EC.presence_of_element_located((By.XPATH, path)) and
                                               EC.visibility_of_element_located((By.XPATH, path)))
        else:
            WebDriverWait(self, timeout).until(EC.presence_of_element_located((By.XPATH, path)))
        element = self.find_element(by=By.XPATH, value=path)
        element.jclick = lambda: self.element_click(element)
        element.alick = lambda: self.element_action_click(element)
        time.sleep(next_delay)
        return element

    @retry(Exception, tries=2, delay=1)
    def wait_xpath_elements(self, path, timeout=10, delay=0, next_delay=0, displayed=None):
        time.sleep(delay)
        if displayed:
            WebDriverWait(self, timeout).until(EC.presence_of_element_located((By.XPATH, path)) and
                                               EC.visibility_of_element_located((By.XPATH, path)))
        else:
            WebDriverWait(self, timeout).until(EC.presence_of_element_located((By.XPATH, path)))
        elements = self.find_elements(by=By.XPATH, value=path)
        for e in elements:
            e.jclick = lambda: self.element_click(e)
            e.aclick = lambda: self.element_action_click(e)
        time.sleep(next_delay)
        return elements

    @retry(Exception, tries=10, delay=3, backoff=2)
    def regex_findall(self, rule):
        text = self.page_source
        result = re.findall(rule, text)
        if len(result) == 0:
            raise Exception("not find")
        return result

    def wait_url(self, url, timeout=10):
        while timeout > 0:
            time.sleep(1)
            timeout -= 1
            if str(self.current_url).startswith(url):
                return

        raise Exception("TimeOut wait url")

    def save_screen(self, name, screenshot_path="./screenshot", delay=1):
        print(name)
        time.sleep(delay)
        os.makedirs(screenshot_path, exist_ok=True)
        path = os.path.join(screenshot_path, date_to_str(format_str="%Y-%m-%d %H:%M:%S ") + name + ".png")
        self.save_screenshot(path)
        self.screenshot_list.append(os.path.abspath(path))

    def wait_for_one(self, elements, timeout=10, check_second=0.5):
        """
        r, v = self.firefox.wait_for_one({
            False: [{"re": "没有找到相关企业",displayed:False}, {"re": "我们只是确认一下你不是机器人"}],
            True: [{"css": ".tips-num"}]
        })

        :param elements:
        :param timeout:
        :param check_second:
        :return:
        """

        def find_by(by: str, value, displayed=None):
            find = False
            r_value = None
            if by == "xpath":
                try:
                    r_value = self.find_element(by=By.XPATH, value=value)
                    find = True
                except Exception:
                    pass
            elif by == "css":
                try:
                    r_value = self.find_element(by=By.CSS_SELECTOR, value=value)
                    find = True
                except Exception:
                    pass
            elif by == "id":
                try:
                    r_value = self.find_element(by=By.ID, value=value)
                    find = True
                except Exception:
                    pass
            elif by == "tag":
                try:
                    r_value = self.find_element(by=By.TAG_NAME, value=value)
                    find = True
                except Exception:
                    pass
            elif by == "re":
                text = self.page_source
                r_value = re.search(value, text)
                if r_value is not None:
                    find = True

            # 决断是否显示
            if displayed and r_value and r_value.is_displayed():
                find = True
            else:
                find = False

            return find, r_value

        while timeout > 0:
            for key in elements.keys():
                try:
                    el = elements[key]
                    if isinstance(el, list):
                        for e in el:
                            by, v, displayed = list(e.keys())[0], list(e.values())[0], e.get("displayed", None)
                            find, r_value = find_by(by, v, displayed)
                            if find:
                                return key, r_value
                    else:
                        by, v, displayed = list(el.keys())[0], list(el.values())[0], el.get("displayed", None)
                        find, r_value = find_by(by, v, displayed)
                        if find:
                            return key, r_value
                except Exception as e:
                    logging.exception(e)

            time.sleep(check_second)
            timeout -= check_second
        raise TimeoutError()

    def get_logs(self):
        logs = [json.loads(log['message'])['message'] for log in self.get_log('performance')]
        return logs

    def get_page_text_by_chrome(self, url=None, timeout=0, delay=0, scroll_end=False):
        """使用Chrome来获取页面源代码"""
        if timeout > 0:
            self.set_page_load_timeout(timeout)
            self.set_script_timeout(timeout)
        try:
            if url:
                self.open(url=url)
        except TimeoutException:
            pass
        time.sleep(delay)
        # 滚动到页面底
        if scroll_end:
            self.scroll_end_page()
        return self.page_source

    def js_loaction(self, url, timeout):
        try:
            self.execute_script(f"window.location.href='{url}'")
            time.sleep(1)
        except:
            pass
        self.wait_url(url, timeout)
