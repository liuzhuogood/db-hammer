# -*- coding: utf-8 -*-
# @Time     : 2021年11月29日 16时33分
# @Email    : liuzhuo@tuzhanai.com
# @Author   : 刘卓
import os
import re
import uuid
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import requests
# 屏蔽warning信息
import requests.packages.urllib3
from retry import retry

from db_hammer.auto.util import urldecode

requests.packages.urllib3.disable_warnings()
import logging


class Download:
    """资源下载器"""

    def __init__(self, headers=None):
        """
        下载器
        """
        self.headers = headers
        self.requests = requests.Session()
        if headers is not None:
            self.requests.headers.update(headers)

    def is_downloadable(self, url):
        """
        Does the url contain a downloadable resource
        """
        h = self.requests.head(url, allow_redirects=True)
        header = h.headers
        content_type = header.get('content-type')
        if 'text' in content_type.lower():
            return False
        if 'html' in content_type.lower():
            return False
        return True

    @staticmethod
    def get_filename_from_cd(cd):
        """
        Get filename from content-disposition
        """
        if not cd:
            return None
        fname = re.findall('filename=(.+)', cd)
        if len(fname) == 0:
            return None
        return fname[0]

    @retry(Exception, tries=3, delay=2, backoff=2)
    def download(self, url, local_path, uuid_filename=False, method="get", headers=None, payload=None,
                 timeout=(360, 360)):
        if headers:
            self.headers.update(headers)
        res = self.requests.request(method=method, url=url, verify=False, allow_redirects=True,
                                    headers=self.headers,
                                    timeout=(360, 360),
                                    data=payload)

        if uuid_filename:
            filename = str(uuid.uuid4()) + ".png"
        else:
            filename = urldecode(self.get_filename_from_cd(res.headers.get('content-disposition')))
            if filename:
                filename = filename.split(";")[0]
            else:
                filename = os.path.basename(url)

        os.makedirs(local_path, exist_ok=True)
        with open(os.path.join(local_path, filename), 'wb+') as f:
            f.write(res.content)
            f.flush()


class ThreadDownload:
    def __init__(self, local_path, headers=None, max_workers=10, timeout=(360, 360)):
        self.all_task = []
        self.headers = headers
        self.timeout = timeout
        self.local_path = local_path
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def start(self, urls, is_wait=False, uuid_filename=False):
        try:
            """开始下载，会阻塞直到全部完成下载"""

            self.all_task = []
            D = Download(headers=self.headers)

            def method(url):
                D.download(url, self.local_path, uuid_filename=uuid_filename, timeout=self.timeout)

            for url in urls:
                self.all_task.append(self.executor.submit(method, url))
            if is_wait:
                wait(self.all_task, return_when=ALL_COMPLETED)
        except Exception as e:
            logging.exception(e)

    def wait_download(self):
        wait(self.all_task, return_when=ALL_COMPLETED)
