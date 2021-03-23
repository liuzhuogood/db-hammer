import gzip
import json
import os
import threading

"""
本来使用神器shelve，但是实际使用下发现会丢失数据，可能是用得不对，自己封装一个简单的，够用了
"""


class Keep:
    def __init__(self, file_name='keep.data', auto_save=True, encoding="utf-16"):
        self.file_name = file_name
        self.d = {}
        self.lock = threading.RLock()
        self.auto_save = auto_save
        self.encoding = encoding
        self.f = None  # type:gzip.GzipFile
        if os.path.exists(file_name):
            with gzip.GzipFile(self.file_name, mode="rb") as f:
                s = f.read()
                if len(s) > 1:
                    self.d = json.loads(str(s, encoding=self.encoding))

    def open(self):
        """打开数据文件"""
        if self.f is not None and not self.f.closed:
            return
        else:
            self.f = gzip.GzipFile(self.file_name, mode="wb+")

    def save(self):
        """保存数据到数据文件"""
        self.lock.acquire()
        try:
            self.open()
            self.f.write(json.dumps(self.d).encode(self.encoding))
            self.f.flush()
        finally:
            self.lock.release()

    def get(self, key, default=None):
        """根据key获取value"""
        return self.d.get(key, default)

    def set(self, key, value):
        """设置值"""
        self.d[key] = value
        if self.auto_save:
            self.save()
            self.close()

    def delete(self, key):
        """删除值"""
        del self.d[key]
        if self.auto_save:
            self.save()
            self.close()

    def close(self):
        """关闭数据文件"""
        if self.f is None or self.f.closed:
            return
        self.f.close()


if __name__ == '__main__':
    k = Keep()
    k.set("AAA", "1212你好12")
    print(k.get("AAA"))
