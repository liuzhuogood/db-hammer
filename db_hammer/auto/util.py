# -*- coding: utf-8 -*-
# @Time     : 2022年03月16日 23时02分
# @Email    : liuzhuo@tuzhanai.com
# @Author   : 刘卓
from urllib.parse import quote, unquote


def unicode_decode(s):
    if s is None:
        return None
    return s.encode("utf-8").decode("unicode_escape")


def urldecode(s):
    if s is None:
        return None
    return unquote(s)


def urlencode(s):
    if s is None:
        return None
    return quote(s)
