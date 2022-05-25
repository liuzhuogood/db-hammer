# -*- coding: utf-8 -*-
def past_params(s: str) -> dict:
    """解决从浏览器参数复制过来的格式问题"""
    result = {}
    lines = s.split("\n")
    for line in lines:
        if str(line).strip() == "" or len(line.split(": ")) == 0:
            continue

        key = line[:line.find(":")].strip()
        value = line[line.find(":"):][1:].strip()
        result[key] = None if value == '' else value

    return result