import datetime


def date_to_str(date=None, format_str="%Y-%m-%d %H:%M:%S"):
    """时期格式化成字符
        :param date 时间
        :param format_str %Y-%m-%d %H:%M:%S
    """
    if date is None:
        date = datetime.datetime.now()
    return date.strftime(format_str)


def get_current():
    return datetime.datetime.now()


def get_date_str(date=None, format_str="%Y-%m-%d"):
    if date is None:
        date = datetime.datetime.now()
    return date.strftime(format_str)
