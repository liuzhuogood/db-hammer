class SqlException(Exception):
    pass


class ExistException(Exception):
    """对象不存在"""
    pass


class FetchRowsException(Exception):
    """影响行数异常"""
    pass
