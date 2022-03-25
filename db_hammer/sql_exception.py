class SqlException(Exception):
    pass


class NoExistException(Exception):
    """对象不存在"""
    pass


class FetchRowsException(Exception):
    """影响行数异常"""
    pass


class EntityException(Exception):
    """实体对象错误"""
    pass
