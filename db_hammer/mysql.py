from .base import BaseConnection

try:
    import pymysql
except ImportError:
    print("cant import pymysql")
    print("===> pip3 install pymysql")
    raise Exception("import pymysql error")


class MySQLConnection(BaseConnection):
    def __init__(self, **kwargs):
        if kwargs.get("host", None) is None:
            raise Exception("host")
        if kwargs.get("user", None) is None:
            raise Exception("user")
        if kwargs.get("db_name", None) is None:
            raise Exception("db_name")
        if kwargs.get("pwd", None) is None:
            raise Exception("pwd")
        port = kwargs.get("port", 3306)
        charset = kwargs.get("charset", "utf8")
        super().__init__(port=port, charset=charset, **kwargs)
        self.conn = pymysql.connect(kwargs["host"], kwargs["user"], kwargs["pwd"], kwargs["db_name"], port=port,
                                    charset=charset)
        self.cursor = self.conn.cursor()

    def convert_str(self, s: str):
        return s.replace("'", "\\'")
