from db_hammer import DB_TYPE_MSSQL
from db_hammer.base import BaseConnection

try:
    import Cython
except ImportError:
    print("cant import Cython")
    print("===> pip3 install Cython")
    raise Exception("import Cython error")
try:
    import pymssql
except ImportError:
    print("cant import pymssql")
    print("===> pip3 install pymssql")
    raise Exception("import pymssql error")


# pip install Cython
# pip install pymssql
class MsSQLConnection(BaseConnection):
    def __init__(self, **kwargs):
        self.db_type = DB_TYPE_MSSQL
        if kwargs.get("host", None) is None:
            raise Exception("host")
        if kwargs.get("user", None) is None:
            raise Exception("user")
        if kwargs.get("db_name", None) is None:
            raise Exception("db_name")
        if kwargs.get("pwd", None) is None:
            raise Exception("pwd")
        port = kwargs.get("port", 1433)
        charset = kwargs.get("charset", "utf8")
        tds_version = kwargs.get("tds_version", "7.0")
        super().__init__(**kwargs)
        self.conn = pymssql.connect(server=kwargs["host"], user=kwargs["user"],
                                    password=kwargs["pwd"],
                                    port=port,
                                    charset=charset,
                                    tds_version=tds_version,
                                    database=kwargs["db_name"])
        self.cursor = self.conn.cursor()
