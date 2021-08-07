from db_hammer import DB_TYPE_MYSQL
from db_hammer.base import BaseConnection
from db_hammer.csv import start as csv_start

try:
    import pymysql
except ImportError:
    print("cant import pymysql")
    print("===> pip3 install pymysql")
    raise Exception("import pymysql error")


class MySQLConnection(BaseConnection):
    def __init__(self, **kwargs):
        self.db_type = DB_TYPE_MYSQL
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
        super().__init__(**kwargs)
        self.conn = pymysql.connect(host=kwargs["host"], user=kwargs["user"], password=kwargs["pwd"],
                                    database=kwargs["db_name"], port=port,
                                    charset=charset)
        self.cursor = self.conn.cursor()


    def convert_str(self, s: str):
        return s.replace("'", "\\'")

    def export_data_file(self, sql, dir_path, file_mode="gz", pack_size=500000, bachSize=10000, add_header=True,
                         data_split_chars=',',
                         data_close_chars='"', encoding="utf-8", outingCallback=None):
        """导出数据文件
        @:param sql 导出时的查询SQL
        @:param dir_path 导出的数据文件存放目录
        @:param file_mode 导出文件格式：txt|gz|csv
        @:param add_header 数据文件是否增加表头
        @:param pack_size  每个数据文件大小，默认为50万行，强烈建议分割数据文件，单文件写入速度会越来越慢
        @:param bachSize   游标大小
        @:param data_split_chars 每条数据字段分隔字符,csv文件默认为英文逗号
        @:param data_close_chars 每条数据字段关闭字符,csv文件默认为英文双引号
        @:param encoding 文件编码格式，默认为utf-8
        @:param outingCallback 导出过程中的回调方法
        """
        # 要使用服务器游标，本地游标会内存溢出
        cursor = self.conn.cursor(pymysql.cursors.SSCursor)
        csv_start(cursor=cursor,
                  sql=sql,
                  path=dir_path,
                  bachSize=bachSize,
                  PACK_SIZE=pack_size,
                  file_mode=file_mode,
                  add_header=add_header,
                  CSV_SPLIT=data_split_chars,
                  CSV_FIELD_CLOSE=data_close_chars,
                  encoding=encoding,
                  callback=outingCallback,
                  log=self.log)
        cursor.close()
