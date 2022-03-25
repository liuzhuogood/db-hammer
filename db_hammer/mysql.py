import logging

from db_hammer import DB_TYPE_MYSQL
from db_hammer.base import BaseConnection
from db_hammer.csv import start as csv_start, get_headers

try:
    import pymysql
except ImportError:
    print("cant import pymysql")
    print("===> pip3 install pymysql")
    raise Exception("import pymysql error")

"""
db_conn:
  # <connection details>

  session_sqls:
    - SET @@session.max_execution_time=0     # No limit
    - SET @@session.net_read_timeout=3600    # 1 hour
    - SET @@session.net_write_timeout=3600   # 1 hour

    # Set other session variables to the default PPW ones
    - SET @@session.time_zone="+0:00"
    - SET @@session.wait_timeout=28800
    - SET @@session.innodb_lock_wait_timeout=3600
"""


class MySQLConnection(BaseConnection):
    def __init__(self,
                 debug=False,
                 db_type=DB_TYPE_MYSQL,
                 log=logging.getLogger(__name__),
                 caps=None,
                 **kwargs):
        self.db_type = DB_TYPE_MYSQL
        if kwargs.get("host", None) is None:
            raise Exception("host")
        if kwargs.get("user", None) is None:
            raise Exception("user")
        if kwargs.get("password", None) is None:
            raise Exception("password")
        kwargs["charset"] = kwargs.get("charset", "utf8")
        super().__init__(debug=debug, db_type=db_type, log=log, caps=caps, **kwargs)
        self.conn = pymysql.connect(**kwargs)
        self.cursor = self.conn.cursor()

    def convert_str(self, s: str):
        return s.replace("'", "\\'")

    def export_data_file(self, sql, dir_path, file_mode="gz", pack_size=500000, fetch_size=10000, add_header=True,
                         data_split_chars=',',
                         data_close_chars='"', encoding="utf-8", outing_callback=None):
        """导出数据文件
        @:param sql 导出时的查询SQL
        @:param dir_path 导出的数据文件存放目录
        @:param file_mode 导出文件格式：txt|gz|csv
        @:param add_header 数据文件是否增加表头
        @:param pack_size  每个数据文件大小，默认为50万行，强烈建议分割数据文件，单文件写入速度会越来越慢
        @:param fetch_size   游标大小
        @:param data_split_chars 每条数据字段分隔字符,csv文件默认为英文逗号
        @:param data_close_chars 每条数据字段关闭字符,csv文件默认为英文双引号
        @:param encoding 文件编码格式，默认为utf-8
        @:param outing_callback 导出过程中的回调方法
        """
        # 要使用服务器游标，本地游标会内存溢出
        cursor = self.conn.cursor(pymysql.cursors.SSCursor)
        csv_start(cursor=cursor,
                  sql=sql,
                  path=dir_path,
                  bachSize=fetch_size,
                  PACK_SIZE=pack_size,
                  file_mode=file_mode,
                  add_header=add_header,
                  CSV_SPLIT=data_split_chars,
                  CSV_FIELD_CLOSE=data_close_chars,
                  encoding=encoding,
                  callback=outing_callback,
                  log=self.log)
        cursor.close()

    def _header_to_map(self, col_names, data):
        r_list = []
        for k in range(len(data)):
            row = {}
            for i in range(len(col_names)):
                name = col_names[i]
                if self.caps is not None:
                    if self.caps == "A":
                        name = name.upper()
                    else:
                        name = name.lower()
                row[name] = data[k][i]
            r_list.append(row)
        return r_list

    def cursor_execute(self, sql, callback, params=None, fetch_size=100):
        cursor = self.conn.cursor(pymysql.cursors.SSCursor)
        _sql, params = self.sql_params(sql, params)
        cursor.execute(_sql, params)
        csv_data = cursor.fetchmany(int(fetch_size))
        col_names = get_headers(cursor)
        while len(csv_data) > 0:
            records: list[{}] = self._header_to_map(col_names, csv_data)
            callback(records)
            csv_data = cursor.fetchmany(int(fetch_size))
