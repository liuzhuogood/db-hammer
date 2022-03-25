import uuid

from db_hammer import DB_TYPE_POSTGRESQL
from db_hammer.base import BaseConnection
from db_hammer.csv import start as csv_start

try:
    import psycopg2
except ImportError:
    print("cant import psycopg2")
    print("===> pip3 install psycopg2")
    raise Exception("import psycopg2 error")


class PostgreSQLConnection(BaseConnection):

    def __init__(self, **kwargs):
        self.db_type = DB_TYPE_POSTGRESQL
        if kwargs.get("host", None) is None:
            raise Exception("host")
        if kwargs.get("user", None) is None:
            raise Exception("user")
        if kwargs.get("database", None) is None:
            raise Exception("database")
        if kwargs.get("pwd", None) is None:
            raise Exception("pwd")
        port = kwargs.get("port", 5432)

        super().__init__(**kwargs)
        self.conn = psycopg2.connect(database=kwargs["database"],
                                     user=kwargs["user"],
                                     password=kwargs["pwd"],
                                     host=kwargs["host"],
                                     port=port)

        self.cursor = self.conn.cursor()

    def db_data_type_mapping(self):
        return {
            "VARCHAR,CHAR,TEXT": str,
            "DECIMAL,FLOAT4,FLOAT8,MONEY": float,
            "INT2,INT4,INT8,SERIAL2,SERIAL4,SERIAL8,BOOLEAN": int,
            "DATE,TIMESTAMP,TIME": "to_date('${VALUE}','yyyy-mm-dd,hh24:mi:ss')",
        }

    def db_column_sql(self, table_name):
        '''
            COLUMN_NAME : 名称
            DATA_TYPE ： 类型
            COMMENTS ： 备注
            DATA_LENGTH ： 长度
            DATA_PRECISION ：精度
        :param table_name:
        :return:
        '''
        return f"""
          SELECT a.attnum,
                   a.attname AS COLUMN_NAME,
                   t.typname AS DATA_TYPE,
                   a.attlen AS DATA_LENGTH,
                   a.atttypmod AS lengthvar,
                   a.attnotnull AS notnull,
                   b.description AS COMMENTS
            FROM pg_class c,
                 pg_attribute a
                     LEFT OUTER JOIN pg_description b ON a.attrelid=b.objoid AND a.attnum = b.objsubid,
                 pg_type t
            WHERE c.relname = '{table_name}'
              and a.attnum > 0
              and a.attrelid = c.oid
              and a.atttypid = t.oid
            ORDER BY a.attnum
            """

    def convert_str(self, s: str):
        return s.replace("'", "''")

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
        cursor = self.conn.cursor(name="export_data_" + str(uuid.uuid1()))
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
