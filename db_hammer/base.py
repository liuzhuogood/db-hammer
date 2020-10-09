import datetime
import logging
from enum import Enum
from db_hammer.csv import start as csv_start
from db_hammer.util.date import date_to_str


class DataType(Enum):
    DateNull = -2177774
    DataPass = -2177771


class BaseConnection(object):
    def __init__(self, **kwargs):
        """
        :param kwargs:
        """
        self.Db_NAME = kwargs.get("db_name")
        self.conn = None  # type:Connect
        self.cursor = None  # type:cursor
        self.table_column_cache = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def select_page_size(self, sql: str, page_size=50) -> int:
        """
        根据sql获取页数
        :param sql:
        :param page_size: 分页大小
        :return: 页数
        """

        count_sql = "select COUNT(0) from ( %s ) temp_count" % sql
        logging.debug("execute sql ==>:" + count_sql.replace("\n", " "))
        self.cursor.execute(count_sql)
        data = self.cursor.fetchone()
        logging.debug("fetch rows  <==:" + str(len(data)))
        num = data[0] // page_size
        if data[0] > page_size * num:
            num = num + 1
        return num

    def select_page_list(self, sql: str, page_size=50, page_start=1, **kwargs) -> list:
        """
        获取分页列表数据,列表方式返回
        :param sql:
        :param page_size: 分页大小
        :param page_start: 开始页 （从1开始）

        **kwargs
        page_end: 结束页 defalut：page_start+1
        add_headers: 是否添加表头 defalut:False
        :return:
        """
        page_end = kwargs["page_end"]
        if page_end is None:
            page_end = page_start + 1
        add_headers = kwargs["add_headers"]
        if add_headers is None:
            add_headers = False

        start = page_size * (page_start - 1)
        end = page_size * (page_end - 1)
        page_sql = """select * FROM (select temp_page.*, rownum row_id from 
                     ( %s ) temp_page 
                   where limit %d,%d
                   """ % (sql, end, start)

        logging.debug("execute sql ==>:" + page_sql.replace("\n", " "))
        self.cursor.execute(page_sql)
        data = self.cursor.fetchall()
        logging.debug("fetch rows  <==:" + str(len(data)))
        if add_headers:
            col_names = self.cursor.description
            list(data).insert(col_names, 0)
        return data

    def select_dict_page_list(self, sql: str, page_size=50, page_start=1, **kwargs) -> list:
        """
        获取分页列表数据，,字典形式返回
        :param sql:
        :param page_size:
        :param page_start:
        :param page_end:
        :return:
        """

        page_end = kwargs["page_end"]
        if page_end is None:
            page_end = page_start + 1
        add_headers = kwargs["add_headers"]
        if add_headers is None:
            add_headers = False

        data = self.select_page_list(sql, page_size, page_start, page_end=page_end, add_headers=add_headers)
        col_names = self.cursor.description
        return self._data_to_map(col_names, data)

    def select_value(self, sql: str) -> str:
        """
        获取第一列第一行的值
        :param sql:
        :return:
        """
        logging.debug("execute sql ==>:" + sql.replace("\n", " "))
        self.cursor.execute(sql)
        data = self.cursor.fetchone()
        logging.debug("fetch rows  <==:" + str(len(data)))
        if data is None:
            return None
        return str(data[0])

    def select_list(self, sql: str) -> list:
        """
        获取列表数据,列表方式返回
        :param sql:
        :return:
        """
        logging.debug("execute sql ==>:" + sql.replace("\n", " "))
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        logging.debug("fetch rows  <==:" + str(len(data)))
        return data

    def select_dict_list(self, sql: str) -> list:
        """
        获取查询语句列表数据，以字典方式返回: List<dict>
        :param sql:
        :return:
        """
        logging.debug("execute sql ==>:" + sql.replace("\n", " "))
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        logging.debug("fetch rows  <==:" + str(len(data)))
        col_names = self.cursor.description
        return self._data_to_map(col_names, data)

    def select_dict(self, sql: str) -> dict:
        """
        获取字典数据，以字典方式返回: dict
        :param sql:
        :return:
        """
        data = self.select_dict_list(sql)
        if data is None or len(data) == 0:
            return None
        return data[0]

    @staticmethod
    def _data_to_map(col_names, data):
        r_list = []
        for k in range(len(data)):
            row = {}
            for i in range(len(col_names)):
                row[col_names[i][0]] = data[k][i]
            r_list.append(row)
        return r_list

    def execute(self, sql: str) -> int:
        """
        执行sql,并返回影响行数
        :param sql:
        :return:
        """
        logging.debug("execute sql ==>:" + sql.replace("\n", " "))
        self.cursor.execute(sql)
        i = self.cursor.rowcount
        logging.debug("fetch rows  <==:" + str(i))
        return i

    def close(self):
        self.conn.close()
        logging.debug("conn close")

    def rollback(self):
        """回滚事务"""
        logging.debug("rollback start")
        self.conn.rollback()
        logging.debug("rollback end")

    def commit(self):
        """提交事务，如果有对数据库进更新，需要手动提交事务
        """
        logging.debug("commit start")
        self.conn.commit()
        logging.debug("commit end")
        return

    def db_data_type_mapping(self):
        """数据库与Python类型映射
            key: 数据库类型
            value: Py类型
            默认为MySQL数据库
        """

        return {
            "CHAR,VARCHAR,TINYBLOB,TINYTEXT,BLOB,TEXT,MEDIUMBLOB,MEDIUMTEXT,LONGBLOB,LONGTEXT": str,
            "TINYINT,SMALLINT,MEDIUMINT,INT,INTEGER,BIGINT": float,
            "FLOAT,DOUBLE,DECIMAL": int,
            "DATE,TIME,YEAR,DATETIME,TIMESTAMP": "STR_TO_DATE('${VALUE}', '%Y-%m-%d %H:%i:%s')",
        }

    def __get_py_type(self, db_type):
        dtm = self.db_data_type_mapping()
        for dt in dtm.keys():
            dd = dt.split(",")
            if str(db_type).upper() in dd:
                return dtm[dt]
        raise Exception("不支持的数据库类：" + db_type)

    def db_column_sql(self, table_name):
        """
        获取数据列的SQL
        占位参数：
            ${TABLE_NAME} : 表名
            ${DB_NAME} : 连接的数据库名

        :return:
            COLUMN_NAME : 名称
            DATA_TYPE ： 类型
            COMMENTS ： 备注
            DATA_LENGTH ： 长度
            DATA_PRECISION ：精度
        """
        return f"""
        SELECT 
         COLUMN_NAME AS COLUMN_NAME,
        DATA_TYPE AS DATA_TYPE,
        COLUMN_COMMENT AS COMMENTS,
        CHARACTER_MAXIMUM_LENGTH AS DATA_LENGTH,
        NUMERIC_PRECISION AS DATA_PRECISION
         FROM INFORMATION_SCHEMA.COLUMNS T
         WHERE 
         TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{self.Db_NAME}'
        """

    def gen_insert_dict_sql(self, dict_data: dict, table_name: str):
        """
        通过字典保存数据，字典的key为表列表
        :param dict_data:
        :param table_name:
        :return:
        """
        columns = self.__select_db_columns(table_name=table_name)
        values = {}
        if dict_data is not None:
            for c in columns:
                column_name = c.get("COLUMN_NAME", None) if c.get("COLUMN_NAME", None) is not None else c["column_name"]
                data_type = c.get("DATA_TYPE", None) if c.get("DATA_TYPE", None) is not None else c["data_type"]
                self.__get_column_values(column_name, data_type, dict_data, values)
        else:
            return None
        sql = f"""INSERT INTO {self.Db_NAME}.{table_name} ({','.join(values.keys())}) VALUES ({','.join(values.values())})"""
        return sql

    def __get_column(self, name, columns):
        for c in columns:
            column_name = c.get("COLUMN_NAME", None) if c.get("COLUMN_NAME", None) is None else c["column_name"]
            if column_name == name:
                return c
        return None

    def __select_db_columns(self, table_name):
        cache = self.table_column_cache.get(table_name, None)
        if cache is not None:
            return cache
        else:
            columns = self.select_dict_list(sql=self.db_column_sql(table_name=table_name))
            self.table_column_cache[table_name] = columns
            return columns

    def gen_update_dict_sql(self, dict_data: dict, table_name: str, where: str):
        """
        根据字典生成Update语句
        :param dict_data:  列新字典
        :param table_name: 表名
        :param where: 带上where条件
        :return:
        """
        columns = self.__select_db_columns(table_name=table_name)
        values = {}
        if dict_data is not None:
            for column_name in dict_data.keys():
                column = self.__get_column(column_name, columns)
                if column is not None:
                    data_type = column.get("DATA_TYPE", None) if column.get("DATA_TYPE", None) is None else column[
                        "data_type"]
                    self.__get_column_values(column_name, data_type, dict_data, values)
        else:
            return None
        sets = []
        for c in values.keys():
            sets.append(f"{c}={values[c]}")
        update_sql = f"UPDATE {self.Db_NAME}.{table_name} SET {','.join(sets)} " + where
        return update_sql

    def __get_column_values(self, column_name, data_type, dict_data, values):
        v = dict_data.get(column_name, None)
        t = self.__get_py_type(db_type=data_type)
        if v is None:
            values[column_name] = "null"
        elif t == str:
            values[column_name] = f"'{self.convert_str(v)}'"
        elif t in (int, float):
            values[column_name] = str(v)
        else:
            if isinstance(v, datetime.datetime) or isinstance(v, datetime.date):
                values[column_name] = t.replace("${VALUE}", date_to_str(v))
            else:
                values[column_name] = t.replace("${VALUE}", self.convert_str(v))

    def select_insert_sql(self, sql: str, table_name: str) -> list:
        """根据SQL查询数据并生成Insert语句"""
        records = self.select_dict_list(sql=sql)
        sql_list = []
        for r in records:
            sql = self.gen_insert_dict_sql(dict_data=r, table_name=table_name)
            sql_list.append(sql)
        return sql_list

    def convert_str(self, s: str):
        return str(s)

    def export_data_file(self, sql, dir_path, file_mode="txt", pack_size=500000, bachSize=10000, add_header=True,
                         data_split_chars=',',
                         data_close_chars='"', encoding="utf-8"):
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
        """
        csv_start(cursor=self.cursor,
                  sql=sql,
                  path=dir_path,
                  bachSize=bachSize,
                  PACK_SIZE=pack_size,
                  file_mode=file_mode,
                  add_header=add_header,
                  CSV_SPLIT=data_split_chars,
                  CSV_FIELD_CLOSE=data_close_chars,
                  encoding=encoding)
