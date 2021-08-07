import datetime
import logging
import re
from enum import Enum

from db_hammer import DB_TYPE_MYSQL
from db_hammer.csv import start as csv_start
from db_hammer.entity_util import update_sql, insert_sql, delete_sql, entity_list, where_entity, where_like_entity, \
    order_by_pagination, get_entity_primary_key
from db_hammer.page import PageOutput, PageInput
from db_hammer.sql_exception import SqlException, FetchRowsException, ExistException
from db_hammer.util.date import date_to_str


class DataType(Enum):
    DateNull = -2177774
    DataPass = -2177771


class BaseConnection(object):
    def __init__(self, **kwargs):
        """
        :param kwargs:
        """
        self.log = kwargs.get("log", logging.getLogger(__name__))
        self.Db_NAME = kwargs.get("db_name")
        self.caps = kwargs.get("caps", None)  # A:大写 a:小写
        self.conn = None  # type:Connect
        self.cursor = None  # type:cursor
        self.table_column_cache = {}
        if self.db_type is None:
            self.db_type = DB_TYPE_MYSQL

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def select_page_size(self, sql: str, params: dict = None, page_size=50) -> (int, int):
        """
        根据sql获取 (页数,行数)
        :param params:
        :param sql:
        :param page_size: 分页大小
        :return: 页数
        """

        count_sql = "select COUNT(0) from ( %s ) temp_count" % sql
        count_sql, params = self.sql_params(count_sql, params)
        self.log.debug("执行SQL:" + count_sql)
        self.log.debug("参数:" + str(params))
        self.cursor.execute(count_sql, params)
        data = self.cursor.fetchone()
        count_rows = data[0]
        self.log.debug("影响行数:" + str(len(data)))
        num = count_rows // page_size
        if count_rows > page_size * num:
            num = num + 1
        return num, count_rows

    def sql_params(self, sql: str, params: dict):
        if params is None:
            return sql, None
        ps = re.findall('(?<!\w):\w+', sql)
        start = 0
        new_params = {}
        for p in ps:
            if p[1:] not in params or len(p) == 1:
                raise SqlException(f"SQL语句中参数[{p}]没有传值")
            new_params[p[1:]] = params[p[1:]]
            start = sql.find(p, start)
            if self.db_type == DB_TYPE_MYSQL:
                param = "%(" + p[1:] + ")s"
            else:
                param = ":" + p[1:] + ""
            sql = sql[:start] + param + sql[start + len(p):]
            start += len(param)
        return sql, new_params

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
        params = kwargs.get("params", None)
        page_end = kwargs["page_end"]
        if page_end is None:
            page_end = page_start + 1
        add_headers = kwargs["add_headers"]
        if add_headers is None:
            add_headers = False

        start = page_size * (page_start - 1)
        end = page_size * (page_end - page_start)
        page_sql = """SELECT * FROM ( %s ) temp_page LIMIT %d,%d """ % (sql, start, end)
        self.log.debug("执行SQL:" + page_sql)
        page_sql, params = self.sql_params(page_sql, params)
        self.log.debug("参数:" + str(params))
        self.cursor.execute(page_sql, params)
        data = self.cursor.fetchall()
        self.log.debug("影响行数:" + str(len(data)))
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

        page_end = kwargs.get("page_end", page_start + 1)
        add_headers = kwargs.get("add_headers", False)
        params = kwargs.get("params", None)
        data = self.select_page_list(sql, page_size, page_start, params=params,
                                     page_end=page_end,
                                     add_headers=add_headers)
        col_names = self.cursor.description
        return self._data_to_map(col_names, data)

    def select_value(self, sql: str, params=None) -> str:
        """
        获取第一列第一行的值
        :param sql:
        :return:
        """
        self.log.debug("执行SQL:" + sql)
        _sql, params = self.sql_params(sql, params)
        self.cursor.execute(_sql, params)
        data = self.cursor.fetchone()
        if data is None:
            return None
        return str(data[0])

    def select_list(self, sql: str, params=None) -> list:
        """
        获取列表数据,列表方式返回
        :param sql:
        :return:
        """
        self.log.debug("执行SQL:" + sql)
        self.log.debug("参数:" + str(params))
        _sql, params = self.sql_params(sql, params)
        self.cursor.execute(_sql, params)
        data = self.cursor.fetchall()
        self.log.debug("影响行数:" + str(len(data)))
        return data

    def select_dict_list(self, sql: str, params=None) -> list:
        """
        获取查询语句列表数据，以字典方式返回: List<dict>
        :param params:
        :param sql:
        :return:
        """
        self.log.debug("执行SQL:" + sql)
        self.log.debug("参数:" + str(params))
        _sql, params = self.sql_params(sql, params)
        self.cursor.execute(_sql, params)
        data = self.cursor.fetchall()
        self.log.debug("影响行数:" + str(len(data)))
        col_names = self.cursor.description
        return self._data_to_map(col_names, data)

    def select_dict(self, sql: str, params=None) -> dict:
        """
        获取字典数据，以字典方式返回: dict
        :param params:
        :param sql:
        :return:
        """
        data = self.select_dict_list(sql, params)
        if data is None or len(data) == 0:
            return None
        return data[0]

    def _data_to_map(self, col_names, data):
        r_list = []
        for k in range(len(data)):
            row = {}
            for i in range(len(col_names)):
                name = col_names[i][0]
                if self.caps is not None:
                    if self.caps == "A":
                        name = name.upper()
                    else:
                        name = name.lower()
                row[name] = data[k][i]
            r_list.append(row)
        return r_list

    def execute(self, sql: str, params=None) -> int:
        """
        执行sql,并返回影响行数
        :param params:
        :param sql:
        :return:
        """
        _sql, params = self.sql_params(sql, params)
        self.log.debug("执行SQL:" + _sql)
        self.log.debug("参数:" + str(params))
        if params is None:
            self.cursor.execute(_sql)
        else:
            self.cursor.execute(_sql, params)
        i = self.cursor.rowcount
        self.log.debug("影响行数:" + str(i))
        return i

    def close(self):
        self.conn.close()
        # self.log.debug("关闭连接")

    def rollback(self):
        """回滚事务"""
        # self.log.debug("回滚事务开始")
        self.conn.rollback()
        self.log.debug("回滚事务完成")

    def commit(self):
        """提交事务，如果有对数据库进更新，需要手动提交事务
        """
        # self.log.debug("提交事务开始")
        self.conn.commit()
        self.log.debug("提交事务完成")
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
        raise Exception("不支持的数据库类：" + str(db_type))

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
                values[column_name] = self.__get_column_value(data_type=data_type, value=dict_data[column_name])
        else:
            return None
        sql = f"""INSERT INTO {self.Db_NAME}.{table_name} ({','.join(values.keys())}) VALUES ({','.join(values.values())})"""
        return sql

    def __get_column(self, name, columns):
        for c in columns:
            column_name = c.get("COLUMN_NAME", None) if c.get("COLUMN_NAME", None) is not None else c["column_name"]
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

    def gen_update_dict_sql(self, dict_data: dict, table_name: str, where: str, not_update_colunms=[]):
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
                    data_type = column.get("DATA_TYPE", None) if column.get("DATA_TYPE", None) is not None else column[
                        "data_type"]
                    values[column_name] = self.__get_column_value(data_type=data_type, value=dict_data[column_name])
        else:
            return None
        sets = []
        for c in values.keys():
            if c not in not_update_colunms:
                sets.append(f"{c}={values[c]}")
        update_sql = f"UPDATE {self.Db_NAME}.{table_name} SET {','.join(sets)} " + where
        return update_sql

    def __get_column_value(self, data_type, value):
        """获取列的值"""
        if data_type is None:
            return f"'{self.convert_str(value)}'"
        t = self.__get_py_type(db_type=data_type)
        if value is None:
            return "null"
        elif t == str:
            return f"'{self.convert_str(value)}'"
        elif t in (int, float):
            return str(value)
        else:
            if isinstance(value, (datetime.datetime, datetime.date)):
                return t.replace("${VALUE}", date_to_str(value))
            else:
                return t.replace("${VALUE}", self.convert_str(value))

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
        csv_start(cursor=self.cursor,
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

    def insert_entity(self, entity, commit=True):
        """传入实体保存到数据库"""
        sql, values = insert_sql(entity=entity)
        sql, values = self.sql_params(sql, values)
        i = self.execute(sql, values)
        primary_key = self.cursor.lastrowid
        if primary_key != 0:
            setattr(entity, get_entity_primary_key(entity)[0], primary_key)
        if i != 1:
            raise FetchRowsException(f"影响条数为{i}")
        if commit:
            self.commit()
        return entity

    def update_entity(self, entity, pass_null=False, commit=True, check_exist=False) -> None:
        if check_exist:
            es = self.select_entity_by_pk(entity=entity)
            if es is None:
                raise ExistException(f"找不到要更新的实体")
        sql, values = update_sql(entity=entity, pass_null=pass_null)
        self.log.debug(f"SQL:{sql}")
        self.log.debug(f"params:{values}")
        sql, values = self.sql_params(sql, values)
        i = self.execute(sql, values)
        if commit:
            self.commit()

    def delete_entity(self, entity, commit=True) -> int:
        sql, values = delete_sql(entity=entity)
        self.log.debug(sql)
        self.log.debug(f"params:{values}")
        sql, values = self.sql_params(sql, values)
        i = self.execute(sql, values)
        if commit:
            self.commit()
        return i

    def select_entity_list(self, cls=None, sql=None, params=None, like_entity=None, entity=None, rel="AND",
                           page: PageInput = None):
        if sql is None:
            if like_entity is not None:
                if cls is None:
                    cls = like_entity.__class__
                table_name = getattr(like_entity, "__table_name__")
            elif entity is not None:
                if cls is None:
                    cls = entity.__class__
                table_name = getattr(entity, "__table_name__")
            else:
                table_name = getattr(cls, "__table_name__")

            sql = "SELECT * FROM " + table_name
        if params is None:
            if like_entity is not None:
                where, values = where_like_entity(entity=like_entity, rel=rel)
                sql += where
                params = values
            elif entity is not None:
                where, values = where_entity(entity=entity, rel=rel)
                sql += where
                params = values
        if sql is None:
            raise Exception("查询SQL不能为空")
        if page is not None:
            pages, rowsNumber = self.select_page_size(sql=sql, params=params,
                                                      page_size=page.page_size)
            order_by = order_by_pagination(entity=cls, pagination=page)
            dict_list = self.select_dict_page_list(sql=f"{sql} {order_by}", params=params,
                                                   page_size=page.page_size,
                                                   page_start=page.page_start)
            values = entity_list(dict_list=dict_list, cls=cls)
            pageOut = PageOutput()
            pageOut.rows = values
            pageOut.page_start = page.page_start
            pageOut.page_size = page.page_size
            pageOut.page_number = pages
            pageOut.rows_number = rowsNumber
            pageOut.sort_by = page.sort_by
            pageOut.descending = page.descending
            return pageOut
        else:
            dict_list = self.select_dict_list(sql=sql, params=params)
            values = entity_list(dict_list=dict_list, cls=cls)
            return values

    def select_entity_first(self, cls=None, sql=None, params=None, like_entity=None, entity=None):
        ll = self.select_entity_list(cls, sql=sql, params=params, like_entity=like_entity, entity=entity)
        if ll is not None and len(ll) > 0:
            return ll[0]

    def select_entity_by_pk(self, entity, pk_value=None):
        primary_key, _pk_value = get_entity_primary_key(entity)
        table_name = getattr(entity, "__table_name__")
        _SQL = ""
        if isinstance(primary_key, list):
            for pk in primary_key:
                _SQL += f"""{pk}=:{pk} AND"""
            _SQL = _SQL[:-3]
        else:
            _SQL = f"""{primary_key}=:{primary_key}"""
        sql = f"""SELECT * FROM {table_name} WHERE {_SQL}"""
        if pk_value is None:
            pk_value = _pk_value

        params = {}
        if isinstance(pk_value, str):
            params[primary_key] = pk_value
        else:
            params = pk_value

        ll = self.select_entity_list(entity, sql=sql, params=params)
        if ll is not None and len(ll) > 0:
            return ll[0]
