from .base import BaseConnection

try:
    import psycopg2
except ImportError:
    print("cant import psycopg2")
    print("===> pip3 install psycopg2")
    raise Exception("import psycopg2 error")


class PostgreSQLConnection(BaseConnection):

    def __init__(self, **kwargs):
        if kwargs.get("host", None) is None:
            raise Exception("host")
        if kwargs.get("user", None) is None:
            raise Exception("user")
        if kwargs.get("db_name", None) is None:
            raise Exception("db_name")
        if kwargs.get("pwd", None) is None:
            raise Exception("pwd")
        port = kwargs.get("port", 5432)

        super().__init__(**kwargs)
        self.conn = psycopg2.connect(database=kwargs["db_name"],
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
