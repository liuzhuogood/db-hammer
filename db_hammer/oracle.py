import os
import sys

from db_hammer import DB_TYPE_ORACLE
from db_hammer.base import BaseConnection

try:
    import cx_Oracle
except ImportError:
    print("cant import cx_Oracle")
    print("===> pip3 install cx_Oracle")
    raise Exception("import cx_Oracle error")


class OracleConnection(BaseConnection):

    def __init__(self, **kwargs):
        self.db_type = DB_TYPE_ORACLE
        if kwargs.get("host", None) is None:
            raise Exception("host")
        if kwargs.get("user", None) is None:
            raise Exception("user")
        if kwargs.get("db_name", None) is None:
            raise Exception("db_name")
        if kwargs.get("pwd", None) is None:
            raise Exception("pwd")
        port = kwargs.get("port", 1521)
        nlsLang = kwargs.get("nlsLang", "SIMPLIFIED CHINESE_CHINA.AL32UTF8")
        super().__init__(**kwargs)
        CONNECT_TNS = False
        # NLS_LANG = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'
        # NLS_LANG = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'
        os.environ['NLS_LANG'] = nlsLang
        if kwargs.get("oracleHome", None) is not None:
            oracleHome = str(kwargs["oracleHome"]).lower().replace("oci.dll", "").replace("OCI.Dll", "")
            sys.path.append(oracleHome)
            os.environ['PATH'] = oracleHome
            os.environ['ORACLE_HOME'] = oracleHome
        self.conn = None
        if not CONNECT_TNS:
            try:
                self.conn = cx_Oracle.connect(kwargs["user"], kwargs["pwd"],
                                              f'{kwargs["host"]}:{port}/{kwargs["db_name"]}',
                                              encoding="UTF-8", nencoding="UTF-8")
            except Exception as e:
                if "12514" in str(e):
                    CONNECT_TNS = True
                else:
                    raise e
        if CONNECT_TNS:
            try:
                print("尝试TNS..")
                dsn = cx_Oracle.makedsn(kwargs["host"], port, service_name=kwargs["db_name"])
                self.conn = cx_Oracle.connect(kwargs["user"], kwargs["pwd"], dsn=dsn, encoding="UTF-8",
                                              nencoding="UTF-8")
            except Exception as e:
                raise e
        self.cursor = self.conn.cursor()

    def db_data_type_mapping(self):
        return {
            "VARCHAR2,NVARCHAR2,CHAR": str,
            "FLOAT,NUMBER": float,
            "INTEGER": int,
            "DATE,TIMESTAMP": "to_date('${VALUE}','yyyy-mm-dd,hh24:mi:ss')",
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
            SELECT TC.COLUMN_ID,
               TC.NULLABLE,
               TC.COLUMN_NAME,
               DATA_TYPE,
               DATA_LENGTH,
               DECODE(DATA_TYPE, 'NUMBER', DATA_PRECISION || ',' || DATA_SCALE, DATA_LENGTH) DATA_PRECISION,
               CC.COMMENTS,
               TC.DATA_DEFAULT,
                CS.COMMENTS TABLE_COMMENTS,
               pk.COLUMN_NAME PK_TAG
        FROM USER_TAB_COLUMNS TC
                 LEFT JOIN USER_COL_COMMENTS CC
                           ON TC.TABLE_NAME = CC.TABLE_NAME AND TC.COLUMN_NAME = CC.COLUMN_NAME
                 left join (
            select col.COLUMN_NAME
            from user_constraints con,
                 user_cons_columns col
            where con.constraint_name = col.constraint_name
              and con.constraint_type = 'P'
              and col.table_name = '{table_name}'
        ) PK on tc.COLUMN_NAME = pk.COLUMN_NAME
        left join user_tab_comments CS
                   ON TC.TABLE_NAME = CS.TABLE_NAME
        WHERE TC.TABLE_NAME = '{table_name}'
        """

    def convert_str(self, s: str):
        return s.replace("'", "''")
