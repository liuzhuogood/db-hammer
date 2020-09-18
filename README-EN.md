# db-hammer
[[中文](./README.md)] [[English](/README-EN.md)] 
**db-hammer** It is a simple database tool library that provides some advanced methods on the Python DB-API; it is convenient for you to do database queries frequently.
Provides a convenient data export function, which can export super-large table data with high performance and stability.
At present, MySQL, Oracle, MSSQL, and PostgreSQL data have been packaged, and it is also very simple to package other structured databases.
### Install
``` shell
pip3 install db-hammer
```

### Python Console
``` python
>>> from db_hammer.mysql import MySQLConnection

>>> db_conf = {"host": "10.0.0.10","user": "dbuser","pwd": "dbpassword","db_name": "db_name"}
>>> db = MySQLConnection(**db_conf):
>>> r = db.select_dict_list("select * from t_student")
>>> r
[{'name': '小明', 'sex': '男', 'age': 18, 'address': '湖南省长沙岳麓区', 'mobile': '13012345678'}, {'name': '小花', 'sex': '女', 'age': 16, 'address': '江苏省南京市鼓楼区', 'mobile': '13100000001'}]
```

### Python With
``` python
from db_hammer.mysql import MySQLConnection

db_conf = {"host": "10.0.0.10","user": "dbuser","pwd": "dbpassword","db_name": "db_name"}

with MySQLConnection(**db_conf) as db:
    i_sql = db.gen_insert_dict_sql(dict_data={
        "name": "小白",
        "sex": "男",
        "age": "20",
        "address": "上海市虹口区",
    }, table_name="t_student")

    db.execute(i_sql)
    db.commit()

    rs = db.select_dict_list(sql="select * from t_student")
    print(rs)
```


### Connect to other database
``` python
from db_hammer.oracle import OracleConnection
from db_hammer.postgresql import PostgreSQLConnection
from db_hammer.mssql import MsSQLConnection

db_oracle = OracleConnection(**db_conf)

db_psql = PostgreSQLConnection(**db_conf)

db_mssql = MsSQLConnection(**db_conf)

```
### Export database data to files
``` python
with MySQLConnection(**mysql_conf) as db:
    db.export_data_file(sql="select * from exam_rule_b", dir_path="./export_data", file_mode="csv")
```

According to SQL: `select * from exam_rule_b` export to `./export_data`, the default is a split file, the export file type supports: `txt`, `csv`, `gz`, file encoding format: `encoding`

Export in cursor mode, and actually test the export of large tables. And the export performance is high. When exporting in the `gz` format, it is automatically compressed, and many tools support direct reading of the `gz` text.


### Logging
``` python
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
```
### List of methods
``` python
# Get the number of pages according to sql
select_page_size(sql: str, page_size=50)

# Get the data of the paging list, return the list in dictionary form
select_page_list(sql: str, page_size=50, page_start=1, **kwargs)

# Get the data of the paging list, return the list in dictionary form
select_dict_page_list(sql: str, page_size=50, page_start=1, **kwargs)

# Get the value of the first column and first row
select_value(sql: str)

# Get list data, return list
select_list(sql: str)

# Get all list data, return list dictionary form
select_dict_list(sql: str)

# Get the first row of data, return in dictionary form
select_dict(sql: str)

# Execute SQL and return the number of affected rows
execute(sql: str)

# Close the connection
close()

# Rollback transaction
rollback()

# Commit transaction
commit()

# Generate Insert SQL from dictionary
gen_insert_dict_sql(dict_data: dict, table_name: str)

# Generate Update SQL according to the dictionary
gen_update_dict_sql(dict_data: dict, table_name: str, where: str)

# Generate Insert SQL based on sql statement
select_insert_sql(sql: str, table_name: str)
```

## Other utils
``` python
# Execute local commands
db_hammer.util.cmd
# Simple date conversion
db_hammer.util.date
# Get file and directory tools (easy to use)
db_hammer.util.file
# Simple save key value tool
db_hammer.util.keep
# A simple tool for sending and receiving emails
db_hammer.util.sim_email

```
