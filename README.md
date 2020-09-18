# db-hammer
##### [[中文](https://github.com/liuzhuogood/db-hammer/blob/master/README.md)] [[English](https://github.com/liuzhuogood/db-hammer/blob/master/README-EN.md)] 
**[db-hammer](https://github.com/liuzhuogood/db-hammer)** 是个简单数据库工具库，在 Python DB-API上提供一些高级方法；方便大家经常做数据库查询。
提供方便的导出数据功能，可高性能、稳定的导出超大表数据。
目前已对 MySQL、Oracle、MSSQL、PostgreSQL 数据进行封装，封装其他结构化数据库也很简单。
### 安装
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


### 连接其他数据库
``` python
from db_hammer.oracle import OracleConnection
from db_hammer.postgresql import PostgreSQLConnection
from db_hammer.mssql import MsSQLConnection

db_oracle = OracleConnection(**db_conf)

db_psql = PostgreSQLConnection(**db_conf)

db_mssql = MsSQLConnection(**db_conf)

```
### 导出数据库数据
``` python
with MySQLConnection(**mysql_conf) as db:
    db.export_data_file(sql="select * from exam_rule_b", dir_path="./export_data", file_mode="csv")
```

根据SQL:`select * from exam_rule_b`导出到`./export_data`，默认为分割文件，导出文件类型支持：`txt`、`csv`、`gz`，可以传入文件编码格式：`encoding`

游标方式导出，实际测试大表导出，20个G都没有问题。且导出性能较高，使用`gz`格式导出时，自动压缩，且很多工具支持直接读取`gz`文本。


### 开启日志
``` python
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
```
### 常用数据库方法列表
``` python
# 根据sql获取页数
select_page_size(sql: str, page_size=50)

# 获取分页列表数据,列表字典形式返回
select_page_list(sql: str, page_size=50, page_start=1, **kwargs)

# 获取分页列表数据,列表字典形式返回
select_dict_page_list(sql: str, page_size=50, page_start=1, **kwargs)

# 获取第一列第一行的值
select_value(sql: str)

# 获取列表数据,列表方式返回
select_list(sql: str)

# 获取所有列表数据，列表字典形式返回
select_dict_list(sql: str)

# 获取第一行数据，字典形式返回
select_dict(sql: str)

# 执行SQL,并返回影响行数
execute(sql: str)

# 关闭连接
close()

# 回滚事务
rollback()

# 提交事务
commit()

# 根据字典生成 Insert SQL
gen_insert_dict_sql(dict_data: dict, table_name: str)

# 根据字典来生成 Update SQL
gen_update_dict_sql(dict_data: dict, table_name: str, where: str)

# 根据sql语句把数据生成 Insert SQL
select_insert_sql(sql: str, table_name: str)
```

## 其他小工具
``` python
# 执行本地命令
db_hammer.util.cmd
# 简单的日期转换
db_hammer.util.date
# 获取文件和目录工具（好用）
db_hammer.util.file
# 简单保存键值工具
db_hammer.util.keep
# 一个简单的收发邮件工具
db_hammer.util.sim_email

```



