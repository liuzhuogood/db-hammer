# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

db-hammer 是一个Python数据库工具库，提供简单易用的数据库操作高级方法，支持MySQL、Oracle、MSSQL、PostgreSQL等数据库。主要功能包括数据库连接、查询、分页、数据导出等。

## 开发环境设置

### 安装依赖
```bash
pip3 install -r requirements.txt
```

主要依赖：
- Cython
- pymssql
- PyMySQL
- selenium

### 安装本地包
```bash
pip3 install -e .
```

## 项目结构

```
db_hammer/
├── __init__.py          # 数据库类型常量定义
├── base.py              # 基础数据库连接类 BaseConnection
├── mysql.py             # MySQL连接实现
├── oracle.py            # Oracle连接实现
├── postgresql.py        # PostgreSQL连接实现
├── mssql.py             # MSSQL连接实现
├── sqlite.py            # SQLite连接实现
├── csv.py               # CSV导出功能
├── page.py              # 分页相关类
├── entity_util.py       # 实体工具类
├── sql_exception.py     # SQL异常类
├── auto/                # 浏览器自动化模块
│   ├── base_driver.py   # 基础驱动类
│   ├── chrome_driver.py # Chrome驱动
│   ├── firefox_driver.py# Firefox驱动
│   └── util.py          # 自动化工具
├── util/                # 工具模块
│   ├── cmd.py           # 命令执行工具
│   ├── date.py          # 日期工具
│   ├── file.py          # 文件工具
│   ├── log.py           # 日志工具
│   └── ...
├── net/                 # 网络模块
│   ├── master.py        # 主节点
│   └── slaver.py        # 从节点
└── test/                # 测试文件
    ├── auto_test.py     # 自动化测试
    └── entity_test.py   # 实体测试
```

## 核心架构

### 基础连接类 (BaseConnection)
- 位于 `db_hammer/base.py:19`
- 提供所有数据库连接的基础功能
- 支持连接管理、事务处理、SQL执行等
- 包含分页查询、字典查询、实体操作等高级方法

### 数据库连接实现
- 各数据库连接类继承自 `BaseConnection`
- `MySQLConnection` (db_hammer/mysql.py:30)
- `OracleConnection` (db_hammer/oracle.py)
- `PostgreSQLConnection` (db_hammer/postgresql.py)
- `MsSQLConnection` (db_hammer/mssql.py)

### 实体操作 (entity_util.py)
- 提供基于Python实体的数据库操作
- 支持实体自动映射到数据库表
- 提供CRUD操作的实体封装

## 常用开发命令

### 运行测试
```bash
python3 -m db_hammer.test.auto_test
python3 -m db_hammer.test.entity_test
```

### 构建发布包
```bash
python3 setup.py sdist bdist_wheel
```

### 安装到本地
```bash
pip3 install -e .
```

## 开发规范

### 数据库连接使用
推荐使用 `with` 语句管理连接：
```python
from db_hammer.mysql import MySQLConnection

db_conf = {"host": "localhost", "user": "user", "password": "pass", "database": "db"}

with MySQLConnection(**db_conf) as db:
    result = db.select_dict_list("SELECT * FROM table")
```

### 编码规范
- 所有文件使用 UTF-8 编码
- 使用中文进行注释和文档
- 每个代码文件不超过300行，适当拆分模块

### 日志配置
项目使用标准logging模块，可通过以下方式开启调试日志：
```python
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
```

## 主要功能模块

### 数据导出 (csv.py)
- 支持大表数据的高性能导出
- 支持多种格式：txt、csv、gz
- 使用游标方式避免内存溢出

### 自动化工具 (auto/)
- 提供浏览器自动化功能
- 支持Chrome和Firefox驱动
- 包含网页操作、截图等功能

### 工具模块 (util/)
- 命令执行工具 (cmd.py)
- 日期转换工具 (date.py)
- 文件操作工具 (file.py)
- 日志工具 (log.py)
- SSH工具 (ssh.py)
- 邮件工具 (sim_email.py)