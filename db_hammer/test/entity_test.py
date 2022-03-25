import datetime

from db_hammer.mysql import MySQLConnection

db_conf = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "11456",
    "database": "db_hammer",
    "debug": True
}


class TableName:
    __table_name__ = "table_name"
    id: int
    name: str
    desc: str
    add_time: datetime.datetime


with MySQLConnection(**db_conf) as db:
    t = TableName()
    t.id = 2
    t.name = "小白"
    t.desc = "test!!"
    t.add_time = datetime.datetime.now()

    db.insert_entity(t)

    rs = db.select_entity_list(entity_class=TableName)
    for r in rs:
        print(r.__dict__)
