import sqlite3
from db_hammer.base import BaseConnection


class Sqlite3Connection(BaseConnection):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        database = kwargs.get("database", None)
        if database is None:
            raise Exception("database")
        del kwargs["database"]
        self.conn = sqlite3.connect(database=database, **kwargs)
        self.cursor = self.conn.cursor()

