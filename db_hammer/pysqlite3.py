import sqlite3
from db_hammer.base import BaseConnection


class Sqlite3Connection(BaseConnection):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if kwargs.get("database", None) is None:
            raise Exception("database")
        self.conn = sqlite3.connect(kwargs["database"], **kwargs)
        self.cursor = self.conn.cursor()
