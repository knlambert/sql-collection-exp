# coding utf-8
"""
Contains DB Class.
"""

from sqlalchemy import (
    create_engine,
    MetaData,
    inspect
)
from sqlalchemy.sql import select
from sqlalchemy.orm import sessionmaker


class DB(object):
    """
    Wrapper around a database schema.
    """
    def __init__(self, url):
        """
        Construct the object.
        Args:
            url (unicode): The DB connection URL.
        """
        self._url = url
        self._tables = []

    def __getattr__(self, name):
        """
        Called when a user try to access to an attribute of the object.
        Used to catch the table name.
        Args:
            name (unicode): The name of the table to fetch.

        Returns:
            The attribute attribute.
        """
        if name not in self.__dict__:
            self.discover_collections()
            self.__dict__[name] = u"test"
        return self.__dict__[name]

    def get_engine(self):
        """
        Creates the SQLAlchemy engine.
        Returns:
            (sqlalchemy.engine.Engine): The created Engine.
        """
        return create_engine(self._url)

    def discover_collections(self):
        meta = MetaData()
        meta.reflect(bind=self.get_engine())
        self._tables = meta.tables

    def list(self):
        engine = self.get_engine()
        conn = engine.connect()
        user = self._tables[u"user"]
        req = select([user])
        for row in conn.execute(req):
            print(row)