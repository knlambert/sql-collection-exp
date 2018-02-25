# coding utf-8
"""
Contains DB Class.
"""

from sqlalchemy import (
    create_engine,
    MetaData
)
from .collection import Collection


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

        return self.__dict__[name]

    def get_engine(self):
        """
        Creates the SQLAlchemy engine.
        Returns:
            (sqlalchemy.engine.Engine): The created Engine.
        """
        return create_engine(self._url)

    def discover_collections(self):
        """
        Discover tables, create a collection object for each one in the
        schema.
        """
        meta = MetaData()
        meta.reflect(bind=self.get_engine(), views=True)
        for key in meta.tables:
            root_table = meta.tables[key]
            setattr(self, key, Collection(
                db_ref=self,
                table=root_table
            ))
