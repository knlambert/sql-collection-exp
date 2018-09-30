# coding utf-8
"""
Contains DB Class.
"""
from sqlalchemy import (
    create_engine,
    MetaData
)
from .collection import Collection
from .utils import parse_url_and_add_param


class DB(object):
    """
    Wrapper around a database schema.
    """
    def __init__(self, url, schema=None, encoding=None):
        """
        Construct the object.
        Args:
            url (unicode): The DB connection URL.
            schema (unicode): Optionnal schema to select.
            encoding (str): The encoding to use during interactions with the database.
        """
        self._url = url
        self._schema = schema
        self._encoding = encoding

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
        if self._encoding is not None:
            url = parse_url_and_add_param(self._url, u"charset", self._encoding)
            return create_engine(
                url, encoding=self._encoding
            )
        else:
            return create_engine(self._url)

    def discover_collections(self):
        """
        Discover tables, create a collection object for each one in the
        schema.
        """
        meta = MetaData(schema=self._schema)
        meta.reflect(bind=self.get_engine(), views=True)

        for key in meta.tables:
            root_table = meta.tables[key]
            if self._schema:
                key = key.split(".")[1]
            setattr(self, key, Collection(
                db_ref=self,
                table=root_table
            ))
