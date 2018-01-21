# coding utf-8
"""
Contains the Client class.
"""

from .db import DB
from sqlalchemy import (
    create_engine,
    inspect
)


class Client(object):
    """
    Handles the connection to a database.
    """
    def __init__(self, url=None):
        """
        Construct the object.
        Args:
            url (unicode): The db connection string.
        """
        self._url = url

    def __getattr__(self, name):
        """
        Called when a user try to access to an attribute of the object.
        Used to catch the DB name.
        Args:
            name (unicode): The name of the db to fetch.

        Returns:
            The attribute attribute.
        """
        if name not in self.__dict__:
            self.discover_databases()

        return self.__dict__[name]

    def get_schema_names(self):
        """
        Get the list of schemas in the instance.
        Returns:
            (list of unicode): List of schemas.
        """
        engine = self.get_engine()
        return inspect(engine).get_schema_names()

    def get_engine(self):
        """
        Creates the SQLAlchemy Engine.
        Returns:
            (sqlalchemy.engine.Engine): The created Engine.
        """
        return create_engine(self._url)

    def discover_databases(self):
        schema_names = inspect(self.get_engine()).get_schema_names()
        for schema_name in schema_names:
            setattr(self, schema_name, DB(
                url=u"{}/{}".format(self._url.rstrip(u"/"), schema_name)
            ))
