# coding utf-8
"""
Contains the Client class.
"""

import re
from .db import DB
from sqlalchemy import (
    create_engine,
    inspect
)
from .utils import parse_url_and_add_param


class Client(object):
    """
    Handles the connection to a database.
    """
    def __init__(self, url=None, schema=None, encoding=None):
        """
        Construct the object.
        Args:
            url (unicode): The db connection string.
            schema (unicode): The schema to use (optionnal)/
            encoding (str): The encoding to use during interactions with the database.
        """
        self._url = url
        self._schema = schema
        self._encoding = encoding

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
            setattr(self, name, DB(
                url=self.adapt_url(name),
                schema=self._schema
            ))

        return self.__dict__[name]

    def get_engine(self):
        """
        Creates the SQLAlchemy Engine.
        Returns:
            (sqlalchemy.engine.Engine): The created Engine.
        """
        kwargs = {}

        if self._encoding is not None:
            kwargs[u"encoding"] = self._encoding

        return create_engine(*[self._url], **kwargs)

    def adapt_url(self, schema_name):
        """
        Adapt the url to connect to the right database.
        Args:
            schema_name (unicode): The name of the DB to inject in the url.

        Returns:
            (unicode): The modified url.
        """
        if u"cloudsql" in self._url:
            regex = u"(\S+)\/([^\/]+)(\?.+\/cloudsql\/.+)"
            m = re.search(regex, self._url)
            groups = list(m.groups())
            groups[1] = schema_name
            url = u"{}/{}{}".format(*groups)

        else:
            url = u"{}/{}".format(self._url.rstrip(u"/"), schema_name)

        return parse_url_and_add_param(url, u"charset", self._encoding)

