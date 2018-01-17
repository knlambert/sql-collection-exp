# coding utf-8
"""
Contains DB Class.
"""

from sqlalchemy.sql import select


class Collection(object):
    """
    Wrapper around a collection.
    """
    def __init__(self, db_ref, meta_table):
        """
        Construct the object.
        Args:
            db_ref (DB): A reference to the DB object (parent).
            meta_table (sqlalchemy.sql.schema.Table): The table the collection wraps.
        """
        self._db_ref = db_ref
        self.meta_table = meta_table

    def get_connection(self):
        """
        Get connection to execute statements.
        Returns:
            (sqlalchemy.engine.base.Connection) A SQLAlchemy connection.
        """
        connection = self._db_ref.get_engine().connect()
        return connection

    def find(self, query=None, projection=None, lookup=None, auto_lookup=0):
        """
        Does a find query on the collection.
        Args:
            query (dict): The mongo like query to execute.
            projection (dict): The projection parameter determines which columns are returned
                in the matching documents.
            lookup (list of dict): The lookup to apply during this query.
            auto_lookup (int): If we don't know what lookup we want, we let the lib to look
                them for us. This can have consequences on optimization as it constructs
                joins. Be careful.
        """
        client = getattr(self._db_ref, u'client').meta_table
        request = select([self.meta_table.c.id.label(u"test"), client.c.name]).select_from(
            self.meta_table.join(client)
        )
        conn = self.get_connection()
        print(request.c)
        return conn.execute(request)
