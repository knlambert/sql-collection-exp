# coding utf-8
"""
Contains DB Class.
"""

from sqlalchemy.sql import select


class Collection(object):
    """
    Wrapper around a collection.
    """
    def __init__(self, db_ref, root_table, related_tables=None):
        """
        Construct the object.
        Args:
            db_ref (DB): A reference to the DB object (parent).
            root_table (sqlalchemy.sql.schema.Table): The table the collection wraps.
            related_tables (list of sqlalchemy.sql.schema.Table): Tables in relation with the root one.
        """
        self._db_ref = db_ref
        self._root_table = root_table
        self._related_tables = related_tables or []

    def get_connection(self):
        """
        Get connection to execute statements.
        Returns:
            (sqlalchemy.engine.base.Connection) A SQLAlchemy connection.
        """
        connection = self._db_ref.get_engine().connect()
        return connection

    def generate_select_joins(self, root_table):
        """
        Generate the list of relations.
        Args:
            root_table (sqlalchemy.sql.schema.Table): The root table we fetch the relations from.

        Returns:
            (lists of sqlalchemy.sql.schema.Table)
        """
        linked_tables = []
        for column in root_table.columns:
                linked_tables += [
                    (column.name, column, foreign_key.column) for foreign_key in column.foreign_keys
                ]
        print(linked_tables)
        return linked_tables

    def generate_select_fields(self):
        """
        Generate the fields for the select request.

        Returns:
            (list of sqlalchemy.sql.elements.Label): List of labels to generate the SQL query.
        """
        return [column.label(unicode(column.name)) for column in self._root_table.columns]

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
        fields = self.generate_select_fields()
        request = select(fields).select_from(
            self._root_table #.join(client)
        )
        conn = self.get_connection()
        print(request.c)
        return conn.execute(request)
