# coding utf-8
"""
Contains DB Class.
"""

import json
from .utils import json_set
from sqlalchemy.sql import select


class Collection(object):
    """
    Wrapper around a collection.
    """
    def __init__(self, db_ref, table, related_tables=None):
        """
        Construct the object.
        Args:
            db_ref (DB): A reference to the DB object (parent).
            table (sqlalchemy.sql.schema.Table): The table the collection wraps.
            related_tables (list of sqlalchemy.sql.schema.Table): Tables in relation with the root one.
        """
        self._db_ref = db_ref
        self._table = table
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

    def generate_select_dependencies(self, lookup=None):
        """
        Generate the fields for the select request.
        Args:
            lookup (list of dict): The lookup to apply during this query.

        Returns:
            (list of sqlalchemy.sql.elements.Label, list of tuples)
        """
        fields_to_ignore = [
            u".".join([look[u"to"], look[u"localField"]]) for look in lookup
        ]

        fields = [
            column.label(unicode(column.name)) for column in self._table.columns
            if u".".join([column.table.name, column.name]) not in fields_to_ignore
        ]

        joins = []
        for relation in lookup:
            from_table = getattr(self._db_ref, relation[u"from"])._table
            to_table = getattr(self._db_ref, relation[u"to"])._table
            joins.append((
                from_table, getattr(to_table.c, relation[u"localField"]), getattr(from_table.c, relation[u"foreignField"])
            ))

            fields.extend([
                column.label(u".".join([relation[u"as"], unicode(column.name)])) for column in from_table.columns
                if u".".join([column.table.name, column.name]) not in fields_to_ignore
            ])

        return (fields, joins)

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
        lookup = lookup or []
        fields, joins = self.generate_select_dependencies(lookup)
        acc = None

        for foreign_table, local_field, foreign_field in joins:
            if acc is None:
                acc = self._table.join(foreign_table, local_field == foreign_field)
            else:
                acc = acc.join(foreign_table, local_field == foreign_field)

        acc = self._table if acc is None else acc

        request = select(fields).select_from(acc)
        conn = self.get_connection()
        rows = conn.execute(request)
        return self.rows_to_dict_generator(request.c, rows)

    @staticmethod
    def rows_to_dict_generator(columns, rows):
        """
        Transforms rows into dict.
        Args:
            columns (list of sqlalchemy.sql.base.ImmutableColumnCollection): List of columns to set in dict.
            rows (sqlalchemy.engine.result.ResultProxy): Result set containing the rows.

        Returns:
            (generator): Generates an iterator on rows transformed in dict.
        """
        for row in rows:
            obj = {}
            for index, column in enumerate(columns):
                json_set(obj, column.name, row[index])

            yield obj

        raise StopIteration
