# coding utf-8
"""
Contains DB Class.
"""

from .utils import json_set


class Cursor(object):

    def __init__(self, collection_ref, select):
        """
        Constructs the object.
        Args:
            collection_ref (Collection): The reference to the collection object.
            select (sqlalchemy.sql.selectable.Select): The select request to process.
        """
        self._select = select
        self._collection_ref = collection_ref
        self._executed = False
        self._rows = None

    def __iter__(self):
        if not self._executed:
            conn = self._collection_ref.get_connection()
            rows = conn.execute(self._select)
            self._executed = True
            self._rows = self._to_dict_generator(self._select.c, rows)
        return self._rows

    @staticmethod
    def _to_dict_generator(columns, rows):
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

    def limit(self, limit):
        """
        Limits the result set.
        Args:
            limit (int): Number of rows to limit.

        Returns:
            (Cursor): The cursor itself.
        """
        self._select = self._select.limit(limit)
        return self

    def skip(self, skip):
        """
        Skips lines in the result set.
        Args:
            skip (int): Number of rows to skip.

        Returns:
            (Cursor): The cursor itself.
        """
        self._select = self._select.offset(skip)
        return self
