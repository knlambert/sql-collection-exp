# coding utf-8
"""
Contains DB Class.
"""

from .utils import json_set


class Cursor(object):

    def __init__(self, collection_ref, select, lookup):
        """
        Constructs the object.
        Args:
            collection_ref (Collection): The reference to the collection object.
            select (sqlalchemy.sql.selectable.Select): The select request to process.
            lookup (list of dict): The lookup parameter used in the find query associated.
        """
        self._select = select
        self._collection_ref = collection_ref
        self._executed = False
        self._rows = None
        self._lookup = lookup

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

    def sort(self, key_or_list, direction=None):
        """
        Sorts the result set.
        Args:
            key_or_list (unicode|list of unicode): A single key or a list of (key, direction) pairs specifying the keys to sort on.
            direction (unicode|list of unicode):  Used if key_or_list is a single key, if not given ASCENDING is assumed

        Returns:
            (Cursor): The cursor itself.
        """
        fields_mapping, _ = self._collection_ref.generate_select_dependencies(self._lookup)

        if isinstance(key_or_list, unicode):
            key_or_list = [key_or_list]

        if direction is None:
            direction = 1

        if isinstance(direction, int):
            direction = [direction]

        if len(direction) != len(key_or_list):
            raise ValueError(u"Wrong parameters : key_or_list size different from direction.")

        binding = {
            1: u"asc",
            -1: u"desc"
        }

        to_order_by = []

        for index, key in enumerate(key_or_list):
            column = fields_mapping.get(key)
            if column is not None:
                to_order_by.append(getattr(column, binding[direction[index]])())

        self._select = self._select.order_by(*to_order_by)
        return self

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
