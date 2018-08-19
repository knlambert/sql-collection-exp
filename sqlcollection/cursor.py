# coding utf-8
"""
Contains DB Class.
"""

import sys
import decimal
from .utils import json_set
from .compatibility import BYTE_TYPE, INT_TYPES
from sqlalchemy import func, select, subquery, union


class Cursor(object):

    def __init__(self, collection_ref, fields, joins, where, lookup):
        """
        Constructs the object.
        Args:
            collection_ref (Collection): The reference to the collection object.
            fields:
            joins:
            where:
            lookup (list of dict): The lookup parameter used in the find query associated.
        """
        self._fields = fields
        self._joins = joins
        self._where = where
        self._limit = None
        self._offset = None
        self._sort = None
        self._order_by = None

        self._collection_ref = collection_ref
        self._lookup = lookup

    def __iter__(self):
        """
        Called when iterating on the cursor. Should be called once per request.
        Returns:
            (iterator): The iterator on items.
        """
        conn = self._collection_ref.get_connection()
        request = self._serialize()
        rows = conn.execute(request)
        return self._to_dict_generator(select(self._fields).c, rows)

    def _to_dict_generator(self, columns, rows):
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

            for key, value in row.items():
                python_type = type(value)
                if python_type in [decimal.Decimal, float]:
                    value = float(value)

                # elif python_type is BYTE_TYPE:
                #     value = value.decode(self._collection_ref._db_ref._encoding)

                json_set(obj, key, value)

            yield obj

        raise StopIteration

    def _serialize_count(self, with_limit_and_skip=False):
        """
        Serialize the request to send the count of items in the cursor.
        Args:
            with_limit_and_skip (boolean): If the count ignores limit / skip or not.

        Returns:
            (sqlalchemy.sql.selectable.Select): A SQLAlchemy representation of the select request used.
        """
        if not with_limit_and_skip:
            request = self._serialize([func.count()], add_limit_and_skip=False)
        else:
            request = select([func.count()]).select_from(self._serialize().alias(u"sub"))

        return request

    def _serialize(self, labels=None, add_limit_and_skip=True):
        """
        Serialize the request using the different elements from the cursor.
        Args:
            labels (list of sqlalchemy.sql.elements.Label): A list of select labels. Used to affect which fields
                are returned (for count and projection features).
        Returns:
            (sqlalchemy.sql.selectable.Select): A SQLAlchemy representation of the select request.
        """

        request = select(labels or self._fields).select_from(self._joins)

        if self._where is not None:
            request = request.where(self._where)

        if self._order_by is not None:
            request = request.order_by(*self._order_by)

        if self._offset is not None and add_limit_and_skip:
            request = request.offset(self._offset)

        if self._limit is not None and add_limit_and_skip:
            request = request.limit(self._limit)

        return request

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

        if isinstance(key_or_list, str):
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

        self._order_by = to_order_by
        return self

    def limit(self, limit):
        """
        Limits the result set.
        Args:
            limit (int): Number of rows to limit.

        Returns:
            (Cursor): The cursor itself.
        """
        self._limit = limit
        return self

    def skip(self, skip):
        """
        Skips lines in the result set.
        Args:
            skip (int): Number of rows to skip.

        Returns:
            (Cursor): The cursor itself.
        """
        self._offset = skip
        return self

    def count(self, with_limit_and_skip=False):
        """
        Return the line count for the cursor.
        Args:
            with_limit_and_skip (boolean): If the count ignores limit / skip or not.
        Returns:
            (int): The line count.
        """
        conn = self._collection_ref.get_connection()
        request = self._serialize_count(with_limit_and_skip)
        count = conn.execute(request)
        return list(count)[0][0]
