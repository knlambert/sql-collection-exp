# coding utf-8
"""
Contains DB Class.
"""

from .cursor import Cursor
from sqlalchemy.sql import (
    select,
    and_,
    or_
)

class Collection(object):
    """
    Wrapper around a collection.
    """
    def __init__(self, db_ref, table):
        """
        Construct the object.
        Args:
            db_ref (DB): A reference to the DB object (parent).
            table (sqlalchemy.sql.schema.Table): The table the collection wraps.
        """
        self._db_ref = db_ref
        self._table = table

        self._conjunctions = {
            u"$or": or_,
            u"$and": and_
        }
        self._operators = [u"$eq", u"$gte", u"$gt", u"$lte", u"$lt", u"$ne"]


    def get_connection(self):
        """
        Get connection to execute statements.
        Returns:
            (sqlalchemy.engine.base.Connection) A SQLAlchemy connection.
        """
        connection = self._db_ref.get_engine().connect()
        return connection

    @staticmethod
    def generate_select_joins(root_table):
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
        return linked_tables

    @staticmethod
    def generate_select_fields_mapping(table, fields_to_ignore, prefix=None):
        fields_mapping = {}
        for column in table.columns:
            label_parts = [column.name]
            if prefix is not None:
                label_parts.insert(0, prefix)

            if u".".join([column.table.name, column.name]) not in fields_to_ignore:
                label = u".".join(label_parts)
                fields_mapping[label] = column

        return fields_mapping

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

        fields = self.generate_select_fields_mapping(self._table, fields_to_ignore)

        joins = []
        for relation in lookup:

            from_table = getattr(self._db_ref, relation[u"from"])._table
            to_table = getattr(self._db_ref, relation[u"to"])._table

            joins.append((
                from_table, getattr(to_table.c, relation[u"localField"]), getattr(from_table.c, relation[u"foreignField"])
            ))

            fields.update(self.generate_select_fields_mapping(from_table, fields_to_ignore, relation[u"as"]))

        return fields, joins

    def generate_lookup(self, table, deep, prefix=None):
        """
        Generate the lookup automatically
        Args:
            table (sqlalchemy.sql.schema.Table): The table where to generate the lookup.
            deep (int): Defines how deep the lookup generated is.
            prefix (unicode): Optional prefix for the lookup.

        Returns:
            (list of dict): The generated lookup.
        """
        lookup = []
        for foreign_key in table.foreign_keys:
            as_parts = [foreign_key.parent.name]

            if prefix is not None:
                as_parts = prefix.split(u".") + as_parts

            lookup.append({
                u"to": table.name,
                u"localField": foreign_key.parent.name,
                u"from": foreign_key.column.table.name,
                u"foreignField": foreign_key.column.name,
                u"as": u".".join(as_parts)
            })
            if deep > 1:
                lookup.extend(self.generate_lookup(foreign_key.column.table, deep-1, u".".join(as_parts)))

        return lookup

    def _parse_query(self, query, fields_mapping, parent=None, conjunction=None):
        """
        Parse a dict filter representation (MongoDB DSL) and convert it
        into SQLAlchemy expression language.
        Args:
            query (dict): The filter we generate the where from.
            fields_mapping (dict): Mapping for available fields (unicode: column)
            conjunction (function): The default operator used to associate filters.

        Returns:
            (sqlalchemy.sql.elements): The elements to generate the WHERE clause.
        """
        conjunction = conjunction or and_
        filters = []
        if not isinstance(query, list):
            query = [query]

        for filt in query:
            for key, value in filt.items():
                if key in fields_mapping:

                    if isinstance(value, dict):
                        filters += [self._parse_query(value, fields_mapping, parent=key)]
                    else:
                        filters.append(fields_mapping[key] == value)

                elif key in self._operators:
                    column = fields_mapping[parent]
                    if key == u"$eq":
                        filters.append(column == value)
                    elif key == u"$gte":
                        filters.append(column >= value)
                    elif key == u"$gt":
                        filters.append(column > value)
                    elif key == u"$lt":
                        filters.append(column < value)
                    elif key == u"$lte":
                        filters.append(column <= value)
                    elif key == u"$ne":
                        filters.append(column != value)

                elif key in self._conjunctions and isinstance(value, list):
                    filters.extend([
                        self._parse_query(value, fields_mapping, conjunction=self._conjunctions[key])
                    ])

        return conjunction(*filters)


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
        Returns:
            (Cursor): Cursor to wrap the request result.
        """
        lookup = (lookup or []) if auto_lookup == 0 else self.generate_lookup(self._table, auto_lookup)
        fields_mapping, joins = self.generate_select_dependencies(lookup)

        acc = None

        for foreign_table, local_field, foreign_field in joins:
            if acc is None:
                acc = self._table.join(foreign_table, local_field == foreign_field)
            else:
                acc = acc.join(foreign_table, local_field == foreign_field)

        acc = self._table if acc is None else acc

        labels = [column.label(label) for label, column in fields_mapping.items()]
        request = select(labels).select_from(acc)

        if query is not None:
            request = request.where(self._parse_query(query, fields_mapping))
        return Cursor(self, request)

