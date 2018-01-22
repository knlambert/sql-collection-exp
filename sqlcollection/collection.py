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
from .results import (
    InsertResultOne
)

from .utils import json_to_one_level


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
    def generate_select_fields_mapping(table, prefix=None):
        """
        Generates the mapping between json keys and table columns.
        Args:
            table (sqlalchemy.sql.schema.Table): The table we want the mapping.
            prefix (unicode): A prefix to add to the keys.

        Returns:
            (dict): The field mapping (unicode: Column).
        """

        fields_mapping = {}
        for column in table.columns:

            label_parts = [column.name]

            if prefix is not None:
                label_parts.insert(0, prefix)

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
        switch_plan = []

        fields_mapping = self.generate_select_fields_mapping(self._table)

        joins = []
        for relation in lookup:

            from_table = getattr(self._db_ref, relation[u"from"])._table
            to_table = getattr(self._db_ref, relation[u"to"])._table

            from_column = getattr(from_table.c, relation[u"foreignField"])
            to_column = getattr(to_table.c, relation[u"localField"])
            joins.append((from_table, to_column, from_column))

            switch_plan.append((from_column, to_column))

            fields_mapping.update(self.generate_select_fields_mapping(from_table, relation[u"as"]))

        for from_column, to_column in switch_plan:
            for key, column in fields_mapping.items():
                if column.table.name == from_column.table.name and column.name == from_column.name:
                    fields_mapping[key] = to_column
                elif column.table.name == to_column.table.name and column.name == to_column.name:
                    del fields_mapping[key]
        return fields_mapping, joins

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
            auto_lookup (int): How many levels of lookup will be generated automatically.
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

    def insert_one(self, document, lookup=None, auto_lookup=0):
        """
        Insert a document in the table.
        Args:
            document (dict): The document to insert.
            lookup (list of dict): The lookup to apply during this query.
            auto_lookup (int): How many levels of lookup will be generated automatically.

        Returns:
            (InsertResultOne): The result object.
        """
        document = json_to_one_level(document)
        lookup = (lookup or []) if auto_lookup == 0 else self.generate_lookup(self._table, auto_lookup)
        fields_mapping, _ = self.generate_select_dependencies(lookup)

        insert_kwargs = {}
        for key in document:
            column = fields_mapping.get(key)

            if column is not None and column.table.name == self._table.name:
                insert_kwargs[column.name] = document[key]

        request = self._table.insert().values(**insert_kwargs)
        result = self.get_connection().execute(request)
        return InsertResultOne(inserted_id=result.inserted_primary_key)

