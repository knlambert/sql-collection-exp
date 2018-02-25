# coding utf-8
"""
Contains DB Class.
"""

import decimal
import datetime
from .cursor import Cursor
from sqlalchemy.sql import (
    delete,
    update,
    func,
    select,
    and_,
    or_
)

from .results import (
    DeleteResult,
    UpdateResult,
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
        lookup = lookup or []

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
        operator_bindings = {
            u"$eq": u"==",
            u"$gte": u">=",
            u"$gt": u">",
            u"$lte": u"<=",
            u"$lt": u"<",
            u"$ne": u"!="
        }
        available_simple_operators = [
            key for key in operator_bindings
        ]
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
                    if key in available_simple_operators:
                        filters.append(eval(u"column {} value".format(operator_bindings[key])))

                elif key in self._conjunctions and isinstance(value, list):
                    filters.extend([
                        self._parse_query(value, fields_mapping, conjunction=self._conjunctions[key])
                    ])

        return conjunction(*filters)

    def _apply_projection(self, labels, projection):
        """
        Apply a projection by filtering labels regarding the value of projection parameter.
        Args:
            labels (list of sqlalchemy.sql.elements.Label): A list of select labels. Used to affect which fields
                are returned (for count and projection features).
            projection (dict): Definition of the fields to keep or not.

        Returns:
            (list of sqlalchemy.sql.elements.Label): The label(s) left after projection.
        """
        projection = json_to_one_level(projection)
        to_keep = [key for key in projection]
        mode = list(projection.items())[0][1]
        labels = list(labels)
        for index, label in reversed(list(enumerate(labels))):

            do_keep = False if mode == 1 else True

            for key in to_keep:
                if label.name.startswith(key):
                    do_keep = not do_keep
                    break

            if not do_keep:
                labels.pop(index)

        return labels

    def generate_joins(self, joins):
        """
        Generate sql alchemy joins part of the request to select from.
        Args:
            joins (list of sqlalchemy.sql.selectable.Join): The generated joins.

        Returns:
            (sqlalchemy.sql.selectable.Join | sqlalchemy.sql.schema.Table) The object(s) to select from.
        """
        acc = None
        for foreign_table, local_field, foreign_field in joins:
            if acc is None:
                acc = self._table.join(foreign_table, local_field == foreign_field)
            else:
                acc = acc.join(foreign_table, local_field == foreign_field)

        return self._table if acc is None else acc

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

        acc = self.generate_joins(joins)

        labels = [column.label(label) for label, column in fields_mapping.items()]

        where = None
        if query is not None:
            where = self._parse_query(query, fields_mapping)

        if projection is not None:
            labels = self._apply_projection(labels, projection)

        return Cursor(self, labels, acc, where, lookup)

    def delete_many(self, filter, lookup=None, auto_lookup=0):
        """
        Delete many items from the collection.
        Args:
            filter (dict): query (dict): The mongo like query to execute.
            lookup (list of dict): The lookup to apply during this query.
            auto_lookup (int): How many levels of lookup will be generated automatically.

        Returns:
            (DeleteResult): The delete operation result.
        """
        lookup = (lookup or []) if auto_lookup == 0 else self.generate_lookup(self._table, auto_lookup)
        fields_mapping, joins = self.generate_select_dependencies(lookup)
        filters = self._parse_query(filter, fields_mapping)
        if str(filters) == u"":
            raise ValueError(u"Filter parameter is missing.")

        join_where = and_(
            *[(local_field == foreign_field) for foreign_table, local_field, foreign_field in joins]
        )

        request = self._table.delete().where(join_where).where(filters)
        result = self.get_connection().execute(request)
        return DeleteResult(deleted_count=result.rowcount)

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
        return InsertResultOne(inserted_id=result.inserted_primary_key[0])

    def update_many(self, filter, update, lookup=None, auto_lookup=0):
        """
        Update many items from the collection.
        Args:
            filter (dict): query (dict): The mongo like query to execute.
            lookup (list of dict): The lookup to apply during this query.
            auto_lookup (int): How many levels of lookup will be generated automatically.

        Returns:
            (UpdateResult): The update operation result.
        """
        lookup = (lookup or []) if auto_lookup == 0 else self.generate_lookup(self._table, auto_lookup)
        fields_mapping, joins = self.generate_select_dependencies(lookup)
        filters = self._parse_query(filter, fields_mapping)
        if str(filters) == u"":
            raise ValueError(u"Filter parameter is missing.")

        join_where = and_(
            *[(local_field == foreign_field) for foreign_table, local_field, foreign_field in joins]
        )

        update_kwargs = {}
        for key in update[u"$set"]:
            column = fields_mapping.get(key)
            if column is not None:
                update_kwargs[column] = update[u"$set"][key]

        request = self._table.update().values(update_kwargs).where(join_where).where(filters)
        result = self.get_connection().execute(request)
        return UpdateResult(matched_count=result.rowcount)

    @staticmethod
    def _python_type_to_string(python_type):
        """
        Convert a python type to a string representing the type in a generic way.
        Args:
            python_type (type): The python type to convert into a generic string.
        Returns:
            (unicode): The translated type.
        """
        if python_type is int:
            type_ = u"integer"
        elif python_type is decimal.Decimal or python_type in [float, long]:
            type_ = u"float"
        elif python_type is datetime.datetime:
            type_ = u"datetime"
        elif python_type is datetime.date:
            type_ = u"date"
        else:
            type_ = u"string"
        return type_

    def get_description(self, lookup=None, auto_lookup=0, table=None):
        """
        Extract table description.
        Args:
            lookup (list of dict): The lookup to apply during this query.
            auto_lookup (int): How many levels of lookup will be generated automatically.

        Returns:
            (list of dict): Get the table and relation description.
        """
        lookup = (lookup or []) if auto_lookup == 0 else self.generate_lookup(self._table, auto_lookup)
        fields = []
        table = self._table if table is None else table

        for column in table.c:

            fields.append({
                u"name": column.name,
                u"primary_key": column.primary_key,
                u"required": not column.nullable,
                u"type": self._python_type_to_string(column.type.python_type)
            })

        for look in lookup:
            if auto_lookup != 0:
                auto_lookup -= 1

            if look.get(u"to") == table.name:

                for index, field in enumerate(fields):
                    if field.get(u"name") == look.get(u"localField"):
                        foreign_table = getattr(self._db_ref, look.get(u"from"))._table
                        description = self.get_description(lookup, auto_lookup, foreign_table)
                        fields[index][u"nested_description"] = {
                            u"fields": description,
                            u"as": look.get(u"as"),
                            u"table": foreign_table.name
                        }
        return fields
