# coding utf-8
"""
Cursor class tests
"""

from pytest import fixture
from sqlcollection.cursor import Cursor
from sqlalchemy.sql.expression import Label
from .collection_test import (
    stubbed_collection,
    project_client_lookup,
    client_table,
    project_table,
    hours_count_db
)


@fixture(scope=u"function")
def stubbed_cursor(stubbed_collection, project_client_lookup, client_table, project_table):
    fields_mapping, joins = stubbed_collection.generate_select_dependencies(project_client_lookup)
    where = stubbed_collection._parse_query({
        u"id": 5
    }, fields_mapping)
    return Cursor(stubbed_collection, [
        Label(u"id", project_table.columns[u"id"]),
        Label(u"name", project_table.columns[u"name"]),
        Label(u"client.id", client_table.columns[u"id"]),
        Label(u"client.name", client_table.columns[u"name"])
    ], joins, where, project_client_lookup)


# def test__serialize_count(stubbed_cursor):
#     result = stubbed_cursor._serialize_count(False)
#     print(str(result))