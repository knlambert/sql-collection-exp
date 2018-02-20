# coding utf-8
"""
Cursor class tests
"""

from mock import Mock
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


def test_limit(stubbed_cursor):
    cursor = stubbed_cursor.limit(25)
    assert type(cursor) == Cursor
    assert cursor._limit == 25


def test_skip(stubbed_cursor):
    cursor = stubbed_cursor.skip(12)
    assert type(cursor) == Cursor
    assert cursor._offset == 12


def test_count(stubbed_cursor):
    fake_connection = Mock()
    fake_connection.execute = Mock(return_value=[[36]])
    fake_collection = Mock()
    fake_collection.get_connection = Mock(return_value=fake_connection)
    stubbed_cursor._collection_ref = fake_collection
    fake_request = Mock()
    stubbed_cursor._serialize_count = Mock(return_value=fake_request)
    count = stubbed_cursor.count(True)
    assert count == 36
    stubbed_cursor._serialize_count.assert_called_with(True)
    fake_connection.execute.assert_called_with(fake_request)
