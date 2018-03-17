# coding utf-8
"""
Collection class tests
"""
import sys
import datetime
from pytest import fixture
from mock import Mock
from sqlcollection.db import DB
from collections import OrderedDict
from sqlcollection.collection import Collection
from sqlalchemy.sql.expression import Label, Alias
from sqlcollection.compatibility import UNICODE_TYPE
from sqlalchemy.types import Integer, String, DateTime
from sqlalchemy.schema import Column, Table, MetaData, ForeignKey

@fixture(scope=u"function")
def client_table():
    client_table = Table(u"client", MetaData(),
                         Column(u"id", Integer()),
                         Column(u"name", String(50))
                         )
    client_table.foreign_keys = []
    return client_table


@fixture(scope=u"function")
def project_table(client_table):
    client_column = Column(u"client", MetaData())
    client_column.foreign_keys = [
        ForeignKey(client_table.columns[u"id"])
    ]

    project_table = Table(u"project", MetaData(),
                          Column(u"id", Integer()),
                          Column(u"name", String(50)),
                          client_column
                          )

    client_foreign = ForeignKey(client_table.columns[u"id"])
    client_foreign.parent = project_table.columns[u"client"]
    project_table.foreign_keys = [
        client_foreign
    ]
    return project_table


@fixture(scope=u"function")
def hours_count_db(client_table, project_table):
    db = DB(u"hours_count")
    db.client = Collection(db, client_table)
    db.project = Collection(db, project_table)
    return db


def add_connection_execute_mock(collection, return_value):
    connection = Mock()
    connection.execute = Mock(return_value=return_value)
    collection.get_connection = Mock(return_value=connection)
    return collection


@fixture(scope=u"function")
def stubbed_collection(hours_count_db, project_table):
    collection = Collection(db_ref=hours_count_db, table=project_table)
    return collection


@fixture(scope=u"function")
def project_client_lookup():
    return [
        {
            u"from": u"client",
            u"to": u"project",
            u"foreignField": u"id",
            u"localField": u"client",
            u"as": u"client"
        }
    ]


def test_generate_lookup(stubbed_collection, project_table):
    lookup = stubbed_collection.generate_lookup(project_table, 2, u"test")
    assert len(lookup) == 1
    assert lookup[0][u"to"] == u"test"
    assert lookup[0][u"localField"] == u"client"
    assert lookup[0][u"from"] == u"client"
    assert lookup[0][u"foreignField"] == u"id"
    assert lookup[0][u"as"] == u"test.client"


@fixture(scope=u"function")
def mock_project_fields_mapping():
    fields = [
        (u"id", Integer()), (u"name", String(50))
    ]
    mock_project_fields_mapping = {}
    for key, type_ in fields:
        mock_project_fields_mapping[key] = Column(
            name=key,
            type_=type_
        )

    return mock_project_fields_mapping


def test__parse_query_basic(stubbed_collection, mock_project_fields_mapping):
    result = stubbed_collection._parse_query(OrderedDict([
        (u"id", 5), (u"name", u"test")
    ]), fields_mapping=mock_project_fields_mapping)
    assert UNICODE_TYPE(result) == u"id = :id_1 AND name = :name_1"


def test__parse_query_like(stubbed_collection, mock_project_fields_mapping):
    result = stubbed_collection._parse_query({
        u"name": {
            u"$like": u"%pouet%"
        }
    }, fields_mapping=mock_project_fields_mapping)
    assert str(result) == u"name LIKE :name_1"


def test__parse_query_regex(stubbed_collection, mock_project_fields_mapping):
    result = stubbed_collection._parse_query({
        u"name": {
            u"$regex": u".*Yeah.*"
        }
    }, fields_mapping=mock_project_fields_mapping)
    assert str(result) == u"name REGEXP :name_1"


def test__parse_query_operators(stubbed_collection, mock_project_fields_mapping):
    result = stubbed_collection._parse_query(
        OrderedDict([(u"id", OrderedDict([(u"$gt", 5), (u"$lt", 10)]))]),
        fields_mapping=mock_project_fields_mapping
    )
    assert str(result) == u"id > :id_1 AND id < :id_2"


def test__parse_query_recursive_operator(stubbed_collection, mock_project_fields_mapping):
    result = stubbed_collection._parse_query({
        u"$or": [{
            u"id": 5
        }, {
            u"id": 3
        }]
    }, fields_mapping=mock_project_fields_mapping)
    assert str(result) == u"id = :id_1 OR id = :id_2"


def test_generate_select_fields_mapping(stubbed_collection, project_table, client_table):
    ret = stubbed_collection.generate_select_fields_mapping(project_table)
    assert ret == {
        u"id": project_table.columns[u"id"],
        u"name": project_table.columns[u"name"],
        u"client": project_table.columns[u"client"]
    }


def test_generate_select_fields_mapping_with_prefix(stubbed_collection, project_table, client_table):
    ret = stubbed_collection.generate_select_fields_mapping(project_table, u"prefix")
    assert ret == {
        u"prefix.id": project_table.columns[u"id"],
        u"prefix.name": project_table.columns[u"name"],
        u"prefix.client": project_table.columns[u"client"]
    }


def test_generate_select_dependencies_no_lookup(stubbed_collection, project_table, client_table):
    ret = stubbed_collection.generate_select_dependencies()
    assert ret == ({
                       u"id": project_table.columns[u"id"],
                       u"name": project_table.columns[u"name"],
                       u"client": project_table.columns[u"client"]
                   }, [])


def test_generate_select_dependencies(
        stubbed_collection,
        project_table,
        client_table,
        project_client_lookup):
    field_mapping, joins = stubbed_collection.generate_select_dependencies(project_client_lookup)

    assert field_mapping[u'id'] == project_table.columns[u"id"]
    assert field_mapping[u'name'] == project_table.columns[u"name"]
    assert field_mapping[u'client.id'] == project_table.columns[u"client"]
    assert field_mapping[u'client.name'].name == client_table.columns[u"name"].name
    assert isinstance(field_mapping[u'client.name'], Column)
    assert joins[0][0].name == u"client"
    assert isinstance(joins[0][0], Alias)
    assert isinstance(joins[0][1], Column)
    assert joins[0][1].name == u"client"
    assert joins[0][1].table.name == u"project"
    assert isinstance(joins[0][2], Column)
    assert joins[0][2].name == u"id"
    assert joins[0][2].table.name == u"client"


def test__apply_projection_keep_one(stubbed_collection, project_table, client_table):
    labels = [
        Label(u"id", project_table.columns[u"id"]),
        Label(u"name", project_table.columns[u"name"]),
        Label(u"client.id", client_table.columns[u"id"]),
        Label(u"client.name", client_table.columns[u"name"])
    ]
    filtered_labels = stubbed_collection._apply_projection(labels, {
        u"client": 1
    })

    assert filtered_labels == [
        labels[2], labels[3]
    ]


def test__apply_projection_hide_one(stubbed_collection, project_table, client_table):
    labels = [
        Label(u"id", project_table.columns[u"id"]),
        Label(u"name", project_table.columns[u"name"]),
        Label(u"client.id", client_table.columns[u"id"]),
        Label(u"client.name", client_table.columns[u"name"])
    ]
    filtered_labels = stubbed_collection._apply_projection(labels, {
        u"name": -1
    })
    assert filtered_labels == [
        labels[0], labels[2], labels[3]
    ]


def test_find(stubbed_collection, project_table, project_client_lookup):
    cursor = stubbed_collection.find(query={
        u"id": {
            u"$ne": 1
        }
    }, projection={
        u"name": 1
    }, lookup=project_client_lookup)

    assert isinstance(cursor._fields[0], Label)
    assert cursor._fields[0].name == u"name"
    assert str(cursor._joins) == u"project JOIN client AS client ON project.client = client.id"
    assert str(cursor._where) == u"project.id != :id_1"
    assert cursor._lookup == project_client_lookup


def test_delete_many(stubbed_collection, project_client_lookup):
    delete_result = Mock()
    delete_result.rowcount = 42
    add_connection_execute_mock(stubbed_collection, delete_result)

    delete_result = stubbed_collection.delete_many(filter={
        u"id": {
            u"$ne": 1
        }
    }, lookup=project_client_lookup)
    assert ((str(stubbed_collection.get_connection().execute.call_args[0][0]))
            == u"DELETE FROM project , client AS client WHERE project.client = "
               u"client.id AND project.id != :id_1")

    assert delete_result.deleted_count == 42


def test_update_many(stubbed_collection, project_client_lookup):
    update_result = Mock()
    update_result.rowcount = 42
    add_connection_execute_mock(stubbed_collection, update_result)

    update_result = stubbed_collection.update_many({
        u"id": {
            u"$ne": 1
        }
    }, {
        u"$set": {
            u"name": u"new value"
        }
    }, project_client_lookup)

    assert ((str(stubbed_collection.get_connection().execute.call_args[0][0]))
            == u"UPDATE project SET name=:name FROM client AS client " \
               u"WHERE project.client = client.id AND project.id != :id_1")

    assert update_result.matched_count == 42


def test_insert_one(stubbed_collection, project_client_lookup):
    insert_one_result = Mock()
    insert_one_result.inserted_primary_key = [42]
    add_connection_execute_mock(stubbed_collection, insert_one_result)

    insert_one_result = stubbed_collection.insert_one({
        u"name": u"new project",
        u"client": {
            u"id": 5,
            u"name": u"World inc"
        }
    }, project_client_lookup)

    assert ((str(stubbed_collection.get_connection().execute.call_args[0][0]))
            == u"INSERT INTO project (name, client) "
               u"VALUES (:name, :client)")

    assert insert_one_result.inserted_id == 42


def test__python_type_to_string(stubbed_collection):
    assert stubbed_collection._python_type_to_string(int) == u"integer"
    assert stubbed_collection._python_type_to_string(float) == u"float"
    if sys.version_info[0] < 3:
        assert stubbed_collection._python_type_to_string(long) == u"integer"
    assert stubbed_collection._python_type_to_string(datetime.datetime) == u"datetime"
    assert stubbed_collection._python_type_to_string(datetime.date) == u"date"
    assert stubbed_collection._python_type_to_string(UNICODE_TYPE) == u"string"


def test__convert_to_python_type(stubbed_collection):
    date_col = Column(u"name", DateTime)
    value = u"2017-01-01 00:00:00"
    assert (
        stubbed_collection._convert_to_python_type(value, date_col) == datetime.datetime(
            2017, 1, 1, 0, 0, 0
        )
    )

