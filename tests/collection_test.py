# coding utf-8
"""
Collection class tests
"""
from pytest import fixture
from mock import Mock
from sqlcollection.collection import Collection
from sqlcollection.db import DB
from sqlalchemy.sql.expression import Label
from sqlalchemy.types import Integer, String
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


@fixture(scope=u"function")
def stubbed_collection(hours_count_db, project_table):
    return Collection(db_ref=hours_count_db, table=project_table)


def test_generate_lookup(stubbed_collection, project_table):
    lookup = stubbed_collection.generate_lookup(project_table, 2, u"test")
    assert len(lookup) == 1
    assert lookup[0][u"to"] == u"project"
    assert lookup[0][u"localField"] == u"client"
    assert lookup[0][u"from"] == u"client"
    assert lookup[0][u"foreignField"] == u"id"
    assert lookup[0][u"as"] == u"test.client"


@fixture(scope=u"function")
def mock_project_fields_mapping():
    fields = [
        u"id", u"name"
    ]
    mock_project_fields_mapping = {}
    for key in fields:
        mock_project_fields_mapping[key] = Column(
            name=key
        )
    return mock_project_fields_mapping


def test__parse_query_basic(stubbed_collection, mock_project_fields_mapping):
    result = stubbed_collection._parse_query({
        u"id": 5,
        u"name": u"test"
    }, fields_mapping=mock_project_fields_mapping)
    assert str(result) == u"id = :id_1 AND name = :name_1"


def test__parse_query_operators(stubbed_collection, mock_project_fields_mapping):
    result = stubbed_collection._parse_query({
        u"id": {
            u"$gt": 5,
            u"$lt": 10
        }
    }, fields_mapping=mock_project_fields_mapping)
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


def test_generate_select_dependencies(stubbed_collection, project_table, client_table):
    field_mapping, joins = stubbed_collection.generate_select_dependencies([
        {
            u"from": u"client",
            u"to": u"project",
            u"foreignField": u"id",
            u"localField": u"client",
            u"as": u"client"
        }
    ])
    assert field_mapping == {
        u'id': project_table.columns[u"id"],
        u'name': project_table.columns[u"name"],
        u'client.id': project_table.columns[u"client"],
        u'client.name': client_table.columns[u"name"]
    }
    assert joins == [
        (client_table, project_table.columns[u"client"], client_table.columns[u"id"])
    ]


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
        u"client": -1
    })

    assert filtered_labels == [
        labels[0], labels[1]
    ]
