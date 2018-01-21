# coding utf-8
"""
Collection class tests
"""
from pytest import fixture
from mock import Mock
from sqlcollection.collection import Collection
from sqlalchemy.schema import Column

@fixture(scope=u"function")
def mock_client_table():
    client_table = Mock()
    client_table.name = u"client"
    client_table.foreign_keys = []
    return client_table


@fixture(scope=u"function")
def mock_project_table(mock_client_table):
    project_table = Mock()
    project_table.name = u"project"
    client_foreign_key = Mock()
    client_foreign_key.parent = Mock()
    client_foreign_key.parent.name = u"client"
    client_foreign_key.column = Mock()
    client_foreign_key.column.name = u"id"
    client_foreign_key.column.table = mock_client_table

    project_table.foreign_keys = [
        client_foreign_key
    ]
    return project_table


@fixture(scope=u"function")
def stubbed_collection(mock_project_table):
    return Collection(db_ref=Mock(), table=mock_project_table)


def test_generate_lookup(stubbed_collection, mock_project_table):
    lookup = stubbed_collection.generate_lookup(mock_project_table, 2, u"test")
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
    print(str(result))
    assert str(result) == u"id = :id_1 OR id = :id_2"


