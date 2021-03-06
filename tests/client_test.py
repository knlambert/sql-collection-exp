# coding utf-8
"""
Collection class tests
"""
import datetime
from pytest import fixture
from sqlcollection import Client
from sqlalchemy.types import Integer, String, DateTime
from sqlcollection.collection import Collection
from sqlalchemy.schema import Column, Table, MetaData, ForeignKey


def test_client_cloud_sql():
    client = Client(url=u"mysql://root:localroot@/stuffs?charset=utf8&unix_socket="
                        u"/cloudsql/project:europe-west1:instance", encoding="utf8")

    assert u"charset=utf8" in client.adapt_url(u"banana")


def test_client():
    client = Client(url=u"mysql://root:localroot@127.0.0.1")

    assert client.adapt_url(u"banana") == u"mysql://root:localroot@127.0.0.1/banana"


