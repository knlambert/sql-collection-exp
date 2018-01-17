# coding: utf-8
"""
This module contains unit tests for the methods in utils file.
"""

from sqlcollection.utils import (
    json_to_one_level,
    json_set,
    json_get
)


def test_json_to_one_level():
    """
    Tests json_to_one_level method.
    """
    ret = json_to_one_level({
        u"name": {
            u"firstname": u"test"
        }
    })
    assert ret[u"name.firstname"] == u"test"

    ret = json_to_one_level({
        u'googleGroup.id': 1,
        u'metadata': {
            u'path': u'primaryEmail',
            u'id': 225.0
        }
    })
    assert u"googleGroup.id" in ret
    assert u"metadata.path" in ret
    assert u"metadata.id" in ret

    ret = json_to_one_level({
        u"names": [
            {u"value": u"kevin"},
            {u"value": u"alexis"},
            {u"value": u"nicolas"}
        ]
    })
    assert ret[u"names.0.value"] == u"kevin"
    assert ret[u"names.1.value"] == u"alexis"
    assert ret[u"names.2.value"] == u"nicolas"

    ret = json_to_one_level({
        u"names": [
            u"kevin",
            u"alexis",
            u"nicolas"
        ]
    })
    assert ret[u"names.0"] == u"kevin"
    assert ret[u"names.1"] == u"alexis"
    assert ret[u"names.2"] == u"nicolas"

    ret = json_to_one_level({
        u"person": {
            u"names": [
                u"kevin",
                u"alexis",
                u"nicolas"
            ]
        }
    })
    assert ret[u"person.names.0"] == u"kevin"
    assert ret[u"person.names.1"] == u"alexis"
    assert ret[u"person.names.2"] == u"nicolas"


def test_json_set_get():
    ret = {}
    ret = json_set(ret, u"banana.color", 5)
    ret = json_set(ret, u"banana.size", 25)
    ret = json_set(ret, u"country", u"France")

    assert json_get(ret, u"banana.color") == 5
    assert json_get(ret, u"banana.size") == 25
    assert json_get(ret, u"country") == u"France"
    assert json_get(ret, u"unknown", u"yeah") == u"yeah"
    assert json_get(ret, u"banana.country") is None
