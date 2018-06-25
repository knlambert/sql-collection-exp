# coding: utf-8
"""
This module contains unit tests for the methods in utils file.
"""

try:
    import urlparse
    from urllib import urlencode
except ImportError:
    import urllib.parse as urlparse
    from urllib.parse import urlencode

from sqlcollection.utils import (
    json_to_one_level,
    json_set,
    json_get,
    parse_url_and_add_param
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


def test_parse_url_and_add_param_nothing_to_add():
    url_cloudsql = u"mysql://root:localroot@/stuffs?charset=utf8&unix_socket=/cloudsql/project:europe-west1:instance"
    resulting_url = parse_url_and_add_param(url_cloudsql, u"charset", u"utf8")
    # Checking result
    original_parts = list(urlparse.urlparse(url_cloudsql))
    resulting_parts = list(urlparse.urlparse(resulting_url))
    assert dict(urlparse.parse_qsl(original_parts[4])) == dict(urlparse.parse_qsl(resulting_parts[4]))

def test_parse_url_and_add_param_add_charset():
    url_cloudsql = u"mysql://root:localroot@/stuffs?unix_socket=/cloudsql/project:europe-west1:instance"
    resulting_url = parse_url_and_add_param(url_cloudsql, u"charset", u"utf8")
    # Checking result
    resulting_parts = list(urlparse.urlparse(resulting_url))
    result_args = dict(urlparse.parse_qsl(resulting_parts[4]))
    assert result_args[u"charset"] == u"utf8"
    assert result_args[u"unix_socket"] == u"/cloudsql/project:europe-west1:instance"