# coding: utf-8
"""
This module contains various utils function at global usage.
"""

import sys
from .compatibility import UNICODE_TYPE


def json_set(item, path, value):
    """
    Set the value corresponding to the path in a dict.
    Arguments:
        item (dict): The object where we want to put a field.
        path (unicode): The path separated with dots to the field.
        value: The value to set on the field.
    Return:
        (dict): The updated object.
    """
    tab = path.split(u".")
    if tab[0] not in item and len(tab) > 1:
        item[tab[0]] = {}
    if len(tab) == 1:
        item[tab[0]] = value
    else:
        item[tab[0]] = json_set(item[tab[0]], u".".join(tab[1:]), value)
    return item


def json_del(item, path):
    """
    Delete the item corresponding to path of the field in a dict.
    Arguments:
        item (dict): The object where we want to delete a field.
        path (unicode): The path separated with dots to the field.
    Return:
        The value.
    """
    tab = path.split(u".")
    if tab[0] in item:
        if len(tab) > 1:
            return json_del(item[tab[0]], u".".join(tab[1:]))
        else:
            del item[tab[0]]

    return item


def json_get(item, path, default=None):
    """
    Return the path of the field in a dict.
    Arguments:
        item (dict): The object where we want to put a field.
        path (unicode): The path separated with dots to the field.
        default: default value if path not found.
    Return:
        The value.
    """
    tab = path.split(u".")

    if isinstance(item, dict) and tab[0] in item:
        if len(tab) > 1:
            return json_get(item[tab[0]], u".".join(tab[1:]), default=default)
        return item[tab[0]]

    return default


def json_to_one_level(obj, parent=None):
    """
    Take a dict and update all the path to be on one level.
    Arguments:
        output (dict): The dict to proceed.
        parent (unicode): The parent key. Used only with recursion.
    Return:
        dict: The updated obj.
    """

    output = {}
    for key, value in obj.items():
        if isinstance(value, dict):
            if parent is None:
                output.update(json_to_one_level(value, key))
            else:
                output.update(json_to_one_level(value, u".".join([parent, key])))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                item = {
                    UNICODE_TYPE(index): item
                }
                if parent is None:
                    output.update(json_to_one_level(item, u".".join([key])))
                else:
                    output.update(json_to_one_level(item, u".".join([parent, key])))
        else:
            if parent is not None:
                output[u".".join([parent, key])] = value
            else:
                output[key] = value
    return output

