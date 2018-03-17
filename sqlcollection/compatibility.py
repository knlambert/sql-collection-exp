# coding utf-8
"""
Variables / functions to ensure python 2 / 3 compatibility.
"""
import sys

UNICODE_TYPE = unicode if sys.version_info[0] < 3 else str
