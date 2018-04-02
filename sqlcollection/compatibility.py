# coding utf-8
"""
Variables / functions to ensure python 2 / 3 compatibility.
"""
import sys

UNICODE_TYPE = unicode if sys.version_info[0] < 3 else str
BYTE_TYPE = str if sys.version_info[0] < 3 else bytes

INT_TYPES = [int]
if sys.version_info[0] < 3:
    INT_TYPES.append(long)
