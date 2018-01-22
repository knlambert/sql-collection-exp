# coding utf-8
"""
Contains DeleteResult Class.
"""


class DeleteResult(object):
    """
    The return type for delete methods.
    """
    def __init__(self, deleted_count):
        self.deleted_count = deleted_count
