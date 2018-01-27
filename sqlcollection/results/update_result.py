# coding utf-8
"""
Contains UpdateResult Class.
"""


class UpdateResult(object):
    """
    The return type for update method.
    """
    def __init__(self, matched_count):
        self.matched_count = matched_count
