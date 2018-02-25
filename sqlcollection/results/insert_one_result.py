# coding utf-8
"""
Contains InsertResultOne Class.
"""


class InsertResultOne(object):
    """
    The return type for insert_one().
    """
    def __init__(self, inserted_id):
        """
        The constructor.
        Args:
            inserted_id (int): The inserted primary key id.
        """
        self.inserted_id = inserted_id
