"""
Storage exceptions module
"""


class KeyNotFoundException(Exception):
    """
    Raised when a given key could not be found in the persistent storage
    """
    pass
