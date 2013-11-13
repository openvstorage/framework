"""
Module containing the exceptions used in the DAL
"""


class ConcurrencyException(Exception):
    """
    Exception raised when a concurrency issue is found
    """
    pass


class InvalidStoreFactoryException(Exception):
    """
    Raised when an invalid store was loaded to the StoredObject
    """
    pass


class ObjectNotFoundException(Exception):
    """
    Raised when an object was queries that doesn't exist
    """
    pass