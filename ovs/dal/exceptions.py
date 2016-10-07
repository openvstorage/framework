# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

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


class LinkedObjectException(Exception):
    """
    Raised when there are linked objects when an object is deleted
    """
    pass


class MissingMandatoryFieldsException(Exception):
    """
    Raised when there are mandatory fields missing
    """
    pass


class RaceConditionException(Exception):
    """
    Raised when an object could not be saved in X attempts
    """
    pass


class InvalidRelationException(Exception):
    """
    Raised when a modeled relation is not confirm the relation's design
    """
    pass


class VolatileObjectException(Exception):
    """
    Raised when certain actions are executed on a volatile object (e.g. save)
    """
    pass

class UniqueContraintViolationException(Exception):
    """
    Raised when a unique constraint is violated
    """
    pass
