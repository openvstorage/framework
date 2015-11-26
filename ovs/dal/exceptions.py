# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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


class SaveRaceConditionException(Exception):
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
    Raised when ceratin actions are executed on a volatile object (e.g. save)
    """
    pass
