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
Module containing various helping structures
"""


class Property(object):
    """
    Property
    """

    def __init__(self, name, property_type, mandatory=True, default=None, unique=False, doc=None):
        """
        Initializes a property
        """
        self.name = name
        self.property_type = property_type
        self.default = default
        self.docstring = doc
        self.mandatory = mandatory
        self.unique = unique


class Relation(object):
    """
    Relation
    """

    def __init__(self, name, foreign_type, foreign_key, mandatory=True, onetoone=False, doc=None):
        """
        Initializes a relation
        """
        self.name = name
        self.foreign_type = foreign_type
        self.foreign_key = foreign_key
        self.mandatory = mandatory
        self.onetoone = onetoone
        self.docstring = doc


class Dynamic(object):
    """
    Dynamic property
    """

    def __init__(self, name, return_type, timeout, locked=False):
        """
        Initializes a dynamic property
        """
        self.name = name
        self.return_type = return_type
        self.timeout = timeout
        self.locked = locked
