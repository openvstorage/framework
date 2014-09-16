# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module containing various helping structures
"""


class Property(object):
    """
    Property
    """

    def __init__(self, name, property_type, mandatory=True, default=None, doc=None):
        """
        Initializes a property
        """
        self.name = name
        self.property_type = property_type
        self.default = default
        self.docstring = doc
        self.mandatory = mandatory


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

    def __init__(self, name, return_type, timeout):
        """
        Initializes a dynamic property
        """
        self.name = name
        self.return_type = return_type
        self.timeout = timeout
