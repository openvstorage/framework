# Copyright (C) 2019 iNuron NV
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
Module containing all possible attributes
These attributes are descriptors
"""
import time
import inspect
from typing import Dict, Union, Type
from ..helpers import DalToolbox
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.storage.volatilefactory import VolatileFactory
# Typing import
# noinspection PyUnreachableCode
if False:
    from .dataobject import DataObject


# @todo replace value as it is class bound!

class BaseProperty(object):
    """
    Basic property descriptor
    Does not allow deleting the property as it does not implement the __del__
    """

    def __init__(self, name, value=None):
        self.name = name
        self.value = value

    def __get__(self, instance, owner):
        if instance is None:  # Accessed as class attribute
            return self
        return self.value

    def __set__(self, instance, value):
        if instance is None:
            return self
        self.value = value


class Property(BaseProperty):
    """
    Property
    """

    def __init__(self, name, property_type, mandatory=True, default=None, unique=False, indexed=False, doc=None, value=None):
        """
        Initializes a property
        """
        super(Property, self).__init__(name, value)

        self.property_type = property_type
        self.default = default
        self.docstring = doc
        self.mandatory = mandatory
        self.unique = unique
        self.indexed = indexed

        if self.value is None:
            self.value = default

    def __set__(self, instance, value):
        correct, allowed_types, given_type = DalToolbox.check_type(value, self.property_type)
        if not correct:
            raise TypeError('Property {0} allows types {1}. {2} given'.format(self.name, str(allowed_types), given_type))
        return super(Property, self).__set__(instance, value)


class Relation(BaseProperty):
    """
    Relation
    """

    def __init__(self, name, foreign_type, foreign_key, mandatory=True, onetoone=False, doc=None):
        """
        Initializes a relation
        """
        super(Relation, self).__init__(name)

        self.foreign_type = foreign_type
        self.foreign_key = foreign_key
        self.mandatory = mandatory
        self.onetoone = onetoone
        self.docstring = doc


class Dynamic(BaseProperty):
    """
    Dynamic property
    """

    def __init__(self, name, return_type, timeout, locked=False):
        """
        Initializes a dynamic property
        """
        super(Dynamic, self).__init__(name)

        self.return_type = return_type
        self.timeout = timeout
        self.locked = locked
        self.timing = -1

    def __get__(self, instance, owner):
        # type: (DataObject, any) -> any
        """
        Return the value of the dynamic.
        Note: the value of the cached data changed from {'data': <value_to_cache>} to <value_to_cache> from 2.13.5
        The change was made when 2.13.4 was still the latest master release
        Backwards compatibility support is not necessary. Memcached is restarted when updating which clears the cache completely
        """
        if instance is None:  # Accessed as class attribute
            return self
        volatile = VolatileFactory.get_client()
        cache_key = '{0}_{1}'.format(instance._key, self.name)
        mutex = volatile_mutex(cache_key)
        associated_dynamic_function = getattr(instance, '_{0}'.format(self.name))
        try:
            dynamic_data = volatile.get(cache_key)
            if dynamic_data is None:
                if self.locked:
                    mutex.acquire()
                    # Might have reloaded after the lock expires on a different node
                    dynamic_data = volatile.get(cache_key)
                if dynamic_data is None:
                    function_info = inspect.getargspec(associated_dynamic_function)
                    start = time.time()
                    if 'dynamic' in function_info.args:
                        dynamic_data = associated_dynamic_function(dynamic=self)  # Load data from backend
                    else:
                        dynamic_data = associated_dynamic_function()
                    instance._dynamic_timings[self.name] = time.time() - start
                    correct, allowed_types, given_type = DalToolbox.check_type(dynamic_data, self.return_type)
                    if not correct:
                        raise TypeError('Dynamic property {0} allows types {1}. {2} given'.format(self.name, str(allowed_types), given_type))
                    # Set the result of the function into a dict to avoid None retrieved from the cache when key is not found
                    if self.timeout > 0:
                        volatile.set(cache_key, dynamic_data, self.timeout)

            return DalToolbox.convert_unicode_to_string(dynamic_data)
        finally:
            mutex.release()

    def __set__(self, instance, value):
        raise AttributeError("can't set attribute")


def get_attributes_of_class(given_class):
    # type: (Type[object]) -> Dict[str, Union[BaseProperty, Property, Dynamic, Relation]]
    """
    Returns all custom attributes on the object
    :return: Dict with property name as key, descriptor instance as value
    :rtype: Dict[str, Union[BaseProperty, Property, Dynamic, Relation]]
    """
    # The descriptors are defined on class level
    return inspect.getmembers(given_class, predicate=lambda o: inspect.isdatadescriptor(o) and isinstance(o, BaseProperty))


def get_attributes_of_object(given_object):
    # type: (object) -> Dict[str, Union[BaseProperty, Property, Dynamic, Relation]]
    """
    Returns all custom attributes on the object
    :return: Dict with property name as key, descriptor instance as value
    :rtype: Dict[str, Union[BaseProperty, Property, Dynamic, Relation]]
    """
    # The descriptors are defined on class level
    return get_attributes_of_class(given_object.__class__)
