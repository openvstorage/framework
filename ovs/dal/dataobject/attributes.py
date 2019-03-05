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
from enum import Enum
from typing import Union, Type, List, Tuple, Optional
from ..helpers import DalToolbox, Descriptor, HybridRunner
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.storage.volatilefactory import VolatileFactory
# Typing import
# noinspection PyUnreachableCode
if False:
    from .dataobject import DataObject


class BaseAttribute(object):
    """
    Basic property descriptor
    Does not allow deleting the property as it does not implement the __del__
    """
    __slots__ = ('name', 'docstring')

    def __init__(self, name=None, doc=None):
        self.name = name
        self.docstring = doc

    def __get__(self, instance, owner):
        raise NotImplementedError('')

    def __set__(self, instance, value):
        raise NotImplementedError('')

    def get_name(self, instance):
        # type: (DataObject) -> str
        """
        Retrieve the attribute name from the instance
        :param instance: Instance that has the descriptor as an attribute
        :return: The name of the attribute
        """
        if not self.name:
            for name, attribute in get_attributes_of_object(instance):
                if attribute == self:
                    self.name = name
            if not self.name:
                raise ValueError('Unable to determine the name of the attribute')
        return self.name


class Property(BaseAttribute):
    """
    Property
    """
    __slots__ = ('property_type', 'default', 'mandatory', 'unique', 'indexed')

    def __init__(self, property_type, mandatory=True, default=None, unique=False, indexed=False, doc=None):
        # type: (Type[type], bool, any, bool, bool, Optional[str]) -> None
        """
        Initializes a property. Requires _data(dict) and dirty(bool) to be declared on the instance
        """
        # @todo support default
        super(Property, self).__init__(doc=doc)

        self.property_type = property_type
        self.default = default
        self.mandatory = mandatory
        self.unique = unique
        self.indexed = indexed

    def __set__(self, instance, value):
        # type: (DataObject, any) -> Union[None, BaseAttribute]
        """
        Set the property value. Requires _data(dict) and dirty(bool) to be declared on the instance
        Returns the Property instance when accessed as class attribute
        """
        instance.dirty = True
        if instance is None:
            return self
        name = self.get_name(instance)
        correct, allowed_types, given_type = DalToolbox.check_type(value, self.property_type)
        if not correct:
            raise TypeError('Property {0} allows types {1}. {2} given'.format(self.name, str(allowed_types), given_type))
        instance._data[name] = value

    def __get__(self, instance, owner):
        # type: (DataObject, Type[DataObject]) -> any
        """
        Retrieve the value of the descriptor
        Returns the Property instance when accessed as class attribute
        """
        if instance is None:  # Accessed as class attribute
            return self
        name = self.get_name(instance)
        return instance._data[name]


class RelationTypes(Enum):
    ONETOMANY = 'onetomany'
    ONETOONE = 'onetoone'
    MANYTOONE = 'manytoone'


# @todo generate descriptors when not present on the class
class Relation(BaseAttribute):
    """
    Relation
    """
    __slots__ = ('foreign_type', 'foreign_key', 'mandatory', 'relation_type')

    def __init__(self, foreign_type, foreign_key, mandatory=True, relation_type=RelationTypes.ONETOMANY, doc=None):
        # type: (DataObject, str, bool, str, Optional[str]) -> None
        """
        Initializes a relation. Requires _objects(dict), _data(dict), _dirty(bool) to be declared on the instance
        """
        super(Relation, self).__init__(doc=doc)

        self.foreign_type = foreign_type
        self.foreign_key = foreign_key
        self.mandatory = mandatory
        self.relation_type = relation_type

        self._build_relation_identifier()

    def __get__(self, instance, owner):
        # type: (DataObject, Type[DataObject]) -> Union[Relation, DataObject]
        """
        Retrieve the object mapped to the relation
        """
        if instance is None:
            return self
        name = self.get_name(instance)
        if name not in instance._objects:
            descriptor = Descriptor().load(instance._data[name])
            instance._objects[name] = descriptor.get_object(instantiate=True)
        return instance._objects[name]

    def __set__(self, instance, value):
        # type: (DataObject, DataObject) -> Union[Relation, None]
        """
        Set the relational object
        """
        if instance is None:
            return self
        instance.dirty = True
        name = self.get_name(instance)
        if value is None:
            instance._objects[name] = None
            instance._data[name]['guid'] = None
        else:
            descriptor = Descriptor(value.__class__).descriptor
            if descriptor['identifier'] != instance._data[name]['identifier']:
                if descriptor['type'] == instance._data[name]['type']:
                    DataObject._logger.error('Corrupt descriptors: {0} vs {1}'.format(descriptor, instance._data[name]))
                raise TypeError('An invalid type was given: {0} instead of {1}'.format(descriptor['type'], instance._data[name]['type']))
            instance._objects[name] = value
            instance._data[name]['guid'] = value.guid

    def _build_relation_identifier(self):
        hybrid_structure = HybridRunner.get_hybrids()
        if self.foreign_type is not None:  # If none -> points to the DataObject
            identifier = Descriptor(self.foreign_type).descriptor['identifier']
            if identifier in hybrid_structure and identifier != hybrid_structure[identifier]['identifier']:
                # Point to relations of the original object when object got extended
                self.foreign_type = Descriptor().load(hybrid_structure[identifier]).get_object()


class RelationGuid(BaseAttribute):
    """
    Relation guid property. Instead of retrieving the complete relational object, only yields the guid
    """
    __slots__ = ('relation',)

    def __init__(self, relation):
        # type: (Relation) -> None
        """
        Initializes the relation guid Requires _data(dict), to be declared on the instance
        :param relation: Relation to generate the guid from
        :type relation: Relation
        """
        super(RelationGuid, self).__init__()
        self.relation = relation

    def __set__(self, instance, value):
        raise AttributeError('Setting a relational guid is prohibited. Use the relational set instead')

    def __get__(self, instance, owner):
        # type: (DataObject, Type[DataObject]) -> Union[List[str], str]
        """
        Retrieve the guid(s) of the mapped relational object(s)
        Multiple guids are given if the relational type is many to one
        :return: The guid(s) of the mapped object(s)
        :rtype: Union[List[str], str]
        """
        if self.relation.relation_type in (RelationTypes.ONETOMANY, RelationTypes.ONETOONE):
            relation_name = self.relation.get_name(instance)
            return instance._data[relation_name]['guid']
        else:
            raise NotImplementedError('Many to one not supported yet')


class Dynamic(BaseAttribute):
    """
    Dynamic property
    """
    __slots__ = ('return_type', 'timeout', 'locked', 'timing')

    def __init__(self, return_type, timeout, locked=False):
        """
        Initializes a dynamic property
        """
        super(Dynamic, self).__init__()

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

        name = self.get_name(instance)

        volatile = VolatileFactory.get_client()
        cache_key = '{0}_{1}'.format(instance._key, name)
        mutex = volatile_mutex(cache_key)
        associated_dynamic_function = getattr(instance, '_{0}'.format(name))
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
                    self.timing = time.time() - start
                    correct, allowed_types, given_type = DalToolbox.check_type(dynamic_data, self.return_type)
                    if not correct:
                        raise TypeError('Dynamic property {0} allows types {1}. {2} given'.format(name, str(allowed_types), given_type))
                    # Set the result of the function into a dict to avoid None retrieved from the cache when key is not found
                    if self.timeout > 0:
                        volatile.set(cache_key, dynamic_data, self.timeout)

            return DalToolbox.convert_unicode_to_string(dynamic_data)
        finally:
            mutex.release()

    def __set__(self, instance, value):
        raise AttributeError("can't set a Dynamic attribute")


def get_attributes_of_class(given_class):
    # type: (Type[object]) -> List[Tuple[str, Union[BaseAttribute, Property, Dynamic, Relation]]]
    """
    Returns all custom attributes on the object
    :return: Dict with property name as key, descriptor instance as value
    :rtype: List[Tuple[str, Union[BaseAttribute, Property, Dynamic, Relation]]
    """
    # The descriptors are defined on class level
    return inspect.getmembers(given_class, predicate=lambda o: inspect.isdatadescriptor(o) and isinstance(o, BaseAttribute))


def get_attributes_of_object(given_object):
    # type: (object) -> List[Tuple[str, Union[BaseAttribute, Property, Dynamic, Relation]]]
    """
    Returns all custom attributes on the object
    :return: Dict with property name as key, descriptor instance as value
    :rtype: List[Tuple[str, Union[BaseAttribute, Property, Dynamic, Relation]]
    """
    # The descriptors are defined on class level
    return get_attributes_of_class(given_object.__class__)
