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
Module containing certain helper classes providing various logic
"""

import os
import re
import imp
import copy
import time
import inspect
import hashlib
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.log.log_handler import LogHandler


class Descriptor(object):
    """
    The descriptor class contains metadata to instantiate objects that can be serialized.
    It points towards the sourcefile, class name and class type
    """
    _logger = LogHandler.get('dal', name='helper')
    object_cache = {}
    descriptor_cache = {}

    def __init__(self, object_type=None, guid=None, cached=True):
        """
        Initializes a descriptor for a given type. Optionally already providing a guid for the instance
        :param object_type: type of the object eg. VDisk
        :param guid: guid of object
        :param cached: cache the identifiers
        """

        # Initialize super class
        super(Descriptor, self).__init__()

        if object_type is None:
            self.initialized = False
        else:
            self.initialized = True

            type_name = object_type.__name__
            module_name = object_type.__module__.split('.')[-1]
            fqm_name = 'ovs.dal.hybrids.{0}'.format(module_name)  # Fully qualified module name
            identifier = '{0}_{1}'.format(type_name, hashlib.sha1(fqm_name).hexdigest())
            if object_type in Descriptor.descriptor_cache and cached is True:
                self._descriptor = Descriptor.descriptor_cache[identifier]
            else:
                try:
                    mod = __import__(fqm_name, level=0, fromlist=[type_name])
                    _ = getattr(mod, type_name)
                except (ImportError, AttributeError):
                    Descriptor._logger.info('Received object type {0} is not a hybrid'.format(object_type))
                    raise TypeError('Invalid type for Descriptor: {0}'.format(object_type))
                self._descriptor = {'fqmn': fqm_name,
                                    'type': type_name,
                                    'identifier': identifier,
                                    'version': 3}  # Versioning :D
                Descriptor.descriptor_cache[identifier] = self._descriptor
            self._descriptor['guid'] = guid

    def load(self, descriptor):
        """
        Loads an instance from a descriptor dictionary representation
        :param descriptor: descriptor dict
        """
        self._descriptor = copy.deepcopy(descriptor)
        self.initialized = True
        return self

    @property
    def descriptor(self):
        """
        Returns a dictionary representation of the descriptor class
        """
        if self.initialized:
            return copy.deepcopy(self._descriptor)
        else:
            raise RuntimeError('Descriptor not yet initialized')

    def get_object(self, instantiate=False):
        """
        This method will yield an instance or the class to which the descriptor points
        :param instantiate: instantiate the class
        """
        if not self.initialized:
            raise RuntimeError('Descriptor not yet initialized')

        if self._descriptor['identifier'] not in Descriptor.object_cache:
            type_name = self._descriptor['type']
            mod = __import__(self._descriptor['fqmn'], level=0, fromlist=[type_name])
            cls = getattr(mod, type_name)
            Descriptor.object_cache[self._descriptor['identifier']] = cls
        else:
            cls = Descriptor.object_cache[self._descriptor['identifier']]
        if instantiate:
            if self._descriptor['guid'] is None:
                return None
            return cls(self._descriptor['guid'])
        else:
            return cls

    @staticmethod
    def isinstance(instance, object_type):
        """"
        Checks (based on descriptors) whether a given instance is of a given type
        """
        try:
            return Descriptor(instance.__class__) == Descriptor(object_type)
        except TypeError:
            return isinstance(instance, object_type)

    def __eq__(self, other):
        """
        Checks the descriptor identifiers
        """
        return self._descriptor['identifier'] == other.descriptor['identifier']

    def __ne__(self, other):
        """
        Checks the descriptor identifiers
        """
        return not self.__eq__(other)


class HybridRunner(object):
    """
    The HybridRunner provides access to generic properties from the hybrid object by means of dynamic code reflection
    """

    cache = {}

    @staticmethod
    def get_hybrids():
        """
        Yields all hybrid classes
        """
        key = 'ovs_hybrid_structure'
        if key in HybridRunner.cache:  # Check local cache
            return HybridRunner.cache[key]
        volatile = VolatileFactory.get_client()
        hybrid_structure = volatile.get(key)  # Check memcache
        if hybrid_structure is not None:
            HybridRunner.cache[key] = hybrid_structure
            return hybrid_structure
        base_hybrids = []
        inherit_table = {}
        translation_table = {}
        path = '/'.join([os.path.dirname(__file__), 'hybrids'])
        for filename in os.listdir(path):  # Query filesystem
            if os.path.isfile('/'.join([path, filename])) and filename.endswith('.py'):
                name = filename.replace('.py', '')
                mod = imp.load_source(name, '/'.join([path, filename]))
                for member in inspect.getmembers(mod):
                    if inspect.isclass(member[1]) and member[1].__module__ == name:
                        current_class = member[1]
                        try:
                            current_descriptor = Descriptor(current_class).descriptor
                        except TypeError:
                            continue
                        current_identifier = current_descriptor['identifier']
                        if current_identifier not in translation_table:
                            translation_table[current_identifier] = current_descriptor
                        if 'DataObject' in current_class.__base__.__name__:  # Further inheritance?
                            if current_identifier not in base_hybrids:
                                base_hybrids.append(current_identifier)
                            else:
                                raise RuntimeError('Duplicate base hybrid found: {0}'.format(current_identifier))
                        elif 'DataObject' not in current_class.__name__:  # Further inheritance than dataobject
                            structure = []
                            this_class = None
                            for this_class in current_class.__mro__:
                                if 'DataObject' in this_class.__name__:
                                    break
                                try:
                                    structure.append(Descriptor(this_class).descriptor['identifier'])
                                except TypeError:
                                    break  # This means we reached one of the built-in classes.
                            if 'DataObject' in this_class.__name__:
                                for index in reversed(range(1, len(structure))):
                                    if structure[index] in inherit_table:
                                        raise RuntimeError('Duplicate hybrid inheritance: {0}({1})'.format(structure[index - 1], structure[index]))
                                    inherit_table[structure[index]] = structure[index - 1]
        items_replaced = True
        hybrids = {hybrid: None for hybrid in base_hybrids[:]}
        while items_replaced is True:
            items_replaced = False
            for hybrid, replacement in inherit_table.iteritems():  # Upgrade the normal hybrid with the extended one
                if hybrid in hybrids.keys() and hybrids[hybrid] is None:
                    hybrids[hybrid] = replacement
                    items_replaced = True
                if hybrid in hybrids.values():
                    for item in hybrids.keys():
                        if hybrids[item] == hybrid:
                            hybrids[item] = replacement
                    items_replaced = True
        hybrid_structure = {hybrid: translation_table[replacement] if replacement is not None else translation_table[hybrid]
                            for hybrid, replacement in hybrids.iteritems()}
        HybridRunner.cache[key] = hybrid_structure
        volatile.set(key, hybrid_structure)
        return hybrid_structure


class DalToolbox(object):
    """
    Generic class for various methods
    """

    @staticmethod
    def check_type(value, required_type):
        """
        Validates whether a certain value is of a given type. Some types are treated as special
        case:
          - A 'str' type accepts 'str', 'unicode' and 'basestring'
          - A 'float' type accepts 'float', 'int'
          - A list instance acts like an enum
        """
        given_type = type(value)
        if required_type is str:
            correct = isinstance(value, basestring) or value is None
            allowed_types = ['str', 'unicode', 'basestring']
        elif required_type is float:
            correct = isinstance(value, float) or isinstance(value, int) or value is None
            allowed_types = ['float', 'int']
        elif required_type is int or required_type is long:
            correct = isinstance(value, int) or isinstance(value, long) or value is None
            allowed_types = ['int', 'long']
        elif isinstance(required_type, list):
            # We're in an enum scenario. Field_type isn't a real type, but a list containing
            # all possible enum values. Here as well, we need to do some str/unicode/basestring
            # checking.
            if isinstance(required_type[0], basestring):
                value = str(value)
            correct = value in required_type
            allowed_types = required_type
            given_type = value
        else:
            correct = isinstance(value, required_type) or value is None
            allowed_types = [required_type.__name__]

        return correct, allowed_types, given_type

    @staticmethod
    def extract_key(obj, field):
        """
        Extracts a sortable tuple from the object using the given keys
        :param obj: The object where the key should be extracted from
        :param field: Field which has to be translated to a key
        """
        regex = re.compile(r'(\d+)')
        value = obj
        for subkey in field.split('.'):
            if '[' in subkey:
                # We're using a dict
                attribute = subkey.split('[')[0]
                dictkey = subkey.split('[')[1][:-1]
                value = getattr(value, attribute)[dictkey]
            else:
                # Normal property
                value = getattr(value, subkey)
            if value is None:
                break
        value = '' if value is None else str(value)
        key = regex.split(value)
        while True:
            try:
                key.remove('')
            except ValueError:
                break
        for index in xrange(len(key)):
            try:
                key[index] = float(key[index])
            except ValueError:
                key[index] = key[index].lower()
        return tuple(key)

    @staticmethod
    def convert_unicode_to_string(original):
        """
        Convert any dict, list or unicode string to regular string
        :param original: Item to do the conversion on
        :type original: dict|list|unicode
        :return: The same item, but all sub-items inside are converted to strings
        :rtype: dict|list|str
        """
        if isinstance(original, dict):
            return {DalToolbox.convert_unicode_to_string(key): DalToolbox.convert_unicode_to_string(value) for key, value in original.iteritems()}
        if isinstance(original, list):
            return [DalToolbox.convert_unicode_to_string(item) for item in original]
        if isinstance(original, unicode):
            return str(original)
        return original


class Migration(object):
    """
    Handles all migrations between versions
    """

    @staticmethod
    def migrate():
        """
        Executes all migrations. It keeps track of an internal "migration version" which is
        a always increasing by one
        """

        def execute(fct, start, end):
            """
            Executes a single migration, syncing versions
            """
            version = fct(start)
            if version > end:
                end = version
            return end

        key = 'ovs_model_version'
        persistent = PersistentFactory.get_client()
        if persistent.exists(key):
            data = persistent.get(key)
        else:
            data = {}

        migrators = []
        path = '/'.join([os.path.dirname(__file__), 'migration'])
        for filename in os.listdir(path):
            if os.path.isfile('/'.join([path, filename])) and filename.endswith('.py'):
                name = filename.replace('.py', '')
                mod = imp.load_source(name, '/'.join([path, filename]))
                for member in inspect.getmembers(mod):
                    if inspect.isclass(member[1]) \
                            and member[1].__module__ == name \
                            and 'object' in [base.__name__ for base in member[1].__bases__]:
                        migrators.append((member[1].identifier, member[1].migrate))
        for identifier, method in migrators:
            base_version = data[identifier] if identifier in data else 0
            new_version = execute(method, base_version, 0)
            data[identifier] = new_version

        persistent.set(key, data)


class timer(object):
    """
    Can be used for timing pieces of code
    """

    def __init__(self, name, ms=False):
        """
        Creates a timer
        :param name: The name of the timer, will be printed
        :type name: basestring
        :param ms: Indicates whether it should be printed in seconds or milliseconds
        :type ms: bool
        """
        self.name = name
        self.ms = ms
        self.start = None

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args, **kwargs):
        _ = args, kwargs
        print '{0}: {1:.3f}{2}'.format(self.name,
                                       (time.time() - self.start) / (1000 if self.ms is True else 1),
                                       'ms' if self.ms is True else 's')
