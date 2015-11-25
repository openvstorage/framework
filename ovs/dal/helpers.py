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
Module containing certain helper classes providing various logic
"""

import os
import imp
import copy
import inspect
import hashlib
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('dal', name='helper')


class Descriptor(object):
    """
    The descriptor class contains metadata to instantiate objects that can be serialized.
    It points towards the sourcefile, class name and class type
    """

    object_cache = {}

    def __init__(self, object_type=None, guid=None, cached=True):
        """
        Initializes a descriptor for a given type. Optionally already providing a guid for the instance
        """

        # Initialize super class
        super(Descriptor, self).__init__()

        if object_type is None:
            self.initialized = False
        else:
            self.initialized = True
            self._volatile = VolatileFactory.get_client()

            type_name = object_type.__name__
            module_name = object_type.__module__.split('.')[-1]
            fqm_name = 'ovs.dal.hybrids.{0}'.format(module_name)
            try:
                module = __import__(fqm_name, level=0, fromlist=[type_name])
                _ = getattr(module, type_name)
            except (ImportError, AttributeError):
                logger.info('Received object type {0} is not a hybrid'.format(object_type))
                raise TypeError('Invalid type for Descriptor: {0}'.format(object_type))
            identifier = '{0}_{1}'.format(type_name, hashlib.sha1(fqm_name).hexdigest())
            key = 'ovs_descriptor_{0}'.format(identifier)

            self._descriptor = self._volatile.get(key)
            if self._descriptor is None or cached is False:
                if self._descriptor is None:
                    logger.debug('Object type {0} was translated to {1}.{2}'.format(
                        object_type, fqm_name, type_name
                    ))
                Toolbox.log_cache_hit('descriptor', False)
                self._descriptor = {'fqmn': fqm_name,
                                    'type': type_name,
                                    'identifier': identifier,
                                    'version': 3}
                self._volatile.set(key, self._descriptor)
            else:
                Toolbox.log_cache_hit('descriptor', True)
            self._descriptor['guid'] = guid

    def load(self, descriptor):
        """
        Loads an instance from a descriptor dictionary representation
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
        """
        if not self.initialized:
            raise RuntimeError('Descriptor not yet initialized')

        if self._descriptor['identifier'] not in Descriptor.object_cache:
            type_name = self._descriptor['type']
            module = __import__(self._descriptor['fqmn'], level=0, fromlist=[type_name])
            cls = getattr(module, type_name)
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
    The HybridRunner provides access to generic properties from the hybrid object by means
    of dynamic code reflection
    """

    @staticmethod
    def get_hybrids():
        """
        Yields all hybrid classes
        """
        key = 'ovs_hybrid_structure'
        volatile = VolatileFactory.get_client()
        hybrid_structure = volatile.get(key)
        if hybrid_structure is None:
            Toolbox.log_cache_hit('hybrid_structure', False)
            base_hybrids = []
            inherit_table = {}
            translation_table = {}
            path = os.path.join(os.path.dirname(__file__), 'hybrids')
            for filename in os.listdir(path):
                if os.path.isfile(os.path.join(path, filename)) and filename.endswith('.py'):
                    name = filename.replace('.py', '')
                    module = imp.load_source(name, os.path.join(path, filename))
                    for member in inspect.getmembers(module):
                        if inspect.isclass(member[1]) \
                                and member[1].__module__ == name:
                            current_class = member[1]
                            try:
                                current_descriptor = Descriptor(current_class).descriptor
                            except TypeError:
                                continue
                            current_identifier = current_descriptor['identifier']
                            if current_identifier not in translation_table:
                                translation_table[current_identifier] = current_descriptor
                            if 'DataObject' in current_class.__base__.__name__:
                                if current_identifier not in base_hybrids:
                                    base_hybrids.append(current_identifier)
                                else:
                                    raise RuntimeError('Duplicate base hybrid found: {0}'.format(current_identifier))
                            elif 'DataObject' not in current_class.__name__:
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
                for hybrid, replacement in inherit_table.iteritems():
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
            volatile.set(key, hybrid_structure)
        else:
            Toolbox.log_cache_hit('hybrid_structure', True)
        return hybrid_structure


class Toolbox(object):
    """
    Generic class for various methods
    """

    @staticmethod
    def try_get(key, fallback):
        """
        Returns a value linked to a certain key from the volatile store.
        If not found in the volatile store, it will try fetch it from the persistent
        store. If not found, it returns the fallback
        """
        volatile = VolatileFactory.get_client()
        data = volatile.get(key)
        if data is None:
            try:
                persistent = PersistentFactory.get_client()
                data = persistent.get(key)
            except:
                data = fallback
            volatile.set(key, data)
        return data

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
            correct = isinstance(value, basestring)
            allowed_types = ['str', 'unicode', 'basestring']
        elif required_type is float:
            correct = isinstance(value, float) or isinstance(value, int)
            allowed_types = ['float', 'int']
        elif required_type is int:
            correct = isinstance(value, int) or isinstance(value, long)
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
            correct = isinstance(value, required_type)
            allowed_types = [required_type.__name__]

        return correct, allowed_types, given_type

    @staticmethod
    def log_cache_hit(cache_type, hit):
        """
        Registers a cache hit or miss with a specific type
        """
        volatile = VolatileFactory.get_client()
        key = 'ovs_stats_cache_{0}_{1}'.format(cache_type, 'hit' if hit else 'miss')
        try:
            successfull = volatile.incr(key)
            if not successfull:
                volatile.set(key, 1)
        except:
            pass


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

        def execute(function, start, end):
            """
            Executes a single migration, syncing versions
            """
            version = function(start)
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
        path = os.path.join(os.path.dirname(__file__), 'migration')
        for filename in os.listdir(path):
            if os.path.isfile(os.path.join(path, filename)) and filename.endswith('.py'):
                name = filename.replace('.py', '')
                module = imp.load_source(name, os.path.join(path, filename))
                for member in inspect.getmembers(module):
                    if inspect.isclass(member[1]) \
                            and member[1].__module__ == name \
                            and 'object' in [base.__name__ for base in member[1].__bases__]:
                        migrators.append((member[1].identifier, member[1].migrate))
        for identifier, method in migrators:
            base_version = data[identifier] if identifier in data else 0
            new_version = execute(method, base_version, 0)
            data[identifier] = new_version

        persistent.set(key, data)
