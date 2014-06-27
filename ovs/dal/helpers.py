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
Module containing certain helper classes providing various logic
"""
import inspect
import os
import imp
import copy
import re
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.generic.volatilemutex import VolatileMutex

from ovs.log.logHandler import LogHandler
logger = LogHandler('api', name='debug')


class Descriptor(object):
    """
    The descriptor class contains metadata to instanciate objects that can be serialized.
    It points towards the sourcefile, class name and class type
    """

    def __init__(self, object_type=None, guid=None):
        """
        Initializes a descriptor for a given type. Optionally already providing a guid for the
        instanciator
        """

        # Initialize super class
        super(Descriptor, self).__init__()

        if object_type is None:
            self.initialized = False
        else:
            self.initialized = True

            key = 'ovs_descriptor_%s' % re.sub('[\W_]+', '', str(object_type))
            self._volatile = VolatileFactory.get_client()
            self._descriptor = self._volatile.get(key)
            if self._descriptor is None:
                Toolbox.log_cache_hit('descriptor', False)
                filename = inspect.getfile(object_type).replace('.pyc', '.py')
                name = filename.replace(os.path.dirname(filename) + os.path.sep, '').replace('.py', '')
                self._descriptor = {'name': name,
                                    'source': os.path.relpath(filename, os.path.dirname(__file__)),
                                    'type': object_type.__name__}
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
        This method will yield an instance or the class to which the decriptor points
        """
        if not self.initialized:
            raise RuntimeError('Descriptor not yet initialized')

        filename = os.path.join(os.path.dirname(__file__), self._descriptor['source'])
        module = imp.load_source(self._descriptor['name'], filename)
        cls = getattr(module, self._descriptor['type'])
        if instantiate:
            if self._descriptor['guid'] is None:
                return None
            return cls(self._descriptor['guid'])
        else:
            return cls


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
                        current_name = Toolbox.get_class_fullname(current_class)
                        if current_name not in translation_table:
                            translation_table[current_name] = current_class
                        if 'DataObject' in current_class.__base__.__name__:
                            if current_name not in base_hybrids:
                                base_hybrids.append(current_name)
                            else:
                                raise RuntimeError('Duplicate base hybrid found: {0}'.format(current_name))
                        elif 'DataObject' not in current_class.__name__:
                            structure = []
                            this_class = None
                            for this_class in current_class.__mro__:
                                if 'DataObject' in this_class.__name__:
                                    break
                                structure.append(Toolbox.get_class_fullname(this_class))
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
        return {hybrid: translation_table[replacement] if replacement is not None else translation_table[hybrid]
                for hybrid, replacement in hybrids.iteritems()}


class Toolbox(object):
    """
    Generic class for various methods
    """

    @staticmethod
    def get_class_fullname(hybrid_class):
        """
        Returns a full, unique name of a hybrid class
        """
        return '{0}.{1}'.format(hybrid_class.__module__.replace('ovs.dal.hybrids.', ''), hybrid_class.__name__)

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
        key = 'ovs_stats_cache_%s_%s' % (cache_type, 'hit' if hit else 'miss')
        try:
            successfull = volatile.incr(key)
            if not successfull:
                volatile.set(key, 1)
        except:
            pass
