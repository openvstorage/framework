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
                filename = inspect.getfile(object_type).replace('.pyc', '.py')
                name = filename.replace(os.path.dirname(filename) + os.path.sep, '') \
                    .replace('.py', '')
                self._descriptor = {'name': name,
                                    'source': os.path.relpath(filename, os.path.dirname(__file__)),
                                    'type': object_type.__name__}
                self._volatile.set(key, self._descriptor)
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
        path = os.path.join(os.path.dirname(__file__), 'hybrids')
        for filename in os.listdir(path):
            if os.path.isfile(os.path.join(path, filename)) and filename.endswith('.py'):
                name = filename.replace('.py', '')
                module = imp.load_source(name, os.path.join(path, filename))
                for member in inspect.getmembers(module):
                    if inspect.isclass(member[1]) and member[1].__module__ == name:
                        yield member[1]


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
        persistent = PersistentFactory.get_client()
        data = volatile.get(key)
        if data is None:
            try:
                data = persistent.get(key)
            except:
                data = fallback
            volatile.set(key, data)
        return data
