import inspect
import os
import imp
import copy
import fcntl
import re
from storedobject import StoredObject


class Descriptor(StoredObject):
    def __init__(self, object_type=None, guid=None):
        if object_type is None:
            self.initialized = False
        else:
            self.initialized = True

            key = 'ovs_descriptor_%s' % re.sub('[\W_]+', '', str(object_type))
            self._descriptor = StoredObject.volatile.get(key)
            if self._descriptor is None:
                filename = inspect.getfile(object_type).replace('.pyc', '.py')
                self._descriptor = {'name'  : filename.replace(os.path.dirname(filename) + os.path.sep, '').replace('.py', ''),
                                    'source': os.path.relpath(filename, os.path.dirname(__file__)),
                                    'type'  : object_type.__name__}
                StoredObject.volatile.set(key, self._descriptor)
            self._descriptor['guid'] = guid

    def load(self, descriptor):
        self._descriptor = copy.deepcopy(descriptor)
        self.initialized = True
        return self

    @property
    def descriptor(self):
        if self.initialized:
            return copy.deepcopy(self._descriptor)
        else:
            raise RuntimeError('Descriptor not yet initialized')

    def get_object(self, instantiate=False):
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
    @staticmethod
    def get_hybrids():
        path = os.path.join(os.path.dirname(__file__), 'hybrids')
        for filename in os.listdir(path):
            if os.path.isfile(os.path.join(path, filename)) and filename.endswith('.py'):
                name = filename.replace('.py', '')
                module = imp.load_source(name, os.path.join(path, filename))
                for member in inspect.getmembers(module):
                    if inspect.isclass(member[1]) and member[1].__module__ == name:
                        yield member[1]


class Toolbox(StoredObject):
    @staticmethod
    def try_get(key, fallback):
        data = StoredObject.volatile.get(key)
        if data is None:
            try:
                data = StoredObject.persistent.get(key)
            except:
                data = fallback
            StoredObject.volatile.set(key, data)
        return data