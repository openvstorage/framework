import inspect
import os
import imp


class Descriptor(object):
    def __init__(self, object_type=None, guid=None):
        if object_type is None:
            self.initialized = False
        else:
            self.initialized = True

            filename = inspect.getfile(object_type).replace('.pyc', '.py')
            self._name   = filename.replace(os.path.dirname(filename) + os.path.sep, '').replace('.py', '')
            self._source = os.path.relpath(filename, os.path.dirname(__file__))
            self._type   = object_type.__name__
            self._guid   = guid

    def load(self, descriptor):
        self._guid = descriptor.get('guid')
        self._name = descriptor['name']
        self._source = descriptor['source']
        self._type = descriptor['type']
        self.initialized = True
        return self

    @property
    def descriptor(self):
        if self.initialized:
            return {'name'  : self._name,
                    'source': self._source,
                    'type'  : self._type,
                    'guid'  : self._guid}
        else:
            raise RuntimeError('Descriptor not yet initialized')

    def get_object(self, instantiate=False):
        if not self.initialized:
            raise RuntimeError('Descriptor not yet initialized')

        filename = os.path.join(os.path.dirname(__file__), self._source)
        module = imp.load_source(self._name, filename)
        cls = getattr(module, self._type)
        if instantiate:
            if self._guid is None:
                return None
            return cls(self._guid)
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