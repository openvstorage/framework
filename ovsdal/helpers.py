import inspect
import os
import imp


class Reflector(object):
    @staticmethod
    def get_object_descriptor(value, include_guid=True):
        filename = inspect.getfile(value.__class__).replace('.pyc', '.py')
        name = filename.replace(os.path.dirname(filename) + os.path.sep, '').replace('.py', '')
        source = os.path.relpath(filename, os.path.dirname(__file__))
        descriptor = {'name': name,
                      'source': source,
                      'type': value.__class__.__name__}
        if include_guid:
            descriptor['guid'] = value.guid
        return descriptor

    @staticmethod
    def load_object_from_descriptor(descriptor, instantiate=False):
        filename = os.path.join(os.path.dirname(__file__), descriptor['source'])
        module = imp.load_source(descriptor['name'], filename)
        cls = getattr(module, descriptor['type'])
        if instantiate:
            return cls(descriptor['guid'])
        else:
            return cls
