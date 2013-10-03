from helpers import Reflector


class DataList(object):
    def __init__(self, objecttype=None):
        self._initialized = False
        if objecttype is not None:
            self.type = objecttype
            self.descriptor = Reflector.get_object_descriptor(objecttype(),
                                                              include_guid=False)
            self.descriptor['guids'] = []
            self._objects = {}
            self._initialized = True

    def _get_object(self, guid):
        if guid not in self._objects:
            self._objects[guid] = self.type(guid)
        return self._objects[guid]

    def initialze(self, descriptor):
        self.descriptor = descriptor
        self.type = Reflector.load_object_from_descriptor(descriptor,
                                                          instantiate=False)
        self._objects = {}
        self._initialized = True

    def append(self, value):
        if not self._initialized:
            raise RuntimeError('DataList not yet initialized')
        if not isinstance(value, self.type):
            raise TypeError('Only an object of type %s can be appended' % self.type.__name__)

        if not value.guid in self.descriptor['guids']:
            self.descriptor['guids'].append(value.guid)
        self._objects[value.guid] = value

    def extend(self, new_list):
        if not self._initialized:
            raise RuntimeError('DataList not yet initialized')
        if not isinstance(new_list, DataList) or new_list.type.__name__ != self.type.__name__:
            raise TypeError('Only a list of type DataList<%s> can be added' % self.type.__name__)

        for guid in new_list.descriptor['guids']:
            if guid not in self.descriptor['guids']:
                self.descriptor['guids'].append(guid)
                self._objects[guid] = new_list._objects[guid]

    def insert(self, index, value):
        if not self._initialized:
            raise RuntimeError('DataList not yet initialized')
        if not isinstance(value, self.type):
            raise TypeError('Only an object of type %s can be inserted' % self.type.__name__)

        if value.guid not in self.descriptor['guids']:
            self.descriptor['guids'].insert(index, value.guid)
            self._objects[value.guid] = value

    def remove(self, value):
        if not self._initialized:
            raise RuntimeError('DataList not yet initialized')

        if value.guid in self.descriptor['guids']:
            self.descriptor['guids'].remove(value.guid)
            del self._objects[value.guid]

    def pop(self, **kwargs):
        if not self._initialized:
            raise RuntimeError('DataList not yet initialized')

        guid = self.descriptor['guids'].pop(**kwargs)
        value = self._get_object(guid)
        del self._objects[guid]
        return value

    def index(self, value, start=None, stop=None):
        if not self._initialized:
            raise RuntimeError('DataList not yet initialized')

        return self.descriptor['guids'].index(value.guid)

    def count(self, value):
        if not self._initialized:
            raise RuntimeError('DataList not yet initialized')

        return self.descriptor['guids'].count(value.guid)

    def sort(self, **kwargs):
        if not self._initialized:
            raise RuntimeError('DataList not yet initialized')

        self.descriptor['guids'].sort(**kwargs)

    def reverse(self):
        if not self._initialized:
            raise RuntimeError('DataList not yet initialized')

        self.descriptor['guids'].reverse()

    def __iter__(self):
        if not self._initialized:
            raise RuntimeError('DataList not yet initialized')

        for guid in self.descriptor['guids']:
            yield self._get_object(guid)

    def __len__(self):
        return len(self.descriptor['guids'])

    def __getitem__(self, item):
        guid = self.descriptor['guids'][item]
        return self._get_object(guid)