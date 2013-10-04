class DataObjectList(object):
    def __init__(self, query_result, cls, readonly=True):
        self._data = query_result  # List of descriptors
        self._guids = [x['guid'] for x in self._data]
        self._readonly = readonly
        self.type = cls
        self._objects = {}

    def _get_object(self, guid):
        if guid not in self._objects:
            self._objects[guid] = self.type(guid)
        return self._objects[guid]

    def append(self, value):
        if self._readonly:
            raise RuntimeError('This method can\'t be executed on readonly lists')
        if not isinstance(value, self.type):
            raise TypeError('Only an object of type %s can be appended' % self.type.__name__)

        #if not value.guid in self.descriptor['guids']:
        #    self.descriptor['guids'].append(value.guid)
        #self._objects[value.guid] = value

    def extend(self, new_list):
        if self._readonly:
            raise RuntimeError('This method can\'t be executed on readonly lists')
        if not isinstance(new_list, DataObjectList) or new_list.type.__name__ != self.type.__name__:
            raise TypeError('Only a list of type DataObjectList<%s> can be added' % self.type.__name__)

        #for guid in new_list.descriptor['guids']:
        #    if guid not in self.descriptor['guids']:
        #        self.descriptor['guids'].append(guid)
        #        self._objects[guid] = new_list._objects[guid]

    def insert(self, index, value):
        if self._readonly:
            raise RuntimeError('This method can\'t be executed on readonly lists')
        if not isinstance(value, self.type):
            raise TypeError('Only an object of type %s can be inserted' % self.type.__name__)

        #if value.guid not in self.descriptor['guids']:
        #    self.descriptor['guids'].insert(index, value.guid)
        #    self._objects[value.guid] = value

    def remove(self, value):
        if self._readonly:
            raise RuntimeError('This method can\'t be executed on readonly lists')

        #if value.guid in self.descriptor['guids']:
        #    self.descriptor['guids'].remove(value.guid)
        #    del self._objects[value.guid]

    def pop(self, **kwargs):
        if self._readonly:
            raise RuntimeError('This method can\'t be executed on readonly lists')

        #guid = self.descriptor['guids'].pop(**kwargs)
        #value = self._get_object(guid)
        #del self._objects[guid]
        #return value

    def index(self, value, start=None, stop=None):
        return self._guids.index(value.guid)

    def count(self, value):
        return self._guids.count(value.guid)

    def sort(self, **kwargs):
        self._guids.sort(**kwargs)

    def reverse(self):
        self._guids.reverse()

    def iterloaded(self):
        for guid in self._guids:
            if guid in self._objects:
                yield self._objects[guid]

    def __iter__(self):
        for guid in self._guids:
            yield self._get_object(guid)

    def __len__(self):
        return len(self._guids)

    def __getitem__(self, item):
        guid = self._guids[item]
        return self._get_object(guid)