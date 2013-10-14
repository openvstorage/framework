class DataObjectList(object):
    def __init__(self, query_result, cls):
        self._guids = [x['guid'] for x in query_result]
        self.type = cls
        self._objects = {}

    def merge(self, query_result):
        # Maintaining order is very important here
        old_guids = self._guids[:]
        new_guids = [x['guid'] for x in query_result]
        self._guids = []
        for guid in old_guids:
            if guid in new_guids:
                self._guids.append(guid)
        for guid in new_guids:
            if guid not in self._guids:
                self._guids.append(guid)
        # Cleaning out old cached objects
        for guid in self._objects.keys():
            if guid not in self._guids:
                del self._objects[guid]

    def _get_object(self, guid):
        if guid not in self._objects:
            self._objects[guid] = self.type(guid)
        return self._objects[guid]

    def index(self, value):
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