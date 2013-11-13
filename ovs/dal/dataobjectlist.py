"""
DataObjectList module
"""


class DataObjectList(object):
    """
    The DataObjectList works on the resulting dataset from a DataList query. It uses the
    descriptor metadata to provide a list-alike experience
    """
    def __init__(self, query_result, cls, reduced=False):
        """
        Initializes a DataObjectList object, using a query result and a class type
        The reduced flag is both used internally and is used to create a DataObjectList
        which will yield reduced objects (with only their guid) for faster code in case
        not all properties are required
        """
        self._guids = [x['guid'] for x in query_result]
        self.type = cls
        self._objects = {}
        self._reduced = reduced
        if not reduced:
            self.reduced = DataObjectList(query_result, cls, reduced=True)

    def merge(self, query_result):
        """
        This method merges in a new query result, preservice objects that might already
        be cached. It also maintains previous sorting, appending new items to the end of the list
        """
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
        if not self._reduced:
            self.reduced.merge(query_result)

    def _get_object(self, guid):
        """
        Yields an instance with a given guid, or a fake class with only a guid property in case
        of a reduced list
        """
        if guid not in self._objects:
            if self._reduced:
                self._objects[guid] = DataObjectList._create_class(self.type.__name__)()
                setattr(self._objects[guid], 'guid', guid)
            else:
                self._objects[guid] = self.type(guid)
        return self._objects[guid]

    def index(self, value):
        """
        Returns the index of a given value (hybrid)
        """
        return self._guids.index(value.guid)

    def count(self, value):
        """
        Returns the count for a given value (hybrid)
        """
        return self._guids.count(value.guid)

    def sort(self, **kwargs):
        """
        Sorts the list with a given set of parameters.
        However, the sorting will be applied to the guids only
        """
        self._guids.sort(**kwargs)

    def reverse(self):
        """
        Reverses the list
        """
        self._guids.reverse()

    def iterloaded(self):
        """
        Allows to iterate only over the objects that are already loaded
        preventing unnessesary object loading
        """
        for guid in self._guids:
            if guid in self._objects:
                yield self._objects[guid]

    def __iter__(self):
        """
        Yields object instances
        """
        for guid in self._guids:
            yield self._get_object(guid)

    def __len__(self):
        """
        Returns the length of the list
        """
        return len(self._guids)

    def __getitem__(self, item):
        """
        Provide indexer behavior to the list
        """
        guid = self._guids[item]
        return self._get_object(guid)

    @staticmethod
    def _create_class(name):
        """
        This method generates a dummy class with the correct naming
        """
        class Dummy():
            """ Dummy class """
            pass
        Dummy.__name__ = name
        return Dummy