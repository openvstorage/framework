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
DataObjectList module
"""
from ovs.dal.exceptions import ObjectNotFoundException


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
        self._guids = query_result
        self.type = cls
        self._objects = {}
        self._reduced = reduced
        self._query_result = query_result

    @property
    def reduced(self):
        if not self._reduced:
            dataobjectlist = DataObjectList(self._query_result, self.type, reduced=True)
            dataobjectlist._guids = self._guids  # Keep sorting
            return dataobjectlist

    def merge(self, query_result):
        """
        This method merges in a new query result, preservice objects that might already
        be cached. It also maintains previous sorting, appending new items to the end of the list
        """
        # Maintaining order is very important here
        old_guids = self._guids[:]
        new_guids = query_result
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

    def _get_object(self, requested_guid):
        """
        Yields an instance with a given guid, or a fake class with only a guid property in case
        of a reduced list
        """
        def load_and_cache(guid):
            """
            Loads and caches the object
            """
            if self._reduced:
                self._objects[guid] = type(self.type.__name__, (), {})()
                setattr(self._objects[guid], 'guid', guid)
            else:
                self._objects[guid] = self.type(guid)

        if requested_guid not in self._objects:
            load_and_cache(requested_guid)
            return self._objects[requested_guid]
        requested_object = self._objects[requested_guid]
        if requested_object.updated_on_datastore():
            load_and_cache(requested_guid)
            return self._objects[requested_guid]
        return requested_object

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
        if len(kwargs) == 0:
            self._guids.sort()
        else:
            self.load()
            objects = [self._objects[guid] for guid in self._guids]
            objects.sort(**kwargs)
            self._guids = [obj.guid for obj in objects]

    def reverse(self):
        """
        Reverses the list
        """
        self._guids.reverse()

    def loadunsafe(self):
        """
        Loads all objects (to use on e.g. sorting)
        """
        for guid in self._guids:
            if guid not in self._objects:
                self._get_object(guid)

    def loadsafe(self):
        """
        Loads all objects (to use on e.g. sorting), but not caring about objects that doesn't exist
        """
        for guid in self._guids:
            if guid not in self._objects:
                try:
                    self._get_object(guid)
                except ObjectNotFoundException:
                    pass

    def load(self):
        """
        Loads all objects
        """
        return self.loadsafe()

    def __add__(self, other):
        if not isinstance(other, DataObjectList):
            raise TypeError('Both operands should be of type DataObjectList')
        new_dol = DataObjectList(self._query_result, self.type)
        new_dol.merge(other._query_result)
        return new_dol

    def __radd__(self, other):
        # This will typically called when "other" is no DataObjectList.
        if other is None:
            return self
        elif isinstance(other, list) and other == []:
            return self
        elif not isinstance(other, DataObjectList):
            raise TypeError('Both operands should be of type DataObjectList')
        new_dol = DataObjectList(self._query_result, self.type)
        new_dol.merge(other._query_result)
        return new_dol

    def iterloaded(self):
        """
        Allows to iterate only over the objects that are already loaded
        preventing unnessesary object loading
        """
        for guid in self._guids:
            if guid in self._objects:
                yield self._objects[guid]

    def iterunsafe(self):
        """
        Yields object instances
        """
        for guid in self._guids:
            yield self._get_object(guid)

    def itersafe(self):
        """
        Yields object instances, but not caring about objects that doesn't exist
        """
        for guid in self._guids:
            try:
                yield self._get_object(guid)
            except ObjectNotFoundException:
                pass

    def __iter__(self):
        """
        Yields object instances
        """
        return self.itersafe()

    def __len__(self):
        """
        Returns the length of the list
        """
        return len(self._guids)

    def __getitem__(self, item):
        """
        Provide indexer behavior to the list
        """
        if isinstance(item, slice):
            guids = self._guids[item.start:item.stop]
            result = [qr_item for qr_item in self._query_result if qr_item in guids]
            data_object_list = DataObjectList(result, self.type)
            # Overwrite some internal fields, making sure we keep already fetched objects
            # and we preseve existing sorting
            data_object_list._objects = dict(item for item in self._objects.iteritems() if item[0] in guids)
            data_object_list._guids = guids
            return data_object_list
        else:
            guid = self._guids[item]
            return self._get_object(guid)
