# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
DataObjectList module
"""
import random
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.helpers import Descriptor


class DataObjectList(object):
    """
    The DataObjectList works on the resulting dataset from a DataList query. It uses the
    descriptor metadata to provide a list-alike experience
    """

    global_object_cache = {}

    def __init__(self, query_result, cls, reduced=False):
        """
        Initializes a DataObjectList object, using a query result and a class type
        The reduced flag is both used internally and is used to create a DataObjectList
        which will yield reduced objects (with only their guid) for faster code in case
        not all properties are required
        """
        self._guids = query_result
        self.type = cls
        self._type_name = cls.__name__
        self._objects = {}
        self._reduced = reduced
        self._query_result = query_result
        if self._type_name not in DataObjectList.global_object_cache:
            DataObjectList.global_object_cache[self._type_name] = {}

    @property
    def reduced(self):
        """
        Reduced property
        :return: Data-object list
        """
        if not self._reduced:
            dataobjectlist = DataObjectList(self._query_result, self.type, reduced=True)
            dataobjectlist._guids = self._guids  # Keep sorting
            return dataobjectlist

    def update(self, query_result):
        """
        This method merges in a new query result, pre-service objects that might already
        be cached. It also maintains previous sorting, appending new items to the end of the list
        :param query_result: Query result to merge in
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
            self.reduced.update(query_result)

    def _get_object(self, requested_guid):
        """
        Yields an instance with a given guid, or a fake class with only a guid property in case
        of a reduced list
        """
        def _load_and_cache(guid):
            """
            Loads and caches the object
            """
            if self._reduced:
                self._objects[guid] = type(self._type_name, (), {})()
                setattr(self._objects[guid], 'guid', guid)
            else:
                self._objects[guid] = self.type(guid)
                DataObjectList.global_object_cache[self._type_name][guid] = self._objects[guid]

        if requested_guid in self._objects:
            requested_object = self._objects[requested_guid]
        elif requested_guid in DataObjectList.global_object_cache[self._type_name]:
            requested_object = DataObjectList.global_object_cache[self._type_name][requested_guid]
            self._objects[requested_guid] = requested_object
        else:
            _load_and_cache(requested_guid)
            return self._objects[requested_guid]
        if requested_object.updated_on_datastore():
            _load_and_cache(requested_guid)
            return self._objects[requested_guid]
        return requested_object

    def index(self, value):
        """
        Returns the index of a given value (hybrid)
        :param value: Value to search index of
        """
        return self._guids.index(value.guid)

    def count(self, value):
        """
        Returns the count for a given value (hybrid)
        :param value: Value to count occurrences for
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
        if Descriptor(self.type) != Descriptor(other.type):
            raise TypeError('Both operands should contain the same data')
        new_dol = DataObjectList(self._query_result, self.type)
        guids = self._guids[:]
        for guid in other._guids:
            if guid not in guids:
                guids.append(guid)
        new_dol._guids = guids
        return new_dol

    def __radd__(self, other):
        # This will typically called when "other" is no DataObjectList.
        if other is None:
            return self
        if isinstance(other, list) and other == []:
            return self
        if not isinstance(other, DataObjectList):
            raise TypeError('Both operands should be of type DataObjectList')
        if Descriptor(self.type) != Descriptor(other.type):
            raise TypeError('Both operands should contain the same data')
        new_dol = DataObjectList(self._query_result, self.type)
        guids = self._guids[:]
        for guid in other._guids:
            if guid not in guids:
                guids.append(guid)
        new_dol._guids = guids
        return new_dol

    def iterloaded(self):
        """
        Allows to iterate only over the objects that are already loaded
        preventing unnecessary object loading
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
            # and we preserve existing sorting
            data_object_list._objects = dict(item for item in self._objects.iteritems() if item[0] in guids)
            data_object_list._guids = guids
            return data_object_list
        else:
            guid = self._guids[item]
            return self._get_object(guid)

    def remove(self, item):
        """
        Remove an item from the data-object list
        Item can be a guid of an object or the object itself
        :param item: Guid or object
        :return: Updated list
        """
        guid = None
        if isinstance(item, basestring):
            if item in self._guids:
                guid = item
        else:
            if Descriptor(self.type) != Descriptor(item.__class__):
                raise TypeError('Item should be of type {0}'.format(self.type))
            guid = item.guid
        if guid is None:
            raise ValueError('Item not in list')
        self._guids.remove(guid)
        self._objects = dict(item for item in self._objects.iteritems() if item[0] in self._guids)

    def pop(self, index):
        """
        Pop an item from the data-object list at the specified index
        :param index: Index of item to pop
        :return: Updated list
        """
        if not isinstance(index, int):
            raise ValueError('Index must be an integer')
        self._guids.pop(index)
        self._objects = dict(item for item in self._objects.iteritems() if item[0] in self._guids)

    def shuffle(self):
        """
        Randomly shuffle the items in the data-object list
        :return: Shuffled data-object list
        """
        self.load()
        objects = [self._objects[guid] for guid in self._guids]
        random.shuffle(objects)
        self._guids = [obj.guid for obj in objects]
