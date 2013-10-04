import json
import inspect
import uuid
import copy
from exceptions import *
from helpers import Descriptor
from storedobject import StoredObject
from relations.relations import Relation, RelationMapper
from dataobjectlist import DataObjectList
from datalist import DataList


class DataObject(StoredObject):
    """
    This base class contains all logic to support our multiple backends and the caching
    * Persistent data
    ** OVS backing data: Arakoon
    ** Read-only reality data: backend (backend libraries contacting e.g. voldrv)
    * Volatile caching: Memcached
    """

    #######################
    ## Attributes
    #######################

    # Properties that needs to be overwritten by implementation
    _blueprint = None            # Blueprint data of the objec type
    _expiry = None               # Timeout of readonly object properties cache

    #######################
    ## Constructor
    #######################

    def __init__(self, guid=None, datastore_wins=False):
        """
        Loads an object with a given guid. If no guid is given, a new object
        is generated with a new guid.
        * guid: The guid indicating which object should be loaded
        * datastoreWins: Optional boolean indicating save conflict resolve management.
        ** True: when saving, external modified fields will not be saved
        ** False: when saving, all changed data will be saved, regardless of external updates
        ** None: in case changed field were also changed externally, an error will be raised
        """

        # Initialize super class
        super(DataObject, self).__init__()

        # Initialize internal fields
        self._datastore_wins = datastore_wins
        self._guid = None             # Guid identifier of the object
        self._original = {}           # Original data copy
        self._metadata = {}           # Some metadata, mainly used for unit testing
        self._data = {}               # Internal data storage
        self._objects = {}            # Internal objects storage

        # Initialize public fields
        self.dirty = False
        self._name = self.__class__.__name__.lower()
        self.namespace = 'ovs_data'   # Namespace of the object

        # Init guid
        new = False
        if guid is None:
            self._guid = str(uuid.uuid4())
            new = True
        else:
            self._guid = str(guid)

        # Build base keys
        self._key = '%s_%s_%s' % (self.namespace, self._name, self._guid)

        # Load data from cache or persistent backend where appropriate
        self._metadata['cache'] = None
        if new:
            self._data = {}
        else:
            self._data = StoredObject.volatile.get(self._key)
            if self._data is None:
                self._metadata['cache'] = False
                self._data = json.loads(StoredObject.persistent.get(self._key))
            else:
                self._metadata['cache'] = True

        # Set default values on new fields
        for key, default in self._blueprint.iteritems():
            if key not in self._data:
                if isinstance(default, Relation):
                    self._data[key] = Descriptor(default.object_type).descriptor
                else:
                    self._data[key] = default

        # Add properties where appropriate, hooking in the correct dictionary
        for attribute, default in self._blueprint.iteritems():
            if attribute not in dir(self):
                if isinstance(default, Relation):
                    self._add_cproperty(attribute, self._data[attribute])
                else:
                    self._add_sproperty(attribute, self._data[attribute])

        # Load foreign keys
        relations = RelationMapper.load_foreign_relations(self.__class__)
        if relations is not None:
            for key, info in relations.iteritems():
                self._objects[key] = info
                self._add_lproperty(key)

        # Store original data
        self._original = copy.deepcopy(self._data)

        # Re-cache the object
        StoredObject.volatile.set(self._key, self._data)

    #######################
    ## Helper methods for dynamic getting and setting
    #######################

    def _add_sproperty(self, attribute, value):
        fget = lambda s: s._get_sproperty(attribute)
        fset = lambda s, v: s._set_sproperty(attribute, v)
        setattr(self.__class__, attribute, property(fget, fset))
        self._data[attribute] = value

    def _add_cproperty(self, attribute, value):
        fget = lambda s: s._get_cproperty(attribute)
        fset = lambda s, v: s._set_cproperty(attribute, v)
        setattr(self.__class__, attribute, property(fget, fset))
        self._data[attribute] = value

    def _add_lproperty(self, attribute):
        fget = lambda s: s._get_lproperty(attribute)
        setattr(self.__class__, attribute, property(fget))

    # Helper method spporting property fetching
    def _get_sproperty(self, attribute):
        return self._data[attribute]

    def _get_cproperty(self, attribute):
        if attribute not in self._objects:
            self._objects[attribute] = Descriptor().load(self._data[attribute]).get_object(instantiate=True)
        return self._objects[attribute]

    def _get_lproperty(self, attribute):
        if not isinstance(self._objects[attribute], DataObjectList):
            remote_class = Descriptor().load(self._objects[attribute]['class']).get_object()
            remote_key   = self._objects[attribute]['key']
            datalist = DataList(key   = '%s_%s' % (self._key, attribute),
                                query = {'object': remote_class,
                                         'data'  : DataList.select.OBJECT,
                                         'query' : {'type': DataList.where_operator.AND,
                                                    'items': [('%s.guid' % remote_key, DataList.operator.EQUALS, self.guid)]}})
            self._objects[attribute] = DataObjectList(datalist.data, remote_class, readonly=True)
        return self._objects[attribute]

    # Helper method supporting property setting
    def _set_sproperty(self, attribute, value):
        self.dirty = True
        self._data[attribute] = value

    def _set_cproperty(self, attribute, value):
        self.dirty = True
        if value is None:
            self._objects[attribute] = None
            self._data[attribute]['guid'] = None
        else:
            descriptor = Descriptor(value.__class__).descriptor
            if descriptor['type'] != self._data[attribute]['type']:
                raise TypeError('An invalid type was given')
            self._objects[attribute] = value
            self._data[attribute]['guid'] = value.guid

    #######################
    ## Saving data to persistent store and invalidating volatile store
    #######################

    def save(self, recursive=False):
        """
        Save the object to the persistent backend and clear cache, making use
        of the specified conflict resolve settings
        """

        if recursive:
            for item in self._objects.values():
                # Only traverse trough loaded sublists
                if isinstance(item, DataObject):
                    item.save(recursive=True)
                elif isinstance(item, DataObjectList):
                    for subitem in item.iterloaded():
                        subitem.save(recursive=True)

        try:
            data = json.loads(StoredObject.persistent.get(self._key))
        except:
            data = {}
        data_conflicts = []
        for attribute in self._data.keys():
            if self._data[attribute] != self._original[attribute]:
                # We changed this value
                if attribute in data and self._original[attribute] != data[attribute]:
                    # Some other process also wrote to the database
                    if self._datastore_wins is None:
                        # In case we didn't set a policy, we raise the conflicts
                        data_conflicts.append(attribute)
                    elif self._datastore_wins is False:
                        # If the datastore should not win, we just overwrite the data
                        data[attribute] = self._data[attribute]
                    # If the datastore should win, we discard/ignore our change
                else:
                    # Normal scenario, saving data
                    data[attribute] = self._data[attribute]
            else:
                data[attribute] = self._data[attribute]
        if data_conflicts:
            raise ConcurrencyException('Got field conflicts while saving %s. Conflicts: %s' % (self._name, ', '.join(data_conflicts)))

        # Save the data
        self._data = copy.deepcopy(data)
        StoredObject.persistent.set(self._key, json.dumps(self._data))
        self._original = copy.deepcopy(self._data)

        # Invalidate the cache
        for key in self._expiry.keys():
            StoredObject.volatile.delete('%s_%s' % (self._key, key))
        StoredObject.volatile.delete(self._key)
        self.dirty = False

    #######################
    ## Other CRUDs
    #######################

    def delete(self):
        """
        Delete the given object
        """

        try:
            StoredObject.persistent.delete(self._key)
        except:
            pass
        for key in self._expiry.keys():
            StoredObject.volatile.delete('%s_%s' % (self._key, key))
        StoredObject.volatile.delete(self._key)

    # Discard all pending changes
    def discard(self):
        """
        Discard all pending changes, reloading the data from the persistent backend
        """

        self.__init__(guid           = self._guid,
                      datastore_wins = self._datastore_wins)

    #######################
    ## The primary key
    #######################

    @property
    def guid(self):
        return self._guid

    #######################
    ## Static helper method
    #######################

    @staticmethod
    def is_dataobject(value):
        return isinstance(value, DataObject)

    @staticmethod
    def fetch_object(object_type, value):
        return object_type(value) if not DataObject.is_dataobject(value) else value

    #######################
    ## Helper method to support 3rd party backend caching
    #######################

    def _backend_property(self, function):
        caller_name = inspect.stack()[1][3]
        cache_key   = '%s_%s' % (self._key, caller_name)
        cached_data = StoredObject.volatile.get(cache_key)
        if cached_data is None:
            cached_data = function()  # Load data from backend
            StoredObject.volatile.set(cache_key, cached_data, self._expiry[caller_name])
        return cached_data
