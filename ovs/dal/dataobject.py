import inspect
import uuid
import copy
from exceptions import *
from helpers import Descriptor, Toolbox
from storedobject import StoredObject
from relations.relations import RelationMapper
from dataobjectlist import DataObjectList
from datalist import DataList
from ovs.extensions.generic.volatilemutex import VolatileMutex


class DataObject(StoredObject):
    """
    This base class contains all logic to support our multiple backends and the caching
    * Storage backends:
      * Persistent backend for persistent storage (key-value store)
      * Volatile backend for volatile but fast storage (key-value store)
      * Storage backends are abstracted and injected into this class, making it possible to use fake backends
    * Features:
      * Hybrid property access:
        * Persistent backend
        * 3rd party component for "live" properties
      * Individual cache settings for "live" properties
      * 1-n relations with automatic property propagation
      * Recursive save
    """
    # @TODO: When deleting an object that has children, those children will still refer to a non-existing fetch_object, possibly raising ObjectNotFoundExceptions
    # @TODO: Currently, there is a limit to the amount of objects per type that is situated around 10k objects. Mostly related to memcache object size
    # @TODO: Currently, self-pointing relations are not yet possible

    #######################
    ## Attributes
    #######################

    # Properties that needs to be overwritten by implementation
    _blueprint = None            # Blueprint data of the objec type
    _expiry = None               # Timeout of readonly object properties cache
    _relations = None            # Blueprint for relations

    #######################
    ## Constructor
    #######################

    def __init__(self, guid=None, data=None, datastore_wins=False):
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
        self._frozen = False
        self._datastore_wins = datastore_wins
        self._guid = None             # Guid identifier of the object
        self._original = {}           # Original data copy
        self._metadata = {}           # Some metadata, mainly used for unit testing
        self._data = {}               # Internal data storage
        self._objects = {}            # Internal objects storage

        # Initialize public fields
        self.dirty = False

        # Worker fields/objects
        self._name = self.__class__.__name__.lower()
        self._namespace = 'ovs_data'   # Namespace of the object
        self._mutex = VolatileMutex('primarykeys_%s' % self._name)

        # Init guid
        new = False
        if guid is None:
            self._guid = str(uuid.uuid4())
            new = True
        else:
            self._guid = str(guid)

        # Build base keys
        self._key = '%s_%s_%s' % (self._namespace, self._name, self._guid)

        # Load data from cache or persistent backend where appropriate
        self._metadata['cache'] = None
        if new:
            self._data = {}
        else:
            try:
                self._data = StoredObject.volatile.get(self._key)
            except:
                self._data = None
            if self._data is None:
                self._metadata['cache'] = False
                try:
                    self._data = StoredObject.persistent.get(self._key)
                except:
                    raise ObjectNotFoundException()
            else:
                self._metadata['cache'] = True

        # Set default values on new fields
        for key, default in self._blueprint.iteritems():
            if key not in self._data:
                self._data[key] = default

        # Add properties where appropriate, hooking in the correct dictionary
        for attribute, default in self._blueprint.iteritems():
            if attribute not in dir(self):
                self._add_sproperty(attribute, self._data[attribute])

        # Load relations
        for attribute, relation in self._relations.iteritems():
            if attribute not in self._data:
                self._data[attribute] = Descriptor(relation[0]).descriptor
            if attribute not in dir(self):
                self._add_cproperty(attribute, self._data[attribute])

        # Load foreign keys
        relations = RelationMapper.load_foreign_relations(self.__class__)
        if relations is not None:
            for key, info in relations.iteritems():
                self._objects[key] = {'info': info,
                                      'data': None}
                self._add_lproperty(key)

        # Store original data
        self._original = copy.deepcopy(self._data)

        if not new:
            # Re-cache the object
            StoredObject.volatile.set(self._key, self._data)

        # Freeze property creation
        self._frozen = True

        # Optionally, initialize some fields
        if data is not None:
            for field, value in data.iteritems():
                setattr(self, field, value)

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
        info = self._objects[attribute]['info']
        remote_class = Descriptor().load(info['class']).get_object()
        remote_key   = info['key']
        datalist = DataList(key   = '%s_%s_%s' % (self._name, self._guid, attribute),
                            query = {'object': remote_class,
                                     'data': DataList.select.DESCRIPTOR,
                                     'query': {'type': DataList.where_operator.AND,
                                               'items': [('%s.guid' % remote_key, DataList.operator.EQUALS, self.guid)]}})

        if self._objects[attribute]['data'] is None:
            self._objects[attribute]['data'] = DataObjectList(datalist.data, remote_class)
        else:
            self._objects[attribute]['data'].merge(datalist.data)
        return self._objects[attribute]['data']

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

    def __setattr__(self, key, value):
        if not hasattr(self, '_frozen') or not self._frozen:
            allowed = True
        else:
            # If our object structure is frozen (which is after __init__), we only allow known
            # property updates: items that are in __dict__ and our own blueprinting dicts
            allowed = key in self.__dict__ \
                or key in self._blueprint \
                or key in self._relations \
                or key in self._expiry
        if allowed:
            super(DataObject, self).__setattr__(key, value)
        else:
            raise RuntimeError('Property %s does not exist on this object.' % key)

    #######################
    ## Saving data to persistent store and invalidating volatile store
    #######################

    def save(self, recursive=False, skip=None):
        """
        Save the object to the persistent backend and clear cache, making use
        of the specified conflict resolve settings
        """

        if recursive:
            # Save objects that point to us (e.g. disk.machine - if this is disk)
            for attribute, default in self._relations.iteritems():
                if attribute != skip:  # disks will be skipped
                    item = getattr(self, attribute)
                    if item is not None:
                        item.save(recursive=True, skip=default[1])

            # Save object we point at (e.g. machine.disks - if this is machine)
            relations = RelationMapper.load_foreign_relations(self.__class__)
            if relations is not None:
                for key, info in relations.iteritems():
                    if key != skip:  # machine will be skipped
                        for item in getattr(self, key).iterloaded():
                            item.save(recursive=True, skip=info['key'])

        new = False
        try:
            data = StoredObject.persistent.get(self._key)
        except:
            new = True
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
        StoredObject.persistent.set(self._key, self._data)
        self._add_pk(self._key)

        # Invalidate lists/queries
        # First, invalidate reverse lists (where we point to a remote object, invalidating that remote list)
        for key, default in self._relations.iteritems():
            if self._original[key]['guid'] != self._data[key]['guid']:
                # The field points to another object
                StoredObject.volatile.delete('%s_%s_%s_%s' % (DataList.namespace,
                                                              default[0].__name__.lower(),
                                                              self._original[key]['guid'],
                                                              default[1]))
                StoredObject.volatile.delete('%s_%s_%s_%s' % (DataList.namespace,
                                                              default[0].__name__.lower(),
                                                              self._data[key]['guid'],
                                                              default[1]))
        # Second, invalidate property lists
        cache_list = Toolbox.try_get('%s_%s' % (DataList.cachelink, self._name), {})
        for field in cache_list.keys():
            clear = False
            if field == '__all' and new:  # This is a no-filter query hook, which can be ignored here
                clear = True
            if field in self._blueprint:
                if self._original[field] != self._data[field]:
                    clear = True
            if field in self._relations:
                if self._original[field]['guid'] != self._data[field]['guid']:
                    clear = True
            if field in self._expiry:
                clear = True
            if clear:
                for list_key in cache_list[field]:
                    StoredObject.volatile.delete(list_key)

        # Invalidate the cache
        for key in self._expiry.keys():
            StoredObject.volatile.delete('%s_%s' % (self._key, key))
        StoredObject.volatile.delete(self._key)

        self._original = copy.deepcopy(self._data)
        self.dirty = False

    #######################
    ## Other CRUDs
    #######################

    def delete(self):
        """
        Delete the given object
        """

        # Invalidate no-filter queries/lists pointing to this object
        cache_list = Toolbox.try_get('%s_%s' % (DataList.cachelink, self._name), {})
        if '__all' in cache_list.keys():
            for list_key in cache_list['__all']:
                StoredObject.volatile.delete(list_key)

        # Delete the object out of the persistent store
        try:
            StoredObject.persistent.delete(self._key)
        except:
            pass

        # Delete the object and its properties out of the volatile store
        for key in self._expiry.keys():
            StoredObject.volatile.delete('%s_%s' % (self._key, key))
        StoredObject.volatile.delete(self._key)
        self._delete_pk(self._key)

    # Discard all pending changes
    def discard(self):
        """
        Discard all pending changes, reloading the data from the persistent backend
        """

        self.__init__(guid           = self._guid,
                      datastore_wins = self._datastore_wins)

    def serialize(self, depth=0):
        data = {'guid': self.guid}
        for key, relation in self._relations.iteritems():
            if depth == 0:
                data['%s_guid' % key] = self._data[key]['guid']
            else:
                instance = getattr(self, key)
                if instance is not None:
                    data[key] = getattr(self, key).serialize(depth=(depth - 1))
                else:
                    data[key] = None
        for key, default in self._blueprint.iteritems():
            data[key] = self._data[key]
        for key in self._expiry.keys():
            data[key] = getattr(self, key)
        return data

    #######################
    ## Properties
    #######################

    @property
    def guid(self):
        return self._guid

    #######################
    ## Helper methods
    #######################

    def _backend_property(self, function):
        caller_name = inspect.stack()[1][3]
        cache_key   = '%s_%s' % (self._key, caller_name)
        cached_data = StoredObject.volatile.get(cache_key)
        if cached_data is None:
            cached_data = function()  # Load data from backend
            StoredObject.volatile.set(cache_key, cached_data, self._expiry[caller_name])
        return cached_data

    def _add_pk(self, key):
        internal_key = 'ovs_primarykeys_%s' % self._name
        try:
            self._mutex.acquire()
            keys = StoredObject.volatile.get(internal_key)
            if keys is None:
                keys = set(StoredObject.persistent.prefix('%s_%s_' % (self._namespace, self._name)))
            else:
                keys.add(key)
            StoredObject.volatile.set(internal_key, keys)
        finally:
            self._mutex.release()

    def _delete_pk(self, key):
        internal_key = 'ovs_primarykeys_%s' % self._name
        try:
            self._mutex.acquire()
            keys = StoredObject.volatile.get(internal_key)
            if keys is None:
                keys = set(StoredObject.persistent.prefix('%s_%s_' % (self._namespace, self._name)))
            else:
                try:
                    keys.remove(key)
                except KeyError:
                    pass
            StoredObject.volatile.set(internal_key, keys)
        finally:
            self._mutex.release()

    def __str__(self):
        return str(self.serialize())