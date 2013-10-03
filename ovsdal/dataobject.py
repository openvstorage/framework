import json
import inspect
import uuid
import copy
from exceptions import *
from helpers import Reflector
from dataobjectlist import DataObjectList


class DataObject(object):
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
    _objectexpiry = None         # Timeout of main object cache
    _expiry = None               # Timeout of readonly object properties cache

    # Class property
    _store_factory = None        # Store factory

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

        self._datastore_wins = datastore_wins
        self._name = self.__class__.__name__.lower()
        self._name = None             # Name of the object
        self._guid = None             # Guid identifier of the object
        self._namespace = 'ovs_data'  # Namespace of the object
        self._original = {}           # Original data copy
        self._metadata = {}           # Some metadata, mainly used for unit testing
        self._data = {}               # Internal data storage
        self._objects = {}            # Internal objects storage

        self.dirty = False

        # Init guid
        new = False
        if guid is None:
            self._guid = str(uuid.uuid4())
            new = True
        else:
            self._guid = str(guid)

        # Build base keys
        self._key = '%s_%s_%s' % (self._namespace, self._name, self._guid)

        # Load backends
        if DataObject._store_factory is None:
            raise InvalidStoreFactoryException
        try:
            self._persistent = DataObject._store_factory.persistent()
            self._volatile = DataObject._store_factory.volatile()
        except:
            raise InvalidStoreFactoryException

        # Load data from cache or persistent backend where appropriate
        self._metadata['cache'] = None
        if new:
            self._data = {}
        else:
            self._data = self._volatile.get(self._key)
            if self._data is None:
                self._metadata['cache'] = False
                self._data = json.loads(self._persistent.get(self._key))
            else:
                self._metadata['cache'] = True

        # Set default values on new fields
        for key, default in self._blueprint.iteritems():
            if key not in self._data:
                if DataObject.is_dataobject(default):
                    self._data[key] = Reflector.get_object_descriptor(default())
                elif isinstance(default, list) and len(default) == 1 and DataObject.is_dataobject(default[0]):
                    self._data[key] = DataObjectList(default[0]).descriptor
                else:
                    self._data[key] = default

        # Add properties where appropriate, hooking in the correct dictionary
        for attribute, default in self._blueprint.iteritems():
            if attribute not in dir(self):
                if DataObject.is_dataobject(default):
                    self._add_cproperty(attribute, self._data[attribute])
                elif isinstance(default, list) and len(default) == 1 and DataObject.is_dataobject(default[0]):
                    self._add_lproperty(attribute, self._data[attribute])
                else:
                    self._add_sproperty(attribute, self._data[attribute])

        # Store original data
        self._original = copy.deepcopy(self._data)

        # Re-cache the object
        self._volatile.set(self._key, self._data, self._objectexpiry)

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

    def _add_lproperty(self, attribute, value):
        fget = lambda s: s._get_lproperty(attribute)
        fset = lambda s, v: s._set_lproperty(attribute, v)
        setattr(self.__class__, attribute, property(fget, fset))
        self._data[attribute] = value

    # Helper method spporting property fetching
    def _get_sproperty(self, attribute):
        return self._data[attribute]

    def _get_cproperty(self, attribute):
        if attribute not in self._objects:
            self._objects[attribute] = Reflector.load_object_from_descriptor(self._data[attribute],
                                                                             instantiate=True)
        return self._objects[attribute]

    def _get_lproperty(self, attribute):
        if attribute not in self._objects:
            value = DataObjectList()
            value.initialze(self._data[attribute])
            self._objects[attribute] = value
        return self._objects[attribute]

    # Helper method supporting property setting
    def _set_sproperty(self, attribute, value):
        self.dirty = True
        self._data[attribute] = value

    def _set_cproperty(self, attribute, value):
        self.dirty = True
        descriptor = Reflector.get_object_descriptor(value)
        if descriptor['type'] != self._data[attribute]['type']:
            raise TypeError('An invalid type was given')
        self._objects[attribute] = value
        self._data[attribute] = Reflector.get_object_descriptor(value)

    def _set_lproperty(self, attribute, value):
        self.dirty = True
        descriptor = value.descriptor
        if descriptor['type'] != self._data[attribute]['type']:
            raise TypeError('An invalid type was given')
        self._objects[attribute] = value
        self._data[attribute] = value.descriptor

    #######################
    ## Class method for setting the store
    #######################

    @classmethod
    def set_storefactory(cls, factory):
        DataObject._store_factory = factory

    #######################
    ## Static helper method
    #######################

    @staticmethod
    def is_dataobject(value):
        return inspect.isclass(value) and issubclass(value, DataObject)


    #######################
    ## Saving data to persistent store and invalidating volatile store
    #######################

    def save(self, recursive=False):
        """
        Save the object to the persistent backend and clear cache, making use
        of the specified conflict resolve settings
        """

        if recursive:
            for key, value in self._blueprint.iteritems():
                if inspect.isclass(value) and issubclass(value, DataObject):
                    self._objects[key].save(recursive=True)
                elif isinstance(value, list) and len(value) == 1 and \
                        inspect.isclass(value[0]) and issubclass(value[0], DataObject):
                    for item in self._objects[key]:
                        item.save(recursive=True)

        try:
            data = json.loads(self._persistent.get(self._key))
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
        self._persistent.set(self._key, json.dumps(self._data))
        self._original = copy.deepcopy(self._data)

        # Invalidate the cache
        for key in self._expiry.keys():
            self._volatile.delete('%s_%s' % (self._key, key))
        self._volatile.delete(self._key)
        self.dirty = False

    #######################
    ## Other CRUDs
    #######################

    def delete(self):
        """
        Delete the given object
        """

        try:
            self._persistent.delete(self._key)
        except:
            pass
        for key in self._expiry.keys():
            self._volatile.delete('%s_%s' % (self._key, key))
        self._volatile.delete(self._key)

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
        """
        The unique identifier of the object
        """

        return self._guid

    #######################
    ## Helper method to support 3rd party backend caching
    #######################

    def _backend_property(self, function):
        caller_name = inspect.stack()[1][3]
        cache_key   = '%s_%s' % (self._key, caller_name)
        cached_data = self._volatile.get(cache_key)
        if cached_data is None:
            cached_data = function()  # Load data from backend
            self._volatile.set(cache_key, cached_data, self._expiry[caller_name])
        return cached_data
