import json
import inspect
import imp
import uuid
import copy
import os
from exceptions import *


class DataObject(object):
    """
    This base class contains all logic to support our multiple backends and the caching
    * Persistent data
    ** OVS backing data: Arakoon
    ** Read-only reality data: backend (backend libraries contacting e.g. voldrv)
    * Volatile caching: Memcached
    """

    # Properties that needs to be overwritten by implementation
    _blueprint = None            # Blueprint data of the objec type
    _objectexpiry = None         # Timeout of main object cache
    _expiry = None               # Timeout of readonly object properties cache

    # Internal properties
    _store_factory = None    # Store factory
    _name = None             # Name of the object
    _guid = None             # Guid identifier of the object
    _namespace = 'ovs_data'  # Namespace of the object
    _original = {}           # Original data copy
    _metadata = {}           # Some metadata, mainly used for unit testing

    # Public properties
    new = False
    dirty = False

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

        self._datastoreWins = datastore_wins
        self._name = self.__class__.__name__.lower()
        self.new = False
        self.dirty = False

        # Init guid
        if guid is None:
            self._guid = str(uuid.uuid4())
            self.new = True
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
        if self.new:
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
                if inspect.isclass(default) and issubclass(default, DataObject):
                    instance = default()
                    self._data[key] = {'id'    : None,
                                       'source': inspect.getfile(instance.__class__),
                                       'type'  : instance.__class__.__name__}
                else:
                    self._data[key] = default

        # Add properties where appropriate, hooking in the correct dictionary
        for attribute, default in self._blueprint.iteritems():
            if attribute not in dir(self):
                if inspect.isclass(default) and issubclass(default, DataObject):
                    self._add_cproperty(attribute, self._data.get(attribute, default))
                else:
                    self._add_sproperty(attribute, self._data.get(attribute, default))

        # Store original data
        self._original = copy.deepcopy(self._data)

        # Re-cache the object
        self._volatile.set(self._key, self._data, self._objectexpiry)

    # Helper method to support dynamic adding of properties
    def _add_sproperty(self, attribute, value):
        fget = lambda self: self._get_sproperty(attribute)
        fset = lambda self, value: self._set_sproperty(attribute, value)
        setattr(self.__class__, attribute, property(fget, fset))
        self._data[attribute] = value

    def _add_cproperty(self, attribute, value):
        fget = lambda self: self._get_cproperty(attribute)
        fset = lambda self, value: self._set_cproperty(attribute, value)
        setattr(self.__class__, attribute, property(fget, fset))
        self._data[attribute] = value

    # Helper method spporting property fetching
    def _get_sproperty(self, attribute):
        return self._data[attribute]

    def _get_cproperty(self, attribute):
        filename = self._data[attribute]['source'].replace('.pyc', '.py')
        name = filename.replace(os.path.dirname(filename) + os.path.sep, '').replace('.py', '')
        module = imp.load_source(name, filename)
        cls = getattr(module, self._data[attribute]['type'])
        return cls(self._data[attribute]['id'])

    # Helper method supporting property setting
    def _set_sproperty(self, attribute, value):
        self.dirty = True
        self._data[attribute] = value

    def _set_cproperty(self, attribute, value):
        self.dirty = True
        self._data[attribute] = {'id'    : value.guid,
                                 'source': inspect.getfile(value.__class__),
                                 'type'  : value.__class__.__name__}

    @classmethod
    def set_storefactory(cls, factory):
        DataObject._store_factory = factory

    # Save method, saving to persistent backend and invalidating cache
    def save(self):
        """
        Save the object to the persistent backend and clear cache, making use
        of the specified conflict resolve settings
        """

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
                    if self._datastoreWins is None:
                        # In case we didn't set a policy, we raise the conflicts
                        data_conflicts.append(attribute)
                    elif self._datastoreWins is False:
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
        self.new = False

    # Delete the object
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
                      datastore_wins = self._datastoreWins)

    # Guid readonly property
    @property
    def guid(self):
        """
        The unique identifier of the object
        """

        return self._guid

    # Helper method supporting cache wrapping the readonly properties
    def _backend_property(self, function):
        caller_name = inspect.stack()[1][3]
        cache_key   = '%s_%s' % (self._key, caller_name)
        cached_data = self._volatile.get(cache_key)
        if cached_data is None:
            cached_data = function()  # Load data from backend
            self._volatile.set(cache_key, cached_data, self._expiry[caller_name])
        return cached_data
