# license see http://www.openvstorage.com/licenses/opensource/
"""
DataObject module
"""
import uuid
import copy
from ovs.dal.exceptions import ObjectNotFoundException, ConcurrencyException
from ovs.dal.helpers import Descriptor, Toolbox
from ovs.dal.relations.relations import RelationMapper
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.datalist import DataList
from ovs.extensions.generic.volatilemutex import VolatileMutex
from ovs.extensions.storage.exceptions import KeyNotFoundException
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory


class MetaClass(type):
    """
    This metaclass provides dynamic __doc__ generation feeding doc generators
    """
    def __new__(mcs, name, bases, dct):
        """
        Overrides instance creation of all DataObject instances
        """
        if name != 'DataObject':
            for attribute, default in dct['_blueprint'].iteritems():
                docstring = default[2] if len(default) == 3 else ''
                if isinstance(default[1], type):
                    itemtype = default[1].__name__
                    extra_info = ''
                else:
                    itemtype = 'Enum(%s)' % default[1][0].__class__.__name__
                    extra_info = '(enum values: %s)' % ', '.join([str(item) for item in default[1]])
                dct[attribute] = property(
                    doc='[persistent] %s %s\n@type: %s'
                        % (docstring, extra_info, itemtype)
                )
            for attribute, relation in dct['_relations'].iteritems():
                itemtype = relation[0].__name__ if relation[0] is not None else name
                dct[attribute] = property(
                    doc='[relation] one-to-many relation with %s.%s\n@type: %s'
                        % (itemtype, relation[1], itemtype)
                )
            for attribute, info in dct['_expiry'].iteritems():
                docstring = dct['_%s' % attribute].__doc__.strip()
                if isinstance(info[1], type):
                    itemtype = info[1].__name__
                    extra_info = ''
                else:
                    itemtype = 'Enum(%s)' % info[1][0].__class__.__name__
                    extra_info = '(enum values: %s)' % ', '.join([str(item) for item in info[1]])
                dct[attribute] = property(
                    fget=dct['_%s' % attribute],
                    doc='[dynamic] (%ds) %s %s\n@rtype: %s'
                        % (info[0], docstring, extra_info, itemtype)
                )

        return super(MetaClass, mcs).__new__(mcs, name, bases, dct)


class DataObject(object):
    """
    This base class contains all logic to support our multiple backends and the caching
      - Storage backends:
        - Persistent backend for persistent storage (key-value store)
        - Volatile backend for volatile but fast storage (key-value store)
        - Storage backends are abstracted and injected into this class, making it possible to use
          fake backends
      - Features:
        - Hybrid property access:
          - Persistent backend
          - 3rd party component for "live" properties
        - Individual cache settings for "live" properties
        - 1-n relations with automatic property propagation
        - Recursive save
    """
    # @TODO: Deleting is not recursive
    # @TODO: There is a soft limit to the amount of objects per type that is situated around 10k

    __metaclass__ = MetaClass

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
        self._mutex_pk = VolatileMutex('primarykeys_%s' % self._name)
        self._mutex_listcache = VolatileMutex('listcache_%s' % self._name)

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
        self._volatile = VolatileFactory.get_client()
        self._persistent = PersistentFactory.get_client()
        self._metadata['cache'] = None
        if new:
            self._data = {}
        else:
            self._data = self._volatile.get(self._key)
            if self._data is None:
                Toolbox.log_cache_hit('object_load', False)
                self._metadata['cache'] = False
                try:
                    self._data = self._persistent.get(self._key)
                except KeyNotFoundException:
                    raise ObjectNotFoundException()
            else:
                Toolbox.log_cache_hit('object_load', True)
                self._metadata['cache'] = True

        # Set default values on new fields
        for key, default in self._blueprint.iteritems():
            if key not in self._data:
                self._data[key] = default[0]

        # Add properties where appropriate, hooking in the correct dictionary
        for attribute, default in self._blueprint.iteritems():
            self._add_sproperty(attribute, self._data[attribute])

        # Load relations
        for attribute, relation in self._relations.iteritems():
            if attribute not in self._data:
                if relation[0] is None:
                    cls = self.__class__
                else:
                    cls = relation[0]
                self._data[attribute] = Descriptor(cls).descriptor
            self._add_cproperty(attribute, self._data[attribute])

        # Add wrapped properties
        for attribute, expiry in self._expiry.iteritems():
            self._add_dproperty(attribute)

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
            self._volatile.set(self._key, self._data)

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
        """
        Adds a simple property to the object
        """
        # pylint: disable=protected-access
        fget = lambda s: s._get_sproperty(attribute)
        fset = lambda s, v: s._set_sproperty(attribute, v)
        # pylint: enable=protected-access
        setattr(self.__class__, attribute, property(fget, fset))
        self._data[attribute] = value

    def _add_cproperty(self, attribute, value):
        """
        Adds a complex property to the object (hybrids)
        """
        # pylint: disable=protected-access
        fget = lambda s: s._get_cproperty(attribute)
        fset = lambda s, v: s._set_cproperty(attribute, v)
        gget = lambda s: s._get_gproperty(attribute)
        # pylint: enable=protected-access
        setattr(self.__class__, attribute, property(fget, fset))
        setattr(self.__class__, '%s_guid' % attribute, property(gget))
        self._data[attribute] = value

    def _add_lproperty(self, attribute):
        """
        Adds a list (readonly) property to the object
        """
        # pylint: disable=protected-access
        fget = lambda s: s._get_lproperty(attribute)
        # pylint: enable=protected-access
        setattr(self.__class__, attribute, property(fget))

    def _add_dproperty(self, attribute):
        """
        Adds a dynamic property to the object
        """
        # pylint: disable=protected-access
        fget = lambda s: s._get_dproperty(attribute)
        # pylint: enable=protected-access
        setattr(self.__class__, attribute, property(fget))

    # Helper method spporting property fetching
    def _get_sproperty(self, attribute):
        """
        Getter for a simple property
        """
        return self._data[attribute]

    def _get_cproperty(self, attribute):
        """
        Getter for a complex property (hybrid)
        It will only load the object once and caches it for the lifetime of this object
        """
        if attribute not in self._objects:
            descriptor = Descriptor().load(self._data[attribute])
            self._objects[attribute] = descriptor.get_object(instantiate=True)
        return self._objects[attribute]

    def _get_gproperty(self, attribute):
        """
        Getter for a foreign key property
        """
        return self._data[attribute]['guid']

    def _get_lproperty(self, attribute):
        """
        Getter for the list property
        It will execute the related query every time to return a list of hybrid objects that
        refer to this object. The resulting data will be stored or merged into the cached list
        preserving as much already loaded objects as possible
        """
        info = self._objects[attribute]['info']
        remote_class = Descriptor().load(info['class']).get_object()
        remote_key   = info['key']
        # pylint: disable=line-too-long
        datalist = DataList(query = {'object': remote_class,
                                     'data': DataList.select.DESCRIPTOR,
                                     'query': {'type': DataList.where_operator.AND,
                                               'items': [('%s.guid' % remote_key, DataList.operator.EQUALS, self.guid)]}},  # noqa
                            key   = '%s_%s_%s' % (self._name, self._guid, attribute))
        # pylint: enable=line-too-long

        if self._objects[attribute]['data'] is None:
            self._objects[attribute]['data'] = DataObjectList(datalist.data, remote_class)
        else:
            self._objects[attribute]['data'].merge(datalist.data)
        return self._objects[attribute]['data']

    def _get_dproperty(self, attribute):
        """
        Getter for dynamic property, wrapping the internal data loading property
        in a caching layer
        """
        data_loader = getattr(self, '_%s' % attribute)
        return self._backend_property(data_loader, attribute)

    # Helper method supporting property setting
    def _set_sproperty(self, attribute, value):
        """
        Setter for a simple property that will validate the type
        """
        self.dirty = True
        if value is None:
            self._data[attribute] = value
        else:
            correct, allowed_types, given_type = Toolbox.check_type(value, self._blueprint[attribute][1])
            if correct:
                self._data[attribute] = value
            else:
                raise TypeError('Property %s allows types %s. %s given'
                                % (attribute, str(allowed_types), given_type))

    def _set_cproperty(self, attribute, value):
        """
        Setter for a complex property (hybrid) that will validate the type
        """
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
        """
        __setattr__ hook that will block creating on the fly new properties, except
        the predefined ones
        """
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
        of the specified conflict resolve settings.
        It will also invalidate certain caches if required. For example lists pointing towards this
        object
        """
        if recursive:
            # Save objects that point to us (e.g. disk.vmachine - if this is disk)
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
            data = self._persistent.get(self._key)
        except KeyNotFoundException:
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
            elif attribute not in data:
                data[attribute] = self._data[attribute]
        if data_conflicts:
            raise ConcurrencyException('Got field conflicts while saving %s. Conflicts: %s'
                                       % (self._name, ', '.join(data_conflicts)))

        # Save the data
        self._data = copy.deepcopy(data)
        self._persistent.set(self._key, self._data)
        self._add_pk(self._key)

        # Invalidate lists/queries
        # First, invalidate reverse lists (where we point to a remote object,
        # invalidating that remote list)
        for key, default in self._relations.iteritems():
            if self._original[key]['guid'] != self._data[key]['guid']:
                if default[0] is None:
                    classname = self.__class__.__name__.lower()
                else:
                    classname = default[0].__name__.lower()
                # The field points to another object
                self._volatile.delete('%s_%s_%s_%s' % (DataList.namespace,
                                                       classname,
                                                       self._original[key]['guid'],
                                                       default[1]))
                self._volatile.delete('%s_%s_%s_%s' % (DataList.namespace,
                                                       classname,
                                                       self._data[key]['guid'],
                                                       default[1]))
        # Second, invalidate property lists
        try:
            self._mutex_listcache.acquire(10)
            cache_key = '%s_%s' % (DataList.cachelink, self._name)
            cache_list = Toolbox.try_get(cache_key, {})
            for field in cache_list.keys():
                clear = False
                if field == '__all' and new:  # This is a no-filter query hook, which can be ignored
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
                        self._volatile.delete(list_key)
                    del cache_list[field]
            self._volatile.set(cache_key, cache_list)
            self._persistent.set(cache_key, cache_list)
        finally:
            self._mutex_listcache.release()

        # Invalidate the cache
        self.invalidate_dynamics()
        self._volatile.delete(self._key)

        self._original = copy.deepcopy(self._data)
        self.dirty = False

    #######################
    ## Other CRUDs
    #######################

    def delete(self):
        """
        Delete the given object. It also invalidates certain lists
        """
        # Invalidate no-filter queries/lists pointing to this object
        cache_list = Toolbox.try_get('%s_%s' % (DataList.cachelink, self._name), {})
        if '__all' in cache_list.keys():
            for list_key in cache_list['__all']:
                self._volatile.delete(list_key)

        # Delete the object out of the persistent store
        try:
            self._persistent.delete(self._key)
        except KeyNotFoundException:
            pass

        # Delete the object and its properties out of the volatile store
        self.invalidate_dynamics()
        self._volatile.delete(self._key)
        self._delete_pk(self._key)

    # Discard all pending changes
    def discard(self):
        """
        Discard all pending changes, reloading the data from the persistent backend
        """
        self.__init__(guid           = self._guid,
                      datastore_wins = self._datastore_wins)

    def invalidate_dynamics(self, properties=None):
        """
        Invalidates all dynamic property caches. Use with caution, as this action can introduce
        a short performance hit.
        """
        for key in self._expiry.keys():
            if properties is None or key in properties:
                self._volatile.delete('%s_%s' % (self._key, key))

    def serialize(self, depth=0):
        """
        Serializes the internal data, getting rid of certain metadata like descriptors
        """
        data = {'guid': self.guid}
        for key in self._relations:
            if depth == 0:
                data['%s_guid' % key] = self._data[key]['guid']
            else:
                instance = getattr(self, key)
                if instance is not None:
                    data[key] = getattr(self, key).serialize(depth=(depth - 1))
                else:
                    data[key] = None
        for key in self._blueprint:
            data[key] = self._data[key]
        for key in self._expiry.keys():
            data[key] = getattr(self, key)
        return data

    def copy_blueprint(self, other_object, include=None, exclude=None, include_relations=False):
        """
        Copies all _blueprint (and optionally _relation) properties over from a given hybrid to
        self. One can pass in a list of properties that should be copied, or a list of properties
        that should not be copied. Exclude > Include
        """
        if include is not None and not isinstance(include, list):
            raise TypeError('Argument include should be None or a list of strings')
        if exclude is not None and not isinstance(exclude, list):
            raise TypeError('Argument exclude should be None or a list of strings')
        if self.__class__.__name__ != other_object.__class__.__name__:
            raise TypeError('Properties can only be loaded from hybrids of the same type')

        if include:
            properties_to_copy = include
        else:
            properties_to_copy = self._blueprint.keys()
            if include_relations:
                properties_to_copy += self._relations.keys()

        if exclude:
            properties_to_copy = [p for p in properties_to_copy if p not in exclude]

        possible_options = self._blueprint.keys() + (self._relations.keys() if include_relations else [])
        properties_to_copy = [p for p in properties_to_copy if p in possible_options]

        for key in properties_to_copy:
            setattr(self, key, getattr(other_object, key))

    #######################
    ## Properties
    #######################

    @property
    def guid(self):
        """
        The primary key of the object
        """
        return self._guid

    #######################
    ## Helper methods
    #######################

    def _backend_property(self, function, caller_name):
        """
        Handles the internal caching of dynamic properties
        """
        cache_key   = '%s_%s' % (self._key, caller_name)
        cached_data = self._volatile.get(cache_key)
        if cached_data is None:
            cached_data = function()  # Load data from backend
            if cached_data is not None:
                correct, allowed_types, given_type = Toolbox.check_type(cached_data, self._expiry[caller_name][1])
                if not correct:
                    raise TypeError('Dynamic property %s allows types %s. %s given'
                                    % (caller_name, str(allowed_types), given_type))
            self._volatile.set(cache_key, cached_data, self._expiry[caller_name][0])
        return cached_data

    def _add_pk(self, key):
        """
        This adds the current primary key to the primary key index
        """
        internal_key = 'ovs_primarykeys_%s' % self._name
        try:
            self._mutex_pk.acquire(10)
            keys = self._volatile.get(internal_key)
            if keys is None:
                keys = set(self._persistent.prefix('%s_%s_' % (self._namespace, self._name)))
            else:
                keys.add(key)
            self._volatile.set(internal_key, keys)
        finally:
            self._mutex_pk.release()

    def _delete_pk(self, key):
        """
        This deletes the current primary key from the primary key index
        """
        internal_key = 'ovs_primarykeys_%s' % self._name
        try:
            self._mutex_pk.acquire(10)
            keys = self._volatile.get(internal_key)
            if keys is None:
                keys = set(self._persistent.prefix('%s_%s_' % (self._namespace, self._name)))
            else:
                try:
                    keys.remove(key)
                except KeyError:
                    pass
            self._volatile.set(internal_key, keys)
        finally:
            self._mutex_pk.release()

    def __str__(self):
        """
        The string representation of a DataObject is the serialized value
        """
        return str(self.serialize())
