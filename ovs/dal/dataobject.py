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
DataObject module
"""
import uuid
import copy
from ovs.dal.exceptions import ObjectNotFoundException, ConcurrencyException, LinkedObjectException
from ovs.dal.helpers import Descriptor, Toolbox, HybridRunner
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
            for internal in ['_blueprint', '_relations', '_expiry']:
                data = {}
                for base in bases:
                    if hasattr(base, internal):
                        data.update(getattr(base, internal))
                if '_{0}_{1}'.format(name, internal) in dct:
                    data.update(dct.pop('_{0}_{1}'.format(name, internal)))
                dct[internal] = data

            for attribute, default in dct['_blueprint'].iteritems():
                docstring = default[2] if len(default) == 3 else ''
                if isinstance(default[1], type):
                    itemtype = default[1].__name__
                    extra_info = ''
                else:
                    itemtype = 'Enum({0})'.format(default[1][0].__class__.__name__)
                    extra_info = '(enum values: {0})'.format(', '.join([str(item) for item in default[1]]))
                dct[attribute] = property(
                    doc='[persistent] {0} {1}\n@type: {2}'.format(docstring, extra_info, itemtype)
                )
            for attribute, relation in dct['_relations'].iteritems():
                itemtype = relation[0].__name__ if relation[0] is not None else name
                dct[attribute] = property(
                    doc='[relation] one-to-many relation with {0}.{1}\n@type: {2}'.format(itemtype, relation[1], itemtype)
                )
            for attribute, info in dct['_expiry'].iteritems():
                if bases[0].__name__ == 'DataObject':
                    if '_{0}'.format(attribute) not in dct:
                        raise LookupError('Dynamic property {0} in {1} could not be resolved'.format(attribute, name))
                    method = dct['_{0}'.format(attribute)]
                else:
                    methods = [getattr(base, '_{0}'.format(attribute)) for base in bases if hasattr(base, '_{0}'.format(attribute))]
                    if len(methods) == 0:
                        raise LookupError('Dynamic property {0} in {1} could not be resolved'.format(attribute, name))
                    method = [0]
                docstring = method.__doc__.strip()
                if isinstance(info[1], type):
                    itemtype = info[1].__name__
                    extra_info = ''
                else:
                    itemtype = 'Enum({0})'.format(info[1][0].__class__.__name__)
                    extra_info = '(enum values: {0})'.format(', '.join([str(item) for item in info[1]]))
                dct[attribute] = property(
                    fget=method,
                    doc='[dynamic] ({0}s) {1} {2}\n@rtype: {3}'.format(info[0], docstring, extra_info, itemtype)
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
    __metaclass__ = MetaClass

    #######################
    ## Attributes
    #######################

    # Properties that needs to be overwritten by implementation
    _blueprint = {}            # Blueprint data of the objec type
    _expiry = {}               # Timeout of readonly object properties cache
    _relations = {}            # Blueprint for relations

    #######################
    ## Constructor
    #######################

    def __new__(cls, *args, **kwargs):
        """
        Initializes the class
        """
        hybrid_structure = HybridRunner.get_hybrids()
        identifier = Descriptor(cls).descriptor['identifier']
        if identifier in hybrid_structure and identifier != hybrid_structure[identifier]['identifier']:
            new_class = Descriptor().load(hybrid_structure[identifier]).get_object()
            return super(cls, new_class).__new__(new_class, *args, **kwargs)
        return super(DataObject, cls).__new__(cls, *args, **kwargs)

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
        self._mutex_listcache = VolatileMutex('listcache_{0}'.format(self._name))

        # Rebuild _relation types
        hybrid_structure = HybridRunner.get_hybrids()
        for relation in self._relations.iterkeys():
            if self._relations[relation][0] is not None:
                identifier = Descriptor(self._relations[relation][0]).descriptor['identifier']
                if identifier in hybrid_structure and identifier != hybrid_structure[identifier]['identifier']:
                    new_type = Descriptor().load(hybrid_structure[identifier]).get_object()
                    if len(self._relations[relation]) == 2:
                        self._relations[relation] = (new_type, self._relations[relation][1])
                    else:
                        self._relations[relation] = (new_type, self._relations[relation][1], self._relations[relation][2])

        # Init guid
        self._new = False
        if guid is None:
            self._guid = str(uuid.uuid4())
            self._new = True
        else:
            self._guid = str(guid)

        # Build base keys
        self._key = '{0}_{1}_{2}'.format(self._namespace, self._name, self._guid)

        # Load data from cache or persistent backend where appropriate
        self._volatile = VolatileFactory.get_client()
        self._persistent = PersistentFactory.get_client()
        self._metadata['cache'] = None
        if self._new:
            self._data = {}
        else:
            self._data = self._volatile.get(self._key)
            if self._data is None:
                Toolbox.log_cache_hit('object_load', False)
                self._metadata['cache'] = False
                try:
                    self._data = self._persistent.get(self._key)
                except KeyNotFoundException:
                    raise ObjectNotFoundException('{0} with guid \'{1}\' could not be found'.format(
                        self.__class__.__name__, self._guid
                    ))
            else:
                Toolbox.log_cache_hit('object_load', True)
                self._metadata['cache'] = True

        # Set default values on new fields
        for key, default in self._blueprint.iteritems():
            if key not in self._data:
                self._data[key] = default[0]

        # Add properties where appropriate, hooking in the correct dictionary
        for attribute, default in self._blueprint.iteritems():
            self._add_blueprint_property(attribute, self._data[attribute])

        # Load relations
        for attribute, relation in self._relations.iteritems():
            if attribute not in self._data:
                if relation[0] is None:
                    cls = self.__class__
                else:
                    cls = relation[0]
                self._data[attribute] = Descriptor(cls).descriptor
            self._add_relation_property(attribute, self._data[attribute])

        # Add wrapped properties
        for attribute, expiry in self._expiry.iteritems():
            self._add_dynamic_property(attribute)

        # Load foreign keys
        relations = RelationMapper.load_foreign_relations(self.__class__)
        if relations is not None:
            for key, info in relations.iteritems():
                self._objects[key] = {'info': info,
                                      'data': None}
                self._add_list_property(key, info['list'])

        # Store original data
        self._original = copy.deepcopy(self._data)

        if not self._new:
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

    def _add_blueprint_property(self, attribute, value):
        """
        Adds a simple property to the object
        """
        # pylint: disable=protected-access
        fget = lambda s: s._get_blueprint_property(attribute)
        fset = lambda s, v: s._set_blueprint_property(attribute, v)
        # pylint: enable=protected-access
        setattr(self.__class__, attribute, property(fget, fset))
        self._data[attribute] = value

    def _add_relation_property(self, attribute, value):
        """
        Adds a complex property to the object (hybrids)
        """
        # pylint: disable=protected-access
        fget = lambda s: s._get_relation_property(attribute)
        fset = lambda s, v: s._set_relation_property(attribute, v)
        gget = lambda s: s._get_guid_property(attribute)
        # pylint: enable=protected-access
        setattr(self.__class__, attribute, property(fget, fset))
        setattr(self.__class__, '{0}_guid'.format(attribute), property(gget))
        self._data[attribute] = value

    def _add_list_property(self, attribute, list):
        """
        Adds a list (readonly) property to the object
        """
        # pylint: disable=protected-access
        fget = lambda s: s._get_list_property(attribute)
        gget = lambda s: s._get_list_guid_property(attribute)
        # pylint: enable=protected-access
        setattr(self.__class__, attribute, property(fget))
        setattr(self.__class__, ('{0}_guids' if list else '{0}_guid').format(attribute), property(gget))

    def _add_dynamic_property(self, attribute):
        """
        Adds a dynamic property to the object
        """
        # pylint: disable=protected-access
        fget = lambda s: s._get_dynamic_property(attribute)
        # pylint: enable=protected-access
        setattr(self.__class__, attribute, property(fget))

    # Helper method spporting property fetching
    def _get_blueprint_property(self, attribute):
        """
        Getter for a simple property
        """
        return self._data[attribute]

    def _get_relation_property(self, attribute):
        """
        Getter for a complex property (hybrid)
        It will only load the object once and caches it for the lifetime of this object
        """
        if attribute not in self._objects:
            descriptor = Descriptor().load(self._data[attribute])
            self._objects[attribute] = descriptor.get_object(instantiate=True)
        return self._objects[attribute]

    def _get_guid_property(self, attribute):
        """
        Getter for a foreign key property
        """
        return self._data[attribute]['guid']

    def _get_list_property(self, attribute):
        """
        Getter for the list property
        It will execute the related query every time to return a list of hybrid objects that
        refer to this object. The resulting data will be stored or merged into the cached list
        preserving as much already loaded objects as possible
        """
        info = self._objects[attribute]['info']
        remote_class = Descriptor().load(info['class']).get_object()
        remote_key   = info['key']
        datalist = DataList.get_relation_set(remote_class, remote_key, self.__class__, attribute, self.guid)
        if self._objects[attribute]['data'] is None:
            self._objects[attribute]['data'] = DataObjectList(datalist.data, remote_class)
        else:
            self._objects[attribute]['data'].merge(datalist.data)
        if info['list'] is True:
            return self._objects[attribute]['data']
        else:
            data = self._objects[attribute]['data']
            return data[0] if len(data) == 1 else None

    def _get_list_guid_property(self, attribute):
        """
        Getter for guid list property
        """
        dataobjectlist = getattr(self, attribute)
        if dataobjectlist is None:
            return None
        if hasattr(dataobjectlist, '_guids'):
            return dataobjectlist._guids
        return dataobjectlist.guid

    def _get_dynamic_property(self, attribute):
        """
        Getter for dynamic property, wrapping the internal data loading property
        in a caching layer
        """
        data_loader = getattr(self, '_{0}'.format(attribute))
        return self._backend_property(data_loader, attribute)

    # Helper method supporting property setting
    def _set_blueprint_property(self, attribute, value):
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
                raise TypeError('Property {0} allows types {1}. {2} given'.format(
                    attribute, str(allowed_types), given_type
                ))

    def _set_relation_property(self, attribute, value):
        """
        Setter for a complex property (hybrid) that will validate the type
        """
        self.dirty = True
        if value is None:
            self._objects[attribute] = None
            self._data[attribute]['guid'] = None
        else:
            descriptor = Descriptor(value.__class__).descriptor
            if descriptor['identifier'] != self._data[attribute]['identifier']:
                raise TypeError('An invalid type was given: {0} instead of {1}'.format(
                    descriptor['type'],  self._data[attribute]['type']
                ))
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
            raise RuntimeError('Property {0} does not exist on this object.'.format(key))

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
                        if info['list'] is True:
                            for item in getattr(self, key).iterloaded():
                                item.save(recursive=True, skip=info['key'])
                        else:
                            item = getattr(self, key)
                            if item is not None:
                                item.save(recursive=True, skip=info['key'])

        try:
            data = self._persistent.get(self._key)
        except KeyNotFoundException:
            if self._new:
                data = {}
            else:
                raise ObjectNotFoundException('{0} with guid \'{1}\' was deleted'.format(
                    self.__class__.__name__, self._guid
                ))
        changed_fields = []
        data_conflicts = []
        for attribute in self._data.keys():
            if self._data[attribute] != self._original[attribute]:
                # We changed this value
                changed_fields.append(attribute)
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
            raise ConcurrencyException('Got field conflicts while saving {0}. Conflicts: {1}'.format(
                self._name, ', '.join(data_conflicts)
            ))

        # Save the data
        self._data = copy.deepcopy(data)
        self._persistent.set(self._key, self._data)
        DataList.add_pk(self._namespace, self._name, self._key)

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
                self._volatile.delete('{0}_{1}_{2}_{3}'.format(
                    DataList.namespace,
                    classname,
                    self._original[key]['guid'],
                    default[1]
                ))
                self._volatile.delete('{0}_{1}_{2}_{3}'.format(
                    DataList.namespace,
                    classname,
                    self._data[key]['guid'],
                    default[1]
                ))
        # Second, invalidate property lists
        try:
            self._mutex_listcache.acquire(60)
            cache_key = '{0}_{1}'.format(DataList.cachelink, self._name)
            cache_list = Toolbox.try_get(cache_key, {})
            for list_key in cache_list.keys():
                fields = cache_list[list_key]
                if ('__all' in fields and self._new) or list(set(fields) & set(changed_fields)):
                    self._volatile.delete(list_key)
                    del cache_list[list_key]
            self._volatile.set(cache_key, cache_list)
            self._persistent.set(cache_key, cache_list)
        finally:
            self._mutex_listcache.release()

        # Invalidate the cache
        self.invalidate_dynamics()
        self._volatile.delete(self._key)

        self._original = copy.deepcopy(self._data)
        self.dirty = False
        self._new = False

    #######################
    ## Other CRUDs
    #######################

    def delete(self, abandon=False):
        """
        Delete the given object. It also invalidates certain lists
        """
        # Check foreign relations
        relations = RelationMapper.load_foreign_relations(self.__class__)
        if relations is not None:
            for key, info in relations.iteritems():
                items = getattr(self, key)
                if info['list'] is True:
                    if len(items) > 0:
                        if abandon is True:
                            for item in items.itersafe():
                                setattr(item, info['key'], None)
                                try:
                                    item.save()
                                except ObjectNotFoundException:
                                    pass
                        else:
                            raise LinkedObjectException('There are {0} items left in self.{1}'.format(len(items), key))
                elif items is not None:
                    # No list (so a 1-to-1 relation), so there should be an object, or None
                    item = items  # More clear naming
                    if abandon is True:
                        setattr(item, info['key'], None)
                        try:
                            item.save()
                        except ObjectNotFoundException:
                            pass
                    else:
                        raise LinkedObjectException('There is still an item linked in self.{0}'.format(key))

        # Invalidate no-filter queries/lists pointing to this object
        try:
            self._mutex_listcache.acquire(60)
            cache_key = '{0}_{1}'.format(DataList.cachelink, self._name)
            cache_list = Toolbox.try_get(cache_key, {})
            for list_key in cache_list.keys():
                fields = cache_list[list_key]
                if '__all' in fields:
                    self._volatile.delete(list_key)
                    del cache_list[list_key]
            self._volatile.set(cache_key, cache_list)
            self._persistent.set(cache_key, cache_list)
        finally:
            self._mutex_listcache.release()

        # Delete the object out of the persistent store
        try:
            self._persistent.delete(self._key)
        except KeyNotFoundException:
            pass

        # Delete the object and its properties out of the volatile store
        self.invalidate_dynamics()
        self._volatile.delete(self._key)
        DataList.delete_pk(self._namespace, self._name, self._key)

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
                self._volatile.delete('{0}_{1}'.format(self._key, key))

    def serialize(self, depth=0):
        """
        Serializes the internal data, getting rid of certain metadata like descriptors
        """
        data = {'guid': self.guid}
        for key in self._relations:
            if depth == 0:
                data['{0}_guid'.format(key)] = self._data[key]['guid']
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
        cache_key   = '{0}_{1}'.format(self._key, caller_name)
        cached_data = self._volatile.get(cache_key)
        if cached_data is None:
            cached_data = function()  # Load data from backend
            if cached_data is not None:
                correct, allowed_types, given_type = Toolbox.check_type(cached_data, self._expiry[caller_name][1])
                if not correct:
                    raise TypeError('Dynamic property {0} allows types {1}. {2} given'.format(
                        caller_name, str(allowed_types), given_type
                    ))
            self._volatile.set(cache_key, cached_data, self._expiry[caller_name][0])
        return cached_data

    def __str__(self):
        """
        The string representation of a DataObject is the serialized value
        """
        return str(self.serialize())
