# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
DataObject module
"""
import uuid
import copy
import time
import json
import logging
import inspect
import hashlib
from random import randint
from ovs.dal.exceptions import (ObjectNotFoundException, ConcurrencyException, LinkedObjectException,
                                MissingMandatoryFieldsException, RaceConditionException, InvalidRelationException,
                                VolatileObjectException, UniqueConstraintViolationException)
from ovs.dal.helpers import Descriptor, DalToolbox, HybridRunner
from ovs.dal.relations import RelationMapper
from ovs.dal.datalist import DataList
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs_extensions.generic.volatilemutex import NoLockAvailableException
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs_extensions.storage.exceptions import KeyNotFoundException, AssertException
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory


class MetaClass(type):
    """
    This metaclass provides dynamic __doc__ generation feeding doc generators
    """

    # noinspection PyInitNewSignature
    def __new__(mcs, name, bases, dct):
        """
        Overrides instance creation of all DataObject instances
        """
        if name != 'DataObject':
            # Property instantiation
            for internal in ['_properties', '_relations', '_dynamics']:
                data = set()
                for base in bases:  # Extend properties for deeper inheritance
                    if hasattr(base, internal):  # if the base already ran the metaclass: append to current class
                        data.update(getattr(base, internal))
                if '_{0}_{1}'.format(name, internal) in dct:  # instance._Testobject__properties. __properties cannot get overruled by inheritance
                    data.update(dct.pop('_{0}_{1}'.format(name, internal)))
                dct[internal] = list(data)
            # Doc generation - properties
            for prop in dct['_properties']:
                docstring = prop.docstring
                if isinstance(prop.property_type, type):
                    itemtype = prop.property_type.__name__
                    extra_info = ''
                else:
                    itemtype = 'Enum({0})'.format(prop.property_type[0].__class__.__name__)
                    extra_info = '(enum values: {0})'.format(', '.join(prop.property_type))
                dct[prop.name] = property(
                    doc='[persistent] {0} {1}\n@type: {2}'.format(docstring, extra_info, itemtype)
                )
            # Doc generation - relations
            for relation in dct['_relations']:
                itemtype = relation.foreign_type.__name__ if relation.foreign_type is not None else name
                dct[relation.name] = property(
                    doc='[relation] one-to-{0} relation with {1}.{2}\n@type: {3}'.format(
                        'one' if relation.onetoone else 'many',
                        itemtype,
                        relation.foreign_key,
                        itemtype
                    )
                )
            # Doc generation - dynamics
            for dynamic in dct['_dynamics']:
                if bases[0].__name__ == 'DataObject':
                    if '_{0}'.format(dynamic.name) not in dct:
                        raise LookupError('Dynamic property {0} in {1} could not be resolved'.format(dynamic.name, name))
                    method = dct['_{0}'.format(dynamic.name)]
                else:
                    methods = [getattr(base, '_{0}'.format(dynamic.name)) for base in bases if hasattr(base, '_{0}'.format(dynamic.name))]
                    if len(methods) == 0:
                        raise LookupError('Dynamic property {0} in {1} could not be resolved'.format(dynamic.name, name))
                    method = methods[0]
                docstring = method.__doc__.strip()
                if isinstance(dynamic.return_type, type):
                    itemtype = dynamic.return_type.__name__
                    extra_info = ''
                else:
                    itemtype = 'Enum({0})'.format(dynamic.return_type[0].__class__.__name__)
                    extra_info = '(enum values: {0})'.format(', '.join(dynamic.return_type))
                dct[dynamic.name] = property(
                    fget=method,
                    doc='[dynamic] ({0}s) {1} {2}\n@rtype: {3}'.format(dynamic.timeout, docstring, extra_info, itemtype)
                )

        return super(MetaClass, mcs).__new__(mcs, name, bases, dct)


class DataObjectAttributeEncoder(json.JSONEncoder):
    """
    Custom JSONEncoder for attributes
    """
    def default(self, obj):
        """
        Default return value
        :param obj: Object to encode
        :return: String
        """
        return "{0}: {1}".format(type(obj), obj)


# noinspection PyProtectedMember
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

    ##############
    # Attributes #
    ##############

    # Properties that needs to be overwritten by implementation
    _properties = []  # Blueprint data of the object type
    _dynamics = []    # Timeout of readonly object properties cache
    _relations = []   # Blueprint for relations
    _logger = logging.getLogger(__name__)

    NAMESPACE = 'ovs_data'  # Arakoon namespace

    ###############
    # Constructor #
    ###############

    def __new__(cls, *args, **kwargs):
        """
        Control the initialization of the class
        """
        hybrid_structure = HybridRunner.get_hybrids()
        identifier = Descriptor(cls).descriptor['identifier']
        if identifier in hybrid_structure and identifier != hybrid_structure[identifier]['identifier']:
            new_class = Descriptor().load(hybrid_structure[identifier]).get_object()  # Load the possible extended hybrid
            # noinspection PyArgumentList
            return super(cls, new_class).__new__(new_class, *args)
        return super(DataObject, cls).__new__(cls)

    def __init__(self, guid=None, data=None, datastore_wins=False, volatile=False, _hook=None):
        """
        Loads an object with a given guid. If no guid is given, a new object is generated with a new guid.
        * guid: The guid indicating which object should be loaded
        * datastore_wins: Optional boolean indicating save conflict resolve management.
        ** True: when saving, external modified fields will not be saved
        ** False: when saving, all changed data will be saved, regardless of external updates
        ** None: in case changed field were also changed externally, an error will be raised
        """

        # Initialize super class
        super(DataObject, self).__init__()

        # Initialize internal fields
        self._frozen = False  # Prevent property setting on the object
        self._datastore_wins = datastore_wins
        self._guid = None    # Guid identifier of the object
        self._original = {}  # Original data copy
        self._metadata = {}  # Some metadata, mainly used for unit testing
        self._data = {}      # Internal data storage
        self._objects = {}   # Internal objects storage
        self._dynamic_timings = {}

        # Initialize public fields
        self.dirty = False
        self.volatile = volatile

        # Worker fields/objects
        self._classname = self.__class__.__name__.lower()

        # Rebuild _relation types
        hybrid_structure = HybridRunner.get_hybrids()
        for relation in self._relations:
            if relation.foreign_type is not None:  # If none -> points to itself
                identifier = Descriptor(relation.foreign_type).descriptor['identifier']
                if identifier in hybrid_structure and identifier != hybrid_structure[identifier]['identifier']:
                    # Point to relations of the original object when object got extended
                    relation.foreign_type = Descriptor().load(hybrid_structure[identifier]).get_object()
        # Init guid
        self._new = False
        if guid is None:
            self._guid = str(uuid.uuid4())
            self._new = True
        else:
            self._guid = str(guid)

        # Build base keys
        self._key = '{0}_{1}_{2}'.format(DataObject.NAMESPACE, self._classname, self._guid)

        # Worker mutexes
        self._mutex_version = volatile_mutex('ovs_dataversion_{0}_{1}'.format(self._classname, self._guid))

        # Load data from cache or persistent backend where appropriate
        self._volatile = VolatileFactory.get_client()
        self._persistent = PersistentFactory.get_client()
        self._metadata['cache'] = None
        if not self._new:
            if data is not None:
                self._data = copy.deepcopy(data)
                self._metadata['cache'] = None
            else:
                self._data = self._volatile.get(self._key)
                if self._data is None:
                    self._metadata['cache'] = False
                    try:
                        self._data = self._persistent.get(self._key)
                    except KeyNotFoundException:
                        raise ObjectNotFoundException('{0} with guid \'{1}\' could not be found'.format(
                            self.__class__.__name__, self._guid
                        ))
                else:
                    self._metadata['cache'] = True

        # Set default values on new fields
        for prop in self._properties:
            if prop.name not in self._data:
                self._data[prop.name] = prop.default
            self._add_property(prop)

        # Load relations
        for relation in self._relations:
            if relation.name not in self._data:
                if relation.foreign_type is None:
                    cls = self.__class__
                else:
                    cls = relation.foreign_type
                self._data[relation.name] = Descriptor(cls).descriptor
            self._add_relation_property(relation)

        # Add wrapped properties
        for dynamic in self._dynamics:
            self._add_dynamic_property(dynamic)

        # Load foreign keys
        relations = RelationMapper.load_foreign_relations(self.__class__)  # To many side of things
        if relations is not None:
            for key, info in relations.iteritems():
                self._objects[key] = {'info': info,
                                      'data': None}
                self._add_list_property(key, info['list'])

        if _hook is not None and 'before_cache' in _hook:
            _hook['before_cache']()

        if not self._new:  # A new object is useless as it has no practical properties
            # Re-cache the object, if required
            if self._metadata['cache'] is False:
                # The data wasn't loaded from the cache, so caching is required now
                try:
                    self._mutex_version.acquire(30)
                    this_version = self._data['_version']
                    if _hook is not None and 'during_cache' in _hook:
                        _hook['during_cache']()
                    store_version = self._persistent.get(self._key)['_version']
                    if this_version == store_version:
                        self._volatile.set(self._key, self._data)
                except KeyNotFoundException:
                    raise ObjectNotFoundException('{0} with guid \'{1}\' could not be found'.format(
                        self.__class__.__name__, self._guid
                    ))
                except NoLockAvailableException:
                    pass
                finally:
                    self._mutex_version.release()

        # Freeze property creation
        self._frozen = True

        # Optionally, initialize some fields
        if data is not None:
            for prop in self._properties:
                if prop.name in data:
                    setattr(self, prop.name, data[prop.name])

        # Store original data
        self._original = copy.deepcopy(self._data)

    ##################################################
    # Helper methods for dynamic getting and setting #
    ##################################################

    def _add_property(self, prop):
        """
        Adds a simple property to the object
        """
        # pylint: disable=protected-access
        fget = lambda s: s._get_property(prop)
        fset = lambda s, v: s._set_property(prop, v)
        # pylint: enable=protected-access
        setattr(self.__class__, prop.name, property(fget, fset))

    def _add_relation_property(self, relation):
        """
        Adds a complex property to the object (hybrids)
        """
        # pylint: disable=protected-access
        fget = lambda s: s._get_relation_property(relation)
        fset = lambda s, v: s._set_relation_property(relation, v)
        gget = lambda s: s._get_guid_property(relation)
        # pylint: enable=protected-access
        setattr(self.__class__, relation.name, property(fget, fset))
        setattr(self.__class__, '{0}_guid'.format(relation.name), property(gget))

    def _add_list_property(self, attribute, islist):
        """
        Adds a list (readonly) property to the object
        """
        # pylint: disable=protected-access
        fget = lambda s: s._get_list_property(attribute)
        gget = lambda s: s._get_list_guid_property(attribute)
        # pylint: enable=protected-access
        setattr(self.__class__, attribute, property(fget))
        setattr(self.__class__, ('{0}_guids' if islist else '{0}_guid').format(attribute), property(gget))

    def _add_dynamic_property(self, dynamic):
        """
        Adds a dynamic property to the object
        """
        # pylint: disable=protected-access
        fget = lambda s: s._get_dynamic_property(dynamic)
        # pylint: enable=protected-access
        setattr(self.__class__, dynamic.name, property(fget))

    # Helper method supporting property fetching
    def _get_property(self, prop):
        """
        Getter for a simple property
        """
        return self._data[prop.name]

    def _get_relation_property(self, relation):
        """
        Getter for a complex property (hybrid)
        It will only load the object once and caches it for the lifetime of this object
        """
        attribute = relation.name
        if attribute not in self._objects:
            descriptor = Descriptor().load(self._data[attribute])
            self._objects[attribute] = descriptor.get_object(instantiate=True)
        return self._objects[attribute]

    def _get_guid_property(self, relation):
        """
        Getter for a foreign key property
        """
        attribute = relation.name
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
        remote_key = info['key']  # Foreign = remote
        datalist = DataList.get_relation_set(remote_class, remote_key, self.__class__, attribute, self.guid)
        if self._objects[attribute]['data'] is None:
            self._objects[attribute]['data'] = datalist
        else:
            self._objects[attribute]['data'].update(datalist)
        if info['list'] is True:
            return self._objects[attribute]['data']
        else:
            data = self._objects[attribute]['data']
            if len(data) > 1:
                raise InvalidRelationException('More than one element found in {0}'.format(attribute))
            return data[0] if len(data) == 1 else None

    def _get_list_guid_property(self, attribute):
        """
        Getter for guid list property
        """
        list_or_item = getattr(self, attribute)
        if list_or_item is None:
            return None
        if hasattr(list_or_item, '_guids'):
            return list_or_item._guids
        return list_or_item.guid

    def _get_dynamic_property(self, dynamic):
        """
        Getter for dynamic property, wrapping the internal data loading property
        in a caching layer
        """
        data_loader = getattr(self, '_{0}'.format(dynamic.name))
        return self._backend_property(data_loader, dynamic)

    # Helper method supporting property setting
    def _set_property(self, prop, value):
        """
        Setter for a simple property that will validate the type
        """
        self.dirty = True
        if value is None:
            self._data[prop.name] = value
        else:
            correct, allowed_types, given_type = DalToolbox.check_type(value, prop.property_type)
            if correct:
                self._data[prop.name] = value
            else:
                raise TypeError('Property {0} allows types {1}. {2} given'.format(
                    prop.name, str(allowed_types), given_type
                ))

    def _set_relation_property(self, relation, value):
        """
        Setter for a complex property (hybrid) that will validate the type
        """
        self.dirty = True
        attribute = relation.name
        if value is None:
            self._objects[attribute] = None
            self._data[attribute]['guid'] = None
        else:
            descriptor = Descriptor(value.__class__).descriptor
            if descriptor['identifier'] != self._data[attribute]['identifier']:
                if descriptor['type'] == self._data[attribute]['type']:
                    DataObject._logger.error('Corrupt descriptors: {0} vs {1}'.format(descriptor, self._data[attribute]))
                raise TypeError('An invalid type was given: {0} instead of {1}'.format(
                    descriptor['type'], self._data[attribute]['type']
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
                or key in (prop.name for prop in self._properties) \
                or key in (relation.name for relation in self._relations) \
                or key in (dynamic.name for dynamic in self._dynamics)
        if allowed:
            super(DataObject, self).__setattr__(key, value)
        else:
            raise RuntimeError('Property {0} does not exist on this object.'.format(key))

    ###############
    # Saving data #
    ###############

    def save(self, recursive=False, skip=None, _hook=None):
        """
        Save the object to the persistent backend and clear cache, making use
        of the specified conflict resolve settings.
        It will also invalidate certain caches if required. For example lists pointing towards this
        object
        :param recursive: Save related sub-objects recursively
        :param skip: Skip certain relations
        :param _hook:
        """
        if self.volatile is True:
            raise VolatileObjectException()

        tries = 0
        successful = False
        optimistic = True
        last_assert = None
        while successful is False:
            tries += 1
            if tries > 5:
                DataObject._logger.error('Raising RaceConditionException. Last AssertException: {0}'.format(last_assert))
                raise RaceConditionException()

            invalid_fields = []
            for prop in self._properties:
                if prop.mandatory is True and self._data[prop.name] is None:
                    invalid_fields.append(prop.name)
            for relation in self._relations:
                if relation.mandatory is True and self._data[relation.name]['guid'] is None:
                    invalid_fields.append(relation.name)
            if len(invalid_fields) > 0:
                raise MissingMandatoryFieldsException('Missing fields on {0}: {1}'.format(self._classname, ', '.join(invalid_fields)))

            if recursive:
                # Save objects that point to us (e.g. disk.vmachine - if this is disk)
                for relation in self._relations:
                    if relation.name != skip:  # disks will be skipped
                        item = getattr(self, relation.name)
                        if item is not None:
                            item.save(recursive=True, skip=relation.foreign_key)

                # Save object we point at (e.g. machine.vdisks - if this is machine)
                # @todo should be within the same transaction to avoid errors
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

            validation_keys = []
            for relation in self._relations:
                if self._data[relation.name]['guid'] is not None:
                    if relation.foreign_type is None:
                        cls = self.__class__
                    else:
                        cls = relation.foreign_type
                    validation_keys.append('{0}_{1}_{2}'.format(DataObject.NAMESPACE, cls.__name__.lower(), self._data[relation.name]['guid']))
            try:
                [_ for _ in self._persistent.get_multi(validation_keys)]
            except KeyNotFoundException:
                raise ObjectNotFoundException('One of the relations specified in {0} with guid \'{1}\' was not found'.format(
                    self.__class__.__name__, self._guid
                ))

            transaction = self._persistent.begin_transaction()
            if self._new is True:
                data = {'_version': 0}
                store_data = {'_version': 0}
            elif optimistic is True:
                self._persistent.assert_value(self._key, self._original, transaction=transaction)
                data = copy.deepcopy(self._original)
                store_data = copy.deepcopy(self._original)
            else:
                try:
                    current_data = self._persistent.get(self._key)
                except KeyNotFoundException:
                    raise ObjectNotFoundException('{0} with guid \'{1}\' was deleted'.format(
                        self.__class__.__name__, self._guid
                    ))
                self._persistent.assert_value(self._key, current_data, transaction=transaction)
                data = copy.deepcopy(current_data)
                store_data = copy.deepcopy(current_data)

            changed_fields = []
            data_conflicts = []
            for attribute in self._data.keys():
                if attribute == '_version':
                    continue
                if self._data[attribute] != self._original[attribute]:
                    # We changed this value
                    changed_fields.append(attribute)
                    if attribute in data and self._original[attribute] != data[attribute]:
                        # Some other process also wrote to the database
                        if self._datastore_wins is None:
                            # In case we didn't set a policy, we raise the conflicts
                            data_conflicts.append(attribute)
                        elif self._datastore_wins is False:
                            # If the data-store should not win, we just overwrite the data
                            data[attribute] = self._data[attribute]
                        # If the data-store should win, we discard/ignore our change
                    else:
                        # Normal scenario, saving data
                        data[attribute] = self._data[attribute]
                elif attribute not in data:
                    data[attribute] = self._data[attribute]
            for attribute in data.keys():
                if attribute == '_version':
                    continue
                if attribute not in self._data:
                    del data[attribute]
            if data_conflicts:
                raise ConcurrencyException('Got field conflicts while saving {0}. Conflicts: {1}'.format(
                    self._classname, ', '.join(data_conflicts)
                ))

            # Refresh internal data structure
            self._data = copy.deepcopy(data)

            # Update indexes
            base_index_key = 'ovs_index_{0}|{1}|{2}'
            for prop in self._properties:
                if prop.indexed is True:
                    if prop.property_type not in [str, int, float, long, bool]:
                        raise RuntimeError('An index can only be set on field of type str, int, float, long, or bool')
                    classname = self.__class__.__name__.lower()
                    key = prop.name
                    if self._new is False and key in changed_fields:
                        original_value = self._original[key]
                        index_key = base_index_key.format(classname, key, hashlib.sha1(str(original_value)).hexdigest())
                        indexed_keys = list(self._persistent.get_multi([index_key], must_exist=False))[0]
                        if indexed_keys is None:
                            self._persistent.assert_value(index_key, None, transaction=transaction)
                        elif self._key in indexed_keys:
                            self._persistent.assert_value(index_key, indexed_keys[:], transaction=transaction)
                            indexed_keys.remove(self._key)
                            if len(indexed_keys) == 0:
                                self._persistent.delete(index_key, transaction=transaction)
                            else:
                                self._persistent.set(index_key, indexed_keys, transaction=transaction)
                    if self._new is True or key in changed_fields:
                        new_value = self._data[key]
                        index_key = base_index_key.format(classname, key, hashlib.sha1(str(new_value)).hexdigest())
                        indexed_keys = list(self._persistent.get_multi([index_key], must_exist=False))[0]
                        if indexed_keys is None:
                            self._persistent.assert_value(index_key, None, transaction=transaction)
                            self._persistent.set(index_key, [self._key], transaction=transaction)
                        elif self._key not in indexed_keys:
                            self._persistent.assert_value(index_key, indexed_keys[:], transaction=transaction)
                            indexed_keys.append(self._key)
                            self._persistent.set(index_key, indexed_keys, transaction=transaction)

            # Update reverse index
            base_reverse_key = 'ovs_reverseindex_{0}_{1}|{2}|{3}'
            for relation in self._relations:
                key = relation.name
                original_guid = self._original[key]['guid']
                new_guid = self._data[key]['guid']
                if original_guid != new_guid:
                    if relation.foreign_type is None:
                        classname = self.__class__.__name__.lower()
                    else:
                        classname = relation.foreign_type.__name__.lower()
                    if original_guid is not None:
                        reverse_key = base_reverse_key.format(classname, original_guid, relation.foreign_key, self.guid)
                        self._persistent.delete(reverse_key, must_exist=False, transaction=transaction)
                    if new_guid is not None:
                        reverse_key = base_reverse_key.format(classname, new_guid, relation.foreign_key, self.guid)
                        self._persistent.assert_exists('{0}_{1}_{2}'.format(DataObject.NAMESPACE, classname, new_guid))
                        self._persistent.set(reverse_key, 0, transaction=transaction)

            # Invalidate property lists
            persistent_cache_key = DataList.generate_persistent_cache_key(self._classname)
            cache_keys = set()
            for key in list(self._persistent.prefix(persistent_cache_key)):
                _, field, cache_key = DataList.get_key_parts(key)
                if field in changed_fields or self._new is True:
                    cache_keys.add(cache_key)
            for cache_key in cache_keys:
                self._volatile.delete(cache_key)
            if self._new:
                # New item. All lists need to be removed
                self._persistent.delete_prefix(persistent_cache_key, transaction=transaction)
            else:
                for field in changed_fields:
                    self._persistent.delete_prefix(DataList.generate_persistent_cache_key(self._classname, field), transaction=transaction)

            # Validate unique constraints
            unique_key = 'ovs_unique_{0}_{{0}}_{{1}}'.format(self._classname)
            for prop in self._properties:
                if prop.unique is True:
                    if prop.property_type not in [str, int, float, long]:
                        raise RuntimeError('A unique constraint can only be set on field of type str, int, float, or long')
                    if self._new is False and prop.name in changed_fields:
                        key = unique_key.format(prop.name, hashlib.sha1(str(store_data[prop.name])).hexdigest())
                        self._persistent.assert_value(key, self._key, transaction=transaction)
                        self._persistent.delete(key, transaction=transaction)
                    key = unique_key.format(prop.name, hashlib.sha1(str(self._data[prop.name])).hexdigest())
                    if self._new is True or prop.name in changed_fields:
                        self._persistent.assert_value(key, None, transaction=transaction)
                    self._persistent.set(key, self._key, transaction=transaction)

            if _hook is not None:
                _hook()

            # Save the data
            self._data['_version'] += 1
            try:
                self._mutex_version.acquire(30)
                self._persistent.set(self._key, self._data, transaction=transaction)
                self._persistent.apply_transaction(transaction)
                self._volatile.delete(self._key)
                successful = True
            except KeyNotFoundException as ex:
                if 'ovs_unique' in ex.message and tries == 1:
                    optimistic = False
                elif ex.message != self._key:
                    raise
                else:
                    raise ObjectNotFoundException('{0} with guid \'{1}\' was deleted'.format(
                        self.__class__.__name__, self._guid
                    ))
            except AssertException as ex:
                if 'ovs_unique' in str(ex.message):
                    field = str(ex.message).split('_', 3)[-1].rsplit('_', 1)[0]
                    raise UniqueConstraintViolationException('The unique constraint on {0}.{1} was violated'.format(
                        self.__class__.__name__, field
                    ))
                last_assert = ex
                optimistic = False
                self._mutex_version.release()  # Make sure it's released before a sleep
                time.sleep(randint(0, 25) / 100.0)
            finally:
                self._mutex_version.release()

        self.invalidate_dynamics()
        self._original = copy.deepcopy(self._data)

        self.dirty = False
        self._new = False

    ###############
    # Other CRUDs #
    ###############

    def delete(self, abandon=None, _hook=None):
        """
        Delete the given object. It also invalidates certain lists
        :param abandon: Indicates whether(which) linked objects can be unlinked. Use with caution
        :param _hook: Hook
        """
        if self.volatile is True:
            raise VolatileObjectException()

        tries = 0
        successful = False
        optimistic = True
        last_assert = None
        while successful is False:
            tries += 1
            if tries > 5:
                DataObject._logger.error('Raising RaceConditionException. Last AssertException: {0}'.format(last_assert))
                raise RaceConditionException()

            transaction = self._persistent.begin_transaction()

            # Check foreign relations
            relations = RelationMapper.load_foreign_relations(self.__class__)
            if relations is not None:
                for key, info in relations.iteritems():
                    items = getattr(self, key)
                    if info['list'] is True:
                        if len(items) > 0:
                            if abandon is not None and (key in abandon or '_all' in abandon):
                                for item in items.itersafe():
                                    setattr(item, info['key'], None)
                                    try:
                                        item.save()
                                    except ObjectNotFoundException:
                                        pass
                            else:
                                multi = 'are {0} items'.format(len(items)) if len(items) > 1 else 'is 1 item'
                                raise LinkedObjectException('There {0} left in self.{1}'.format(multi, key))
                    elif items is not None:
                        # No list (so a 1-to-1 relation), so there should be an object, or None
                        item = items  # More clear naming
                        if abandon is not None and (key in abandon or '_all' in abandon):
                            setattr(item, info['key'], None)
                            try:
                                item.save()
                            except ObjectNotFoundException:
                                pass
                        else:
                            raise LinkedObjectException('There is still an item linked in self.{0}'.format(key))

            # Delete the object out of the persistent store
            try:
                self._persistent.delete(self._key, transaction=transaction)
            except KeyNotFoundException:
                pass

            # Clean indexes
            base_index_key = 'ovs_index_{0}|{1}|{2}'
            for prop in self._properties:
                if prop.indexed is True:
                    classname = self.__class__.__name__.lower()
                    key = prop.name
                    current_value = self._original[key]
                    index_key = base_index_key.format(classname, key, hashlib.sha1(str(current_value)).hexdigest())
                    indexed_keys = list(self._persistent.get_multi([index_key], must_exist=False))[0]
                    if indexed_keys is not None and self._key in indexed_keys:
                        self._persistent.assert_value(index_key, indexed_keys[:], transaction=transaction)
                        indexed_keys.remove(self._key)
                        if len(indexed_keys) == 0:
                            self._persistent.delete(index_key, transaction=transaction)
                        else:
                            self._persistent.set(index_key, indexed_keys, transaction=transaction)

            # Clean reverse indexes
            base_reverse_key = 'ovs_reverseindex_{0}_{1}|{2}|{3}'
            for relation in self._relations:
                key = relation.name
                original_guid = self._original[key]['guid']
                if original_guid is not None:
                    if relation.foreign_type is None:
                        classname = self.__class__.__name__.lower()
                    else:
                        classname = relation.foreign_type.__name__.lower()
                    reverse_key = base_reverse_key.format(classname, original_guid, relation.foreign_key, self.guid)
                    self._persistent.delete(reverse_key, must_exist=False, transaction=transaction)

            # Invalidate property lists
            cache_keys = set()
            persistent_cache_key = DataList.generate_persistent_cache_key(self._classname)
            for key in list(self._persistent.prefix(persistent_cache_key)):
                cache_key = DataList.extract_cache_key(key)
                if cache_key not in cache_keys:
                    cache_keys.add(cache_key)
                    self._volatile.delete(cache_key)
            self._persistent.delete_prefix(persistent_cache_key, transaction=transaction)

            # Delete constraints
            if optimistic is False:
                store_data = self._persistent.get(self._key)
            else:
                store_data = self._original
            unique_key = 'ovs_unique_{0}_{{0}}_{{1}}'.format(self._classname)
            for prop in self._properties:
                if prop.unique is True:
                    if prop.property_type not in [str, int, float, long]:
                        raise RuntimeError('A unique constraint can only be set on field of type str, int, float, or long')
                    key = unique_key.format(prop.name, hashlib.sha1(str(store_data[prop.name])).hexdigest())
                    self._persistent.assert_value(key, self._key, transaction=transaction)
                    self._persistent.delete(key, transaction=transaction)

            if _hook is not None:
                _hook()

            try:
                self._persistent.apply_transaction(transaction)
                successful = True
            except KeyNotFoundException as ex:
                if 'ovs_unique' in ex.message and tries == 1:
                    optimistic = False
                elif ex.message != self._key:
                    raise
                else:
                    successful = True
            except AssertException as ex:
                if 'ovs_unique' in str(ex.message):
                    optimistic = False
                last_assert = ex

        # Delete the object and its properties out of the volatile store
        self.invalidate_dynamics()
        self._volatile.delete(self._key)

    # Discard all pending changes
    def discard(self):
        """
        Discard all pending changes, reloading the data from the persistent backend
        """
        self.__init__(guid=self._guid,
                      datastore_wins=self._datastore_wins)

    def invalidate_dynamics(self, properties=None):
        """
        Invalidates all dynamic property caches. Use with caution, as this action can introduce
        a short performance hit.
        :param properties: Properties to invalidate
        """
        if properties is not None and not isinstance(properties, list):
            properties = [properties]
        for dynamic in self._dynamics:
            if properties is None or dynamic.name in properties:
                key = '{0}_{1}'.format(self._key, dynamic.name)
                mutex = volatile_mutex(key)
                try:
                    if dynamic.locked:
                        mutex.acquire()
                    self._volatile.delete(key)
                finally:
                    mutex.release()

    def invalidate_cached_objects(self):
        """
        Invalidates cached objects so they are reloaded when used.
        """
        for relation in self._relations:
            self._objects.pop(relation.name, None)

    def export(self):
        """
        Exports this object's data for import in another object
        """
        return dict((prop.name, self._data[prop.name]) for prop in self._properties)

    def serialize(self, depth=0, contents=None):
        """
        Serializes the internal data, getting rid of certain metadata like descriptors
        :param depth: Depth of relations to serialize
        """
        data = {'guid': self.guid}
        hybrid = type(self)
        if not isinstance(contents, ContentOptions):
            contents = ContentOptions(contents)
        for relation in self._relations:
            key = relation.name
            if depth == 0:
                data['{0}_guid'.format(key)] = self._data[key]['guid']
            else:
                instance = getattr(self, key)
                if instance is not None:
                    data[key] = getattr(self, key).serialize(depth=(depth - 1))
                else:
                    data[key] = None
        for prop in self._properties:
            data[prop.name] = self._data[prop.name]
        if '_dynamics' in contents:  #hier dynamics geskipt, mag weg?
            for dynamic in self._dynamics:
                data[dynamic.name] = getattr(self, dynamic.name)
        foreign_relations = RelationMapper.load_foreign_relations(hybrid)  # To many side of things, items pointing towards this object
        if contents.has_content is False or (foreign_relations is None and len(hybrid._relations) == 0) or depth == 0:
                    return
        # Foreign relations is a dict, relations is a relation object, need to differentiate
        relation_contents = contents.get_option('_relations_contents')
        relation_contents_options = copy.deepcopy(contents) if relation_contents == 're-use' else ContentOptions(relation_contents)
        relations_data = {'foreign': foreign_relations or {}, 'own': hybrid._relations}
        for relation_type, relations in relations_data.iteritems():
            for relation in relations:
                relation_key = relation.name if relation_type == 'own' else relation
                relation_hybrid = relation.foreign_type if relation_type == 'own' else Descriptor().load(relations[relation]['class']).get_object()
                # Possible extra content supplied for a relation
                relation_content = contents.get_option('_relation_contents_{0}'.format(relation_key))
                if relation_content is None and relation_contents == 're-use':
                    relation_content_options = relation_contents_options
                else:
                    relation_content_options = ContentOptions(relation_content)
                # Use the depth given by the contents when it's the first item to serialize
                relation_depth = contents.get_option('_relations_depth', 1 if relation_content_options.has_content else 0) if depth is None else depth
                if relation_depth is None:  # Can be None when no value is give to _relations_depth
                    relation_depth = 0
                if relation_depth == 0:
                    continue
                # # @Todo prevent the same one-to-one relations from being serialized multiple times? Not sure if helpful though
                # todo fix recursive serializations
                # self.fields[relation_key] = FullSerializer(relation_hybrid, contents=relation_content_options, depth=relation_depth - 1)

        return data

    def copy(self, other_object, include=None, exclude=None, include_relations=False):
        """
        Copies all _properties (and optionally _relations) properties over from a given hybrid to
        self. One can pass in a list of properties that should be copied, or a list of properties
        that should not be copied. Exclude > Include
        :param other_object: Other object to copy properties from into current 1
        :param include: Properties to include
        :param exclude: Properties to exclude
        :param include_relations: Include all relations
        """
        if include is not None and not isinstance(include, list):
            raise TypeError('Argument include should be None or a list of strings')
        if exclude is not None and not isinstance(exclude, list):
            raise TypeError('Argument exclude should be None or a list of strings')
        if self.__class__.__name__ != other_object.__class__.__name__:
            raise TypeError('Properties can only be loaded from hybrids of the same type')

        all_properties = [prop.name for prop in self._properties]
        all_relations = [relation.name for relation in self._relations]
        properties_to_copy = all_properties if include is None else include
        if include_relations:
            properties_to_copy += all_relations

        if exclude:
            properties_to_copy = [p for p in properties_to_copy if p not in exclude]

        possible_options = all_properties + (all_relations if include_relations else [])
        properties_to_copy = [p for p in properties_to_copy if p in possible_options]

        for key in properties_to_copy:
            setattr(self, key, getattr(other_object, key))

    def clone(self, reload_object=False):
        """
        Make an identical clone of the DataObject
        :param reload_object: Reload the object data again when cloning
        :type reload_object: bool
        """
        if reload_object:
            clone = self.__class__(self.guid)
        else:
            clone = self.__class__(self.guid,
                                   data=self._data,
                                   datastore_wins=self._datastore_wins,
                                   volatile=self.volatile)
        return clone

    def updated_on_datastore(self):
        """
        Checks whether this object has been modified on the data-store
        """
        if self.volatile is True:
            return False

        this_version = self._data['_version']
        cached_object = self._volatile.get(self._key)
        if cached_object is None:
            try:
                backend_version = self._persistent.get(self._key)['_version']
            except KeyNotFoundException:
                backend_version = -1
        else:
            backend_version = cached_object['_version']
        return this_version != backend_version

    def get_timings(self):
        """
        Retrieve the timings for collecting the dynamic properties of this DataObject
        """
        return self._dynamic_timings

    def reset_timings(self):
        """
        Reset the timings it took for collecting the dynamic properties of this DataObject
        """
        self._dynamic_timings = {}

    ##############
    # Properties #
    ##############

    @property
    def guid(self):
        """
        The primary key of the object
        """
        return self._guid

    ##################
    # Helper methods #
    ##################

    def _backend_property(self, fct, dynamic):
        """
        Handles the internal caching of dynamic properties
        """
        caller_name = dynamic.name
        cache_key = '{0}_{1}'.format(self._key, caller_name)
        mutex = volatile_mutex(cache_key)
        try:
            cached_data = self._volatile.get(cache_key)
            if cached_data is None:
                if dynamic.locked:
                    mutex.acquire()
                    cached_data = self._volatile.get(cache_key)
                if cached_data is None:
                    function_info = inspect.getargspec(fct)
                    start = time.time()
                    if 'dynamic' in function_info.args:
                        dynamic_data = fct(dynamic=dynamic)  # Load data from backend
                    else:
                        dynamic_data = fct()
                    self._dynamic_timings[caller_name] = time.time() - start
                    correct, allowed_types, given_type = DalToolbox.check_type(dynamic_data, dynamic.return_type)
                    if not correct:
                        raise TypeError('Dynamic property {0} allows types {1}. {2} given'.format(
                            caller_name, str(allowed_types), given_type
                        ))
                    # Set the result of the function into a dict to avoid None retrieved from the cache when key is not found
                    cached_data = {'data': dynamic_data}
                    if dynamic.timeout > 0:
                        self._volatile.set(cache_key, cached_data, dynamic.timeout)
            return DalToolbox.convert_unicode_to_string(cached_data['data'])
        finally:
            mutex.release()

    def __repr__(self):
        """
        A short self-representation
        """
        return '<{0} (guid: {1}, at: {2})>'.format(self.__class__.__name__, self._guid, hex(id(self)))

    def __str__(self):
        """
        The string representation of a DataObject is the serialized value
        """
        # cls= acts as a fallback
        return json.dumps(self.serialize(), indent=4, cls=DataObjectAttributeEncoder)

    def __hash__(self):
        """
        Defines a hashing equivalent for a given object. The key (object type and guid) is considered to be identifying
        """
        return hash(self._key)

    def __eq__(self, other):
        """
        Checks whether two objects are the same.
        """
        return self.__hash__() == other.__hash__()

    def __ne__(self, other):
        """
        Checks whether two objects are not the same.
        """
        return not self.__eq__(other)

    def _benchmark(self, iterations=100, dynamics=None):
        """
        Benchmark the dynamics
        CAUTION: when a dynamic calls another dynamic in its implementations, the timings might be off
        as the implementing dynamic might be cached
        :param iterations:  amount of iterations
        :param dynamics: dynamics to benchmark
        :return:
        """
        import time
        begin = time.time()
        stats = {}
        totals = []
        if dynamics is None:
            dynamics = self._dynamics
        else:
            if isinstance(dynamics, basestring):
                dynamics = [dynamics]
            dynamics = [dyn for dyn in self._dynamics if dyn.name in dynamics]
        for _ in range(iterations):
            istart = time.time()
            for dynamic in dynamics:
                start = time.time()
                fct = getattr(self, '_{0}'.format(dynamic.name))
                function_info = inspect.getargspec(fct)
                if 'dynamic' in function_info.args:
                    fct(dynamic=dynamic)
                else:
                    fct()
                duration = time.time() - start
                if dynamic.name not in stats:
                    stats[dynamic.name] = []
                stats[dynamic.name].append(duration)
            totals.append(time.time() - istart)
        print "Object: {0}('{1}')".format(self.__class__.__name__, self._guid)
        for dyn in stats:
            print '- {0}: avg {1:.3f}s (min: {2:.3f}s, max: {3:.3f}s)'.format(dyn, sum(stats[dyn]) / float(len(stats[dyn])), min(stats[dyn]), max(stats[dyn]))
        print 'Took {0:.3f}s for {1} iterations'.format(time.time() - begin, iterations)
        print 'Avg: {0:.3f}s, min: {1:.3f}s, max: {2:.3f}s'.format(sum(totals) / float(len(totals)), min(totals), max(totals))

    @staticmethod
    def enumerator(name, items):
        """
        Generates an enumerator
        :param name: Name of enumerator
        :param items: Enumerator items
        """
        class Enumerator(dict):
            """
            Enumerator class
            """
            def __init__(self, *args, **kwargs):
                super(Enumerator, self).__init__(*args, **kwargs)

        if isinstance(items, list):
            enumerator = Enumerator(zip(items, items))
        elif isinstance(items, dict):
            enumerator = Enumerator(**items)
        else:
            raise ValueError("Argument 'items' should be a list or a dict. A '{0}' was given".format(type(items)))
        enumerator.__name__ = name
        for item in enumerator:
            setattr(enumerator, item, enumerator[item])
        return enumerator

class UnsupportContentException(ValueError):
    """
    Exception raised when an unsupported content string has been given
    """
    pass

class ContentOptions(object):
    """
    Content options to give to the serializer
    """
    OPTION_TYPES = {'_relations_depth': (int, None, False),
                    '_relations_content': (str, None, False)}
    OPTION_STARTS = {'_relation_contents_': (str, None, False)}

    def __init__(self, contents=None):
        """
        Initializes a ContentOptions object based on a string representing the contents
        :param contents: Comma separated string or list of contents to serialize
        When contents is given, all non-dynamic properties would be serialized
        Further options are:
        - _dynamics: Include all dynamic properties
        - _relations: Include foreign keys and lists of primary keys of linked objects
        - _relations_contents: Apply the contents to the relations. The relation contents can be a bool or a new contents item
          - If the relations_contents=re-use: the current contents are also applied to the relation object
          - If the relations_contents=contents list: That item is subjected to the same rules as other contents
        - _relation_contents_RELATION_NAME: Apply the contents the the given relation. Same rules as _relation_contents apply here
        _ _relations_depth: Depth of relational serialization. Defaults to 0.
        Specifying a form of _relations_contents change the depth to 1 (if depth was 0) as the relation is to be serialized
        Specifying it 2 with _relations_contents given will serialize the relations of the fetched relation. This causes a chain of serializations
        - dynamic_property_1,dynamic_property_2 (results in static properties plus 2 dynamic properties)
        Properties can also be excluded by prefixing the field with '-':
        - contents=_dynamic,-dynamic_property_2,_relations (static properties, all dynamic properties except for dynamic_property_2 plus all relations)
        Relation serialization can be done by asking for it:
        - contents=_relations,_relations_contents=re-use
        :type contents: list or str
        :raises UnsupportedContentException: If a content string is passed which is not valid
        """
        super(ContentOptions, self).__init__()

        verify_params = copy.deepcopy(self.OPTION_TYPES)
        self.content_options = {}
        self.has_content = False
        if contents is not None:
            if isinstance(contents, basestring):
                contents_list = contents.split(',')
            elif isinstance(contents, list):
                contents_list = contents
            else:
                raise UnsupportContentException('Contents should be a comma-separated list instead of \'{0}\''.format(contents))
        else:
            return
        self.has_content = True
        errors = []
        for option in contents_list:
            if not isinstance(option, basestring):
                errors.append('Provided option \'{0}\' is not a string but \'{1}\''.format(option, type(option)))
                continue
            split_options = option.split('=')
            if len(split_options) > 2:  # Unsupported format
                errors.append('Found \'=\' multiple times for entry {0}'.format(split_options[0]))
                continue
            starts = [v for k, v in self.OPTION_STARTS.iteritems() if option.startswith(k)]
            if len(starts) == 1:
                verify_params[option] = starts[0]
            # Convert to some work-able types
            value = split_options[1] if len(split_options) == 2 else None
            if isinstance(value, str) and value.isdigit():
                value = int(value)
            self.content_options[split_options[0]] = value
        errors.extend(ExtensionsToolbox.verify_required_params(verify_params, self.content_options, return_errors=True))
        if len(errors) > 0:
            raise UnsupportContentException('Contents is using an unsupported format: \n - {0}'.format('\n - '.join(errors)))

    def __contains__(self, item):  # In operator
        return self.has_option(item)

    def has_option(self, option):
        """
        Returns True if the contentOption has the given option
        :param option: Option to search for
        :type option: str
        :return: bool
        """
        return option in self.content_options

    def get_option(self, option, default=None):
        """
        Returns the value of the given option
        :param option: Option to retrieve the value for
        :type option: str
        :param default: Default value when the key does not exist
        :type default: any
        :return: None if the value is not found else the value specified
        :rtype: NoneType or any
        """
        return self.content_options.get(option, default)

    def set_option(self, option, value, must_exist=True):
        """
        Sets an options value
        :param option: Option to set the value for
        :type option: str
        :param value: Value of the option
        :type value: any
        :param must_exist: The option must already exist before setting the option
        :type must_exist: bool
        :return: The given value (None if the key does not exist)
        :rtype: NoneType or any
        """
        if must_exist is True and self.has_option(option) is False:
            return None
        self.content_options[option] = value
        return value

    def increment_option(self, option):
        """
        Increments the value for the given option. If the option is not present or no value passed, this won't do anything
        :param option: Option to increment the value for
        :type option: str
        :return: The new value or None if they key is not found or not an integer
        :rtype: int or NoneType
        """
        value = self.get_option(option)
        if isinstance(value, int):
            return self.set_option(option, value + 1, must_exist=True)
        return None  # For readability

    def decrement_options(self, option):
        """
        Decrements the value for the given option. If the option is not present or no value passed, this won't do anything
        :param option: Option to increment the value for
        :type option: str
        :return: The new value or None if they key is not found or not an integer
        :rtype: int or NoneType
        """
        value = self.get_option(option)
        if isinstance(value, int):
            return self.set_option(option, value - 1, must_exist=True)
        return None  # For readability
