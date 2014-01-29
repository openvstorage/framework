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
Basic test module
"""
import uuid
import time
import sys
from unittest import TestCase
from ovs.dal.exceptions import *
from ovs.dal.dataobjectlist import DataObjectList
from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore
from ovs.dal.tests.mockups import VolumeStorageRouter, LoaderModule, FactoryModule


class Basic(TestCase):
    """
    The basic unittestsuite will test all basic functionality of the DAL framework
    It will also try accessing all dynamic properties of all hybrids making sure
    that code actually works. This however means that all loaded 3rd party libs
    need to be mocked
    """

    VDisk = None
    VolatileMutex = None
    TestMachine = None
    TestDisk = None
    DataList = None
    HybridRunner = None
    Descriptor = None
    RelationMapper = None,
    VolatileFactory = None,
    PersistentFactory = None

    @classmethod
    def setUpClass(cls):
        """
        Sets up the unittest, mocking a certain set of 3rd party libraries and extensions.
        This makes sure the unittests can be executed without those libraries installed
        """
        # Replace mocked classes
        sys.modules['ovs.extensions.storageserver.volumestoragerouter'] = VolumeStorageRouter
        sys.modules['ovs.plugin.injection.loader'] = LoaderModule
        sys.modules['ovs.extensions.hypervisor.factory'] = FactoryModule

        # Importing, preparing, mocking, ...
        from ovs.extensions.storage.persistentfactory import PersistentFactory
        from ovs.extensions.storage.volatilefactory import VolatileFactory
        global VolatileFactory
        global PersistentFactory
        _ = PersistentFactory, VolatileFactory
        PersistentFactory.store = DummyPersistentStore()
        VolatileFactory.store = DummyVolatileStore()
        from ovs.dal.relations.relations import RelationMapper
        from ovs.dal.helpers import HybridRunner, Descriptor
        from ovs.dal.datalist import DataList
        from ovs.dal.hybrids.vdisk import VDisk
        from ovs.extensions.generic.volatilemutex import VolatileMutex
        from ovs.dal.hybrids.t_testmachine import TestMachine
        from ovs.dal.hybrids.t_testdisk import TestDisk
        global RelationMapper
        global HybridRunner
        global Descriptor
        global DataList
        global VDisk
        global VolatileMutex
        global TestMachine
        global TestDisk
        _ = VDisk, VolatileMutex, TestMachine, TestDisk, DataList, HybridRunner, Descriptor, RelationMapper

        # Cleaning storage
        VolatileFactory.store.clean()
        PersistentFactory.store.clean()
        # Test to make sure the clean doesn't raise if there is nothing to clean
        VolatileFactory.store.clean()
        PersistentFactory.store.clean()

    @classmethod
    def setUp(cls):
        """
        (Re)Sets the stores on every test
        """
        PersistentFactory.store = DummyPersistentStore()
        VolatileFactory.store = DummyVolatileStore()

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the unittest
        """
        pass

    def test_invalidobject(self):
        """
        Validates the behavior when a non-existing object is loaded
        """
        # Loading an non-existing object should raise
        self.assertRaises(ObjectNotFoundException, TestDisk, uuid.uuid4(), None)

    def test_newobjet_delete(self):
        """
        Validates the behavior on object deletions
        """
        disk = TestDisk()
        disk.save()
        # An object should always have a guid
        guid = disk.guid
        self.assertIsNotNone(guid, 'Guid should not be None')
        # After deleting, the object should not be retreivable
        disk.delete()
        self.assertRaises(Exception, TestDisk, guid, None)

    def test_discard(self):
        """
        Validates the behavior regarding pending changes discard
        """
        disk = TestDisk()
        disk.name = 'one'
        disk.save()
        disk.name = 'two'
        # Discarding an object should rollback all changes
        disk.discard()
        self.assertEqual(disk.name, 'one', 'Data should be discarded')
        disk.delete()

    def test_updateproperty(self):
        """
        Validates the behavior regarding updating properties
        """
        disk = TestDisk()
        disk.name = 'test'
        disk.description = 'desc'
        # A property should be writable
        self.assertIs(disk.name, 'test', 'Property should be updated')
        self.assertIs(disk.description, 'desc', 'Property should be updated')
        disk.delete()

    def test_preinit(self):
        """
        Validates whether initial data is loaded on object creation
        """
        disk = TestDisk(data={'name': 'diskx'})
        disk.save()
        self.assertEqual(disk.name, 'diskx', 'Disk name should be preloaded')
        disk.delete()

    def test_datapersistent(self):
        """
        Validates whether data is persisted correctly
        """
        disk = TestDisk()
        guid = disk.guid
        disk.name = 'test'
        disk.save()
        # Retreiving an object should return the data as when it was saved
        disk2 = TestDisk(guid)
        self.assertEqual(disk.name, disk2.name, 'Data should be persistent')
        disk.delete()

    def test_readonlyproperty(self):
        """
        Validates whether all dynamic properties are actually read-only
        """
        disk = TestDisk()
        # Readonly properties should return data
        self.assertIsNotNone(disk.used_size, 'RO property should return data')
        disk.delete()

    def test_datastorewins(self):
        """
        Validates the "datastore_wins" behavior in the usecase where it wins
        """
        disk = TestDisk()
        disk.name = 'initial'
        disk.save()
        disk2 = TestDisk(disk.guid, datastore_wins=True)
        disk.name = 'one'
        disk.save()
        disk2.name = 'two'
        disk2.save()
        # With datastore_wins set to True, the datastore wins concurrency conflicts
        self.assertEqual(disk2.name, 'one', 'Data should be overwritten')
        disk.delete()

    def test_datastoreloses(self):
        """
        Validates the "datastore_wins" behavior in the usecase where it loses
        """
        disk = TestDisk()
        disk.name = 'initial'
        disk.save()
        disk2 = TestDisk(disk.guid, datastore_wins=False)
        disk.name = 'one'
        disk.save()
        disk2.name = 'two'
        disk2.save()
        # With datastore_wins set to False, the datastore loses concurrency conflicts
        self.assertEqual(disk2.name, 'two', 'Data should not be overwritten')
        disk.delete()

    def test_silentdatarefresh(self):
        """
        Validates whether the default scenario (datastore_wins=False) will execute silent
        data refresh
        """
        disk = TestDisk()
        disk.name = 'initial'
        disk.save()
        disk2 = TestDisk(disk.guid, datastore_wins=False)
        disk.name = 'one'
        disk.save()
        disk2.name = 'two'
        disk2.save()
        disk.save()  # This should not overwrite anything but instead refresh data
        # With datastore_wins set to False, the datastore loses concurrency conflicts
        self.assertEqual(disk2.name, 'two', 'Data should not be overwritten')
        self.assertEqual(disk.name, 'two', 'Data should be refreshed')
        disk.delete()

    def test_datastoreraises(self):
        """
        Validates the "datastore_wins" behavior in the usecase where it's supposed to raise
        """
        disk = TestDisk()
        disk.name = 'initial'
        disk.save()
        disk2 = TestDisk(disk.guid, datastore_wins=None)
        disk.name = 'one'
        disk.save()
        disk2.name = 'two'
        # with datastore_wins set to None, concurrency conflicts are raised
        self.assertRaises(ConcurrencyException, disk2.save)
        disk.delete()

    def test_volatileproperty(self):
        """
        Validates the volatile behavior of dynamic properties
        """
        disk = TestDisk()
        disk.size = 1000000
        value = disk.used_size
        # Volatile properties should be stored for the correct amount of time
        time.sleep(2)
        self.assertEqual(disk.used_size, value, 'Value should still be from cache')
        time.sleep(2)
        self.assertEqual(disk.used_size, value, 'Value should still be from cache')
        time.sleep(2)
        # ... after which they should be reloaded from the backend
        self.assertNotEqual(disk.used_size, value, 'Value should be different')
        disk.delete()

    def test_persistency(self):
        """
        Validates whether the object is fetches from the correct storage backend
        """
        disk = TestDisk()
        disk.name = 'test'
        disk.save()
        # Right after a save, the cache is invalidated
        disk2 = TestDisk(disk.guid)
        self.assertFalse(disk2._metadata['cache'],
                         'Object should be retreived from persistent backend')
        # Subsequent calls will retreive the object from cache
        disk3 = TestDisk(disk.guid)
        self.assertTrue(disk3._metadata['cache'],
                        'Object should be retreived from cache')
        # After the object expiry passed, it will be retreived from backend again
        DummyVolatileStore().delete(disk._key)  # We clear the entry
        disk4 = TestDisk(disk.guid)
        self.assertFalse(disk4._metadata['cache'],
                         'Object should be retreived from persistent backend')
        disk.delete()

    def test_objectproperties(self):
        """
        Validates the correctness of all hybrid objects:
        * They should contain all required properties
        * Properties should have the correct type
        * All dynamic properties should be implemented
        """
        # Some stuff here to dynamically test all hybrid properties
        for cls in HybridRunner.get_hybrids():
            relation_info = RelationMapper.load_foreign_relations(cls)
            remote_properties = []
            if relation_info is not None:
                for key in relation_info.keys():
                    remote_properties.append(key)
            # Make sure certain attributes are correctly set
            self.assertIsInstance(cls._blueprint, dict, '_blueprint required: %s' % cls.__name__)
            self.assertIsInstance(cls._relations, dict, '_relations required: %s' % cls.__name__)
            self.assertIsInstance(cls._expiry, dict, '_expiry required: %s' % cls.__name__)
            # Check types
            allowed_types = [int, float, str, bool, list, dict]
            for key in cls._blueprint:
                is_allowed_type = cls._blueprint[key][1] in allowed_types \
                    or isinstance(cls._blueprint[key][1], list)
                self.assertTrue(is_allowed_type,
                                '_blueprint types in %s should be one of %s'
                                % (cls.__name__, str(allowed_types)))
            for key in cls._expiry:
                is_allowed_type = cls._expiry[key][1] in allowed_types \
                    or isinstance(cls._expiry[key][1], list)
                self.assertTrue(is_allowed_type,
                                '_expiry types in %s should be one of %s'
                                % (cls.__name__, str(allowed_types)))
            instance = cls()
            for key, default in cls._blueprint.iteritems():
                self.assertEqual(getattr(instance, key), default[0],
                                 'Default property set correctly')
            # Make sure the type can be instantiated
            self.assertIsNotNone(instance.guid)
            properties = []
            for item in dir(instance):
                if hasattr(cls, item) and isinstance(getattr(cls, item), property):
                    properties.append(item)
            # All expiries should be implemented
            missing_props = []
            for attribute in instance._expiry.keys():
                if attribute not in properties:
                    missing_props.append(attribute)
                else:  # ... and should work
                    _ = getattr(instance, attribute)
            self.assertEqual(len(missing_props), 0,
                             'Missing dynamic properties in %s: %s'
                             % (cls.__name__, missing_props))
            # An all properties should be either in the blueprint, relations or expiry
            missing_metadata = []
            for prop in properties:
                found = prop in cls._blueprint \
                    or prop in cls._relations \
                    or prop in ['%s_guid' % key for key in cls._relations.keys()] \
                    or prop in cls._expiry \
                    or prop in remote_properties \
                    or prop == 'guid'
                if not found:
                    missing_metadata.append(prop)
            self.assertEqual(len(missing_metadata), 0,
                             'Missing metadata for properties in %s: %s'
                             % (cls.__name__, missing_metadata))
            instance.delete()

    def test_queries(self):
        """
        Validates whether executing queries returns the expected results
        """
        machine = TestMachine()
        machine.name = 'machine'
        machine.save()
        for i in xrange(0, 20):
            disk = TestDisk()
            disk.name = 'test_%d' % i
            disk.size = i
            if i < 10:
                disk.machine = machine
            else:
                disk.storage = machine
            disk.save()
        self.assertEqual(len(machine.disks), 10, 'query should find added machines')
        # pylint: disable=line-too-long
        list_1 = DataList({'object': TestDisk,
                           'data': DataList.select.COUNT,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': [('size', DataList.operator.EQUALS, 1)]}}).data  # noqa
        self.assertEqual(list_1, 1, 'list should contain int 1')
        list_2 = DataList({'object': TestDisk,
                           'data': DataList.select.DESCRIPTOR,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': [('size', DataList.operator.EQUALS, 1)]}}).data  # noqa
        found_object = Descriptor().load(list_2[0]).get_object(True)
        self.assertEqual(found_object.name, 'test_1', 'list should contain corret machine')
        list_3 = DataList({'object': TestDisk,
                           'data': DataList.select.COUNT,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': [('size', DataList.operator.GT, 3),
                                               ('size', DataList.operator.LT, 6)]}}).data  # noqa
        self.assertEqual(list_3, 2, 'list should contain int 2')  # disk 4 and 5
        list_4 = DataList({'object': TestDisk,
                           'data': DataList.select.COUNT,
                           'query': {'type': DataList.where_operator.OR,
                                     'items': [('size', DataList.operator.LT, 3),
                                               ('size', DataList.operator.GT, 6)]}}).data  # noqa
        # at least disk 0, 1, 2, 7, 8, 9, 10-19
        self.assertGreaterEqual(list_4, 16, 'list should contain >= 16')
        list_5 = DataList({'object': TestDisk,
                           'data': DataList.select.COUNT,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': [('machine.guid', DataList.operator.EQUALS, machine.guid),  # noqa
                                               {'type': DataList.where_operator.OR,
                                                'items': [('size', DataList.operator.LT, 3),
                                                          ('size', DataList.operator.GT, 6)]}]}}).data  # noqa
        self.assertEqual(list_5, 6, 'list should contain int 6')  # disk 0, 1, 2, 7, 8, 9
        list_6 = DataList({'object': TestDisk,
                           'data': DataList.select.COUNT,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': [('size', DataList.operator.LT, 3),
                                               ('size', DataList.operator.GT, 6)]}}).data  # noqa
        self.assertEqual(list_6, 0, 'list should contain int 0')  # no disks
        list_7 = DataList({'object': TestDisk,
                           'data': DataList.select.COUNT,
                           'query': {'type': DataList.where_operator.OR,
                                     'items': [('machine.guid', DataList.operator.EQUALS, '123'),  # noqa
                                               ('used_size', DataList.operator.EQUALS, -1),
                                               {'type': DataList.where_operator.AND,
                                                'items': [('size', DataList.operator.GT, 3),
                                                          ('size', DataList.operator.LT, 6)]}]}}).data  # noqa
        self.assertEqual(list_7, 2, 'list should contain int 2')  # disk 4 and 5
        list_8 = DataList({'object': TestDisk,
                           'data': DataList.select.COUNT,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': [('machine.name', DataList.operator.EQUALS, 'machine'),  # noqa
                                               ('name', DataList.operator.EQUALS, 'test_3')]}}).data  # noqa
        self.assertEqual(list_8, 1, 'list should contain int 1')  # disk 3
        list_9 = DataList({'object': TestDisk,
                           'data': DataList.select.COUNT,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': [('size', DataList.operator.GT, 3),
                                               {'type': DataList.where_operator.AND,
                                                'items': [('size', DataList.operator.LT, 6)]}]}}).data  # noqa
        self.assertEqual(list_9, 2, 'list should contain int 2')  # disk 4 and 5
        list_10 = DataList({'object': TestDisk,
                            'data': DataList.select.COUNT,
                            'query': {'type': DataList.where_operator.OR,
                                      'items': [('size', DataList.operator.LT, 3),
                                                {'type': DataList.where_operator.OR,
                                                 'items': [('size', DataList.operator.GT, 6)]}]}}).data  # noqa
        # at least disk 0, 1, 2, 7, 8, 9, 10-19
        self.assertGreaterEqual(list_10, 16, 'list should contain >= 16')
        list_11 = DataList({'object': TestDisk,
                            'data': DataList.select.COUNT,
                            'query': {'type': DataList.where_operator.AND,
                                      'items': [('storage.name', DataList.operator.EQUALS, 'machine')]}}).data  # noqa
        self.assertEqual(list_11, 10, 'list should contain int 10')  # disk 10-19
        # pylint: enable=line-too-long
        for disk in machine.stored_disks:
            disk.delete()
        for disk in machine.disks:
            disk.delete()
        machine.delete()

    def test_invalidpropertyassignment(self):
        """
        Validates whether the correct exception is raised when properties are assigned with a wrong
        type
        """
        disk = TestDisk()
        disk.size = 100
        with self.assertRaises(TypeError):
            disk.machine = TestDisk()
        disk.delete()

    def test_recursive(self):
        """
        Validates the recursive save
        """
        machine = TestMachine()
        machine.name = 'original'
        machine.save()
        machine2 = TestMachine()
        machine2.save()
        diskx = TestDisk()
        diskx.name = 'storage_test'
        diskx.storage = machine2
        diskx.save()
        machine2.delete()  # Creating an orphaned object
        disks = []
        for i in xrange(0, 10):
            disk = TestDisk()
            disk.name = 'test_%d' % i
            if i % 2:
                disk.machine = machine
            else:
                disk.machine = machine
                self.assertEqual(disk.machine.name, 'original', 'child should be set')
                disk.machine = None
                self.assertIsNone(disk.machine, 'child should be cleared')
                disks.append(disk)
            disk.save()
        counter = 1
        for disk in machine.disks:
            disk.size = counter
            counter += 1
        machine.save(recursive=True)
        disk = TestDisk(machine.disks[0].guid)
        self.assertEqual(disk.size, 1, 'lists should be saved recursively')
        disk.machine.name = 'mtest'
        disk.save(recursive=True)
        machine2 = TestMachine(machine.guid)
        self.assertEqual(machine2.disks[1].size, 2, 'lists should be saved recursively')
        self.assertEqual(machine2.name, 'mtest', 'properties should be saved recursively')
        for disk in machine.disks:
            disk.delete()
        machine.delete()
        for disk in disks:
            disk.delete()
        diskx.delete()

    def test_descriptors(self):
        """
        Validates the correct behavior of the Descriptor
        """
        with self.assertRaises(RuntimeError):
            _ = Descriptor().descriptor
        with self.assertRaises(RuntimeError):
            _ = Descriptor().get_object()

    def test_relationcache(self):
        """
        Validates whether the relational properties are cached correctly, and whether
        they are invalidated when required
        """
        machine = TestMachine()
        machine.name = 'machine'
        machine.save()
        disk1 = TestDisk()
        disk1.name = 'disk1'
        disk1.save()
        disk2 = TestDisk()
        disk2.name = 'disk2'
        disk2.save()
        disk3 = TestDisk()
        disk3.name = 'disk3'
        disk3.save()
        self.assertEqual(len(machine.disks), 0, 'There should be no disks on the machine')
        disk1.machine = machine
        disk1.save()
        self.assertEqual(len(machine.disks), 1, 'There should be 1 disks on the machine')
        disk2.machine = machine
        disk2.save()
        self.assertEqual(len(machine.disks), 2, 'There should be 2 disks on the machine')
        disk3.machine = machine
        disk3.save()
        self.assertEqual(len(machine.disks), 3, 'There should be 3 disks on the machine')
        machine.disks[0].name = 'disk1_'
        machine.disks[1].name = 'disk2_'
        machine.disks[2].name = 'disk3_'
        disk1.machine = None
        disk1.save()
        disk2.machine = None
        disk2.save()
        self.assertEqual(len(machine.disks), 1, 'There should be 1 disks on the machine')
        disk1.delete()
        disk2.delete()
        disk3.delete()
        machine.delete()

    def test_datalistactions(self):
        """
        Validates all actions that can be executed agains DataLists
        """
        machine = TestMachine()
        machine.name = 'machine'
        machine.save()
        disk1 = TestDisk()
        disk1.name = 'disk1'
        disk1.machine = machine
        disk1.save()
        disk2 = TestDisk()
        disk2.name = 'disk2'
        disk2.machine = machine
        disk2.save()
        disk3 = TestDisk()
        disk3.name = 'disk3'
        disk3.machine = machine
        disk3.save()
        self.assertEqual(machine.disks.count(disk1), 1, 'Disk should be available only once')
        self.assertGreaterEqual(machine.disks.index(disk1), 0, 'We should retreive an index')
        machine.disks.sort()
        guid = machine.disks[0].guid
        machine.disks.reverse()
        self.assertEqual(machine.disks[-1].guid, guid, 'Reverse and sort should work')
        machine.disks.sort()
        self.assertEqual(machine.disks[0].guid, guid, 'And the guid should be first again')
        for disk in machine.disks:
            disk.delete()
        machine.delete()

    def test_listcache(self):
        """
        Validates whether lists are cached and invalidated correctly
        """
        keys = ['list_cache', None]
        for key in keys:
            disk0 = TestDisk()
            disk0.save()
            list_cache = DataList(key=key,
                                  query={'object': TestDisk,
                                         'data': DataList.select.COUNT,
                                         'query': {'type': DataList.where_operator.AND,
                                                   'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}})  # noqa
            self.assertFalse(list_cache.from_cache, 'List should not be loaded from cache (mode: %s)' % key)
            self.assertEqual(list_cache.data, 0, 'List should find no entries (mode: %s)' % key)
            machine = TestMachine()
            machine.name = 'machine'
            machine.save()
            disk1 = TestDisk()
            disk1.machine = machine
            disk1.save()
            list_cache = DataList(key=key,
                                  query={'object': TestDisk,
                                         'data': DataList.select.COUNT,
                                         'query': {'type': DataList.where_operator.AND,
                                                   'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}})  # noqa
            self.assertFalse(list_cache.from_cache, 'List should not be loaded from cache (mode: %s)' % key)
            self.assertEqual(list_cache.data, 1, 'List should find one entry (mode: %s)' % key)
            list_cache = DataList(key=key,
                                  query={'object': TestDisk,
                                         'data': DataList.select.COUNT,
                                         'query': {'type': DataList.where_operator.AND,
                                                   'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}})  # noqa
            self.assertTrue(list_cache.from_cache, 'List should be loaded from cache (mode: %s)' % key)
            disk2 = TestDisk()
            disk2.machine = machine
            disk2.save()
            list_cache = DataList(key=key,
                                  query={'object': TestDisk,
                                         'data': DataList.select.COUNT,
                                         'query': {'type': DataList.where_operator.AND,
                                                   'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}})  # noqa
            self.assertFalse(list_cache.from_cache, 'List should not be loaded from cache (mode: %s)' % key)
            self.assertEqual(list_cache.data, 2, 'List should find two entries (mode: %s)' % key)
            machine.name = 'x'
            machine.save()
            list_cache = DataList(key=key,
                                  query={'object': TestDisk,
                                         'data': DataList.select.COUNT,
                                         'query': {'type': DataList.where_operator.AND,
                                                   'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}})  # noqa
            self.assertFalse(list_cache.from_cache, 'List should not be loaded from cache (mode: %s)' % key)
            self.assertEqual(list_cache.data, 0, 'List should have no matches (mode: %s)' % key)
            for disk in machine.disks:
                disk.delete()
            machine.delete()
            disk0.delete()

    def test_emptyquery(self):
        """
        Validates whether an certain query returns an empty set
        """
        amount = DataList({'object': TestDisk,
                           'data': DataList.select.COUNT,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}}).data  # noqa
        self.assertEqual(amount, 0, 'There should be no data')

    def test_nofilterquery(self):
        """
        Validates whether empty queries return the full resultset
        """
        disk1 = TestDisk()
        disk1.save()
        disk2 = TestDisk()
        disk2.save()
        amount = DataList(key='some_list',
                          query={'object': TestDisk,
                                 'data': DataList.select.COUNT,
                                 'query': {'type': DataList.where_operator.AND,
                                           'items': []}}).data
        self.assertEqual(amount, 2, 'There should be two disks')
        disk3 = TestDisk()
        disk3.save()
        amount = DataList(key='some_list',
                          query={'object': TestDisk,
                                 'data': DataList.select.COUNT,
                                 'query': {'type': DataList.where_operator.AND,
                                           'items': []}}).data
        self.assertEqual(amount, 3, 'There should be three disks')
        disk1.delete()
        disk2.delete()
        disk3.delete()

    def test_invalidqueries(self):
        """
        Validates invalid queries
        """
        machine = TestMachine()
        machine.name = 'machine'
        machine.save()
        disk = TestDisk()
        disk.name = 'disk'
        disk.machine = machine
        disk.save()
        setattr(DataList.select, 'SOMETHING', 'SOMETHING')
        with self.assertRaises(NotImplementedError):
            DataList({'object': TestDisk,
                      'data': DataList.select.SOMETHING,
                      'query': {'type': DataList.where_operator.AND,
                                'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}})  # noqa
        setattr(DataList.where_operator, 'SOMETHING', 'SOMETHING')
        with self.assertRaises(NotImplementedError):
            DataList({'object': TestDisk,
                      'data': DataList.select.COUNT,
                      'query': {'type': DataList.where_operator.SOMETHING,
                                'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}})  # noqa
        setattr(DataList.operator, 'SOMETHING', 'SOMETHING')
        with self.assertRaises(NotImplementedError):
            DataList({'object': TestDisk,
                      'data': DataList.select.COUNT,
                      'query': {'type': DataList.where_operator.AND,
                                'items': [('machine.name', DataList.operator.SOMETHING, 'machine')]}})  # noqa
        disk.delete()
        machine.delete()

    def test_clearedcache(self):
        """
        Validates the correct behavior when the volatile cache is cleared
        """
        disk = TestDisk()
        disk.name = 'somedisk'
        disk.save()
        VolatileFactory.store.delete(disk._key)
        disk2 = TestDisk(disk.guid)
        self.assertEqual(disk2.name, 'somedisk', 'Disk should be fetched from persistent store')
        disk.delete()

    def test_serialization(self):
        """
        Validates whether serialization works as expected
        """
        machine = TestMachine()
        machine.name = 'machine'
        machine.save()
        disk = TestDisk()
        disk.name = 'disk'
        disk.machine = machine
        disk.save()
        dictionary = disk.serialize()
        self.assertIn('name', dictionary, 'Serialized object should have correct properties')
        self.assertEqual(dictionary['name'], 'disk', 'Serialized object should have correct name')
        self.assertIn('machine_guid', dictionary, 'Serialized object should have correct depth')
        self.assertEqual(dictionary['machine_guid'], machine.guid,
                         'Serialized object should have correct properties')
        dictionary = disk.serialize(depth=1)
        self.assertIn('machine', dictionary, 'Serialized object should have correct depth')
        self.assertEqual(dictionary['machine']['name'], 'machine',
                         'Serialized object should have correct properties at all depths')
        disk.delete()
        machine.delete()

    def test_primarykeys(self):
        """
        Validates whether the primary keys are kept in sync
        """
        disk = TestDisk()
        VolatileFactory.store.delete('ovs_primarykeys_%s' % disk._name)
        keys = DataList.get_pks(disk._namespace, disk._name)
        self.assertEqual(len(keys), 0, 'There should be no primary keys')
        disk.save()
        keys = DataList.get_pks(disk._namespace, disk._name)
        self.assertEqual(len(keys), 1, 'There should be one primary key')
        disk.delete()

    def test_reduceddatalist(self):
        """
        Validates the reduced list
        """
        disk = TestDisk()
        disk.name = 'test'
        disk.save()
        data = DataList({'object': TestDisk,
                         'data': DataList.select.DESCRIPTOR,
                         'query': {'type': DataList.where_operator.AND,
                                   'items': []}}).data
        datalist = DataObjectList(data, TestDisk)
        self.assertEqual(len(datalist), 1, 'There should be only one item (%s)' % len(datalist))
        item = datalist.reduced[0]
        with self.assertRaises(AttributeError):
            print item.name
        self.assertEqual(item.guid, disk.guid, 'The guid should be available')
        disk.delete()

    def test_volatiemutex(self):
        """
        Validates the volatile mutex
        """
        mutex = VolatileMutex('test')
        mutex.acquire()
        mutex.acquire()  # Should not raise errors
        mutex.release()
        mutex.release()  # Should not raise errors
        mutex._volatile.add(mutex.key(), 1, 10)
        with self.assertRaises(RuntimeError):
            mutex.acquire(wait=1)
        mutex._volatile.delete(mutex.key())
        mutex.acquire()
        time.sleep(0.5)
        mutex.release()

    def test_typesafety(self):
        """
        Validates typesafety checking on object properties
        """
        disk = TestDisk()
        disk.name = 'test'
        disk.name = u'test'
        disk.name = None
        disk.size = 100
        disk.size = 100.5
        disk.order = 100
        with self.assertRaises(TypeError):
            disk.order = 100.5
        with self.assertRaises(TypeError):
            disk.__dict__['wrong_type_data'] = None
            disk.wrong_type_data = 'string'
            _ = disk.wrong_type
        with self.assertRaises(TypeError):
            disk.type = 'THREE'
        disk.type = 'ONE'

    def test_ownrelations(self):
        """
        Validates whether relations to the object itself are working
        """
        pdisk = TestDisk()
        pdisk.name = 'parent'
        pdisk.save()
        cdisk1 = TestDisk()
        cdisk1.name = 'child 1'
        cdisk1.size = 100
        cdisk1.parent = pdisk
        cdisk1.save()
        cdisk2 = TestDisk()
        cdisk2.name = 'child 2'
        cdisk2.size = 100
        cdisk2.parent = pdisk
        cdisk2.save()
        self.assertEqual(len(pdisk.children), 2, 'There should be 2 children.')
        self.assertEqual(cdisk1.parent.name, 'parent', 'Parent should be loaded correctly')
        data = DataList({'object': TestDisk,
                         'data': DataList.select.DESCRIPTOR,
                         'query': {'type': DataList.where_operator.AND,
                                   'items': [('parent.name', DataList.operator.EQUALS, 'parent')]}}).data
        datalist = DataObjectList(data, TestDisk)
        self.assertEqual(len(datalist), 2, 'There should be two items (%s)' % len(datalist))
        cdisk2.parent = None
        cdisk2.save()
        data = DataList({'object': TestDisk,
                         'data': DataList.select.DESCRIPTOR,
                         'query': {'type': DataList.where_operator.AND,
                                   'items': [('parent.name', DataList.operator.EQUALS, 'parent')]}}).data
        datalist = DataObjectList(data, TestDisk)
        self.assertEqual(len(datalist), 1, 'There should be one item (%s)' % len(datalist))
        cdisk1.delete()
        cdisk2.delete()
        pdisk.delete()

    def test_copy_blueprint(self):
        """
        Validates whether the copy_blueprint function works correct
        """
        machine = TestMachine()
        machine.name = 'testmachine1'
        machine.save()
        disk1 = TestDisk()
        disk1.name = 'test1'
        disk1.size = 100
        disk1.order = 1
        disk1.type = 'ONE'
        disk1.machine = machine
        disk1.save()
        disk2 = TestDisk()
        disk2.copy_blueprint(disk1)
        self.assertEqual(disk2.name, 'test1', 'Properties should be copied')
        self.assertEqual(disk2.size, 100, 'Properties should be copied')
        self.assertEqual(disk2.order, 1, 'Properties should be copied')
        self.assertEqual(disk2.type, 'ONE', 'Properties should be copied')
        self.assertEqual(disk2.machine, None, 'Relations should not be copied')
        disk3 = TestDisk()
        disk3.copy_blueprint(disk1, include_relations=True)
        self.assertEqual(disk3.machine.name, 'testmachine1', 'Relations should be copied')
        disk4 = TestDisk()
        disk4.copy_blueprint(disk1, include=['name'])
        self.assertEqual(disk4.name, 'test1', 'Name should be copied')
        self.assertEqual(disk4.size, 0, 'Size should not be copied')
        self.assertEqual(disk4.machine, None, 'Relations should not be copied')
        disk5 = TestDisk()
        disk5.copy_blueprint(disk1, exclude=['name'])
        self.assertEqual(disk5.name, None, 'Name should not be copied')
        self.assertEqual(disk5.size, 100, 'Size should be copied')
        self.assertEqual(disk5.machine, None, 'Relations should not be copied')

        disk1.delete()
        machine.delete()

    def test_querydynamic(self):
        """
        Validates whether a query that queried dynamic properties is never cached
        """
        def get_disks():
            return DataList({'object': TestDisk,
                             'data': DataList.select.DESCRIPTOR,
                             'query': {'type': DataList.where_operator.AND,
                                       'items': [('used_size', DataList.operator.NOT_EQUALS, -1)]}})
        disk1 = TestDisk()
        disk1.size = 100
        disk1.save()
        disk2 = TestDisk()
        disk2.size = 100
        disk2.save()
        query_result = get_disks()
        self.assertEqual(len(query_result.data), 2, 'There should be 2 disks')
        self.assertFalse(query_result.from_cache, 'Disk should not be loaded from cache')
        query_result = get_disks()
        self.assertFalse(query_result.from_cache, 'Disk should not be loaded from cache')
        disk1.delete()
        disk2.delete()
