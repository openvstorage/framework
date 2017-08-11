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
Basic test module
"""
import time
import uuid
import hashlib
import unittest
from ovs.dal.datalist import DataList
from ovs.dal.exceptions import *
from ovs.dal.helpers import Descriptor, DalToolbox
from ovs.dal.hybrids.t_testdisk import TestDisk
from ovs.dal.hybrids.t_testemachine import TestEMachine
from ovs.dal.hybrids.t_testmachine import TestMachine
from ovs.dal.hybrids.t_teststoragedriver import TestStorageDriver
from ovs.dal.hybrids.t_teststoragerouter import TestStorageRouter
from ovs.dal.hybrids.t_testvpool import TestVPool
from ovs.dal.tests.helpers import DalHelper
from ovs_extensions.generic.volatilemutex import NoLockAvailableException
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory


class Basic(unittest.TestCase):
    """
    The basic unit-test suite will test all basic functionality of the DAL framework
    It will also try accessing all dynamic properties of all hybrids making sure
    that code actually works. This however means that all loaded 3rd party libs
    need to be mocked
    """

    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        self.volatile, self.persistent = DalHelper.setup(fake_sleep=True)

    def tearDown(self):
        """
        Clean up the unittest
        """
        DalHelper.teardown(fake_sleep=True)

    def test_invalidobject(self):
        """
        Validates the behavior when a non-existing object is loaded
        """
        # Loading an non-existing object should raise
        self.assertRaises(ObjectNotFoundException, TestDisk, uuid.uuid4(), None)

    def test_newobject_delete(self):
        """
        Validates the behavior on object deletions
        """
        disk = TestDisk()
        disk.name = 'disk'
        disk.save()
        # An object should always have a guid
        guid = disk.guid
        self.assertIsNotNone(guid, 'Guid should not be None')
        # After deleting, the object should not be retrievable
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

    def test_preinit(self):
        """
        Validates whether initial data is loaded on object creation
        """
        disk = TestDisk(data={'name': 'disk_x'})
        disk.save()
        self.assertEqual(disk.name, 'disk_x', 'Disk name should be pre-loaded')
        disk = TestDisk(data={'name': 'disk_y', 'foo': 'bar'})
        disk.save()
        self.assertEqual(disk.name, 'disk_y', 'Disk name should be pre-loaded, without raising for invalid pre-load data')

    def test_datapersistent(self):
        """
        Validates whether data is persisted correctly
        """
        disk = TestDisk()
        guid = disk.guid
        disk.name = 'test'
        disk.save()
        # Retrieving an object should return the data as when it was saved
        disk2 = TestDisk(guid)
        self.assertEqual(disk.name, disk2.name, 'Data should be persistent')

    def test_readonlyproperty(self):
        """
        Validates whether all dynamic properties are actually read-only
        """
        disk = TestDisk()
        # Readonly properties should return data
        self.assertIsNotNone(disk.used_size, 'RO property should return data')

    def test_datastorewins(self):
        """
        Validates the "datastore_wins" behavior in the use-case where it wins
        """
        disk = TestDisk()
        disk.name = 'initial'
        disk.save()
        disk2 = TestDisk(disk.guid, datastore_wins=True)
        disk.name = 'one'
        disk.save()
        disk2.name = 'two'
        disk2.save()
        # With datastore_wins set to True, the data-store wins concurrency conflicts
        self.assertEqual(disk2.name, 'one', 'Data should be overwritten')

    def test_datastoreloses(self):
        """
        Validates the "datastore_wins" behavior in the use-case where it loses
        """
        disk = TestDisk()
        disk.name = 'initial'
        disk.save()
        disk2 = TestDisk(disk.guid, datastore_wins=False)
        disk.name = 'one'
        disk.save()
        disk2.name = 'two'
        disk2.save()
        # With datastore_wins set to False, the data-store loses concurrency conflicts
        self.assertEqual(disk2.name, 'two', 'Data should not be overwritten')

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
        # With datastore_wins set to False, the data-store loses concurrency conflicts
        self.assertEqual(disk2.name, 'two', 'Data should not be overwritten')
        self.assertEqual(disk.name, 'two', 'Data should be refreshed')

    def test_datastoreraises(self):
        """
        Validates the "datastore_wins" behavior in the use-case where it's supposed to raise
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

    def test_persistency(self):
        """
        Validates whether the object is fetches from the correct storage backend
        """
        disk = TestDisk()
        disk.name = 'test'
        disk.save()
        # Right after a save, the cache is invalidated
        disk2 = TestDisk(disk.guid)
        self.assertFalse(disk2._metadata['cache'], 'Object should be retrieved from persistent backend')
        # Subsequent calls will retrieve the object from cache
        disk3 = TestDisk(disk.guid)
        self.assertTrue(disk3._metadata['cache'], 'Object should be retrieved from cache')
        # After the object expiry passed, it will be retrieved from backend again
        self.volatile.delete(disk._key)  # We clear the entry
        disk4 = TestDisk(disk.guid)
        self.assertFalse(disk4._metadata['cache'], 'Object should be retrieved from persistent backend')

    def test_queries(self):
        """
        Validates whether executing queries returns the expected results
        """
        machine = TestMachine()
        machine.name = 'machine'
        machine.save()
        for i in xrange(0, 20):
            disk = TestDisk()
            disk.name = 'test_{0}'.format(i)
            disk.size = i
            if i < 10:
                disk.machine = machine
            else:
                disk.storage = machine
            disk.save()
        self.assertEqual(len(machine.disks), 10, 'query should find added machines')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('size', DataList.operator.EQUALS, 1)]})
        self.assertEqual(len(dlist), 1, 'list should contain 1')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('size', DataList.operator.EQUALS, 1)]})
        found_object = dlist[0]
        self.assertEqual(found_object.name, 'test_1', 'list should contain correct machine')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('size', DataList.operator.GT, 3),
                                              ('size', DataList.operator.LT, 6)]})
        self.assertEqual(len(dlist), 2, 'list should contain 2')  # disk 4 and 5
        dlist = DataList(TestDisk, {'type': DataList.where_operator.OR,
                                    'items': [('size', DataList.operator.LT, 3),
                                              ('size', DataList.operator.GT, 6)]})
        self.assertGreaterEqual(len(dlist), 16, 'list should contain 16')  # at least disk 0, 1, 2, 7, 8, 9, 10-19
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('machine.guid', DataList.operator.EQUALS, machine.guid),
                                              {'type': DataList.where_operator.OR,
                                               'items': [('size', DataList.operator.LT, 3),
                                                         ('size', DataList.operator.GT, 6)]}]})
        self.assertEqual(len(dlist), 6, 'list should contain 6')  # disk 0, 1, 2, 7, 8, 9
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('size', DataList.operator.LT, 3),
                                              ('size', DataList.operator.GT, 6)]})
        self.assertEqual(len(dlist), 0, 'list should contain 0')  # no disks
        dlist = DataList(TestDisk, {'type': DataList.where_operator.OR,
                                    'items': [('machine.guid', DataList.operator.EQUALS, '123'),
                                              ('used_size', DataList.operator.EQUALS, -1),
                                              {'type': DataList.where_operator.AND,
                                               'items': [('size', DataList.operator.GT, 3),
                                                         ('size', DataList.operator.LT, 6)]}]})
        self.assertEqual(len(dlist), 2, 'list should contain 2')  # disk 4 and 5
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('machine.name', DataList.operator.EQUALS, 'machine'),
                                              ('name', DataList.operator.EQUALS, 'test_3')]})
        self.assertEqual(len(dlist), 1, 'list should contain 1')  # disk 3
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('size', DataList.operator.GT, 3),
                                              {'type': DataList.where_operator.AND,
                                               'items': [('size', DataList.operator.LT, 6)]}]})
        self.assertEqual(len(dlist), 2, 'list should contain 2')  # disk 4 and 5
        dlist = DataList(TestDisk, {'type': DataList.where_operator.OR,
                                    'items': [('size', DataList.operator.LT, 3),
                                              {'type': DataList.where_operator.OR,
                                               'items': [('size', DataList.operator.GT, 6)]}]})
        self.assertGreaterEqual(len(dlist), 16, 'list should contain 16')  # at least disk 0, 1, 2, 7, 8, 9, 10-19
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('storage.name', DataList.operator.EQUALS, 'machine')]})
        self.assertEqual(len(dlist), 10, 'list should contain 10')  # disk 10-19
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.EQUALS, 'test_1')]})
        self.assertEqual(len(dlist), 1, 'list should contain 1')  # single disk
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.EQUALS, 'tESt_1', False)]})
        self.assertEqual(len(dlist), 1, 'list should contain 1')  # single disk
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.EQUALS, 'tESt_1')]})
        self.assertEqual(len(dlist), 0, 'list should contain 0')  # no disk
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.EQUALS, 'Test_1')]})
        self.assertEqual(len(dlist.guids), 0, 'list should contain 0')  # no disk, but executed though guids property
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.CONTAINS, 'test_1')]})
        self.assertEqual(len(dlist), 11, 'list should contain 11')  # disk test_1, test_10-19
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.IN, ['test_1', 'test_2'])]})
        self.assertEqual(len(dlist), 2, 'list should contain 2')  # disk test_1, test_2
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.IN, ['test_1', 'tEst_2'])]})
        self.assertEqual(len(dlist), 1, 'list should contain 1')  # disk test_1
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.IN, ['test_1', 'tEst_2'], False)]})
        self.assertEqual(len(dlist), 2, 'list should contain 2')  # disk test_1, test_2
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.IN, 'foo_test_1_bar')]})
        self.assertEqual(len(dlist), 1, 'list should contain 1')  # disk test_1
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.IN, 'foo_tEst_1_bar')]})
        self.assertEqual(len(dlist), 0, 'list should contain 0')  # no disk
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.IN, 'foo_tEst_1_bar', False)]})
        self.assertEqual(len(dlist), 1, 'list should contain 1')  # disk test_1

    def test_invalidpropertyassignment(self):
        """
        Validates whether the correct exception is raised when properties are assigned with a wrong
        type
        """
        disk = TestDisk()
        disk.size = 100
        with self.assertRaises(TypeError):
            disk.machine = TestDisk()

    def test_recursive(self):
        """
        Validates the recursive save
        """
        machine = TestMachine()
        machine.name = 'original'
        machine.save()
        disks = []
        for i in xrange(0, 10):
            disk = TestDisk()
            disk.name = 'test_{0}'.format(i)
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
        disk.machine.name = 'm_test'
        disk.save(recursive=True)
        machine2 = TestMachine(machine.guid)
        self.assertEqual(machine2.disks[1].size, 2, 'lists should be saved recursively')
        self.assertEqual(machine2.name, 'm_test', 'properties should be saved recursively')

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

    def test_datalistactions(self):
        """
        Validates all actions that can be executed against DataLists
        """
        machine = TestMachine()
        machine.name = 'machine'
        machine.save()
        sizes = [7, 2, 0, 4, 6, 1, 5, 0, 3, 8]
        guids = []
        disks = []
        disk = None
        for i in xrange(0, 10):
            disk = TestDisk()
            disk.name = 'disk_{0}'.format(i)
            disk.size = sizes[i]
            disk.machine = machine
            disk.save()
            disks.append(disk)
            guids.append(disk.guid)
        self.assertEqual(machine.disks.count(disk), 1, 'Disk should be available only once')
        self.assertGreaterEqual(machine.disks.index(disk), 0, 'We should retrieve an index')
        machine.disks.sort()
        guids.sort()
        self.assertEqual(machine.disks[0].guid, guids[0], 'Reverse and sort should work')
        machine.disks.reverse()
        self.assertEqual(machine.disks[-1].guid, guids[0], 'Reverse and sort should work')
        machine.disks.sort()
        self.assertEqual(machine.disks[0].guid, guids[0], 'And the guid should be first again')
        disks = DataList(TestDisk, {'type': DataList.where_operator.AND, 'items': []})
        disks.sort(key=lambda a: a.size)
        self.assertEqual(disks[0].size, 0, 'Disks should be sorted on size')
        self.assertEqual(disks[4].size, 3, 'Disks should be sorted on size')
        disks.sort(key=lambda a: a.name)
        filtered = disks[1:4]
        self.assertEqual(filtered[0].name, 'disk_1', 'Disks should be properly sliced')
        self.assertEqual(filtered[2].name, 'disk_3', 'Disks should be properly sliced')
        fields = [('name', True), ('size', False)]
        for field in fields:
            disks.sort(key=lambda a: DalToolbox.extract_key(a, field[0]), reverse=field[1])
        self.assertEqual(disks[0].size, 0, 'Disk should be properly sorted')
        self.assertEqual(disks[1].size, 0, 'Disk should be properly sorted')
        self.assertEqual(disks[0].name, 'disk_7', 'Disk should be properly sorted')
        self.assertEqual(disks[1].name, 'disk_2', 'Disk should be properly sorted')
        fields = [('name', False), ('predictable', False)]
        for field in fields:
            disks.sort(key=lambda a: DalToolbox.extract_key(a, field[0]), reverse=field[1])
        self.assertEqual(disks[0].predictable, 0, 'Disk should be properly sorted')
        self.assertEqual(disks[1].predictable, 0, 'Disk should be properly sorted')
        self.assertEqual(disks[2].predictable, 1, 'Disk should be properly sorted')
        self.assertEqual(disks[0].name, 'disk_2', 'Disk should be properly sorted')
        self.assertEqual(disks[1].name, 'disk_7', 'Disk should be properly sorted')
        self.assertEqual(disks[2].name, 'disk_5', 'Disk should be properly sorted')

    def test_listcache(self):
        """
        Validates whether lists are cached and invalidated correctly
        """
        keys = ['list_cache', None]
        for key in keys:
            disk0 = TestDisk()
            disk0.name = 'disk0_{0}'.format(key)
            disk0.save()
            list_cache = DataList(TestDisk, key=key,
                                  query={'type': DataList.where_operator.AND,
                                         'items': [('machine.name', DataList.operator.EQUALS, 'machine')]})
            list_cache._execute_query()
            self.assertFalse(list_cache.from_cache, 'List should not be loaded from cache (mode: {0})'.format(key))
            self.assertEqual(len(list_cache), 0, 'List should find no entries (mode: {0})'.format(key))
            machine = TestMachine()
            machine.name = 'machine'
            machine.save()
            disk1 = TestDisk()
            disk1.name = 'disk1_{0}'.format(key)
            disk1.machine = machine
            disk1.save()
            list_cache = DataList(TestDisk, key=key,
                                  query={'type': DataList.where_operator.AND,
                                         'items': [('machine.name', DataList.operator.EQUALS, 'machine')]})
            list_cache._execute_query()
            self.assertFalse(list_cache.from_cache, 'List should not be loaded from cache (mode: {0})'.format(key))
            self.assertEqual(len(list_cache), 1, 'List should find one entry (mode: {0})'.format(key))
            list_cache = DataList(TestDisk, key=key,
                                  query={'type': DataList.where_operator.AND,
                                         'items': [('machine.name', DataList.operator.EQUALS, 'machine')]})
            list_cache._execute_query()
            self.assertTrue(list_cache.from_cache, 'List should be loaded from cache (mode: {0})'.format(key))
            disk2 = TestDisk()
            disk2.machine = machine
            disk2.name = 'disk2_{0}'.format(key)
            disk2.save()
            list_cache = DataList(TestDisk, key=key,
                                  query={'type': DataList.where_operator.AND,
                                         'items': [('machine.name', DataList.operator.EQUALS, 'machine')]})
            list_cache._execute_query()
            self.assertFalse(list_cache.from_cache, 'List should not be loaded from cache (mode: {0})'.format(key))
            self.assertEqual(len(list_cache), 2, 'List should find two entries (mode: {0})'.format(key))
            machine.name = 'x'
            machine.save()
            list_cache = DataList(TestDisk, key=key,
                                  query={'type': DataList.where_operator.AND,
                                         'items': [('machine.name', DataList.operator.EQUALS, 'machine')]})
            list_cache._execute_query()
            self.assertFalse(list_cache.from_cache, 'List should not be loaded from cache (mode: {0})'.format(key))
            self.assertEqual(len(list_cache), 0, 'List should have no matches (mode: {0})'.format(key))

    def test_emptyquery(self):
        """
        Validates whether an certain query returns an empty set
        """
        amount = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                     'items': [('machine.name', DataList.operator.EQUALS, 'machine')]})
        self.assertEqual(len(amount), 0, 'There should be no data')

    def test_nofilterquery(self):
        """
        Validates whether empty queries return the full result set
        """
        disk1 = TestDisk()
        disk1.name = 'disk 1'
        disk1.save()
        disk2 = TestDisk()
        disk2.name = 'disk 2'
        disk2.save()
        amount = DataList(TestDisk, key='some_list',
                          query={'type': DataList.where_operator.AND,
                                 'items': []})
        self.assertEqual(len(amount), 2, 'There should be two disks ({0})'.format(amount))
        disk3 = TestDisk()
        disk3.name = 'disk 3'
        disk3.save()
        amount = DataList(TestDisk, key='some_list',
                          query={'type': DataList.where_operator.AND,
                                 'items': []})
        self.assertEqual(len(amount), 3, 'There should be three disks ({0})'.format(amount))

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
        setattr(DataList.where_operator, 'SOMETHING', 'SOMETHING')
        with self.assertRaises(NotImplementedError):
            dlist = DataList(TestDisk, {'type': DataList.where_operator.SOMETHING,
                                        'items': [('machine.name', DataList.operator.EQUALS, 'machine')]})
            dlist._execute_query()
        with self.assertRaises(NotImplementedError):
            dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                        'items': [{'type': DataList.where_operator.SOMETHING,
                                                   'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}]})
            dlist._execute_query()
        setattr(DataList.operator, 'SOMETHING', 'SOMETHING')
        with self.assertRaises(NotImplementedError):
            dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                        'items': [('machine.name', DataList.operator.SOMETHING, 'machine')]})
            dlist._execute_query()

    def test_clearedcache(self):
        """
        Validates the correct behavior when the volatile cache is cleared
        """
        disk = TestDisk()
        disk.name = 'some_disk'
        disk.save()
        VolatileFactory.store.delete(disk._key)
        disk2 = TestDisk(disk.guid)
        self.assertEqual(disk2.name, 'some_disk', 'Disk should be fetched from persistent store')

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
        self.assertEqual(dictionary['machine_guid'], machine.guid, 'Serialized object should have correct properties')
        dictionary = disk.serialize(depth=1)
        self.assertIn('machine', dictionary, 'Serialized object should have correct depth')
        self.assertEqual(dictionary['machine']['name'], 'machine', 'Serialized object should have correct properties at all depths')

    def test_volatiemutex(self):
        """
        Validates the volatile mutex
        """
        mutex = volatile_mutex('test')
        mutex.acquire()
        mutex.acquire()  # Should not raise errors
        mutex.release()
        mutex.release()  # Should not raise errors
        mutex._volatile.add(mutex.key(), 1, 10)
        with self.assertRaises(NoLockAvailableException):
            mutex.acquire(wait=1)
        mutex._volatile.delete(mutex.key())
        mutex.acquire()
        time.sleep(0.5)
        mutex.release()

    def test_typesafety(self):
        """
        Validates type safety checking on object properties
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
        self.assertEqual(len(pdisk.children), 2, 'There should be 2 children ({0})'.format(len(pdisk.children)))
        self.assertEqual(cdisk1.parent.name, 'parent', 'Parent should be loaded correctly')
        data = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                   'items': [('parent.name', DataList.operator.EQUALS, 'parent')]})
        self.assertEqual(len(data), 2, 'There should be two items ({0})'.format(len(data)))
        cdisk2.parent = None
        cdisk2.save()
        data = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                   'items': [('parent.name', DataList.operator.EQUALS, 'parent')]})
        self.assertEqual(len(data), 1, 'There should be one item ({0})'.format(len(data)))

    def test_copy(self):
        """
        Validates whether the copy function works correct
        """
        machine = TestMachine()
        machine.name = 'test_machine1'
        machine.save()
        disk1 = TestDisk()
        disk1.name = 'test1'
        disk1.size = 100
        disk1.order = 1
        disk1.type = 'ONE'
        disk1.machine = machine
        disk1.save()
        disk2 = TestDisk()
        disk2.copy(disk1)
        self.assertEqual(disk2.name, 'test1', 'Properties should be copied')
        self.assertEqual(disk2.size, 100, 'Properties should be copied')
        self.assertEqual(disk2.order, 1, 'Properties should be copied')
        self.assertEqual(disk2.type, 'ONE', 'Properties should be copied')
        self.assertEqual(disk2.machine, None, 'Relations should not be copied')
        disk3 = TestDisk()
        disk3.copy(disk1, include_relations=True)
        self.assertEqual(disk3.machine.name, 'test_machine1', 'Relations should be copied')
        disk4 = TestDisk()
        disk4.copy(disk1, include=['name'])
        self.assertEqual(disk4.name, 'test1', 'Name should be copied')
        self.assertEqual(disk4.size, 0, 'Size should not be copied')
        self.assertEqual(disk4.machine, None, 'Relations should not be copied')
        disk5 = TestDisk()
        disk5.copy(disk1, exclude=['name'])
        self.assertEqual(disk5.name, None, 'Name should not be copied')
        self.assertEqual(disk5.size, 100, 'Size should be copied')
        self.assertEqual(disk5.machine, None, 'Relations should not be copied')

    def test_querydynamic(self):
        """
        Validates whether a query that queried dynamic properties is never cached
        """
        def _get_disks():
            return DataList(TestDisk, {'type': DataList.where_operator.AND,
                                       'items': [('used_size', DataList.operator.NOT_EQUALS, -1)]})
        disk1 = TestDisk()
        disk1.name = 'disk 1'
        disk1.size = 100
        disk1.save()
        disk2 = TestDisk()
        disk2.name = 'disk 2'
        disk2.size = 100
        disk2.save()
        query_result = _get_disks()
        self.assertEqual(len(query_result), 2, 'There should be 2 disks ({0})'.format(len(query_result)))
        self.assertFalse(query_result.from_cache, 'Disk should not be loaded from cache')
        query_result = _get_disks()
        query_result._execute_query()
        self.assertFalse(query_result.from_cache, 'Disk should not be loaded from cache')

    def test_delete_abandoning(self):
        """
        Validates the abandoning behavior of the delete method
        """
        machine = TestMachine()
        machine.name = 'machine'
        machine.save()
        disk_1 = TestDisk()
        disk_1.name = 'disk 1'
        disk_1.machine = machine
        disk_1.save()
        disk_2 = TestDisk()
        disk_2.name = 'disk 2'
        disk_2.machine = machine
        disk_2.save()
        self.assertRaises(LinkedObjectException, machine.delete)
        disk_3 = TestDisk(disk_1.guid)
        self.assertIsNotNone(disk_3.machine, 'The machine should still be linked')
        _ = machine.disks  # Make sure we loaded the list
        disk_2.delete()
        machine.delete(abandon=['disks'])  # Should not raise
        disk_4 = TestDisk(disk_1.guid)
        self.assertIsNone(disk_4.machine, 'The machine should be unlinked')

    def test_save_deleted(self):
        """
        Validates whether saving a previously deleted object raises
        """
        disk = TestDisk()
        disk.name = 'disk'
        disk.save()
        disk.delete()
        self.assertRaises(ObjectNotFoundException, disk.save, 'Cannot re-save a deleted object')

    def test_itemchange_during_list_build(self):
        """
        Validates whether changing, creating or deleting objects while running a depending list will cause the list to
        be invalidated
        """
        def _inject_new(datalist_object):
            """
            Creates a new object
            """
            _ = datalist_object
            disk_x = TestDisk()
            disk_x.name = 'test_x'
            disk_x.save()

        def _inject_delete(datalist_object):
            """
            Deletes an object
            """
            _ = datalist_object
            disk_1.delete()

        def _inject_update(datalist_object):
            """
            Updates an object
            """
            _ = datalist_object
            disk_2.name = 'x'
            disk_2.save()

        disk_z = None  # Needs to be there
        disk_1 = TestDisk()
        disk_1.name = 'test1'
        disk_1.save()
        disk_2 = TestDisk()
        disk_2.name = 'test2'
        disk_2.save()
        # Validates new object creation
        DataList._test_hooks['post_query'] = _inject_new
        disks = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.CONTAINS, 'test')]})
        self.assertEqual(len(disks), 2, 'Two disks should be found ({0})'.format(len(disks)))
        del DataList._test_hooks['post_query']
        disks = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.CONTAINS, 'test')]})
        self.assertEqual(len(disks), 3, 'Three disks should be found ({0})'.format(len(disks)))
        # Clear the list cache for the next test
        VolatileFactory.store.delete('ovs_list_6cf5641344cb2ec305c89920407d0def867e1744c88605fdc0f6665a348724a8')
        # Validates object change
        DataList._test_hooks['post_query'] = _inject_update
        disks = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.CONTAINS, 'test')]})
        self.assertEqual(len(disks), 3, 'Three disks should be found ({0})'.format(len(disks)))
        del DataList._test_hooks['post_query']
        disks = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.CONTAINS, 'test')]})
        self.assertEqual(len(disks), 2, 'Two disk should be found ({0})'.format(len(disks)))
        # Clear the list cache for the next test
        VolatileFactory.store.delete('ovs_list_6cf5641344cb2ec305c89920407d0def867e1744c88605fdc0f6665a348724a8')
        # Validates object deletion
        DataList._test_hooks['post_query'] = _inject_delete
        disks = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.CONTAINS, 'test')]})
        self.assertEqual(len(disks), 2, 'Two disks should be found ({0})'.format(len(disks)))
        del DataList._test_hooks['post_query']
        disks = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('name', DataList.operator.CONTAINS, 'test')]})
        self.assertEqual(len(disks), 1, 'One disk should be found ({0})'.format(len(disks)))
        _ = disk_z  # Ignore this object not being used

    def test_guid_query(self):
        """
        Validates whether queries can use the _guid fields
        """
        machine = TestMachine()
        machine.name = 'machine'
        machine.save()
        disk = TestDisk()
        disk.name = 'test'
        disk.machine = machine
        disk.save()

        disks = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('machine_guid', DataList.operator.EQUALS, machine.guid)]})
        self.assertEqual(len(disks), 1, 'There should be one disk ({0})'.format(len(disks)))

    def test_1_to_1(self):
        """
        Validates whether 1-to-1 relations work correct
        """
        machine = TestMachine()
        machine.name = 'machine'
        machine.save()

        self.assertIsNone(machine.one, 'The machine should not have a reverse disk relation')
        self.assertIsNone(machine.one_guid, 'The machine should have an empty disk _guid property')

        disk = TestDisk()
        disk.name = 'test'
        disk.one = machine
        disk.save()

        self.assertIsNotNone(machine.one, 'The machine should have a reverse disk relation')
        self.assertEqual(machine.one.name, 'test', 'The reverse 1-to-1 relation should work')
        self.assertEqual(disk.one.name, 'machine', 'The normal 1-to-1 relation should work')
        self.assertEqual(machine.one_guid, disk.guid, 'The reverse disk should be the correct one')

        with self.assertRaises(RuntimeError):
            machine.one = disk

    def test_auto_inheritance(self):
        """
        Validates whether fetching a base hybrid will result in the extended object
        """
        machine = TestMachine()
        self.assertEqual(Descriptor(machine.__class__), Descriptor(TestEMachine), 'The fetched TestMachine should be a TestEMachine')

    def test_relation_inheritance(self):
        """
        Validates whether relations on inherited hybrids behave OK
        """
        machine = TestMachine()
        machine.name = 'machine'
        machine.save()
        disk = TestDisk()
        disk.name = 'disk'
        disk.machine = machine  # Validates relation acceptance (accepts TestEMachine)
        disk.save()
        machine.the_disk = disk  # Validates whether _relations is build correctly
        machine.save()

        disk2 = TestDisk(disk.guid)
        self.assertEqual(Descriptor(disk2.machine.__class__), Descriptor(TestEMachine), 'The machine should be a TestEMachine')

    def test_extended_property(self):
        """
        Validates whether an inherited object has all properties
        """
        machine = TestEMachine()
        machine.name = 'e_machine'
        machine.extended = 'ext'
        machine.save()

        machine2 = TestEMachine(machine.guid)
        self.assertEqual(machine2.name, 'e_machine', 'The name of the extended machine should be correct')
        self.assertEqual(machine2.extended, 'ext', 'The extended property of the extended machine should be correct')

    def test_extended_filter(self):
        """
        Validates whether base and extended hybrids behave the same in lists
        """
        machine1 = TestMachine()
        machine1.name = 'basic'
        machine1.save()
        machine2 = TestEMachine()
        machine2.name = 'extended'
        machine2.save()
        data = DataList(TestMachine, {'type': DataList.where_operator.AND,
                                      'items': []})
        self.assertEqual(len(data), 2, 'There should be two machines if searched for TestMachine ({0})'.format(len(data)))
        data = DataList(TestEMachine, {'type': DataList.where_operator.AND,
                                       'items': []})
        self.assertEqual(len(data), 2, 'There should be two machines if searched for TestEMachine ({0})'.format(len(data)))

    def test_mandatory_fields(self):
        """
        Validates whether mandatory properties and relations work
        """
        machine = TestMachine()
        machine.extended = 'extended'
        machine.name = 'machine'
        machine.save()
        disk = TestDisk()
        # Modify relation to mandatory
        [_ for _ in disk._relations if _.name == 'machine'][0].mandatory = True
        # Continue test
        disk.name = None
        with self.assertRaises(MissingMandatoryFieldsException) as exception:
            disk.save()
        self.assertIn('name', exception.exception.message, 'Field name should be in exception message: {0}'.format(exception.exception.message))
        self.assertIn('machine', exception.exception.message, 'Field machine should be in exception message: {0}'.format(exception.exception.message))
        disk.name = 'disk'
        disk.machine = machine
        disk.save()
        disk.description = 'test'
        disk.storage = machine
        disk.save()
        # Restore relation
        [_ for _ in disk._relations if _.name == 'machine'][0].mandatory = False

    def test_versioning(self):
        """
        Validates whether the version system works
        """
        machine = TestMachine()
        machine.name = 'machine0'
        machine.save()
        self.assertEqual(machine._data['_version'], 1, 'Version should be 1, is {0}'.format(machine._data['_version']))
        machine.save()
        self.assertEqual(machine._data['_version'], 2, 'Version should be 2, is {0}'.format(machine._data['_version']))
        machine_x = TestMachine(machine.guid)
        machine_x.name = 'machine1'
        machine_x.save()
        self.assertTrue(machine.updated_on_datastore(), 'Machine should be updated on data-store')
        machine.name = 'machine2'
        machine.save()
        self.assertEqual(machine._data['_version'], 4, 'Version should be 4, is {0}'.format(machine._data['_version']))
        self.assertFalse(machine.updated_on_datastore(), 'Machine should not be updated on data-store')

    def test_outdated_listobjects(self):
        """
        Validates whether elements in a (cached) list are reloaded if they are changed externally
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
        self.assertListEqual(['disk1', 'disk2'], sorted([disk.name for disk in machine.disks]), 'Names should be disk1 and disk2')
        disk2.name = 'disk_'
        self.assertListEqual(['disk1', 'disk2'], sorted([disk.name for disk in machine.disks]), 'Names should still be disk1 and disk2')
        disk2.save()
        self.assertListEqual(['disk1', 'disk_'], sorted([disk.name for disk in machine.disks]), 'Names should be disk1 and disk_')

    def test_invalidonetoone(self):
        """
        Validates that if a one-to-one is used as a one-to-many an exception will be raised
        """
        machine = TestMachine()
        machine.name = 'machine'
        machine.save()
        self.assertIsNone(machine.one, 'There should not be any disk(s)')
        disk1 = TestDisk()
        disk1.name = 'disk1'
        disk1.one = machine
        disk1.save()
        self.assertEqual(machine.one, disk1, 'The correct disk should be returned')
        disk2 = TestDisk()
        disk2.name = 'disk2'
        disk2.one = machine
        disk2.save()
        with self.assertRaises(InvalidRelationException):
            _ = machine.one

    def test_object_cleanup(self):
        """
        Validates whether using an object property to delete these entries does not cause issues when deleting the
        object itself afterwards
        """
        _ = self
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
        for disk in machine.disks:
            disk.delete()
        machine.delete()

    def test_relation_set_build(self):
        """
        Validates whether relation sets are (re)build correctly
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
        datalist = DataList.get_relation_set(TestDisk, 'machine', TestEMachine, 'disks', machine.guid)
        datalist._execute_query()
        self.assertIsNone(datalist.from_cache, 'The relation set should be fetched from the index')
        self.assertEqual(len(datalist), 2, 'There should be two disks')
        for key in PersistentFactory.store.prefix('ovs_reverseindex_testemachine_{0}'.format(machine.guid)):
            PersistentFactory.store.delete(key)
        datalist = DataList.get_relation_set(TestDisk, 'machine', TestEMachine, 'disks', machine.guid)
        datalist._execute_query()
        self.assertIsNone(datalist.from_cache, 'The relation set should be fetched from the index')
        self.assertEqual(len(datalist), 0, 'No disks should be found')

    def test_instance_checks(self):
        """
        Validates whether Descriptor.isinstance works
        """
        machine = TestMachine()
        machine.name = 'machine'
        machine.save()
        disk = TestDisk()
        disk.name = 'disk'
        disk.machine = machine
        disk.save()
        self.assertTrue(Descriptor.isinstance(disk, TestDisk), 'The disk should be a TestDisk')
        self.assertFalse(Descriptor.isinstance(disk, TestMachine), 'The disk is no TestMachine')
        self.assertTrue(Descriptor.isinstance(disk.machine, TestEMachine), 'The disk.machine is a TestEMachine')

    def test_cache_and_save_racecondition(self):
        """
        Validates whether concurrent save/loads won't result in outdated structures being cached
        """
        guid = None
        preloaded_machine = None

        def _update():
            local_machine = TestMachine(guid)
            local_machine.name = 'updated'
            local_machine.save()

        def _update2():
            preloaded_machine.name = 'updated2'
            with self.assertRaises(NoLockAvailableException):
                preloaded_machine.save()
            raw_data = preloaded_machine._persistent.get(preloaded_machine._key)
            version = raw_data['_version']
            self.assertEqual(version, 2, 'Version should be 2 instead of {0}'.format(version))
            raw_data['_version'] = version + 1
            raw_data['name'] = 'updated3'
            preloaded_machine._persistent.set(preloaded_machine._key, raw_data)

        machine = TestMachine()
        self.assertIsNone(machine._metadata['cache'], "A new object shouldn't imply caching")
        machine.name = 'one'
        machine.save()
        guid = machine.guid

        preloaded_machine = TestMachine(machine.guid, _hook={'before_cache': _update})
        self.assertEqual(preloaded_machine.name, 'one', 'The machine\'s name should still be one')
        self.assertFalse(preloaded_machine._metadata['cache'], 'The machine should be loaded from persistent store')
        machine = TestMachine(machine.guid, _hook={'during_cache': _update2})
        self.assertFalse(machine._metadata['cache'], 'Race condition should have prevented caching')
        self.assertEqual(machine.name, 'updated', 'The machine\'s name should be updated')
        machine = TestMachine(machine.guid)
        self.assertFalse(machine._metadata['cache'], 'Version check should have prevented caching')
        self.assertEqual(machine.name, 'updated3', 'The machine\'s name should be updated3')
        self.assertEqual(machine._data['_version'], 3, 'The machine\'s version should be 3 instead of {0}'.format(machine._data['_version']))

    def test_delete_during_object_load(self):
        """
        Validates whether removing an object during the load does raise a correct exception
        """
        guid = None

        def _delete():
            local_machine = TestMachine(guid)
            local_machine.delete()

        machine = TestMachine()
        machine.name = 'one'
        machine.save()
        guid = machine.guid

        with self.assertRaises(ObjectNotFoundException):
            _ = TestMachine(guid, _hook={'before_cache': _delete})

    def test_object_save_reverseindex_build(self):
        """
        Validates whether saving an object won't create an empty reverse index if not required
        """
        machine = TestMachine()
        machine.name = 'machine'
        machine.save()
        key = 'ovs_reverseindex_testemachine_{0}'.format(machine.guid)
        self.assertListEqual(list(PersistentFactory.store.prefix(key)), [], 'The reverse index should be created (save on new object)')
        disk1 = TestDisk()
        disk1.name = 'disk1'
        disk1.machine = machine
        disk1.save()
        disk2 = TestDisk()
        disk2.name = 'disk2'
        disk2.machine = machine
        disk2.save()
        amount = len(machine.disks)
        self.assertEqual(amount, 2, 'There should be 2 disks ({0} found)'.format(amount))

    def test_save_nonexisting_relation(self):
        """
        Validates the behavior when saving ab object having non-existing relations
        """
        machine = TestMachine()
        machine.name = 'machine'
        disk1 = TestDisk()
        disk1.name = 'disk'
        disk1.machine = machine
        with self.assertRaises(ObjectNotFoundException):
            disk1.save()
        machine.save()
        disk1.save()

    def test_invalidate_dynamics(self):
        """
        Validates whether the invalidate_dynamics call actually works.
        """
        disk = TestDisk()
        disk._frozen = False
        disk.dynamic_int = 0
        disk.name = 'test'
        disk.save()
        value = disk.updatable_int
        self.assertEqual(value, 0, 'Dynamic should be 0')
        disk.dynamic_int = 5
        value = disk.updatable_int
        self.assertEqual(value, 0, 'Dynamic should still be 0 ({0})'.format(value))
        time.sleep(5)
        value = disk.updatable_int
        self.assertEqual(value, 5, 'Dynamic should be 5 now ({0})'.format(value))
        disk.dynamic_int = 10
        value = disk.updatable_int
        self.assertEqual(value, 5, 'Dynamic should still be 5 ({0})'.format(value))
        disk.invalidate_dynamics(['updatable_int'])
        value = disk.updatable_int
        self.assertEqual(value, 10, 'Dynamic should be 10 now ({0})'.format(value))

    def test_enumerator(self):
        """
        Validates whether the internal enumerator generator works as expected
        """
        from ovs.dal.dataobject import DataObject
        list_items = ['ONE', 'TWO', 'THREE']
        enum = DataObject.enumerator('ListTest', list_items)
        self.assertEqual(enum.__name__, 'ListTest', 'Name should be ListTest')
        for item in list_items:
            self.assertIn(item, enum, '{0} should be in the enumerator'.format(item))
            self.assertTrue(hasattr(enum, item), 'Enumerator should have property {0}'.format(item))
        self.assertNotIn('ZERO', enum, 'ZERO should not be in the enumerator')
        extract = [item for item in enum]
        self.assertListEqual(sorted(list_items), sorted(extract), 'Iterating the enum should yield the initial list')
        self.assertEqual(enum.ONE, 'ONE', 'Value should be correct')

        dict_items = {'ONE': 'one', 'TWO': 'two', 'THREE': 'three'}
        enum = DataObject.enumerator('DictTest', dict_items)
        self.assertEqual(enum.__name__, 'DictTest', 'Name should be DictTest')
        for key, value in dict_items.iteritems():
            self.assertIn(key, enum, '{0} should be in the enumerator'.format(key))
            self.assertEqual(getattr(enum, key), value, "Value for key '{0}' should be '{1}' instead of '{2}'".format(key, value, getattr(enum, key)))

    def test_pop_and_remove_from_datalist(self):
        """
        Removes multiple items from a data-object list
        """
        disk1 = TestDisk()
        disk2 = TestDisk()
        disk1.name = 'disk1'
        disk2.name = 'disk2'
        disk1.save()
        disk2.save()

        datalist1 = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                        'items': []})
        self.assertEqual(len(datalist1), 2, 'Expected 2 items in list')
        # Remove from list by specifying object itself
        datalist1.remove(disk1)
        self.assertEqual(len(datalist1), 1, 'Expected 1 item in list')
        with self.assertRaises(ValueError):
            datalist1.remove(disk1)
        # Remove from list by specifying guid of object
        datalist1.remove(disk2.guid)
        self.assertEqual(len(datalist1), 0, 'Expected 0 items in list')
        # Raise error by specifying incorrect object type
        machine1 = TestMachine()
        machine1.name = 'machine1'
        machine1.save()
        with self.assertRaises(TypeError):
            datalist1.remove(machine1)

        datalist2 = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                        'items': []})
        self.assertEqual(len(datalist2), 2, 'Expected 2 items in list')
        # Raise error by attempting to pop item which does not exist
        with self.assertRaises(IndexError):
            datalist2.pop(3)
            datalist2.pop(-3)
        # Raise error by attempting to specify non-integer index
        with self.assertRaises(ValueError):
            datalist2.pop('test')
        # Pop all items by using 0 and a negative index
        datalist2.pop(0)
        datalist2.pop(-1)
        self.assertEqual(len(datalist2), 0, 'Expected 0 items in list')
        # Raise error by attempting to pop from empty list
        with self.assertRaises(IndexError):
            datalist2.pop(0)

    def test_error_during_save(self):
        """
        Validates whether an error during save doesn't leave the system in an inconsistent state
        """
        def _raise_error():
            raise RuntimeError()
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
        try:
            disk2.save(_hook=_raise_error)
        except RuntimeError:
            pass
        self.assertEqual(len(machine.disks), 1, 'There should be one disk')
        for disk in machine.disks:
            disk.delete()
        machine.delete()

    def test_shuffle_object_list(self):
        """
        Shuffle a data-object list randomly
        """
        for i in range(10):
            disk = TestDisk()
            disk.name = 'disk{0}'.format(i)
            disk.save()

        datalist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                       'items': []})
        starting_order = [disk.name for disk in datalist]

        datalist.shuffle()
        new_order = [disk.name for disk in datalist]

        self.assertNotEqual(starting_order, new_order, 'Data-object list still has same order after shuffling')
        self.assertEqual(set(starting_order), set(new_order), 'Items disappeared from the data-object list after shuffling')

    def test_volatile_objects(self):
        """
        Validates whether volatile objects behave as expected
        """
        disk = TestDisk(data={'name': 'test'}, volatile=True)
        self.assertEqual(disk.name, 'test', 'Name should be set correctly')
        self.assertRaises(VolatileObjectException, disk.save, 'A volatile object cannot be saved')
        self.assertRaises(VolatileObjectException, disk.delete, 'A volatile object cannot be deleted')

    def test_datalist_add(self):
        """
        Validates whether one can add DataLists
        """
        disk1 = TestDisk()
        disk1.name = 'disk1'
        disk1.save()
        disk2 = TestDisk()
        disk2.name = 'disk2'
        disk2.save()
        datalist1 = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                        'items': [('name', DataList.operator.EQUALS, 'disk1')]})
        datalist2 = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                        'items': [('name', DataList.operator.EQUALS, 'disk2')]})
        datalist3 = datalist1 + datalist2
        self.assertEqual(len(datalist1), 1, 'First query should contain one item')
        self.assertEqual(len(datalist2), 1, 'Second query should contain one item')
        self.assertEqual(len(datalist3), 2, 'Sum should contain both items')

    def test_delete_retries(self):
        """
        Validates whether a delete will retry on an assert error
        """
        def _change_list():
            if Basic._executed is False:
                PersistentFactory.store.set('ovs_listcache_testdisk', {})
                Basic._executed = True

        _ = self
        PersistentFactory.store.set('ovs_listcache_testdisk', {'foo': 'bar'})
        Basic._executed = False
        disk = TestDisk()
        disk.name = 'disk'
        disk.save()
        disk.delete(_hook=_change_list)

    def test_object_comparison_on_different_level(self):
        """
        Validates whether 2 objects are the same, but when approached from a different level
        """
        _ = self
        sr = TestStorageRouter()
        sr.name = 'storage_router1'
        sr.save()
        vpool = TestVPool()
        vpool.name = 'vpool1'
        vpool.save()
        sd = TestStorageDriver()
        sd.name = 'storage_driver1'
        sd.vpool = vpool
        sd.storagerouter = sr
        sd.save()
        error_message = 'DAL object comparison failure'
        self.assertTrue(sr == sr, error_message)
        self.assertFalse(sr != sr, error_message)
        self.assertTrue(sd.storagerouter == sr, error_message)
        self.assertFalse(sd.storagerouter != sr, error_message)
        self.assertTrue(vpool.storagedrivers[0].storagerouter == sr, error_message)
        self.assertFalse(vpool.storagedrivers[0].storagerouter != sr, error_message)

    def test_racecondition_datalist_multiget(self):
        """
        Validates whether the list can handle objects being deleted by another process
        """
        disk1 = TestDisk()
        disk1.name = 'disk1'
        disk1.save()
        disk2 = TestDisk()
        disk2.name = 'disk2'
        disk2.save()
        datalist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                       'items': []})
        self.assertEqual(len(datalist), 2, 'Datalist should have two testdisks')
        PersistentFactory.store.delete('ovs_data_testdisk_{0}'.format(disk1.guid))
        datalist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                       'items': []})
        self.assertEqual(len(datalist), 1, 'Datalist should have one testdisk')

    def test_racecondition_for_reverseindex(self):
        """
        Validates whether concurrent delete/saves of different objects can cause race conditions related to reverse index
        """
        _ = self
        machine = TestMachine()
        machine.name = 'machine'
        machine.save()
        disk = TestDisk()
        disk.machine = machine
        disk.name = 'disk'
        disk.save()
        PersistentFactory.store.delete('ovs_reverseindex_testemachine_{0}|disks|{1}'.format(machine.guid, disk.guid))
        disk.delete()
        disk = TestDisk()
        disk.machine = machine
        disk.name = 'disk1'
        disk.save()
        machine2 = TestMachine()
        machine2.name = 'machine2'
        machine2.save()
        PersistentFactory.store.delete('ovs_reverseindex_testemachine_{0}|disks|{1}'.format(machine.guid, disk.guid))
        disk.machine = machine2
        disk.save()

    def test_unique_constraint(self):
        """
        Validates whether the unique constraint works as expected
        """
        disk1 = TestDisk()
        disk1.name = 'disk1'
        disk1.description = 'disk1'
        disk1.save()  # Works, it's the first 'disk1'
        disk2 = TestDisk()
        disk2.name = 'disk1'
        disk2.description = 'disk1'
        with self.assertRaises(UniqueConstraintViolationException) as exception:
            disk2.save()  # Fails, there's already a 'disk1'
        self.assertIn('TestDisk.name', exception.exception.message, '\TestDisk.name\' should be in exception message: {0}'.format(exception.exception.message))
        disk2.name = 'disk2'
        disk2.save()  # Works, it's the first 'disk2'
        disk1.save()  # Works, nothing changed, it's still the only 'disk'
        disk1.name = 'disk2'
        with self.assertRaises(UniqueConstraintViolationException):
            disk1.save()  # Fails, it can't be renamed to 'disk2', since there's already a 'disk2'
        disk3 = TestDisk(disk1.guid)  # Currently is 'disk1'
        disk3.save()
        disk3.name = 'disk2'
        with self.assertRaises(UniqueConstraintViolationException):
            disk3.save()  # Fails, it can't be renamed to 'disk2', since there's already a 'disk2'
        disk3.name = 'disk3'
        disk3.save()  # Works, it's the first 'disk3'
        disk3.delete()  # Works, no problem deleting 'disk3'
        disk4 = TestDisk()
        disk4.name = 'disk3'
        disk4.save()  # Works, it's again the first 'disk3', since the previous one was deleted

        disk2a = TestDisk(disk2.guid)
        disk2b = TestDisk(disk2.guid)
        disk2b.name = 'disk2b'
        disk2b.save()  # Works, it's the first 'disk2b', so 'disk2' can be renamed to 'disk2b'
        disk2a.delete()  # Works, no problem deleting 'disk2b'
        disk2c = TestDisk()
        disk2c.name = 'disk2b'
        disk2c.save()  # Works, 'disk2b' was deleted
        disk2c.name = 'disk2'
        disk2c.save()  # Works, there is no 'disk2'

        disk5 = TestDisk()
        disk5.name = 'disk5'
        disk5.save()

        def _delete():
            disk5.delete()
        disk5.delete(_hook=_delete)

        disk6 = TestDisk()
        disk6.name = 'disk6'
        disk6.save()
        disk6a = TestDisk(disk6.guid)
        disk6b = TestDisk(disk6.guid)
        disk6a.name = 'disk6a'
        disk6a.save()  # Works, it's the first 'disk6a', so 'disk6' can be renamed to 'disk6a'
        disk6c = TestDisk()
        disk6c.name = 'disk6'
        disk6c.save()  # Works, at this point, there's no 'disk6' anymore
        disk6b.delete()  # Works, no problem deleting 'disk6a'
        disk6d = TestDisk()
        disk6d.name = 'disk6'
        with self.assertRaises(UniqueConstraintViolationException):
            disk6d.save()  # Fails, 'disk6' already exists
        disk6e = TestDisk()
        disk6e.name = 'disk6a'
        disk6e.save()

    def test_dynamic_unicode_return(self):
        """
        Validates whether dynamic properties return string values (iso unicode)
        """
        disk = TestDisk()
        disk._frozen = False
        disk.dynamic_string = u'test'
        self.assertIsInstance(disk.updatable_string, str, 'Return value should be string, not unicode')
        self.assertIsInstance(disk.updatable_string, str, 'Return value should be string, not unicode')
        disk.dynamic_list = [u'test1', 'test2']
        dynamic = disk.updatable_list
        self.assertIsInstance(dynamic, list, 'Return value should be list')
        self.assertIsInstance(dynamic[0], str, 'Return value should be string, not unicode')
        self.assertIsInstance(dynamic[1], str, 'Return value should be string, not unicode')
        dynamic = disk.updatable_list
        self.assertIsInstance(dynamic[0], str, 'Return value should be string, not unicode')
        self.assertIsInstance(dynamic[1], str, 'Return value should be string, not unicode')
        disk.dynamic_dict = {'a': u'foo',
                             u'b': 'bar'}
        dynamic = disk.updatable_dict
        self.assertIsInstance(dynamic, dict, 'Return value should be dict')
        keys = dynamic.keys()
        values = dynamic.values()
        self.assertIsInstance(keys[0], str, 'Return value should be string, not unicode')
        self.assertIsInstance(keys[1], str, 'Return value should be string, not unicode')
        self.assertIsInstance(values[0], str, 'Return value should be string, not unicode')
        self.assertIsInstance(values[1], str, 'Return value should be string, not unicode')
        dynamic = disk.updatable_dict
        keys = dynamic.keys()
        values = dynamic.values()
        self.assertIsInstance(keys[0], str, 'Return value should be string, not unicode')
        self.assertIsInstance(keys[1], str, 'Return value should be string, not unicode')
        self.assertIsInstance(values[0], str, 'Return value should be string, not unicode')
        self.assertIsInstance(values[1], str, 'Return value should be string, not unicode')
        disk.dynamic_int = 5
        dynamic = disk.updatable_int
        self.assertIsInstance(dynamic, int, 'Return value should be int')

    def test_acquired_lock_during_caching(self):
        """
        Validates whether loading an object won't fail on a non-acquirable cache lock
        """
        _ = self
        disk = TestDisk()
        disk.name = 'test'
        disk.save()

        def _lock():
            disk._mutex_version.acquire()

        _ = TestDisk(disk.guid, _hook={'before_cache': _lock})
        disk._mutex_version.release()

    def test_indexes(self):
        """
        Validates whether indexes work as expected
        """
        one_hash = hashlib.sha1('one').hexdigest()
        two_hash = hashlib.sha1('two').hexdigest()
        three_hash = hashlib.sha1('three').hexdigest()

        def _validate_index(content):
            namespace = 'ovs_index_testdisk|something|{0}'
            indexes = dict(self.persistent.prefix_entries(namespace.format('')))
            index_keys = set(indexes.keys())
            expected_keys = set(namespace.format(key) for key in content)
            difference_left = index_keys - expected_keys
            difference_right = expected_keys - index_keys
            self.assertEqual(len(difference_left), 0, 'Unexpected indexes found: {0}'.format(', '.join(difference_left)))
            self.assertEqual(len(difference_right), 0, 'Some indexes are missing: {0}'.format(', '.join(difference_right)))
            self.assertEqual(len(index_keys), len(content), 'A different amount of indexes were found')
            for key, amount in content.iteritems():
                self.assertIsInstance(indexes[namespace.format(key)], list, 'No index was found for this property content')
                self.assertEqual(len(indexes[namespace.format(key)]), amount, 'An unexpected amount of entries is linked to this content')

        _validate_index({})
        disk1 = TestDisk()
        disk1.name = 'disk1'
        disk1.something = 'one'
        disk1.save()
        _validate_index({one_hash: 1})
        disk1.save()
        _validate_index({one_hash: 1})
        disk2 = TestDisk()
        disk2.name = 'disk2'
        disk2.something = 'two'
        disk2.save()
        _validate_index({one_hash: 1,
                         two_hash: 1})
        disk2.something = 'one'
        disk2.save()
        _validate_index({one_hash: 2})
        disk2.something = 'three'
        disk2.save()
        _validate_index({one_hash: 1,
                         three_hash: 1})
        disk2.something = 'two'
        disk2.save()
        _validate_index({one_hash: 1,
                         two_hash: 1})
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something2', DataList.operator.EQUALS, None)]})
        self.assertEqual(len(dlist), 2)
        disk2.delete()
        _validate_index({one_hash: 1})
        disk1.delete()
        _validate_index({})
        disk1 = TestDisk()
        disk1.name = 'disk1'
        disk1.something = 'one'
        disk1.something2 = 'one'
        disk1.save()
        _validate_index({one_hash: 1})
        disk2 = TestDisk()
        disk2.name = 'disk2'
        disk2.something = 'two'
        disk2.something2 = 'one'
        disk2.save()
        _validate_index({one_hash: 1,
                         two_hash: 1})
        disk3 = TestDisk()
        disk3.name = 'disk3'
        disk3.something = 'two'
        disk3.something2 = 'two'
        disk3.save()
        _validate_index({one_hash: 1,
                         two_hash: 2})
        disk4 = TestDisk()
        disk4.name = 'disk4'
        disk4.something = 'two'
        disk4.save()
        _validate_index({one_hash: 1,
                         two_hash: 3})
        disk4.delete()
        _validate_index({one_hash: 1,
                         two_hash: 2})
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.EQUALS, 'two')]})
        self.assertEqual(len(dlist), 2)
        self.assertEqual(dlist.from_index, 'full')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.EQUALS, 'one')]})
        self.assertEqual(len(dlist), 1)
        self.assertEqual(dlist.from_index, 'full')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.EQUALS, 'one'),
                                              ('something', DataList.operator.EQUALS, 'two')]})
        self.assertEqual(len(dlist), 0)
        self.assertEqual(dlist.from_index, 'full')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.OR,
                                    'items': [('something', DataList.operator.EQUALS, 'one'),
                                              ('something', DataList.operator.EQUALS, 'two')]})
        self.assertEqual(len(dlist), 3)
        self.assertEqual(dlist.from_index, 'full')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.EQUALS, 'one'),
                                              ('something', DataList.operator.EQUALS, 'three')]})
        self.assertEqual(len(dlist), 0)
        self.assertEqual(dlist.from_index, 'full')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.OR,
                                    'items': [('something', DataList.operator.EQUALS, 'one'),
                                              ('something', DataList.operator.EQUALS, 'three')]})
        self.assertEqual(len(dlist), 1)
        self.assertEqual(dlist.from_index, 'full')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.IN, ['zero'])]})
        self.assertEqual(len(dlist), 0)
        self.assertEqual(dlist.from_index, 'full')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.IN, ['zero', 'one'])]})
        self.assertEqual(len(dlist), 1)
        self.assertEqual(dlist.from_index, 'full')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.IN, ['zero', 'one', 'two'])]})
        self.assertEqual(len(dlist), 3)
        self.assertEqual(dlist.from_index, 'full')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.IN, ['zero', 'two'])]})
        self.assertEqual(len(dlist), 2)
        self.assertEqual(dlist.from_index, 'full')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.IN, ['one', 'two'])]})
        self.assertEqual(len(dlist), 3)
        self.assertEqual(dlist.from_index, 'full')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.CONTAINS, 'foo')]})
        self.assertEqual(len(dlist), 0)
        self.assertEqual(dlist.from_index, 'none')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.EQUALS, 'foo'),
                                              ('something', DataList.operator.CONTAINS, 'foo')]})
        self.assertEqual(len(dlist), 0)
        self.assertEqual(dlist.from_index, 'partial')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.EQUALS, 'one'),
                                              {'type': DataList.where_operator.OR,
                                               'items': [('something', DataList.operator.EQUALS, 'two'),
                                                         ('name', DataList.operator.EQUALS, 'disk1')]}]})
        self.assertEqual(len(dlist), 1)
        self.assertEqual(dlist.from_index, 'none')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.EQUALS, 'one'),
                                              {'type': DataList.where_operator.OR,
                                               'items': [('something', DataList.operator.EQUALS, 'two'),
                                                         ('name', DataList.operator.EQUALS, 'disk2')]}]})
        self.assertEqual(len(dlist), 0)
        self.assertEqual(dlist.from_index, 'none')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.EQUALS, 'one'),
                                              ('name', DataList.operator.EQUALS, 'disk1')]})
        self.assertEqual(len(dlist), 1)
        self.assertEqual(dlist.from_index, 'partial')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.EQUALS, 'one'),
                                              {'type': DataList.where_operator.AND,
                                               'items': [('something', DataList.operator.IN, ['one', 'two']),
                                                         ('something2', DataList.operator.EQUALS, 'one')]}]})
        self.assertEqual(len(dlist), 1)
        self.assertEqual(dlist.from_index, 'full')
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [{'type': DataList.where_operator.AND,
                                               'items': [('something', DataList.operator.IN, ['one', 'two']),
                                                         ('something2', DataList.operator.EQUALS, 'one')]},
                                              ('something', DataList.operator.EQUALS, 'one')]})
        self.assertEqual(len(dlist), 1)
        self.assertEqual(dlist.from_index, 'full')

    def test_index_change_racecondition(self):
        """
        Validates whether the DataList can cope with an index that is outdated right after it has been used, before it
        starts iterating over the data
        """
        def _inject_delete(datalist_object):
            """
            Deletes an object
            """
            _ = datalist_object
            disk1.delete()

        disk1 = TestDisk()
        disk1.name = 'disk1'
        disk1.something = 'one'
        disk1.save()
        disk2 = TestDisk()
        disk2.name = 'disk2'
        disk2.something = 'one'
        disk2.save()
        DataList._test_hooks['data_generator'] = _inject_delete
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('something', DataList.operator.EQUALS, 'one')]})
        self.assertEqual(len(dlist), 1)
        self.assertEqual(dlist.from_index, 'full')

    def test_indexed_guid(self):
        """
        Validates whether the 'guid' is an implicitly indexed property
        """
        Basic._called = None

        def _uses_indexes(datalist_object):
            """
            Sets a flag
            """
            _ = datalist_object
            Basic._called = True

        disk1 = TestDisk()
        disk1.name = 'disk1'
        disk1.save()
        disk2 = TestDisk()
        disk2.name = 'disk2'
        disk2.save()
        DataList._test_hooks['data_generator'] = _uses_indexes
        Basic._called = False
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('guid', DataList.operator.IN, [disk1.guid])]})
        self.assertEqual(len(dlist), 1)
        self.assertTrue(Basic._called)
        self.assertEqual(dlist.from_index, 'full')
        Basic._called = False
        dlist = DataList(TestDisk, {'type': DataList.where_operator.AND,
                                    'items': [('guid', DataList.operator.EQUALS, disk1.guid)]})
        self.assertEqual(len(dlist), 1)
        self.assertTrue(Basic._called)
        self.assertEqual(dlist.from_index, 'full')

    def test_clone(self):
        """
        Validates whether the clone function works correctly
        """
        machine1 = TestMachine()
        machine1.name = 'test_machine1'
        machine1.save()
        machine2 = machine1.clone()
        self.assertEqual(machine1.name, machine2.name)
        self.assertEqual(machine1.guid, machine2.guid)
        self.assertEqual(machine1._datastore_wins, machine2._datastore_wins)
        self.assertDictEqual(machine1._data, machine2._data)
        self.assertEqual(machine1, machine2)
        machine2.name = 'test_machine2'
        machine2.save()
        self.assertEqual(machine1.name, 'test_machine1')
        self.assertEqual(machine2.name, 'test_machine2')

        machine3 = TestMachine(volatile=True)
        machine4 = machine3.clone()
        with self.assertRaises(VolatileObjectException):
            machine3.save()
        with self.assertRaises(VolatileObjectException):
            machine4.save()

    def test_equal(self):
        """
        Verify the equality of 2 objects is done based on guid
        """
        machine1 = TestMachine()
        machine1.name = 'test_machine1'
        machine1.save()
        machine2 = TestMachine(machine1.guid)
        self.assertEqual(machine1, machine2)
        machine2.name = 'test_machine2'
        machine2.save()
        self.assertEqual(machine1, machine2)
