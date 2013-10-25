import uuid
import time
from unittest import TestCase
from ovs.dal.storedobject import StoredObject
from ovs.dal.hybrids.vdisk import vDisk
from ovs.dal.hybrids.vmachine import vMachine
from ovs.dal.datalist import DataList
from ovs.extensions.storage.dummies import DummyPersistentStore, DummyVolatileStore
from ovs.dal.exceptions import *
from ovs.dal.helpers import HybridRunner, Descriptor
from ovs.dal.relations.relations import RelationMapper


#noinspection PyUnresolvedReferences
class Basic(TestCase):
    @classmethod
    def setUpClass(cls):
        DummyVolatileStore.clean()
        DummyPersistentStore.clean()
        # Test to make sure the clean doesn't raise if there is nothing to clean
        DummyVolatileStore.clean()
        DummyPersistentStore.clean()

    @classmethod
    def setUp(cls):
        StoredObject.persistent = DummyPersistentStore()
        StoredObject.volatile = DummyVolatileStore()

    @classmethod
    def tearDownClass(cls):
        pass

    def test_invalidobject(self):
        # Loading an non-existing object should raise
        self.assertRaises(ObjectNotFoundException, vDisk, uuid.uuid4(), None)

    def test_newobjet_delete(self):
        disk = vDisk()
        disk.save()
        # An object should always have a guid
        guid = disk.guid
        self.assertIsNotNone(guid, 'Guid should not be None')
        # After deleting, the object should not be retreivable
        disk.delete()
        self.assertRaises(Exception, vDisk,  guid, None)

    def test_discard(self):
        disk = vDisk()
        disk.name = 'one'
        disk.save()
        disk.name = 'two'
        # Discarding an object should rollback all changes
        disk.discard()
        self.assertEqual(disk.name, 'one', 'Data should be discarded')
        disk.delete()

    def test_updateproperty(self):
        disk = vDisk()
        disk.name = 'test'
        disk.description = 'desc'
        # A property should be writable
        self.assertIs(disk.name, 'test', 'Property should be updated')
        self.assertIs(disk.description, 'desc', 'Property should be updated')
        disk.delete()

    def test_preinit(self):
        disk = vDisk(data={'name': 'diskx'})
        disk.save()
        self.assertEqual(disk.name, 'diskx', 'Disk name should be preloaded')
        disk.delete()

    def test_datapersistent(self):
        disk = vDisk()
        guid = disk.guid
        disk.name = 'test'
        disk.save()
        # Retreiving an object should return the data as when it was saved
        disk2 = vDisk(guid)
        self.assertEqual(disk.name, disk2.name, 'Data should be persistent')
        disk.delete()

    def test_readonlyproperty(self):
        disk = vDisk()
        # Readonly properties should return data
        self.assertIsNotNone(disk.used_size, 'RO property should return data')
        disk.delete()

    def test_datastorewins(self):
        disk = vDisk()
        disk.name = 'initial'
        disk.save()
        disk2 = vDisk(disk.guid, datastore_wins=True)
        disk.name = 'one'
        disk.save()
        disk2.name = 'two'
        disk2.save()
        # With datastore_wins set to True, the datastore wins concurrency conflicts
        self.assertEqual(disk2.name, 'one', 'Data should be overwritten')
        disk.delete()

    def test_datastoreloses(self):
        disk = vDisk()
        disk.name = 'initial'
        disk.save()
        disk2 = vDisk(disk.guid, datastore_wins=False)
        disk.name = 'one'
        disk.save()
        disk2.name = 'two'
        disk2.save()
        # With datastore_wins set to False, the datastore loses concurrency conflicts
        self.assertEqual(disk2.name, 'two', 'Data should not be overwritten')
        disk.delete()

    def test_datastoreraises(self):
        disk = vDisk()
        disk.name = 'initial'
        disk.save()
        disk2 = vDisk(disk.guid, datastore_wins=None)
        disk.name = 'one'
        disk.save()
        disk2.name = 'two'
        # with datastore_wins set to None, concurrency conflicts are raised
        self.assertRaises(ConcurrencyException, disk2.save)
        disk.delete()

    def test_volatileproperty(self):
        disk = vDisk()
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
        disk = vDisk()
        disk.name = 'test'
        disk.save()
        # Right after a save, the cache is invalidated
        disk2 = vDisk(disk.guid)
        self.assertFalse(disk2._metadata['cache'], 'Object should be retreived from persistent backend')
        # Subsequent calls will retreive the object from cache
        disk3 = vDisk(disk.guid)
        self.assertTrue(disk3._metadata['cache'], 'Object should be retreived from cache')
        # After the object expiry passed, it will be retreived from backend again
        DummyVolatileStore().delete(disk._key)  # We clear the entry
        disk4 = vDisk(disk.guid)
        self.assertFalse(disk4._metadata['cache'], 'Object should be retreived from persistent backend')
        disk.delete()

    def test_objectproperties(self):
        # Some stuff here to dynamically test all hybrid properties
        for cls in HybridRunner.get_hybrids():
            relation_info = RelationMapper.load_foreign_relations(cls)
            remote_properties = []
            if relation_info is not None:
                for key in relation_info.keys():
                    remote_properties.append(key)
            # Make sure certain attributes are correctly set
            self.assertIsInstance(cls._blueprint, dict, '_blueprint is a required property on %s' % cls.__name__)
            self.assertIsInstance(cls._relations, dict, '_relations is a required property on %s' % cls.__name__)
            self.assertIsInstance(cls._expiry, dict, '_expiry is a required property on %s' % cls.__name__)
            instance = cls()
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
                    data = getattr(instance, attribute)
            self.assertEqual(len(missing_props), 0, 'Missing dynamic properties in %s: %s' % (cls.__name__, missing_props))
            # An all properties should be either in the blueprint, relations or expiry
            missing_metadata = []
            for prop in properties:
                found = prop in cls._blueprint \
                    or prop in cls._relations \
                    or prop in cls._expiry \
                    or prop in remote_properties \
                    or prop == 'guid'
                if not found:
                    missing_metadata.append(prop)
            self.assertEqual(len(missing_metadata), 0, 'Missing metadata for properties in %s: %s' % (cls.__name__, missing_metadata))
            instance.delete()

    def test_queries(self):
        machine = vMachine()
        machine.name = 'machine'
        machine.save()
        for i in xrange(0, 20):
            disk = vDisk()
            disk.name = 'test_%d' % i
            disk.size = i
            if i < 10:
                disk.machine = machine
            else:
                disk.storage = machine
            disk.save()
        self.assertEqual(len(machine.disks), 10, 'query should find added machines')
        list_1 = DataList(key   = 'list_1',
                          query = {'object': vDisk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.AND,
                                              'items': [('size', DataList.operator.EQUALS, 1)]}}).data
        self.assertEqual(list_1, 1, 'list should contain int 1')
        list_2 = DataList(key   = 'list_2',
                          query = {'object': vDisk,
                                   'data'  : DataList.select.DESCRIPTOR,
                                   'query' : {'type': DataList.where_operator.AND,
                                              'items': [('size', DataList.operator.EQUALS, 1)]}}).data
        found_object = Descriptor().load(list_2[0]).get_object(True)
        self.assertEqual(found_object.name, 'test_1', 'list should contain corret machine')
        list_3 = DataList(key   = 'list_3',
                          query = {'object': vDisk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.AND,
                                              'items': [('size', DataList.operator.GT, 3),
                                                        ('size', DataList.operator.LT, 6)]}}).data
        self.assertEqual(list_3, 2, 'list should contain int 2')  # disk 4 and 5
        list_4 = DataList(key   = 'list_4',
                          query = {'object': vDisk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.OR,
                                              'items': [('size', DataList.operator.LT, 3),
                                                        ('size', DataList.operator.GT, 6)]}}).data
        self.assertGreaterEqual(list_4, 16, 'list should contain >= 16')  # at least disk 0, 1, 2, 7, 8, 9, 10-19
        list_5 = DataList(key   = 'list_5',
                          query = {'object': vDisk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.AND,
                                              'items': [('machine.guid', DataList.operator.EQUALS, machine.guid),
                                                        {'type' : DataList.where_operator.OR,
                                                         'items': [('size', DataList.operator.LT, 3),
                                                                   ('size', DataList.operator.GT, 6)]}]}}).data
        self.assertEqual(list_5, 6, 'list should contain int 6')  # disk 0, 1, 2, 7, 8, 9
        list_6 = DataList(key   = 'list_6',
                          query = {'object': vDisk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.AND,
                                              'items': [('size', DataList.operator.LT, 3),
                                                        ('size', DataList.operator.GT, 6)]}}).data
        self.assertEqual(list_6, 0, 'list should contain int 0')  # no disks
        list_7 = DataList(key   = 'list_7',
                          query = {'object': vDisk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.OR,
                                              'items': [('machine.guid', DataList.operator.EQUALS, '123'),
                                                        ('used_size', DataList.operator.EQUALS, -1),
                                                        {'type' : DataList.where_operator.AND,
                                                         'items': [('size', DataList.operator.GT, 3),
                                                                   ('size', DataList.operator.LT, 6)]}]}}).data
        self.assertEqual(list_7, 2, 'list should contain int 2')  # disk 4 and 5
        list_8 = DataList(key   = 'list_8',
                          query = {'object': vDisk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.AND,
                                              'items': [('machine.name', DataList.operator.EQUALS, 'machine'),
                                                        ('name', DataList.operator.EQUALS, 'test_3')]}}).data
        self.assertEqual(list_8, 1, 'list should contain int 1')  # disk 3
        list_9 = DataList(key   = 'list_9',
                          query = {'object': vDisk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.AND,
                                              'items': [('size', DataList.operator.GT, 3),
                                                        {'type' : DataList.where_operator.AND,
                                                         'items': [('size', DataList.operator.LT, 6)]}]}}).data
        self.assertEqual(list_9, 2, 'list should contain int 2')  # disk 4 and 5
        list_10 = DataList(key   = 'list_10',
                           query = {'object': vDisk,
                                    'data'  : DataList.select.COUNT,
                                    'query' : {'type': DataList.where_operator.OR,
                                               'items': [('size', DataList.operator.LT, 3),
                                                         {'type': DataList.where_operator.OR,
                                                          'items': [('size', DataList.operator.GT, 6)]}]}}).data
        self.assertGreaterEqual(list_10, 16, 'list should contain >= 16')  # at least disk 0, 1, 2, 7, 8, 9, 10-19
        list_11 = DataList(key   = 'list_11',
                           query = {'object': vDisk,
                                    'data'  : DataList.select.COUNT,
                                    'query' : {'type': DataList.where_operator.AND,
                                               'items': [('storage.name', DataList.operator.EQUALS, 'machine')]}}).data
        self.assertEqual(list_11, 10, 'list should contain int 10')  # disk 10-19
        for disk in machine.stored_disks:
            disk.delete()
        for disk in machine.disks:
            disk.delete()
        machine.delete()

    def test_invalidpropertyassignment(self):
        disk = vDisk()
        disk.size = 100
        with self.assertRaises(TypeError):
            disk.machine = vDisk()
        disk.delete()

    def test_recursive(self):
        machine = vMachine()
        machine.name = 'original'
        machine.save()
        machine2 = vMachine()
        machine2.save()
        diskx = vDisk()
        diskx.name = 'storage_test'
        diskx.storage = machine2
        diskx.save()
        machine2.delete()  # Creating an orphaned object
        for i in xrange(0, 10):
            disk = vDisk()
            disk.name = 'test_%d' % i
            if i % 2:
                disk.machine = machine
            else:
                disk.machine = machine
                self.assertEqual(disk.machine.name, 'original', 'child should be set')
                disk.machine = None
                self.assertIsNone(disk.machine, 'child should be cleared')
            disk.save()
        counter = 1
        for disk in machine.disks:
            disk.size = counter
            counter += 1
        machine.save(recursive=True)
        disk = vDisk(machine.disks[0].guid)
        self.assertEqual(disk.size, 1, 'lists should be saved recursively')
        disk.machine.name = 'mtest'
        disk.save(recursive=True)
        machine2 = vMachine(machine.guid)
        self.assertEqual(machine2.disks[1].size, 2, 'lists should be saved recursively')
        self.assertEqual(machine2.name, 'mtest', 'properties should be saved recursively')
        for disk in machine.disks:
            disk.delete()
        machine.delete()
        diskx.delete()

    def test_descriptors(self):
        with self.assertRaises(RuntimeError):
            descriptor = Descriptor().descriptor
        with self.assertRaises(RuntimeError):
            value = Descriptor().get_object()

    def test_relationcache(self):
        machine = vMachine()
        machine.name = 'machine'
        machine.save()
        disk1 = vDisk()
        disk1.name = 'disk1'
        disk1.save()
        disk2 = vDisk()
        disk2.name = 'disk2'
        disk2.save()
        disk3 = vDisk()
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
        machine = vMachine()
        machine.name = 'machine'
        machine.save()
        disk1 = vDisk()
        disk1.name = 'disk1'
        disk1.machine = machine
        disk1.save()
        disk2 = vDisk()
        disk2.name = 'disk2'
        disk2.machine = machine
        disk2.save()
        disk3 = vDisk()
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
        disk0 = vDisk()
        disk0.save()
        list_cache = DataList(key   = 'list_cache',
                              query = {'object': vDisk,
                                       'data'  : DataList.select.COUNT,
                                       'query' : {'type': DataList.where_operator.AND,
                                                  'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}})
        self.assertFalse(list_cache.from_cache, 'List should not be loaded from cache')
        self.assertEqual(list_cache.data, 0, 'List should find no entries')
        machine = vMachine()
        machine.name = 'machine'
        machine.save()
        disk1 = vDisk()
        disk1.machine = machine
        disk1.save()
        list_cache = DataList(key   = 'list_cache',
                              query = {'object': vDisk,
                                       'data'  : DataList.select.COUNT,
                                       'query' : {'type': DataList.where_operator.AND,
                                                  'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}})
        self.assertFalse(list_cache.from_cache, 'List should not be loaded from cache')
        self.assertEqual(list_cache.data, 1, 'List should find one entry')
        list_cache = DataList(key   = 'list_cache',
                              query = {'object': vDisk,
                                       'data'  : DataList.select.COUNT,
                                       'query' : {'type': DataList.where_operator.AND,
                                                  'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}})
        self.assertTrue(list_cache.from_cache, 'List should be loaded from cache')
        disk2 = vDisk()
        disk2.machine = machine
        disk2.save()
        list_cache = DataList(key   = 'list_cache',
                              query = {'object': vDisk,
                                       'data'  : DataList.select.COUNT,
                                       'query' : {'type': DataList.where_operator.AND,
                                                  'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}})
        self.assertFalse(list_cache.from_cache, 'List should not be loaded from cache')
        self.assertEqual(list_cache.data, 2, 'List should find two entries')
        machine.name = 'x'
        machine.save()
        list_cache = DataList(key   = 'list_cache',
                              query = {'object': vDisk,
                                       'data'  : DataList.select.COUNT,
                                       'query' : {'type': DataList.where_operator.AND,
                                                  'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}})
        self.assertFalse(list_cache.from_cache, 'List should not be loaded from cache')
        self.assertEqual(list_cache.data, 0, 'List should have no matches')
        for disk in machine.disks:
            disk.delete()
        machine.delete()
        disk0.delete()

    def test_emptyquery(self):
        amount = DataList(key   = 'some_list',
                          query = {'object': vDisk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.AND,
                                              'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}}).data
        self.assertEqual(amount, 0, 'There should be no data')

    def test_nofilterquery(self):
        disk1 = vDisk()
        disk1.save()
        disk2 = vDisk()
        disk2.save()
        amount = DataList(key   = 'some_list',
                          query = {'object': vDisk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.AND,
                                              'items': []}}).data
        self.assertEqual(amount, 2, 'There should be two disks')
        disk3 = vDisk()
        disk3.save()
        amount = DataList(key   = 'some_list',
                          query = {'object': vDisk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.AND,
                                              'items': []}}).data
        self.assertEqual(amount, 3, 'There should be three disks')
        disk1.delete()
        disk2.delete()
        disk3.delete()

    def test_invalidqueries(self):
        machine = vMachine()
        machine.name = 'machine'
        machine.save()
        disk = vDisk()
        disk.name = 'disk'
        disk.machine = machine
        disk.save()
        setattr(DataList.select, 'SOMETHING', 'SOMETHING')
        with self.assertRaises(NotImplementedError):
            DataList(key   = 'some_list',
                     query = {'object': vDisk,
                              'data'  : DataList.select.SOMETHING,
                              'query' : {'type': DataList.where_operator.AND,
                                         'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}})
        setattr(DataList.where_operator, 'SOMETHING', 'SOMETHING')
        with self.assertRaises(NotImplementedError):
            DataList(key   = 'some_list',
                     query = {'object': vDisk,
                              'data'  : DataList.select.COUNT,
                              'query' : {'type': DataList.where_operator.SOMETHING,
                                         'items': [('machine.name', DataList.operator.EQUALS, 'machine')]}})
        setattr(DataList.operator, 'SOMETHING', 'SOMETHING')
        with self.assertRaises(NotImplementedError):
            DataList(key   = 'some_list',
                     query = {'object': vDisk,
                              'data'  : DataList.select.COUNT,
                              'query' : {'type': DataList.where_operator.AND,
                                         'items': [('machine.name', DataList.operator.SOMETHING, 'machine')]}})
        disk.delete()
        machine.delete()

    def test_clearedcache(self):
        disk = vDisk()
        disk.name = 'somedisk'
        disk.save()
        StoredObject.volatile.delete(disk._key)
        disk2 = vDisk(disk.guid)
        self.assertEqual(disk2.name, 'somedisk', 'Disk should be fetched from persistent store')
        disk.delete()

    def test_serialization(self):
        machine = vMachine()
        machine.name = 'machine'
        machine.save()
        disk = vDisk()
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
        disk.delete()
        machine.delete()

    def test_primarykeys(self):
        disk = vDisk()
        StoredObject.volatile.delete('ovs_primarykeys_%s' % disk._name)
        keys = DataList.get_pks(disk._namespace, disk._name)
        self.assertEqual(len(keys), 0, 'There should be no primary keys')
        disk.save()
        keys = DataList.get_pks(disk._namespace, disk._name)
        self.assertEqual(len(keys), 1, 'There should be one primary key')
        disk.delete()
