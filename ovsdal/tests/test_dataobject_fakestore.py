import uuid
import time
import inspect
import imp
import os
from unittest import TestCase
from ovsdal.storedobject import StoredObject
from ovsdal.hybrids.disk import Disk
from ovsdal.hybrids.machine import Machine
from ovsdal.datalist import DataList
from ovsdal.storage.dummies import DummyPersistentStore, DummyVolatileStore
from ovsdal.exceptions import *
from ovsdal.helpers import HybridRunner, Descriptor


#noinspection PyUnresolvedReferences,PyProtectedMember
class TestDataObject(TestCase):
    @classmethod
    def setUpClass(cls):
        DummyVolatileStore.clean()
        DummyPersistentStore.clean()
        Disk._objectexpiry = 10  # Artificially change expiry times for faster tests

    @classmethod
    def setUp(cls):
        StoredObject.set_stores(DummyPersistentStore(), DummyVolatileStore())

    @classmethod
    def tearDownClass(cls):
        DummyVolatileStore.clean()
        DummyPersistentStore.clean()
        # Test to make sure the clean doesn't raise if there is nothing to clean
        DummyVolatileStore.clean()
        DummyPersistentStore.clean()

    def test_invalidobject(self):
        # Loading an non-existing object should raise
        self.assertRaises(Exception, Disk, uuid.uuid4(), None)

    def test_newobjet_delete(self):
        disk = Disk()
        disk.save()
        # An object should always have a guid
        guid = disk.guid
        self.assertIsNotNone(guid, 'Guid should not be None')
        # After deleting, the object should not be retreivable
        disk.delete()
        self.assertRaises(Exception, Disk,  guid, None)

    def test_discard(self):
        disk = Disk()
        disk.name = 'one'
        disk.save()
        disk.name = 'two'
        # Discarding an object should rollback all changes
        disk.discard()
        self.assertEqual(disk.name, 'one', 'Data should be discarded')
        disk.delete()

    def test_updateproperty(self):
        disk = Disk()
        disk.name = 'test'
        disk.description = 'desc'
        # A property should be writable
        self.assertIs(disk.name, 'test', 'Property should be updated')
        self.assertIs(disk.description, 'desc', 'Property should be updated')
        disk.delete()

    def test_datapersistent(self):
        disk = Disk()
        guid = disk.guid
        disk.name = 'test'
        disk.save()
        # Retreiving an object should return the data as when it was saved
        disk2 = Disk(guid)
        self.assertEqual(disk.name, disk2.name, 'Data should be persistent')
        disk.delete()

    def test_readonlyproperty(self):
        disk = Disk()
        # Readonly properties should return data
        self.assertIsNotNone(disk.used_size, 'RO property should return data')

    def test_datastorewins(self):
        disk = Disk()
        disk.name = 'initial'
        disk.save()
        disk2 = Disk(disk.guid, datastore_wins=True)
        disk.name = 'one'
        disk.save()
        disk2.name = 'two'
        disk2.save()
        # With datastore_wins set to True, the datastore wins concurrency conflicts
        self.assertEqual(disk2.name, 'one', 'Data should be overwritten')
        disk.delete()

    def test_datastoreloses(self):
        disk = Disk()
        disk.name = 'initial'
        disk.save()
        disk2 = Disk(disk.guid, datastore_wins=False)
        disk.name = 'one'
        disk.save()
        disk2.name = 'two'
        disk2.save()
        # With datastore_wins set to False, the datastore loses concurrency conflicts
        self.assertEqual(disk2.name, 'two', 'Data should not be overwritten')
        disk.delete()

    def test_datastoreraises(self):
        disk = Disk()
        disk.name = 'initial'
        disk.save()
        disk2 = Disk(disk.guid, datastore_wins=None)
        disk.name = 'one'
        disk.save()
        disk2.name = 'two'
        # with datastore_wins set to None, concurrency conflicts are raised
        self.assertRaises(ConcurrencyException, disk2.save)
        disk.delete()

    def test_volatileproperty(self):
        disk = Disk()
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
        disk = Disk()
        disk.name = 'test'
        disk.save()
        # Right after a save, the cache is invalidated
        disk2 = Disk(disk.guid)
        self.assertFalse(disk2._metadata['cache'], 'Object should be retreived from persistent backend')
        # Subsequent calls will retreive the object from cache
        disk3 = Disk(disk.guid)
        self.assertTrue(disk3._metadata['cache'], 'Object should be retreived from cache')
        # After the object expiry passed, it will be retreived from backend again
        time.sleep(12)
        disk4 = Disk(disk.guid)
        self.assertFalse(disk4._metadata['cache'], 'Object should be retreived from persistent backend')
        disk.delete()

    def test_objectproperties(self):
        # Some stuff here to dynamically test all hybrid properties
        for cls in HybridRunner.get_hybrids():
            # Make sure certain attributes are correctly set
            self.assertIsInstance(cls._blueprint, dict, '_blueprint is a required property on %s' % cls.__name__)
            self.assertIsInstance(cls._objectexpiry, int, '_objectexpiry is a required property on %s' % cls.__name__)
            self.assertIsInstance(cls._expiry, dict, '_expiry is a required property on %s' % cls.__name__)
            instance = cls()
            # Make sure the type can be instantiated
            self.assertIsNotNone(instance.guid)
            properties = []
            for item in dir(instance):
                if hasattr(cls, item) and isinstance(getattr(cls, item), property):
                    properties.append(item)
            # All expiries should be implemented
            for attribute in instance._expiry.keys():
                self.assertIn(attribute, properties, '%s should be a property' % attribute)
                # ... and should work
                data = getattr(instance, attribute)

    def test_relations(self):
        machine = Machine()
        machine.name = 'machine'
        machine.save()
        for i in xrange(0, 20):
            disk = Disk()
            disk.name = 'test_%d' % i
            disk.size = i
            if i < 10:
                disk.machine = machine
            else:
                disk.machine = machine
                self.assertEqual(disk.machine.name, 'machine', 'child should be set')
                disk.machine = None
                self.assertIsNone(disk.machine, 'child should be cleared')
            disk.save()
        self.assertEqual(len(machine.disks), 10, 'query should find added machines')
        list_1 = DataList(key   = 'list_1',
                          query = {'object': Disk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.AND,
                                              'items': [('size', DataList.operator.EQUALS, 1)]}}).data
        self.assertEqual(list_1, 1, 'list should contain int 1')
        list_2 = DataList(key   = 'list_2',
                          query = {'object': Disk,
                                   'data'  : DataList.select.OBJECT,
                                   'query' : {'type': DataList.where_operator.AND,
                                              'items': [('size', DataList.operator.EQUALS, 1)]}}).data
        found_object = Descriptor().load(list_2[0]).get_object(True)
        self.assertEqual(found_object.name, 'test_1', 'list should contain corret machine')
        list_3 = DataList(key   = 'list_3',
                          query = {'object': Disk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.AND,
                                              'items': [('size', DataList.operator.GT, 3),
                                                        ('size', DataList.operator.LT, 6)]}}).data
        self.assertEqual(list_3, 2, 'list should contain int 2')  # disk 4 and 5
        list_4 = DataList(key   = 'list_4',
                          query = {'object': Disk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.OR,
                                              'items': [('size', DataList.operator.LT, 3),
                                                        ('size', DataList.operator.GT, 6)]}}).data
        self.assertEqual(list_4, 16, 'list should contain int 16')  # disk 0, 1, 2, 7, 8, 9, 10-19
        list_5 = DataList(key   = 'list_5',
                          query = {'object': Disk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.AND,
                                              'items': [('machine', DataList.operator.EQUALS, machine.guid),
                                                        {'type' : DataList.where_operator.OR,
                                                         'items': [('size', DataList.operator.LT, 3),
                                                                   ('size', DataList.operator.GT, 6)]}]}}).data
        self.assertEqual(list_5, 6, 'list should contain int 6')  # disk 0, 1, 2, 7, 8, 9
        list_6 = DataList(key   = 'list_6',
                          query = {'object': Disk,
                                   'data'  : DataList.select.COUNT,
                                   'query' : {'type': DataList.where_operator.AND,
                                              'items': [('size', DataList.operator.LT, 3),
                                                        ('size', DataList.operator.GT, 6)]}}).data
        self.assertEqual(list_6, 0, 'list should contain int 0')  # no disks


    #def test_nostore(self):
    #    # Instantiating an object should check if there is a store set
    #    TestObject.set_storefactory(None)
    #    with self.assertRaises(InvalidStoreFactoryException):
    #        test = TestObject()
#
    #def test_invalidstore(self):
    #    # Instantiating an object should check whether the store factory can provide the required stores
    #    TestObject.set_storefactory(InvalidStoreFactory)
    #    with self.assertRaises(InvalidStoreFactoryException):
    #        test = TestObject()
#
    #def test_parentobjects(self):
    #    test = TestObject()
    #    self.assertIsNone(test.child, 'Child should not be instantiated by default')
    #    with self.assertRaises(TypeError):
    #        # A child can only be set to the configured type
    #        test.child = TestObject()
    #    test.child = OtherObject()
    #    test.child.name = 'something'
    #    self.assertIsNotNone(test.child.name, 'Child should be browsable')
    #    test.child.description = 'else'
    #    # The data set to a child's properties should be available
    #    self.assertEqual(test.child.name, 'something', 'Child should be persistent')
    #    test.child.save()
    #    test.save()
    #    test2 = TestObject(test.guid)
    #    # Child properties should also be saved correctly
    #    self.assertEqual(test2.child.name, test.child.name, 'Child link should be persistent')
    #    self.assertEqual(test2.child.description, 'else', 'Child link should be persistent')
    #    test.child.delete()
    #    test.delete()
#
    #def test_parentlists(self):
    #    test = TestObject()
    #    # Children should be instantiated as empty list
    #    self.assertEqual(len(test.children), 0, 'Children should be empty')
    #    # DataObjectList object should behave as a default python list
    #    test.children.append(OtherObject())
    #    test.children.append(OtherObject())
    #    test.children[0].name = 'first'
    #    test.children[1].name = 'second'
    #    test.children[0].description = 'first other'
    #    test.children[1].description = 'second other'
    #    # Modifying children should be persistent
    #    self.assertEqual(test.children[0].name, 'first', 'Children should be persistent')
    #    for item in test.children:
    #        self.assertIn(item.name, ['first', 'second'], 'Children should be iterable')
    #        item.save()
    #    test.save()
    #    # Children structure should be persistent
    #    test2 = TestObject(test.guid)
    #    self.assertEqual(test2.children[1].description, 'second other', 'Children should be persistent')
    #    test.children.sort()
    #    guid = test.children[0].guid
    #    self.assertEqual(test.children.count(test.children[0]), 1, 'Children should be countable')
    #    self.assertEqual(test.children.index(test.children[0]), 0, 'Indexer should work')
    #    test.children.reverse()
    #    self.assertEqual(test.children[-1].guid, guid, 'Sort and reverse should work')
    #    item = test.children.pop()
    #    self.assertNotIn(item.guid, test.children.descriptor['guids'], 'Popped child should be removed from list')
    #    test.children.insert(1, item)
    #    self.assertEqual(test.children[1].guid, item.guid, 'Insert should work')
    #    new_list = DataObjectList(OtherObject)
    #    new_object = OtherObject()
    #    new_object.name = 'third'
    #    new_object.save()
    #    new_list.append(new_object)
    #    test.children.extend(new_list)
    #    self.assertEqual(len(test.children), 3, 'List should be extended')
    #    with self.assertRaises(TypeError):
    #        test.children = None
    #    # Test the lazy loading
    #    test.children._objects = {}
    #    for item in test.children:
    #        self.assertIn(item.name, ['first', 'second', 'third'], 'Dynamic loading should work')
    #        # Children should be removable
    #        test.children.remove(item)
    #        item.delete()
    #    # We can only set a list property to one of the defined type
    #    with self.assertRaises(TypeError):
    #        test.children = DataObjectList(TestObject)
    #    test.children = DataObjectList(OtherObject)
    #    test.delete()
#
    #def test_datalistvalidation(self):
    #    # A list created with an empty constructor should raise errors on every call
    #    test = DataObjectList()
    #    self.assertRaises(RuntimeError, test.append, None)
    #    self.assertRaises(RuntimeError, test.extend, None)
    #    self.assertRaises(RuntimeError, test.insert, 0, None)
    #    self.assertRaises(RuntimeError, test.remove, None)
    #    self.assertRaises(RuntimeError, test.pop)
    #    self.assertRaises(RuntimeError, test.index, None)
    #    self.assertRaises(RuntimeError, test.count, None)
    #    self.assertRaises(RuntimeError, test.sort)
    #    self.assertRaises(RuntimeError, test.reverse)
    #    with self.assertRaises(RuntimeError):
    #        # Also itteration should be impossible
    #        x = [i for i in test]
    #    # After initialisation, it should check for the correct types
    #    test.initialze(Reflector.get_object_descriptor(OtherObject()))
    #    self.assertRaises(TypeError, test.append, TestObject())
    #    self.assertRaises(TypeError, test.insert, 0, TestObject())
    #    self.assertRaises(TypeError, test.extend, DataObjectList(TestObject))
#
    #def test_datalistrecursivesave(self):
    #    test = TestObject()
    #    test.child = OtherObject()
    #    test.child.name = 'one'
    #    test.children.append(OtherObject())
    #    test.children[0].name = 'one'
    #    test.save()
    #    time.sleep(11)
    #    test2 = TestObject(test.guid)
    #    with self.assertRaises(Exception):
    #        item = test2.children[0]
    #    with self.assertRaises(Exception):
    #        item = test2.child.name
    #    test.save(recursive=True)
    #    test3 = TestObject(test.guid)
    #    self.assertEqual(test3.children[0].name, 'one', 'Save should work recursively')
    #    self.assertEqual(test3.child.name, 'one', 'Save should work recursively')
    #    test.children[0].delete()
    #    test.child.delete()
    #    test.delete()
