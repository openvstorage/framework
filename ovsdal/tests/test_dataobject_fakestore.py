import uuid
import time
import inspect
import imp
import os
from unittest import TestCase
from ovsdal.dataobject import DataObject
from ovsdal.datalist import DataList
from ovsdal.tests.store import DummyStoreFactory, InvalidStoreFactory
from ovsdal.helpers import Reflector
from ovsdal.exceptions import *


#noinspection PyUnresolvedReferences,PyProtectedMember
class TestDataObject(TestCase):
    @classmethod
    def setUpClass(cls):
        DummyStoreFactory.clean()

    @classmethod
    def setUp(cls):
        DataObject.set_storefactory(DummyStoreFactory)

    @classmethod
    def tearDownClass(cls):
        DummyStoreFactory.clean()
        # Test to make sure the clean doesn't raise if there is nothing to clean
        DummyStoreFactory.clean()

    def test_invalidobject(self):
        # Loading an non-existing object should raise
        self.assertRaises(Exception, TestObject, uuid.uuid4(), None)

    def test_newobjet_delete(self):
        test = TestObject()
        test.save()
        # An object should always have a guid
        guid = test.guid
        self.assertIsNotNone(guid, 'Guid should not be None')
        # After deleting, the object should not be retreivable
        test.delete()
        self.assertRaises(Exception, TestObject,  guid, None)

    def test_discard(self):
        test = TestObject()
        test.name = 'one'
        test.save()
        test.name = 'two'
        # Discarding an object should rollback all changes
        test.discard()
        self.assertEqual(test.name, 'one', 'Data should be discarded')
        test.delete()

    def test_updateproperty(self):
        test = TestObject()
        test.name = 'test'
        test.description = 'desc'
        # A property should be writable
        self.assertIs(test.name, 'test', 'Property should be updated')
        self.assertIs(test.description, 'desc', 'Property should be updated')
        test.delete()

    def test_datapersistent(self):
        test = TestObject()
        guid = test.guid
        test.name = 'test'
        test.save()
        # Retreiving an object should return the data as when it was saved
        test2 = TestObject(guid)
        self.assertEqual(test.name, test2.name, 'Data should be persistent')
        test.delete()

    def test_readonlyproperty(self):
        test = TestObject()
        # Readonly properties should return data
        self.assertIsNotNone(test.time, 'RO property should return data')

    def test_datastorewins(self):
        test = TestObject()
        test.name = 'initial'
        test.save()
        test2 = TestObject(test.guid, datastore_wins=True)
        test.name = 'one'
        test.save()
        test2.name = 'two'
        test2.save()
        # With datastore_wins set to True, the datastore wins concurrency conflicts
        self.assertEqual(test2.name, 'one', 'Data should be overwritten')
        test.delete()

    def test_datastoreloses(self):
        test = TestObject()
        test.name = 'initial'
        test.save()
        test2 = TestObject(test.guid, datastore_wins=False)
        test.name = 'one'
        test.save()
        test2.name = 'two'
        test2.save()
        # With datastore_wins set to False, the datastore loses concurrency conflicts
        self.assertEqual(test2.name, 'two', 'Data should not be overwritten')
        test.delete()

    def test_datastoreraises(self):
        test = TestObject()
        test.name = 'initial'
        test.save()
        test2 = TestObject(test.guid, datastore_wins=None)
        test.name = 'one'
        test.save()
        test2.name = 'two'
        # with datastore_wins set to None, concurrency conflicts are raised
        self.assertRaises(ConcurrencyException, test2.save)
        test.delete()

    def test_volatileproperty(self):
        test = TestObject()
        value = test.time
        # Volatile properties should be stored for the correct amount of time
        time.sleep(2)
        self.assertEqual(test.time, value, 'Value should still be from cache')
        time.sleep(2)
        self.assertEqual(test.time, value, 'Value should still be from cache')
        time.sleep(2)
        # ... after which they should be reloaded from the backend
        self.assertNotEqual(test.time, value, 'Value should be different')

    def test_persistency(self):
        test = TestObject()
        test.name = 'test'
        test.save()
        # Right after a save, the cache is invalidated
        test2 = TestObject(test.guid)
        self.assertFalse(test2._metadata['cache'], 'Object should be retreived from persistent backend')
        # Subsequent calls will retreive the object from cache
        test3 = TestObject(test.guid)
        self.assertTrue(test3._metadata['cache'], 'Object should be retreived from cache')
        # After the object expiry passed, it will be retreived from backend again
        time.sleep(12)
        test4 = TestObject(test.guid)
        self.assertFalse(test4._metadata['cache'], 'Object should be retreived from persistent backend')

    def test_objectproperties(self):
        # Some stuff here to dynamically test all hybrid properties
        path = os.path.join(os.path.dirname(__file__), '..', 'hybrids')
        for filename in os.listdir(path):
            if os.path.isfile(os.path.join(path, filename)) and filename.endswith('.py'):
                module = imp.load_source(filename.replace('.py', ''), os.path.join(path, filename))
                for member in inspect.getmembers(module):
                    member_type = member[1]
                    if member_type is not DataObject and inspect.isclass(member_type):
                        # Make sure certain attributes are correctly set
                        self.assertIsInstance(member_type._blueprint, dict, '_blueprint is a required property on %s' % member_type.__name__)
                        self.assertIsInstance(member_type._objectexpiry, int, '_objectexpiry is a required property on %s' % member_type.__name__)
                        self.assertIsInstance(member_type._expiry, dict, '_expiry is a required property on %s' % member_type.__name__)
                        instance = member_type()
                        # Make sure the type can be instantiated
                        self.assertIsNotNone(instance.guid)
                        properties = []
                        for item in dir(instance):
                            if hasattr(member[1], item) and isinstance(getattr(member[1], item), property):
                                properties.append(item)
                        # All expiries should be implemented
                        for attribute in instance._expiry.keys():
                            self.assertIn(attribute, properties, '%s should be a property' % attribute)
                            # ... and should work
                            data = getattr(instance, attribute)

    def test_nostore(self):
        # Instantiating an object should check if there is a store set
        TestObject.set_storefactory(None)
        with self.assertRaises(InvalidStoreFactoryException):
            test = TestObject()

    def test_invalidstore(self):
        # Instantiating an object should check whether the store factory can provide the required stores
        TestObject.set_storefactory(InvalidStoreFactory)
        with self.assertRaises(InvalidStoreFactoryException):
            test = TestObject()

    def test_parentobjects(self):
        test = TestObject()
        # A child should be lazy instantiated
        self.assertIsNotNone(test.child.name, 'Child should be browsable')
        with self.assertRaises(TypeError):
            # A child can only be set to the configured type
            test.child = TestObject()
        test.child = OtherObject()
        test.child.name = 'something'
        test.child.description = 'else'
        # The data set to a child's properties should be available
        self.assertEqual(test.child.name, 'something', 'Child should be persistent')
        test.child.save()
        test.save()
        test2 = TestObject(test.guid)
        # Child properties should also be saved correctly
        self.assertEqual(test2.child.name, test.child.name, 'Child link should be persistent')
        self.assertEqual(test2.child.description, 'else', 'Child link should be persistent')
        test.child.delete()
        test.delete()

    def test_parentlists(self):
        test = TestObject()
        # Children should be instantiated as empty list
        self.assertEqual(len(test.children), 0, 'Children should be empty')
        # DataList object should behave as a default python list
        test.children.append(OtherObject())
        test.children.append(OtherObject())
        test.children[0].name = 'first'
        test.children[1].name = 'second'
        test.children[0].description = 'first other'
        test.children[1].description = 'second other'
        # Modifying children should be persistent
        self.assertEqual(test.children[0].name, 'first', 'Children should be persistent')
        for item in test.children:
            self.assertIn(item.name, ['first', 'second'], 'Children should be iterable')
            item.save()
        test.save()
        # Children structure should be persistent
        test2 = TestObject(test.guid)
        self.assertEqual(test2.children[1].description, 'second other', 'Children should be persistent')
        test.children.sort()
        guid = test.children[0].guid
        self.assertEqual(test.children.count(test.children[0]), 1, 'Children should be countable')
        self.assertEqual(test.children.index(test.children[0]), 0, 'Indexer should work')
        test.children.reverse()
        self.assertEqual(test.children[-1].guid, guid, 'Sort and reverse should work')
        item = test.children.pop()
        self.assertNotIn(item.guid, test.children.descriptor['guids'], 'Popped child should be removed from list')
        test.children.insert(1, item)
        self.assertEqual(test.children[1].guid, item.guid, 'Insert should work')
        new_list = DataList(OtherObject)
        new_object = OtherObject()
        new_object.name = 'third'
        new_object.save()
        new_list.append(new_object)
        test.children.extend(new_list)
        self.assertEqual(len(test.children), 3, 'List should be extended')
        # Test the lazy loading
        test.children._objects = {}
        for item in test.children:
            self.assertIn(item.name, ['first', 'second', 'third'], 'Dynamic loading should work')
            # Children should be removable
            test.children.remove(item)
            item.delete()
        # We can only set a list property to one of the defined type
        with self.assertRaises(TypeError):
            test.children = DataList(TestObject)
        test.children = DataList(OtherObject)
        test.delete()

    def test_datalistvalidation(self):
        # A list created with an empty constructor should raise errors on every call
        test = DataList()
        self.assertRaises(RuntimeError, test.append, None)
        self.assertRaises(RuntimeError, test.extend, None)
        self.assertRaises(RuntimeError, test.insert, 0, None)
        self.assertRaises(RuntimeError, test.remove, None)
        self.assertRaises(RuntimeError, test.pop)
        self.assertRaises(RuntimeError, test.index, None)
        self.assertRaises(RuntimeError, test.count, None)
        self.assertRaises(RuntimeError, test.sort)
        self.assertRaises(RuntimeError, test.reverse)
        with self.assertRaises(RuntimeError):
            # Also itteration should be impossible
            x = [i for i in test]
        # After initialisation, it should check for the correct types
        test.initialze(Reflector.get_object_descriptor(OtherObject()))
        self.assertRaises(TypeError, test.append, TestObject())
        self.assertRaises(TypeError, test.insert, 0, TestObject())
        self.assertRaises(TypeError, test.extend, DataList(TestObject))

    def test_datalistrecursivesave(self):
        test = TestObject()
        test.child.name = 'one'
        test.children.append(OtherObject())
        test.children[0].name = 'one'
        test.save()
        time.sleep(11)
        test2 = TestObject(test.guid)
        with self.assertRaises(Exception):
            item = test2.children[0]
        with self.assertRaises(Exception):
            item = test2.child.name
        test.save(recursive=True)
        test3 = TestObject(test.guid)
        self.assertEqual(test3.children[0].name, 'one', 'Save should work recursively')
        self.assertEqual(test3.child.name, 'one', 'Save should work recursively')
        test.children[0].delete()
        test.child.delete()
        test.delete()


class OtherObject(DataObject):
    _blueprint = {'name'       : 'Other',
                  'description': 'Test other'}
    _objectexpiry = 10
    _expiry = {}


class TestObject(DataObject):
    _blueprint = {'name'       : 'Object',
                  'description': 'Test object',
                  'child'      : OtherObject,
                  'children'   : [OtherObject],
                  'number'     : 0}
    _objectexpiry = 10
    _expiry = {'time': 5}

    @property
    def time(self):
        def get_data():
            import time
            return time.time()
        return self._backend_property(get_data)
