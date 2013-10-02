import uuid
import time
import inspect
import imp
import os
from unittest import TestCase
from ovsdal.dataobject import DataObject
from ovsdal.tests.store import DummyStoreFactory, InvalidStoreFactory
from ovsdal.exceptions import *


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

    def test_invalidobject(self):
        self.assertRaises(Exception, TestObject, uuid.uuid4(), None)

    def test_newobjet_delete(self):
        test = TestObject()
        test.save()
        guid = test.guid
        self.assertIsNotNone(guid, 'Guid should not be None')
        test.delete()
        self.assertRaises(Exception, TestObject,  guid, None)

    def test_discard(self):
        test = TestObject()
        test.name = 'one'
        test.save()
        test.name = 'two'
        test.discard()
        self.assertEqual(test.name, 'one', 'Data should be discarded')
        test.delete()

    def test_updateproperty(self):
        test = TestObject()
        test.name = 'test'
        self.assertIs(test.name, 'test', 'Name should be updated')
        test.delete()

    def test_datapersistent(self):
        test = TestObject()
        guid = test.guid
        test.name = 'test'
        test.save()
        test2 = TestObject(guid)
        self.assertEqual(test.name, test2.name, 'Data should be persistent')
        test.delete()

    def test_readonlyproperty(self):
        test = TestObject()
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
        self.assertRaises(ConcurrencyException, test2.save)
        test.delete()

    def test_volatileproperty(self):
        test = TestObject()
        test.size = 1000000
        value = test.time
        time.sleep(2)
        self.assertEqual(test.time, value, 'Value should still be from cache')
        time.sleep(2)
        self.assertEqual(test.time, value, 'Value should still be from cache')
        time.sleep(2)
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
        path = os.path.join(os.path.dirname(__file__), '..', 'hybrids')
        for filename in os.listdir(path):
            if os.path.isfile(os.path.join(path, filename)) and filename.endswith('.py'):
                module = imp.load_source(filename.replace('.py', ''), os.path.join(path, filename))
                for member in inspect.getmembers(module):
                    member_type = member[1]
                    if member_type is not DataObject and inspect.isclass(member_type):
                        self.assertIsInstance(member_type._blueprint, dict, '_blueprint is a required property on %s' % member_type.__name__)
                        self.assertIsInstance(member_type._objectexpiry, int, '_objectexpiry is a required property on %s' % member_type.__name__)
                        self.assertIsInstance(member_type._expiry, dict, '_expiry is a required property on %s' % member_type.__name__)
                        instance = member_type()
                        self.assertIsNotNone(instance.guid)
                        properties = []
                        for item in dir(instance):
                            if hasattr(member[1], item) and isinstance(getattr(member[1], item), property):
                                properties.append(item)
                        for attribute in instance._expiry.keys():
                            self.assertIn(attribute, properties, '%s should be a property' % attribute)
                            data = getattr(instance, attribute)

    def test_nostore(self):
        TestObject.set_storefactory(None)
        with self.assertRaises(InvalidStoreFactoryException):
            test = TestObject()

    def test_invalidstore(self):
        TestObject.set_storefactory(InvalidStoreFactory)
        with self.assertRaises(InvalidStoreFactoryException):
            test = TestObject()


class TestObject(DataObject):
    _blueprint = {'name'       : 'Object',
                  'description': 'Test object',
                  'number'     : 0}
    _objectexpiry = 10
    _expiry = {'time': 5}

    @property
    def time(self):
        def get_data():
            import time
            return time.time()
        return self._backend_property(get_data)