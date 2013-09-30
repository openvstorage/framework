import uuid
import time
import inspect
import imp
import os
from unittest import TestCase
from ovsdal.dataobject import DataObject
from ovsdal.tests.store import DummyStores
from ovsdal.exceptions import *


class TestDataObject(TestCase):
    def setUp(self):
        DummyStores.clean()

    def test_invalidobject(self):
        self.assertRaises(Exception, TestObject, uuid.uuid4(), None, DummyStores)

    def test_newobjet_delete(self):
        test = TestObject(store=DummyStores)
        test.save()
        guid = test.guid
        self.assertIsNotNone(guid, 'Guid should not be None')
        test.delete()
        self.assertRaises(Exception, TestObject, guid, None, DummyStores)

    def test_discard(self):
        test = TestObject(store=DummyStores)
        test.name = 'one'
        test.save()
        test.name = 'two'
        test.discard()
        self.assertEqual(test.name, 'one', 'Data should be discarded')
        test.delete()

    def test_updateproperty(self):
        test = TestObject(store=DummyStores)
        test.name = 'test'
        self.assertIs(test.name, 'test', 'Name should be updated')
        test.delete()

    def test_datapersistent(self):
        test = TestObject(store=DummyStores)
        guid = test.guid
        test.name = 'test'
        test.save()
        test2 = TestObject(guid, store=DummyStores)
        self.assertEqual(test.name, test2.name, 'Data should be persistent')
        test.delete()

    def test_readonlyproperty(self):
        test = TestObject(store=DummyStores)
        self.assertIsNotNone(test.used_size, 'RO property should return data')

    def test_datastorewins(self):
        test = TestObject(store=DummyStores)
        test.name = 'initial'
        test.save()
        test2 = TestObject(test.guid, datastore_wins=True, store=DummyStores)
        test.name = 'one'
        test.save()
        test2.name = 'two'
        test2.save()
        self.assertEqual(test2.name, 'one', 'Data should be overwritten')
        test.delete()

    def test_datastoreloses(self):
        test = TestObject(store=DummyStores)
        test.name = 'initial'
        test.save()
        test2 = TestObject(test.guid, datastore_wins=False, store=DummyStores)
        test.name = 'one'
        test.save()
        test2.name = 'two'
        test2.save()
        self.assertEqual(test2.name, 'two', 'Data should not be overwritten')
        test.delete()

    def test_datastoreraises(self):
        test = TestObject(store=DummyStores)
        test.name = 'initial'
        test.save()
        test2 = TestObject(test.guid, datastore_wins=None, store=DummyStores)
        test.name = 'one'
        test.save()
        test2.name = 'two'
        self.assertRaises(ConcurrencyException, test2.save)
        test.delete()

    def test_volatileproperty(self):
        test = TestObject(store=DummyStores)
        test.size = 1000000
        value = test.used_size
        time.sleep(2)
        self.assertEqual(test.used_size, value, 'Value should still be from cache')
        time.sleep(2)
        self.assertEqual(test.used_size, value, 'Value should still be from cache')
        time.sleep(2)
        self.assertNotEqual(test.used_size, value, 'Value should be different')

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
                        instance = member_type(store=DummyStores)
                        self.assertIsNotNone(instance.guid)
                        properties = []
                        for item in dir(instance):
                            if hasattr(member[1], item) and isinstance(getattr(member[1], item), property):
                                properties.append(item)
                        for attribute in instance._expiry.keys():
                            self.assertIn(attribute, properties, '%s should be a property' % attribute)
                            data = getattr(instance, attribute)


class TestObject(DataObject):
    _blueprint = {'name'       : 'Object',
                  'description': 'Test object',
                  'size'       : 0}
    _objectexpiry = 300
    _expiry = {'used_size': 5}

    @property
    def used_size(self):
        def get_data():
            # Simulate fetching real data
            from random import randint
            return randint(0, self._data['size'])
        return self._backend_property(get_data)