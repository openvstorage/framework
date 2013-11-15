import time
from unittest import TestCase
from ovs.lib.dummy import dummy
from ovs.lib.disk import vdisk

class Basic(TestCase):
    @classmethod
    def setUp(cls):
        dummy = dummy()
        vdisk = vdisk()

    def test_dummy_sleep_sync(self):
        result = dummy.sleep(1)
        self.assertEqual(result, True, 'Synchronous dummy sleep failed')

    def test_dummy_sleep_async(self):
        asyncResult = dummy.sleep.apply_async((1,))
        result = asyncResult.wait(5)
        self.assertEqual(result, True, 'Asynchronous dummy sleep failed')

    def test_volume_list_sync(self):
        result = vdisk.listVolumes()
        self.assertIsInstance(result, list(), 'Synchronous listVolumes return invalid')

    def test_volume_list_async(self):
        asyncResult = vdisk.listVolumes.apply_async()
        result = asyncResult.wait(5)
        self.assertIsInstance(result, list(), 'Asynchronous listVolumes return invalid')