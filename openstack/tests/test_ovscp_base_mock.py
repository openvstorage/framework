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
Mock basic unit tests for the OVS Cinder Plugin
"""

import uuid, mock
from cinder import test
import cinder.volume.drivers.ovs_volume_driver as ovsvd

#MOCKUPS
MOCK_hostname = 'test-hostname'
MOCK_mountpoint = '/mnt/test'
MOCK_vdisk_guid = '0000'
MOCK_vdisk_devicename = 'volume-test.raw'
MOCK_volume_name = 'volume-test'
MOCK_volume_size = 10
MOCK_volume_type_id = 'RANDOM'
MOCK_volume_id = '0'
MOCK_volume_provider_location = '{0}/{1}'.format(MOCK_mountpoint, MOCK_vdisk_devicename)

CALLED = {}
class MockVDiskController():
    def create_volume(self, location, size):
        CALLED['create_volume'] = {'location': location, 'size': size}
class MockStorageRouter():
    name = MOCK_hostname
class MockStorageDriver():
    storagerouter = MockStorageRouter()
    mountpoint = MOCK_mountpoint
class MockVPool():
    storagedrivers = [MockStorageDriver()]
class MockVDisk():
    vpool = MockVPool()
    devicename = MOCK_vdisk_devicename
    guid = MOCK_vdisk_guid
    cinder_id = None
    def __init__(self, guid):
        pass
    def save(self):
        pass
class MockVPoolList():
    def get_vpool_by_name(self, name):
        return MockVPool()
class MockVDiskList():
    def get_vdisks(self):
        return [MockVDisk(MOCK_vdisk_guid)]

class MOCK_log():
    def debug(self, *args, **kwargs):
        pass
    def error(self, *args, **kwargs):
        pass
    def info(self, *args, **kwargs):
        pass

class MOCK_volume():
    host = MOCK_hostname
    display_name = MOCK_volume_name
    size = MOCK_volume_size
    volume_type_id = MOCK_volume_type_id
    id = MOCK_volume_id
    provider_location = MOCK_volume_provider_location

    def __setitem__(self, attribute, value):
        setattr(self, attribute, value)
    def __getitem__(self, attribute):
        return getattr(self, attribute)

class OVSPluginBaseTestCase(test.TestCase):
    """
    Basic tests - mocked
    """

    def setUp(self):
        super(OVSPluginBaseTestCase, self).setUp()
        ovsvd.VDiskController = MockVDiskController()
        ovsvd.VPoolList = MockVPoolList()
        ovsvd.VDiskList = MockVDiskList()
        ovsvd.VDisk = MockVDisk
        ovsvd.LOG = MOCK_log()
        self.driver = ovsvd.OVSVolumeDriver(configuration = mock.Mock())

    def tearDown(self):
        super(OVSPluginBaseTestCase, self).tearDown()

    def test__get_hostname_mountpoint(self):
        mountpoint = self.driver._get_hostname_mountpoint(MOCK_hostname)
        self.assertTrue(mountpoint == MOCK_mountpoint, 'Wrong mountpoint')

    def test__find_ovs_model_disk_by_location(self):
        location = '{0}/{1}'.format(MOCK_mountpoint, MOCK_vdisk_devicename)
        vdisk = self.driver._find_ovs_model_disk_by_location(location, MOCK_hostname)
        self.assertTrue(vdisk.devicename == MOCK_vdisk_devicename, 'Wrong devicename')

    def test_create_volume_mock(self):
        result = self.driver.create_volume(MOCK_volume())
        self.assertTrue(result['provider_location'] == '{0}/{1}.raw'.format(MOCK_mountpoint, MOCK_volume_name), 'Wrong location')
        self.assertTrue(CALLED['create_volume'] == {'location': MOCK_volume_provider_location, 'size': MOCK_volume_size}, 'Wrong params')



