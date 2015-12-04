# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Vdisk operations test module
"""
import sys
import unittest
from unittest import TestCase
from ovs.lib.tests.mockups import StorageDriverModule, KVMModule
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore
from ovs.extensions.generic.system import System

class VDiskOperations(TestCase):
    """
    This test class will test the various operations of the vdisk controller
    """
    VDisk = None
    PMachine = None
    BackendType = None
    VPool = None
    StorageRouter = None
    StorageDriver = None

    @classmethod
    def setUpClass(cls):
        """
        Sets up the unittest, mocking a certain set of 3rd party libraries and extensions.
        This makes sure the unittests can be executed without those libraries installed
        """
        # Load dummy stores
        PersistentFactory.store = DummyPersistentStore()
        VolatileFactory.store = DummyVolatileStore()
        # Replace mocked classes
        sys.modules['ovs.extensions.storageserver.storagedriver'] = StorageDriverModule
        sys.modules['ovs.extensions.hypervisor.hypervisors.kvm'] = KVMModule

        # Import required modules/classes after mocking is done
        from ovs.dal.hybrids.backendtype import BackendType
        from ovs.dal.hybrids.vdisk import VDisk
        from ovs.dal.hybrids.j_mdsservice import MDSService
        from ovs.dal.hybrids.j_mdsservicevdisk import MDSServiceVDisk
        from ovs.lib.vdisk import VDiskController
        from ovs.dal.hybrids.pmachine import PMachine
        from ovs.dal.hybrids.vmachine import VMachine
        from ovs.dal.hybrids.vpool import VPool
        from ovs.dal.hybrids.storagedriver import StorageDriver
        from ovs.dal.hybrids.storagerouter import StorageRouter
        from ovs.dal.hybrids.failuredomain import FailureDomain
        from ovs.dal.hybrids.service import Service
        from ovs.dal.hybrids.servicetype import ServiceType
        from ovs.dal.lists.vdisklist import VDiskList
        from ovs.lib.mdsservice import MDSServiceController

        # Globalize mocked classes
        global VDisk
        global VDiskController
        global PMachine
        global VMachine
        global BackendType
        global VPool
        global StorageDriver
        global StorageRouter
        global FailureDomain
        global MDSService
        global MDSServiceVDisk
        global Service
        global ServiceType
        global VDiskList
        global MDSServiceController
        _ = VDisk(), PMachine(), VMachine(), VDiskController, VPool(), BackendType(), StorageDriver(), StorageRouter(), \
            FailureDomain(), MDSService(), MDSServiceVDisk(), Service(), ServiceType(), VDiskList, MDSServiceController

         # Cleaning storage
        VolatileFactory.store.clean()
        PersistentFactory.store.clean()

    @classmethod
    def setUp(cls):
        """
        (Re)Sets the stores on every test
        """
        PersistentFactory.store = DummyPersistentStore()
        PersistentFactory.store.clean()
        VolatileFactory.store = DummyVolatileStore()
        VolatileFactory.store.clean()

    def _prepare(self):
        # Setup
        failure_domain = FailureDomain()
        failure_domain.name = 'Test'
        failure_domain.save()
        backend_type = BackendType()
        backend_type.name = 'BackendType'
        backend_type.code = 'BT'
        backend_type.save()
        vpool = VPool()
        vpool.name = 'vpool'
        vpool.backend_type = backend_type
        vpool.save()
        pmachine = PMachine()
        pmachine.name = 'PMachine'
        pmachine.username = 'root'
        pmachine.ip = '127.0.0.1'
        pmachine.hvtype = 'KVM'
        pmachine.save()
        vmachine_1 = VMachine()
        vmachine_1.name = 'vmachine_1'
        vmachine_1.devicename = 'dummy'
        vmachine_1.pmachine = pmachine
        vmachine_1.is_vtemplate = True
        vmachine_1.save()
        vdisk_1_1 = VDisk()
        vdisk_1_1.name = 'vdisk_1_1'
        vdisk_1_1.volume_id = 'vdisk_1_1'
        vdisk_1_1.vmachine = vmachine_1
        vdisk_1_1.vpool = vpool
        vdisk_1_1.devicename = 'dummy'
        vdisk_1_1.size = 0
        vdisk_1_1.save()
        vdisk_1_1.reload_client()
        storage_router = StorageRouter()
        storage_router.name = 'storage_router'
        storage_router.ip = '127.0.0.1'
        storage_router.pmachine = pmachine
        storage_router.machine_id = System.get_my_machine_id()
        storage_router.rdma_capable = False
        storage_router.primary_failure_domain = failure_domain
        storage_router.save()
        storagedriver = StorageDriver()
        storagedriver.vpool = vpool
        storagedriver.storagerouter = storage_router
        storagedriver.name = '1'
        storagedriver.mountpoint = '/'
        storagedriver.cluster_ip = storage_router.ip
        storagedriver.storage_ip = '127.0.0.1'
        storagedriver.storagedriver_id = '1'
        storagedriver.ports = [1, 2, 3]
        storagedriver.save()
        service_type = ServiceType()
        service_type.name = 'MetadataServer'
        service_type.save()
        s_id = '{0}-{1}'.format(storagedriver.storagerouter.name, '1')
        service = Service()
        service.name = s_id
        service.storagerouter = storagedriver.storagerouter
        service.ports = [1]
        service.type = service_type
        service.save()
        mds_service = MDSService()
        mds_service.service = service
        mds_service.number = 0
        mds_service.capacity = 10
        mds_service.vpool = storagedriver.vpool
        mds_service.save()

        def ensure_safety(vdisk):
            pass
        class Dtl_Checkup():
            @staticmethod
            def delay(vpool_guid=None, vdisk_guid=None, storagerouters_to_exclude=None):
                pass
        MDSServiceController.ensure_safety = staticmethod(ensure_safety)
        VDiskController.dtl_checkup = Dtl_Checkup
        return vdisk_1_1, pmachine

    def test_clone_from_template_happypath(self):
        """
        Test clone from template - happy path
        """
        StorageDriverModule.use_good_client()
        vdisk_1_1, pmachine = self._prepare()

        VDiskController.create_from_template(vdisk_1_1.guid, 'vmachine_2', 'vdisk_1_1-clone', pmachine.guid)

        vdisks = VDiskList.get_vdisk_by_name('vdisk_1_1')
        self.assertEqual(len(vdisks), 1, 'Vdisk not modeled')
        clones = VDiskList.get_vdisk_by_name('vdisk_1_1-clone')
        self.assertEqual(len(clones), 1, 'Clone not modeled')

    def test_clone_from_template_error_handling(self):
        """
        Test clone from template - error during create
        """
        StorageDriverModule.use_bad_client()
        vdisk_1_1, pmachine = self._prepare()

        self.assertRaises(RuntimeError, VDiskController.create_from_template, vdisk_1_1.guid, 'vmachine_2', 'vdisk_1_1-clone', pmachine.guid)

        clones = VDiskList.get_vdisk_by_name('vdisk_1_1-clone')
        self.assertIsNone(clones, 'Clone not deleted after exception')


    def test_clone_from_template_error_handling2(self):
        """
        Test clone from template - error during create, then error during delete
        """
        StorageDriverModule.use_bad_client()
        global VDisk
        def delete(self, *args, **kwargs):
            raise RuntimeError('DAL Error')
        _delete = VDisk.delete
        VDisk.delete = delete
        vdisk_1_1, pmachine = self._prepare()

        self.assertRaises(RuntimeError, VDiskController.create_from_template, vdisk_1_1.guid, 'vmachine_2', 'vdisk_1_1-clone', pmachine.guid)

        clones = VDiskList.get_vdisk_by_name('vdisk_1_1-clone')
        self.assertEqual(len(clones), 1, 'Clone deleted')
        VDisk.delete = _delete

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(VDiskOperations)
    unittest.TextTestRunner(verbosity=2).run(suite)
