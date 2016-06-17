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
DTL checkup test module
"""
import unittest
from ovs.dal.hybrids.backendtype import BackendType
from ovs.dal.hybrids.domain import Domain
from ovs.dal.hybrids.j_storagerouterdomain import StorageRouterDomain
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.tests.mockups import MockStorageRouterClient
from ovs.lib.vdisk import VDiskController
from volumedriver.storagerouter.storagerouterclient import DTLConfigMode


class DTLCheckup(unittest.TestCase):
    """
    This test class will validate the various scenarios of the DTL checkup logic
    """
    @classmethod
    def setUpClass(cls):
        """
        Sets up the unittest, mocking a certain set of 3rd party libraries and extensions.
        This makes sure the unittests can be executed without those libraries installed
        """
        cls.persistent = PersistentFactory.get_client()
        cls.persistent.clean()

        cls.volatile = VolatileFactory.get_client()
        cls.volatile.clean()
        ServiceManager.clean()
        MockStorageRouterClient.clean()

    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        # Cleaning storage
        self.volatile.clean()
        self.persistent.clean()
        ServiceManager.clean()
        MockStorageRouterClient.clean()

    def tearDown(self):
        """
        Clean up the unittest
        """
        # Cleaning storage
        self.volatile.clean()
        self.persistent.clean()
        ServiceManager.clean()
        MockStorageRouterClient.clean()

    @staticmethod
    def _build_model_structure(structure):
        """
        Builds a structure in model to test with
        """
        backend_type = BackendType()
        backend_type.name = 'BackendType'
        backend_type.code = 'BT'
        backend_type.save()

        pmachine = PMachine()
        pmachine.name = 'physical_machine'
        pmachine.username = 'root'
        pmachine.ip = '127.0.0.1'
        pmachine.hvtype = 'VMWARE'
        pmachine.save()

        vpool = VPool()
        vpool.name = 'vpool'
        vpool.status = 'RUNNING'
        vpool.backend_type = backend_type
        vpool.save()

        vdisks = {}
        domains = {}
        storagerouters = {}
        storagedrivers = {}
        storagerouter_domains = {}
        for domain_id in structure['domains']:
            domain = Domain()
            domain.name = 'domain_{0}'.format(domain_id)
            domain.save()
            domains[domain_id] = domain

        for storage_router_id in structure['storagerouters']:
            storagerouter = StorageRouter()
            storagerouter.name = str(storage_router_id)
            storagerouter.ip = '10.0.0.{0}'.format(storage_router_id)
            storagerouter.pmachine = pmachine
            storagerouter.rdma_capable = False
            storagerouter.save()
            storagerouters[storage_router_id] = storagerouter

            storagedriver = StorageDriver()
            storagedriver.vpool = vpool
            storagedriver.storagerouter = storagerouter
            storagedriver.name = str(storage_router_id)
            storagedriver.mountpoint = '/'
            storagedriver.cluster_ip = storagerouter.ip
            storagedriver.storage_ip = '127.0.0.1'
            storagedriver.storagedriver_id = str(storage_router_id)
            storagedriver.ports = {'management': 1,
                                   'xmlrpc': 2,
                                   'dtl': 3,
                                   'edge': 4}
            storagedriver.save()
            storagedrivers[storage_router_id] = storagedriver

        for storage_driver_id, storage_router_id, domain_id, backup in structure['storagerouter_domains']:
            sr_domain = StorageRouterDomain()
            sr_domain.backup = backup
            sr_domain.domain = domains[domain_id]
            sr_domain.storagerouter = storagerouters[storage_router_id]
            sr_domain.save()
            storagerouter_domains[storage_driver_id] = sr_domain

        for vdisk_id, storage_driver_id in structure['vdisks']:
            vdisk = VDisk()
            vdisk.name = str(vdisk_id)
            vdisk.devicename = 'vdisk_{0}'.format(vdisk_id)
            vdisk.volume_id = 'vdisk_{0}'.format(vdisk_id)
            vdisk.vpool = vpool
            vdisk.size = 0
            vdisk.save()
            vdisk.reload_client()
            # MockStorageRouterClient.vrouter_id['vdisk_{0}'.format(vdisk_id)] = str(storage_driver_id)
            vdisks[vdisk_id] = vdisk

        return vpool, vdisks, storagerouters, storagedrivers, domains, storagerouter_domains

    def test_happypath(self):
        """
        Validates the happy path; Hourly snapshots are taken with a few manual consistent
        every now an then. The delete policy is executed every day
        """
        vpool, vdisks, storagerouters, _, _, _ = self._build_model_structure(structure={'vdisks': [(1, 1)],  # (<id>, <storagedriver_id>)
                                                                                        'domains': [],
                                                                                        'storagerouters': [1, 2, 3],
                                                                                        'storagerouter_domains': []})
        vdisk_1 = vdisks[1]
        this_sr = StorageRouter(vdisk_1.storagerouter_guid)
        service_name = 'dtl_{0}'.format(vpool.name)
        ServiceManager.add_service(name=service_name, client=None)
        ServiceManager.start_service(name=service_name, client=None)
        VDiskController.dtl_checkup(vdisk_guid=vdisk_1.guid)
        dtl_config = vdisk_1.storagedriver_client.get_dtl_config(vdisk_1.volume_id)
        self.assertTrue(expr=dtl_config.host in [sr.ip for sr in storagerouters.values() if sr != this_sr],
                        msg='DTL not configured correctly')
        self.assertEqual(first=dtl_config.dtl_config_mode,
                         second=DTLConfigMode.MANUAL,
                         msg='DTL mode should be manual')
        self.assertEqual(first=dtl_config.port,
                         second=3,
                         msg='DTL port should be 3')
