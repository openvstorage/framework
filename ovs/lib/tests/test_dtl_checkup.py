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
from ovs.dal.hybrids.j_vdiskdomain import VDiskDomain
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.storagedriver import StorageDriverClient
from ovs.extensions.storageserver.tests.mockups import MockStorageRouterClient
from ovs.lib.vdisk import VDiskController
from volumedriver.storagerouter.storagerouterclient import DTLConfig, DTLConfigMode, DTLMode


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

        for storage_router_id, domain_id, backup in structure['storagerouter_domains']:
            sr_domain = StorageRouterDomain()
            sr_domain.backup = backup
            sr_domain.domain = domains[domain_id]
            sr_domain.storagerouter = storagerouters[storage_router_id]
            sr_domain.save()

        for vdisk_id, storage_driver_id in structure['vdisks']:
            vdisk = VDisk()
            vdisk.name = str(vdisk_id)
            vdisk.devicename = 'vdisk_{0}'.format(vdisk_id)
            vdisk.volume_id = 'vdisk_{0}'.format(vdisk_id)
            vdisk.vpool = vpool
            vdisk.size = 0
            vdisk.save()
            vdisk.reload_client()
            MockStorageRouterClient.vrouter_id['vdisk_{0}'.format(vdisk_id)] = str(storage_driver_id)
            vdisks[vdisk_id] = vdisk

        return vpool, vdisks, storagerouters, domains

    def _run_and_validate_dtl_checkup(self, vdisk, validations, single_node, test_phase=None):
        """
        Execute the DTL checkup for a vDisk and validate the settings afterwards
        """
        VDiskController.dtl_checkup(vdisk_guid=vdisk.guid)
        config = vdisk.storagedriver_client.get_dtl_config(vdisk.volume_id)
        config_mode = vdisk.storagedriver_client.get_dtl_config_mode(vdisk.volume_id)
        msg = '{0} node - {{0}} - Actual: {{1}} - Expected: {{2}}'.format('Single' if single_node is True else 'Multi')
        if test_phase is not None:
            msg += ' - {0}'.format(test_phase)

        for validation in validations:
            key = validation['key']
            value = validation['value']
            if key == 'host':
                actual_value = config.host
            elif key == 'port':
                actual_value = config.port
            elif key == 'mode':
                actual_value = config.mode
            else:
                actual_value = config_mode

            if isinstance(value, list):
                self.assertTrue(expr=actual_value in value,
                                msg=msg.format(key.capitalize(), actual_value, ', '.join(value)))
            else:
                self.assertEqual(first=actual_value,
                                 second=value,
                                 msg=msg.format(key.capitalize(), actual_value, value))
        return config

    def test_single_node(self):
        """
        Execute some DTL checkups on a single node installation
        """
        # Create 1 vdisk in single node without domains
        vpool, vdisks, storagerouters, _ = self._build_model_structure(structure={'vdisks': [(1, 1)],  # (<id>, <storagedriver_id>)
                                                                                  'domains': [],
                                                                                  'storagerouters': [1],
                                                                                  'storagerouter_domains': []})
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain ||     DTL Target    ||
        #  |       1        |   1   |                 |                  |                    |
        vdisk_1 = vdisks[1]
        service_name = 'dtl_{0}'.format(vpool.name)
        ServiceManager.add_service(name=service_name, client=None)
        ServiceManager.start_service(name=service_name, client=None)
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=True,
                                           validations=[{'key': 'host', 'value': 'null'},
                                                        {'key': 'port', 'value': None},
                                                        {'key': 'mode', 'value': StorageDriverClient.FRAMEWORK_DTL_NO_SYNC},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

        # Create some domains, but do not attach them yet
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain ||     DTL Target    ||
        #  |       1        |   1   |                 |                  |                    |
        domains = {}
        for domain_id in range(1, 3):
            domain = Domain()
            domain.name = 'domain_{0}'.format(domain_id)
            domain.save()
            domains[domain_id] = domain

        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=True,
                                           validations=[{'key': 'host', 'value': 'null'},
                                                        {'key': 'port', 'value': None},
                                                        {'key': 'mode', 'value': StorageDriverClient.FRAMEWORK_DTL_NO_SYNC},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

        # Attach a regular Domain to the single Storage Router
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain ||     DTL Target    ||
        #  |      sr 1      |   1   |     domain 1    |                  |                    |
        sr_domain = StorageRouterDomain()
        sr_domain.backup = False
        sr_domain.domain = domains[1]
        sr_domain.storagerouter = storagerouters[1]
        sr_domain.save()

        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=True,
                                           validations=[{'key': 'host', 'value': 'null'},
                                                        {'key': 'port', 'value': None},
                                                        {'key': 'mode', 'value': StorageDriverClient.FRAMEWORK_DTL_NO_SYNC},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

        # Attach a recovery Domain to the single Storage Router
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain ||     DTL Target    ||
        #  |      sr 1      |   1   |                 |     domain 1     |                    |
        for junction in storagerouters[1].domains:
            junction.delete()
        sr_domain = StorageRouterDomain()
        sr_domain.backup = True
        sr_domain.domain = domains[1]
        sr_domain.storagerouter = storagerouters[1]
        sr_domain.save()
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=True,
                                           validations=[{'key': 'host', 'value': 'null'},
                                                        {'key': 'port', 'value': None},
                                                        {'key': 'mode', 'value': StorageDriverClient.FRAMEWORK_DTL_NO_SYNC},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

    def test_multi_node_without_domains(self):
        """
        Test DTL checkup on a multi node setup without Domains
        """
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |                 |                  |             |
        #  |      sr 2      |       |                 |                  |      1      |
        #  |      sr 3      |       |                 |                  |      1      |
        #  |      sr 4      |       |                 |                  |      1      |
        #  |      sr 5      |       |                 |                  |      1      |
        vpool, vdisks, storagerouters, _ = self._build_model_structure(structure={'vdisks': [(1, 1)],  # (<id>, <storagedriver_id>)
                                                                                  'domains': [],
                                                                                  'storagerouters': [1, 2, 3, 4, 5],
                                                                                  'storagerouter_domains': []})
        vdisk_1 = vdisks[1]
        service_name = 'dtl_{0}'.format(vpool.name)
        ServiceManager.add_service(name=service_name, client=None)
        ServiceManager.start_service(name=service_name, client=None)
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': [sr.ip for sr in storagerouters.values()[1:]]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

    def test_multi_node_with_unused_domains(self):
        """
        Test DTL checkup on a multi node setup and create some Domains, but do not link them to any Storage Router
        """
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |                 |                  |             |
        #  |      sr 2      |       |                 |                  |      1      |
        #  |      sr 3      |       |                 |                  |      1      |
        #  |      sr 4      |       |                 |                  |      1      |
        #  |      sr 5      |       |                 |                  |      1      |
        vpool, vdisks, storagerouters, _ = self._build_model_structure(structure={'vdisks': [(1, 1)],  # (<id>, <storagedriver_id>)
                                                                                  'domains': [1, 2, 3],
                                                                                  'storagerouters': [1, 2, 3, 4, 5],
                                                                                  'storagerouter_domains': []})
        vdisk_1 = vdisks[1]
        service_name = 'dtl_{0}'.format(vpool.name)
        ServiceManager.add_service(name=service_name, client=None)
        ServiceManager.start_service(name=service_name, client=None)
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': [sr.ip for sr in storagerouters.values()[1:]]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

    def test_multi_node_with_used_domains_on_local_sr(self):
        """
        Test DTL checkup on a multi node setup and create some Domains and link them to the Storage Router on which the vDisk lives
        """
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |     domain 1    |                  |             |
        #  |      sr 2      |       |                 |                  |             |
        #  |      sr 3      |       |                 |                  |             |
        #  |      sr 4      |       |                 |                  |             |
        #  |      sr 5      |       |                 |                  |             |
        vpool, vdisks, storagerouters, domains = self._build_model_structure(structure={'vdisks': [(1, 1)],  # (<id>, <storagedriver_id>)
                                                                                        'domains': [1, 2, 3],
                                                                                        'storagerouters': [1, 2, 3, 4, 5],
                                                                                        'storagerouter_domains': [(1, 1, False)]})  # (<storage_router_id>, <domain_id>, <backup>)
        vdisk_1 = vdisks[1]
        service_name = 'dtl_{0}'.format(vpool.name)
        ServiceManager.add_service(name=service_name, client=None)
        ServiceManager.start_service(name=service_name, client=None)

        # When domains have been attached to the StorageRouter on which the vDisk resides, but no other Storage Routers have same Domain --> Stand Alone
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': 'null'},
                                                        {'key': 'port', 'value': None},
                                                        {'key': 'mode', 'value': StorageDriverClient.FRAMEWORK_DTL_NO_SYNC},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

        # Remove the linked Domain and add a recovery Domain instead --> DTL is still disabled at this point --> DTL checkup should not change anything
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |                 |     domain 1     |             |
        #  |      sr 2      |       |                 |                  |             |
        #  |      sr 3      |       |                 |                  |             |
        #  |      sr 4      |       |                 |                  |             |
        #  |      sr 5      |       |                 |                  |             |
        for junction in storagerouters[1].domains:
            junction.delete()
        sr_domain = StorageRouterDomain()
        sr_domain.backup = True
        sr_domain.domain = domains[1]
        sr_domain.storagerouter = storagerouters[1]
        sr_domain.save()
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': 'null'},
                                                        {'key': 'port', 'value': None},
                                                        {'key': 'mode', 'value': StorageDriverClient.FRAMEWORK_DTL_NO_SYNC},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

    def test_multi_node_with_regular_domains(self):
        """
        Test DTL checkup on a multi node setup and create some Domains and link them to the several Storage Routers
        """
        # Add a regular domain to the Storage Router serving the vDisk and another Storage Router --> DTL target should be the specific Storage Router
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |     domain 1    |                  |             |
        #  |      sr 2      |       |     domain 1    |                  |      1      |
        #  |      sr 3      |       |                 |                  |             |
        #  |      sr 4      |       |                 |                  |             |
        #  |      sr 5      |       |                 |                  |             |
        vpool, vdisks, storagerouters, domains = self._build_model_structure(structure={'vdisks': [(1, 1)],  # (<id>, <storagedriver_id>)
                                                                                        'domains': [1, 2, 3],
                                                                                        'storagerouters': [1, 2, 3, 4, 5],
                                                                                        'storagerouter_domains': [(1, 1, False), (2, 1, False)]})  # (<storage_router_id>, <domain_id>, <backup>)
        vdisk_1 = vdisks[1]
        service_name = 'dtl_{0}'.format(vpool.name)
        ServiceManager.add_service(name=service_name, client=None)
        ServiceManager.start_service(name=service_name, client=None)
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': storagerouters[2].ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

        # Add the regular Domain as regular Domain to additional Storage Routers --> DTL target should remain on same Storage Router
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |     domain 1    |                  |             |
        #  |      sr 2      |       |     domain 1    |                  |      1      |
        #  |      sr 3      |       |     domain 1    |                  |             |
        #  |      sr 4      |       |     domain 1    |                  |             |
        #  |      sr 5      |       |     domain 1    |                  |             |
        for storagerouter in storagerouters.values()[2:]:
            sr_domain = StorageRouterDomain()
            sr_domain.backup = False
            sr_domain.domain = domains[1]
            sr_domain.storagerouter = storagerouter
            sr_domain.save()
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': storagerouters[2].ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

        # Add recovery Domain to the Storage Router on which the vDisks lives --> nothing should change for now
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |     domain 1    |     domain 2     |             |
        #  |      sr 2      |       |     domain 1    |                  |      1      |
        #  |      sr 3      |       |     domain 1    |                  |             |
        #  |      sr 4      |       |     domain 1    |                  |             |
        #  |      sr 5      |       |     domain 1    |                  |             |
        sr_domain = StorageRouterDomain()
        sr_domain.backup = True
        sr_domain.domain = domains[2]
        sr_domain.storagerouter = storagerouters[1]
        sr_domain.save()
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': storagerouters[2].ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

        # # Add the recovery Domain as regular Domain to additional StorageRouters --> Recovery Domain should have priority over regular Domain
        # # || StorageRouter || vDisk | Regular Domain    || Recovery Domain || DTL Target ||
        # #  |      sr 1      |   1   | domain 1           |     domain 2     |             |
        # #  |      sr 2      |       | domain 1           |                  |             |
        # #  |      sr 3      |       | domain 1, domain 2 |                  |      1      |
        # #  |      sr 4      |       | domain 1, domain 2 |                  |      1      |
        # #  |      sr 5      |       | domain 1, domain 2 |                  |      1      |
        for storagerouter in storagerouters.values()[2:]:
            sr_domain = StorageRouterDomain()
            sr_domain.backup = False
            sr_domain.domain = domains[2]
            sr_domain.storagerouter = storagerouter
            sr_domain.save()
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': [sr.ip for sr in storagerouters.values()[2:]]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

    def test_multi_node_with_recovery_domains(self):
        """
        Test DTL checkup on a multi node setup and create some Domains and link them to the several Storage Routers
        """
        # Add a recovery Domain to the Storage Router serving the vDisk --> DTL should be random
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |                 |      domain 1    |             |
        #  |      sr 2      |       |                 |                  |      1      |
        #  |      sr 3      |       |                 |                  |      1      |
        #  |      sr 4      |       |                 |                  |      1      |
        #  |      sr 5      |       |                 |                  |      1      |
        vpool, vdisks, storagerouters, domains = self._build_model_structure(structure={'vdisks': [(1, 1)],  # (<id>, <storagedriver_id>)
                                                                                        'domains': [1, 2, 3],
                                                                                        'storagerouters': [1, 2, 3, 4, 5],
                                                                                        'storagerouter_domains': [(1, 1, True)]})  # (<storage_router_id>, <domain_id>, <backup>)
        vdisk_1 = vdisks[1]
        service_name = 'dtl_{0}'.format(vpool.name)
        ServiceManager.add_service(name=service_name, client=None)
        ServiceManager.start_service(name=service_name, client=None)
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': [sr.ip for sr in storagerouters.values()[1:]]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

        # Add the recovery domain as regular Domain of the same Storage Router --> nothing should change
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |                 |      domain 1    |             |
        #  |      sr 2      |       |    domain 1     |                  |      1      |
        #  |      sr 3      |       |                 |                  |             |
        #  |      sr 4      |       |                 |                  |             |
        #  |      sr 5      |       |                 |                  |             |
        sr_domain = StorageRouterDomain()
        sr_domain.backup = False
        sr_domain.domain = domains[1]
        sr_domain.storagerouter = storagerouters[2]
        sr_domain.save()
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': storagerouters[2].ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

        # Add the recovery domain as regular Domain to the other Storage Routers --> nothing should change
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |                 |      domain 1    |             |
        #  |      sr 2      |       |    domain 1     |                  |      1      |
        #  |      sr 3      |       |    domain 1     |                  |             |
        #  |      sr 4      |       |    domain 1     |                  |             |
        #  |      sr 5      |       |    domain 1     |                  |             |
        for storagerouter in storagerouters.values()[2:]:
            sr_domain = StorageRouterDomain()
            sr_domain.backup = False
            sr_domain.domain = domains[1]
            sr_domain.storagerouter = storagerouter
            sr_domain.save()
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': storagerouters[2].ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

        # Remove the domain from the Storage Router which is used as DTL target
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |                 |      domain 1    |             |
        #  |      sr 2      |       |                 |                  |             |
        #  |      sr 3      |       |    domain 1     |                  |      1      |
        #  |      sr 4      |       |    domain 1     |                  |      1      |
        #  |      sr 5      |       |    domain 1     |                  |      1      |
        for junction in storagerouters[2].domains:
            junction.delete()
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': [sr.ip for sr in storagerouters.values()[2:]]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

        # Add regular domain to the Storage Router serving the vDisk and some other Storage Routers --> recovery Domain should still get priority
        # || StorageRouter || vDisk | Regular Domain    || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   | domain 2           |      domain 1    |             |
        #  |      sr 2      |       | domain 2           |                  |             |
        #  |      sr 3      |       | domain 2           |                  |             |
        #  |      sr 4      |       | domain 1, domain 2 |                  |      1      |
        #  |      sr 5      |       | domain 2           |                  |             |
        for junction in storagerouters[3].domains:
            junction.delete()
        for junction in storagerouters[5].domains:
            junction.delete()
        for storagerouter in storagerouters.values():
            sr_domain = StorageRouterDomain()
            sr_domain.backup = False
            sr_domain.domain = domains[2]
            sr_domain.storagerouter = storagerouter
            sr_domain.save()
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': storagerouters[4].ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

    def test_dtl_no_unnecessary_reconfiguration(self):
        """
        Verify that when more than 3 Storage Routers are available as possible DTL target, the same target is used over and over again
        """
        vpool, vdisks, storagerouters, _ = self._build_model_structure(structure={'vdisks': [(1, 1)],  # (<id>, <storagedriver_id>)
                                                                                  'domains': [1],
                                                                                  'storagerouters': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                                                                                  'storagerouter_domains': [(1, 1, True), (2, 1, False), (3, 1, False), (4, 1, False),
                                                                                                            (5, 1, False), (6, 1, False), (7, 1, False), (8, 1, False),
                                                                                                            (9, 1, False), (10, 1, False)]})  # (<storage_router_id>, <domain_id>, <backup>)
        vdisk_1 = vdisks[1]
        service_name = 'dtl_{0}'.format(vpool.name)
        ServiceManager.add_service(name=service_name, client=None)
        ServiceManager.start_service(name=service_name, client=None)
        config = self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                                    single_node=False,
                                                    validations=[{'key': 'host', 'value': [sr.ip for sr in storagerouters.values()[1:]]},
                                                                 {'key': 'port', 'value': 3},
                                                                 {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS},
                                                                 {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])
        # Rerun DTL checkup 10 times and validate target does not change even though 9 Storage Routers are potential candidate
        for _ in xrange(10):
            self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                               single_node=False,
                                               validations=[{'key': 'host', 'value': config.host},
                                                            {'key': 'port', 'value': 3},
                                                            {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS},
                                                            {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

    def test_manually_overruled_dtl(self):
        """
        The DTL target of a vDisk can be manually overruled by the customer
        """
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |                 |      domain 1    |             |
        #  |      sr 2      |       |      domain 1   |                  |             |
        #  |      sr 3      |       |      domain 1   |                  |             |
        #  |      sr 4      |       |      domain 2   |                  |             |
        #  |      sr 5      |       |                 |                  |             |
        vpool, vdisks, storagerouters, domains = self._build_model_structure(structure={'vdisks': [(1, 1)],  # (<id>, <storagedriver_id>)
                                                                                        'domains': [1, 2],
                                                                                        'storagerouters': [1, 2, 3, 4, 5],
                                                                                        'storagerouter_domains': [(1, 1, True), (2, 1, False), (3, 1, False), (4, 2, False)]})  # (<storage_router_id>, <domain_id>, <backup>)
        vdisk_1 = vdisks[1]
        service_name = 'dtl_{0}'.format(vpool.name)
        ServiceManager.add_service(name=service_name, client=None)
        ServiceManager.start_service(name=service_name, client=None)
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': [sr.ip for sr in storagerouters.values()[1:3]]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

        # Set DTL manually to node 2 and add 2 vdisk domains to the vdisk
        vdisk_1.storagedriver_client.set_manual_dtl_config(volume_id=vdisk_1.volume_id,
                                                           config=DTLConfig(str(storagerouters[2].ip), 3, DTLMode.SYNCHRONOUS))
        vdomain1 = VDiskDomain()
        vdomain2 = VDiskDomain()
        vdomain1.vdisk = vdisk_1
        vdomain2.vdisk = vdisk_1
        vdomain1.domain = domains[1]
        vdomain2.domain = domains[2]
        vdomain1.save()
        vdomain2.save()
        vdisk_1.has_manual_dtl = True
        vdisk_1.save()
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': storagerouters[2].ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.SYNCHRONOUS},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

        # Delete the vDiskDomain on which the DTL resides, 1 other vDiskDomain remains
        vdomain1.delete()
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': storagerouters[4].ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.SYNCHRONOUS},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])

        # Delete the last vDiskDomain --> DTL is no longer manual
        vdomain2.delete()
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': [sr.ip for sr in storagerouters.values()[1:3]]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])
        vdisk_1.discard()
        self.assertFalse(expr=vdisk_1.has_manual_dtl,
                         msg='vDisk "vdisk_1" should have manual_dtl flag set to False')

        # Overrules the DTL manually to None and validate DTL checkup leaves it as it is
        vdisk_1.storagedriver_client.set_manual_dtl_config(volume_id=vdisk_1.volume_id, config=None)
        self._run_and_validate_dtl_checkup(vdisk=vdisk_1,
                                           single_node=False,
                                           validations=[{'key': 'host', 'value': 'null'},
                                                        {'key': 'port', 'value': None},
                                                        {'key': 'mode', 'value': StorageDriverClient.FRAMEWORK_DTL_NO_SYNC},
                                                        {'key': 'config_mode', 'value': DTLConfigMode.MANUAL}])
