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
DTL allocation rules:
    - First priority to StorageRouters located in the vDisk's StorageRouter's Recovery Domain
    - Second priority to StorageRouters located in the vDisk's StorageRouter's Regular Domain
    - If Domains configured, but no StorageRouters are found matching any of the Domains on the vDisk's StorageRouter, a random SR in the same vPool is chosen
    - If no Domains configured on the vDisk StorageRouter, any other StorageRouter on which the vPool has been extended is chosen
"""
import unittest
from ovs.dal.hybrids.domain import Domain
from ovs.dal.hybrids.j_storagerouterdomain import StorageRouterDomain
from ovs.dal.hybrids.j_vdiskdomain import VDiskDomain
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.tests.helpers import DalHelper
from ovs.extensions.generic.sshclient import SSHClient
from ovs_extensions.log.log_handler import LogHandler
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storageserver.storagedriver import DTLConfig, DTLConfigMode, DTLMode
from ovs.lib.vdisk import VDiskController


class DTLCheckup(unittest.TestCase):
    """
    This test class will validate the various scenarios of the DTL checkup logic
    """
    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        DalHelper.setup()

    def tearDown(self):
        """
        Clean up the unittest
        """
        DalHelper.teardown()

    def _run_and_validate_dtl_checkup(self, vdisk, validations):
        """
        Execute the DTL checkup for a vDisk and validate the settings afterwards
        """
        single_node = len(StorageRouterList.get_storagerouters()) == 1
        VDiskController.dtl_checkup(vdisk_guid=vdisk.guid)
        config = vdisk.storagedriver_client.get_dtl_config(vdisk.volume_id)
        config_mode = vdisk.storagedriver_client.get_dtl_config_mode(vdisk.volume_id)
        msg = '{0} node - {{0}} - Actual: {{1}} - Expected: {{2}}'.format('Single' if single_node is True else 'Multi')

        validations.append({'key': 'config_mode', 'value': DTLConfigMode.MANUAL})
        for validation in validations:
            key = validation['key']
            value = validation['value']
            if key == 'config':
                actual_value = config
            elif key == 'host':
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

    @staticmethod
    def _roll_out_dtl_services(vpool, storagerouters):
        """
        Deploy and start the DTL service on all storagerouters
        :param storagerouters: StorageRouters to deploy and start a DTL service on
        :return: None
        """
        service_manager = ServiceFactory.get_manager()
        service_name = 'dtl_{0}'.format(vpool.name)
        for sr in storagerouters.values():
            client = SSHClient(sr, 'root')
            service_manager.add_service(name=service_name, client=client)
            service_manager.start_service(name=service_name, client=client)

    def test_single_node(self):
        """
        Execute some DTL checkups on a single node installation
        """
        # Create 1 vdisk in single node without domains
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <sr_id>)
        )
        vpool = structure['vpools'][1]
        vdisk = structure['vdisks'][1]
        storagerouter = structure['storagerouters'][1]
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain ||     DTL Target    ||
        #  |       1        |   1   |                 |                  |                    |
        self._roll_out_dtl_services(vpool=vpool, storagerouters=structure['storagerouters'])
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'config', 'value': None}])

        # Create some domains, but do not attach them yet
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain ||     DTL Target    ||
        #  |       1        |   1   |                 |                  |                    |
        domains = {}
        for domain_id in range(1, 3):
            domain = Domain()
            domain.name = 'domain_{0}'.format(domain_id)
            domain.save()
            domains[domain_id] = domain

        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'config', 'value': None}])

        # Attach a regular Domain to the single Storage Router
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain ||     DTL Target    ||
        #  |      sr 1      |   1   |     domain 1    |                  |                    |
        sr_domain = StorageRouterDomain()
        sr_domain.backup = False
        sr_domain.domain = domains[1]
        sr_domain.storagerouter = storagerouter
        sr_domain.save()

        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'config', 'value': None}])

        # Attach a recovery Domain to the single Storage Router
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain ||     DTL Target    ||
        #  |      sr 1      |   1   |                 |     domain 1     |                    |
        for junction in storagerouter.domains:
            junction.delete()
        sr_domain = StorageRouterDomain()
        sr_domain.backup = True
        sr_domain.domain = domains[1]
        sr_domain.storagerouter = storagerouter
        sr_domain.save()
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'config', 'value': None}])

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
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2, 3, 4, 5],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5)]}  # (<id>, <vpool_id>, <sr_id>)
        )
        vpool = structure['vpools'][1]
        vdisk = structure['vdisks'][1]
        storagerouters = structure['storagerouters']

        self._roll_out_dtl_services(vpool=vpool, storagerouters=storagerouters)
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': [sr.storagedrivers[0].storage_ip for sr in storagerouters.values()[1:]]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

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
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'domains': [1, 2, 3],
             'storagerouters': [1, 2, 3, 4, 5],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5)]}  # (<id>, <vpool_id>, <sr_id>)
        )
        vpool = structure['vpools'][1]
        vdisk = structure['vdisks'][1]
        storagerouters = structure['storagerouters']

        self._roll_out_dtl_services(vpool=vpool, storagerouters=storagerouters)
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': [sr.storagedrivers[0].storage_ip for sr in storagerouters.values()[1:]]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

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
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'domains': [1, 2, 3],
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2, 3, 4, 5],
             'storagerouter_domains': [(1, 1, 1, False)],  # (<sr_domain_id>, <sr_id>, <domain_id>, <backup>)
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5)]}  # (<id>, <vpool_id>, <sr_id>)
        )
        vpool = structure['vpools'][1]
        vdisk = structure['vdisks'][1]
        domain = structure['domains'][1]
        storagerouters = structure['storagerouters']
        storagerouter = storagerouters[1]

        # When domains have been attached to the StorageRouter on which the vDisk resides, but no other Storage Routers have same Domain, random SR is chosen
        self._roll_out_dtl_services(vpool=vpool, storagerouters=structure['storagerouters'])
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': [sr.storagedrivers[0].storage_ip for sr in storagerouters.values()]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

        # Remove the linked Domain and add a recovery Domain instead --> DTL checkup should not change anything
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |                 |     domain 1     |             |
        #  |      sr 2      |       |                 |                  |             |
        #  |      sr 3      |       |                 |                  |             |
        #  |      sr 4      |       |                 |                  |             |
        #  |      sr 5      |       |                 |                  |             |
        for junction in storagerouter.domains:
            junction.delete()
        sr_domain = StorageRouterDomain()
        sr_domain.backup = True
        sr_domain.domain = domain
        sr_domain.storagerouter = storagerouter
        sr_domain.save()
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': [sr.storagedrivers[0].storage_ip for sr in storagerouters.values()]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

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
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'domains': [1, 2, 3],
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2, 3, 4, 5],
             'storagerouter_domains': [(1, 1, 1, False), (2, 2, 1, False)],  # (<sr_domain_id>, <sr_id>, <domain_id>, <backup>)
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5)]}  # (<id>, <vpool_id>, <sr_id>)
        )
        vpool = structure['vpools'][1]
        vdisk = structure['vdisks'][1]
        domains = structure['domains']
        storagerouters = structure['storagerouters']

        self._roll_out_dtl_services(vpool=vpool, storagerouters=storagerouters)
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': storagerouters[2].storagedrivers[0].storage_ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

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
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': storagerouters[2].storagedrivers[0].storage_ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

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
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': storagerouters[2].storagedrivers[0].storage_ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

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
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': [sr.storagedrivers[0].storage_ip for sr in storagerouters.values()[2:]]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

    def test_multi_node_with_recovery_domains(self):
        """
        Test DTL checkup on a multi node setup and create some Domains and link them to the several Storage Routers
        """
        # Add a recovery Domain to the Storage Router serving the vDisk --> DTL should be random
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |                 |      domain 1    |             |
        #  |      sr 2      |       |                 |                  |             |
        #  |      sr 3      |       |                 |                  |             |
        #  |      sr 4      |       |                 |                  |             |
        #  |      sr 5      |       |                 |                  |             |
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'domains': [1, 2, 3],
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2, 3, 4, 5],
             'storagerouter_domains': [(1, 1, 1, True)],  # (<sr_domain_id>, <sr_id>, <domain_id>, <backup>)
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5)]}  # (<id>, <vpool_id>, <sr_id>)
        )
        vpool = structure['vpools'][1]
        vdisk = structure['vdisks'][1]
        domains = structure['domains']
        storagerouters = structure['storagerouters']

        self._roll_out_dtl_services(vpool=vpool, storagerouters=storagerouters)
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': [sr.storagedrivers[0].storage_ip for sr in storagerouters.values()[1:]]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

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
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': storagerouters[2].storagedrivers[0].storage_ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

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
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': storagerouters[2].storagedrivers[0].storage_ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

        # Remove the domain from the Storage Router which is used as DTL target
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |                 |      domain 1    |             |
        #  |      sr 2      |       |                 |                  |             |
        #  |      sr 3      |       |    domain 1     |                  |      1      |
        #  |      sr 4      |       |    domain 1     |                  |      1      |
        #  |      sr 5      |       |    domain 1     |                  |      1      |
        for junction in storagerouters[2].domains:
            junction.delete()
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': [sr.storagedrivers[0].storage_ip for sr in storagerouters.values()[2:]]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

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
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': storagerouters[4].storagedrivers[0].storage_ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

    def test_dtl_no_unnecessary_reconfiguration(self):
        """
        Verify that when more than 3 Storage Routers are available as possible DTL target, the same target is used over and over again
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'domains': [1],
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
             'storagerouter_domains': [(1, 1, 1, True), (2, 2, 1, False), (3, 3, 1, False), (4, 4, 1, False),
                                       (5, 5, 1, False), (6, 6, 1, False), (7, 7, 1, False), (8, 8, 1, False),
                                       (9, 9, 1, False), (10, 10, 1, False)],  # (<sr_domain_id>, <sr_id>, <domain_id>, <backup>)
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5),
                                (6, 1, 6), (7, 1, 7), (8, 1, 8), (9, 1, 9), (10, 1, 10)]}  # (<id>, <vpool_id>, <sr_id>)
        )
        vpool = structure['vpools'][1]
        vdisk = structure['vdisks'][1]
        storagerouters = structure['storagerouters']

        self._roll_out_dtl_services(vpool=vpool, storagerouters=storagerouters)
        config = self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                                    validations=[{'key': 'host', 'value': [sr.storagedrivers[0].storage_ip for sr in storagerouters.values()[1:]]},
                                                                 {'key': 'port', 'value': 3},
                                                                 {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])
        # Rerun DTL checkup 10 times and validate target does not change even though 9 Storage Routers are potential candidate
        for _ in xrange(10):
            self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                               validations=[{'key': 'host', 'value': config.host},
                                                            {'key': 'port', 'value': 3},
                                                            {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

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
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'domains': [1, 2],
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2, 3, 4, 5],
             'storagerouter_domains': [(1, 1, 1, True), (2, 2, 1, False), (3, 3, 1, False), (4, 4, 2, False)],  # (<sr_domain_id>, <sr_id>, <domain_id>, <backup>)
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5)]}  # (<id>, <vpool_id>, <sr_id>)
        )
        vpool = structure['vpools'][1]
        vdisk = structure['vdisks'][1]
        domains = structure['domains']
        storagerouters = structure['storagerouters']

        self._roll_out_dtl_services(vpool=vpool, storagerouters=storagerouters)
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': [sr.storagedrivers[0].storage_ip for sr in storagerouters.values()[1:3]]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

        # Set DTL manually to node 2 and add 2 vdisk domains to the vdisk
        vdisk.storagedriver_client.set_manual_dtl_config(volume_id=vdisk.volume_id,
                                                         config=DTLConfig(str(storagerouters[2].storagedrivers[0].storage_ip), 3, DTLMode.SYNCHRONOUS))
        vdomain1 = VDiskDomain()
        vdomain2 = VDiskDomain()
        vdomain1.vdisk = vdisk
        vdomain2.vdisk = vdisk
        vdomain1.domain = domains[1]
        vdomain2.domain = domains[2]
        vdomain1.save()
        vdomain2.save()
        vdisk.has_manual_dtl = True
        vdisk.save()
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': storagerouters[2].storagedrivers[0].storage_ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.SYNCHRONOUS}])
        # Delete the vDiskDomain on which the DTL resides, 1 other vDiskDomain remains, no changes should be made, but OVS_WARNING should be logged
        vdomain1.delete()
        LogHandler._logs = {}
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': storagerouters[2].storagedrivers[0].storage_ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.SYNCHRONOUS}])
        warning_logs = []
        for log in LogHandler._logs['lib_vdisk']:
            if 'OVS_WARNING' in log and 'manual DTL configuration is no longer' in log and vdisk.guid in log:
                warning_logs.append(log)
        self.assertEqual(first=1, second=len(warning_logs))

        # Delete the last vDiskDomain --> DTL should not be changed
        vdomain2.delete()
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': storagerouters[2].storagedrivers[0].storage_ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.SYNCHRONOUS}])
        vdisk.discard()
        self.assertTrue(expr=vdisk.has_manual_dtl)

        # Overrules the DTL manually to None and validate DTL checkup leaves it as it is
        vdisk.storagedriver_client.set_manual_dtl_config(volume_id=vdisk.volume_id, config=None)
        vdisk.has_manual_dtl = True
        vdisk.save()
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'config', 'value': None}])

    def test_from_single_node_to_multi_node(self):
        """
        Deploy a vDisk on a single node --> This should result in no DTL configured
        Add an additional node and verify DTL will be set
        """
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |                 |                  |             |
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1],
             'storagedrivers': [(1, 1, 1)]}  # (<id>, <vpool_id>, <sr_id>)
        )
        vpool = structure['vpools'][1]
        vdisk = structure['vdisks'][1]
        storagerouters = structure['storagerouters']

        self._roll_out_dtl_services(vpool=vpool, storagerouters=storagerouters)
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'config', 'value': None}])

        # Add a Storage Router
        # || StorageRouter || vDisk | Regular Domain || Recovery Domain || DTL Target ||
        #  |      sr 1      |   1   |                 |                  |             |
        #  |      sr 2      |       |                 |                  |      1      |
        storagerouter = StorageRouter()
        storagerouter.name = '2'
        storagerouter.ip = '10.0.0.2'
        storagerouter.rdma_capable = False
        storagerouter.save()
        storagerouters[2] = storagerouter
        self._roll_out_dtl_services(vpool=vpool, storagerouters=storagerouters)

        storagedriver = StorageDriver()
        storagedriver.vpool = vpool
        storagedriver.storagerouter = storagerouter
        storagedriver.name = '2'
        storagedriver.mountpoint = '/'
        storagedriver.cluster_ip = storagerouter.ip
        storagedriver.storage_ip = '10.0.1.2'
        storagedriver.storagedriver_id = '2'
        storagedriver.ports = {'management': 1,
                               'xmlrpc': 2,
                               'dtl': 3,
                               'edge': 4}
        storagedriver.save()
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': storagerouters[2].storagedrivers[0].storage_ip},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

    def test_incorrect_dtl_fixup(self):
        """
        Validates whether the DTL checkup logic can fix a vDisk who's DTL is configured to an unexpected ip
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'vdisks': [(1, 1, 1, 1)],  # (<id>, <storagedriver_id>, <vpool_id>, <mds_service_id>)
             'mds_services': [(1, 1)],  # (<id>, <storagedriver_id>)
             'storagerouters': [1, 2],
             'storagedrivers': [(1, 1, 1), (2, 1, 2)]}  # (<id>, <vpool_id>, <sr_id>)
        )
        vpool = structure['vpools'][1]
        vdisk = structure['vdisks'][1]
        storagerouters = structure['storagerouters']

        self._roll_out_dtl_services(vpool=vpool, storagerouters=storagerouters)
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': [sr.storagedrivers[0].storage_ip for sr in storagerouters.values()]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.ASYNCHRONOUS}])

        # Set DTL manually to an unexpected IP
        vdisk.storagedriver_client.set_manual_dtl_config(volume_id=vdisk.volume_id,
                                                         config=DTLConfig(str(storagerouters[1].ip), 3, DTLMode.SYNCHRONOUS))

        # And after another DTL checkup, it should be restored again
        self._run_and_validate_dtl_checkup(vdisk=vdisk,
                                           validations=[{'key': 'host', 'value': [sr.storagedrivers[0].storage_ip for sr in storagerouters.values()]},
                                                        {'key': 'port', 'value': 3},
                                                        {'key': 'mode', 'value': DTLMode.SYNCHRONOUS}])
