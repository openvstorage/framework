# Copyright (C) 2018 iNuron NV
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

import unittest
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonAssertionFailed, ArakoonNotFound
from ovs_extensions.db.arakoon.arakooninstaller import ArakoonNodeConfig
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig, ArakoonInstaller
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.services.servicefactory import ServiceFactory


class PyrakoonTestCase(unittest.TestCase):
    """
    Test Pyrakoon
    This tests the integration of the Pyrakoonclient
    """
    NAMESPACE = 'ovs_testing'

    def setUp(self):
        self.persistent = PersistentFactory.get_client()
        self.pyrakoon_client = self.persistent._client
        self.service_manager = ServiceFactory.get_manager()
        self.killed_arakoons = []

    def tearDown(self):
        self.pyrakoon_client.delete_prefix(self.NAMESPACE)
        while self.killed_arakoons:
            client, service_name = self.killed_arakoons.pop()
            self.service_manager.restart_service(service_name, client)

    # def test_operations(self):
    #     """
    #     Test basic operations
    #     UnFINISHED. SHOULD BE TESTED BY ARAKOON SO NOT REALLY NECESSARY
    #     """
    #     pyrakoon = self.pyrakoon_client
    #
    #     # Test empty get
    #     with self.assertRaises(ArakoonNotFound):
    #         pyrakoon.get(self.NAMESPACE)
    #
    #     set_val = 'test'
    #     pyrakoon.set(self.NAMESPACE, set_val)
    #     self.assertEqual(pyrakoon.get(self.NAMESPACE), set_val)
    #     self.assertTrue(pyrakoon.exists(self.NAMESPACE))

    def drop_master(self, async=True):
        # type: (bool) -> None
        """
        Drop the ArakoonMaster
        :param async: Drop the master async
        :type async: bool
        """
        pyrakoon = self.pyrakoon_client
        cluster = PersistentFactory._get_store_info()['cluster']
        arakoon_config = ArakoonClusterConfig(cluster_id=cluster)
        master_id = pyrakoon._client.whoMaster()
        try:
            master_node_config = [node for node in arakoon_config.nodes if node.name == master_id][0]  # type: ArakoonNodeConfig
        except IndexError:
            raise RuntimeError('Master not found in Arakoonconfig')
        sr = StorageRouterList.get_by_ip(master_node_config.ip)
        client = SSHClient(sr, 'root')
        if async:
            # Master needs to be killed
            service_name = ArakoonInstaller.get_service_name_for_cluster(arakoon_config.cluster_id)
            service_pid = self.service_manager.get_service_pid(service_name, client)
            client.run(['kill', service_pid])
            self.killed_arakoons.append((client, service_name))
            return
        client.run(['arakoon', '--drop-master', arakoon_config.cluster_id, master_node_config.ip, master_node_config.client_port])

    # def test_master_switch_handling(self):
    #     """
    #     Test if the PyrakoonClient can handle master switching
    #     """
    #     pyrakoon = self.pyrakoon_client
    #     # Drop the master
    #     self.drop_master(async=True)
    #     with self.assertRaises(ArakoonNotFound):
    #         pyrakoon.get(self.NAMESPACE)
    #     # logs = Logger._logs['extensions']
    #     # retrying_logs = [l for l in logs if 'Retrying in' in l]
    #     # self.assertGreater(len(retrying_logs), 0)

    def test_transaction_retry(self):
        val = 'test'

        def transaction_callback():
            transaction = pyrakoon.begin_transaction()
            pyrakoon.set(self.NAMESPACE, val, transaction=transaction)
            return transaction

        pyrakoon = self.pyrakoon_client
        # Drop the master
        self.drop_master(async=True)
        pyrakoon.apply_callback_transaction(transaction_callback)
        self.assertEquals(pyrakoon.get(self.NAMESPACE), val)
