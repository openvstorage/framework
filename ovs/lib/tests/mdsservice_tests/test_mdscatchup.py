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
MDSService test module
"""

import gevent
import logging
import gevent.hub
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.tests.helpers import DalHelper
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storageserver.tests.mockups import MDSClient
from ovs_extensions.testing.exceptions import WorkerLossException
from ovs.lib.helpers.mds.catchup import MDSCatchUp
from ovs.lib.mdsservice import MDSServiceController
from ovs_extensions.testing.testcase import LogTestCase


class MDSCatchupTest(LogTestCase):
    """
    This test class will validate the various scenarios of the MDSService logic
    """
    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        super(MDSCatchupTest, self).setUp()

        # Suppress the greenlet outputting the workerlost exception
        self.gevent_hub_not_error_old = gevent.hub.Hub.NOT_ERROR
        gevent.hub.Hub.NOT_ERROR = (WorkerLossException,)

        self.volatile, self.persistent = DalHelper.setup()
        Configuration.set('/ovs/framework/logging|path', '/var/log/ovs')
        Configuration.set('/ovs/framework/logging|level', 'DEBUG')
        Configuration.set('/ovs/framework/logging|default_file', 'generic')
        Configuration.set('/ovs/framework/logging|default_name', 'logger')
        self.maxDiff = None

    def tearDown(self):
        """
        Clean up test suite
        """
        super(MDSCatchupTest, self).tearDown()

        gevent.hub.Hub.NOT_ERROR = self.gevent_hub_not_error_old

        DalHelper.teardown()

    @classmethod
    def _prepare_catchup(cls, dal_structure):
        """
        Prepare everything related to catchup
        - Setup worker services
        - Setup volumedriver services
        - Setup System.get_my_storagerouter
        :param dal_structure: The built DAL structure for the case
        :return: None
        :rtype: NoneType
        """
        storagerouter = dal_structure['storagerouters'][1]
        # New scrubbing code changes requires local Storagerouter to be available
        System._machine_id['none'] = System._machine_id[storagerouter.ip]
        # Setup worker information
        cls._setup_worker_service(dal_structure['storagerouters'].values())
        cls._setup_volumedriver_service(dal_structure['storagedrivers'].values())

    @staticmethod
    def sort_contexts(contexts):
        return sorted(contexts, key=lambda c: c['storagerouter_guid'])

    @staticmethod
    def _setup_worker_service(storagerouters):
        """
        Sets mocked ovs-worker service
        :param storagerouters: StorageRouter to setup workers for
        :type storagerouters: list
        :return: None
        :rtype: NoneType
        """
        service_name = 'ovs-workers'
        service_manager = ServiceFactory.get_manager()
        for storagerouter in storagerouters:
            client = SSHClient(storagerouter, 'root')
            service_manager.add_service(service_name, client)
            service_manager.start_service(service_name, client)

    @staticmethod
    def _setup_volumedriver_service(storagedrivers):
        service_manager = ServiceFactory.get_manager()
        for storagedriver in storagedrivers:
            storagerouter = storagedriver.storagerouter
            service_name = 'ovs-volumedriver_{0}'.format(storagedriver.vpool.name)
            client = SSHClient(storagerouter, 'root')
            service_manager.add_service(service_name, client)
            service_manager.start_service(service_name, client)

    def test_worker_invalidation(self):
        """
        Validates if out-of-date information is handled properly
        - Check for re-acquiring leftover locks because the workers died
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'domains': [1, 2],
             'storagerouters': [1, 2, 3, 4, 5, 6],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4)],  # <id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 1), (3, 2), (4, 3), (5, 4)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': [(1, 1, 1, False), (2, 1, 2, True), (3, 2, 2, False), (4, 2, 1, True),
                                       (5, 3, 1, False), (6, 3, 2, True),
                                       (7, 4, 2, False), (8, 4, 1, True), (9, 5, 1, False), (10, 6, 2, False),
                                       (11, 6, 1, True)]}  # (<id>, <storagerouter_id>, <domain_id>, <backup>)
        )
        mds_services = structure['mds_services']
        # Capacity is set to 10 vdisks. Creating only 1 disk for this mds will set the load to 10%
        vdisk = DalHelper.create_vdisks_for_mds_service(amount=1, start_id=1, mds_service=mds_services.values()[0]).values()[0]
        # Nothing is overloaded so a disk should have 2 mdses set
        MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)  # Set the safety
        # Set number of tlogs to replay above the threshold
        services_by_socket = MDSCatchUp.map_mds_services_by_socket(vdisk)
        catch_up_list = []
        default_tlog_behind = 1000
        for index, socket_config in enumerate(vdisk.info['metadata_backend_config']):
            if index == 0:
                continue
            socket = '{0}:{1}'.format(socket_config['ip'], socket_config['port'])
            service = services_by_socket.get(socket)
            # Set the number of tlogs to threshold so catchup can be tested
            mds_key = '{0}:{1}'.format(service.storagerouter.ip, service.ports[0])
            MDSClient.set_catchup(mds_key, vdisk.volume_id, default_tlog_behind)
            MDSClient.set_catchup_hook(mds_key, vdisk.volume_id, self.simulate_worker_failure)
            catch_up_list.append((mds_key, vdisk.volume_id, default_tlog_behind))

        self._prepare_catchup(structure)

        with self.assertLogs(level=logging.DEBUG) as logging_watcher:
            catch_up_1 = MDSCatchUp(vdisk.guid)
            catch_up_thread = gevent.spawn(self.catch_up_worker, catch_up_1)
            gevent.sleep(0)  # Start the gevent scheduling
            catch_up_thread.join()  # Wait for the worker lost to raise
        MDSCatchUp.reset_cache()
        # No catchup should have happened
        for mds_key, volume_id, tlogs_start in catch_up_list:
            current_tlogs = MDSClient.get_tlogs_behind(mds_key, volume_id)
            self.assertEqual(tlogs_start, current_tlogs, 'No catchup should have been invoked')
            # Clear the exception
            MDSClient.set_catchup_hook(mds_key, vdisk.volume_id, lambda: None)

        # Next iteration should not fail
        with self.assertLogs(level=logging.DEBUG) as logging_watcher:
            catch_up_2 = MDSCatchUp(vdisk.guid)
            catch_up_2.catch_up(async=False)
        logs = logging_watcher.get_message_severity_map()
        no_longer_relevant_logs = [log for log in logs if log.endswith('on the next save as it is no longer relevant')]
        self.assertEqual(len(no_longer_relevant_logs), 1, 'Items should have been discarded as the state was no longer relevant')
        for mds_key, volume_id, tlogs in catch_up_list:
            current_tlogs = MDSClient.get_tlogs_behind(mds_key, volume_id)
            self.assertEqual(current_tlogs, 0, 'catchup should have been invoked')
        self.assertNotEqual(self.sort_contexts(catch_up_1._relevant_contexts), self.sort_contexts(catch_up_2._relevant_contexts), 'Contexts should have changed')

    def test_volumedriver_invalidation(self):
        """
        Validates if out-of-date information is handled properly
        - Check for re-acquiring leftover locks because volumedriver died
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'domains': [1, 2],
             'storagerouters': [1, 2, 3, 4, 5, 6],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4)],  # <id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 1), (3, 2), (4, 3), (5, 4)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': [(1, 1, 1, False), (2, 1, 2, True), (3, 2, 2, False), (4, 2, 1, True),
                                       (5, 3, 1, False), (6, 3, 2, True),
                                       (7, 4, 2, False), (8, 4, 1, True), (9, 5, 1, False), (10, 6, 2, False),
                                       (11, 6, 1, True)]}  # (<id>, <storagerouter_id>, <domain_id>, <backup>)
        )
        mds_services = structure['mds_services']
        # Capacity is set to 10 vdisks. Creating only 1 disk for this mds will set the load to 10%
        vdisk = DalHelper.create_vdisks_for_mds_service(amount=1, start_id=1, mds_service=mds_services.values()[0]).values()[0]
        # Nothing is overloaded so a disk should have 2 mdses set
        MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)  # Set the safety
        # Set number of tlogs to replay above the threshold
        services_by_socket = MDSCatchUp.map_mds_services_by_socket(vdisk)
        catch_up_list = []
        default_tlog_behind = 1000
        for index, socket_config in enumerate(vdisk.info['metadata_backend_config']):
            if index == 0:
                continue
            socket = '{0}:{1}'.format(socket_config['ip'], socket_config['port'])
            service = services_by_socket.get(socket)
            service_storagedriver = None
            for storagedriver in service.storagerouter.storagedrivers:
                if storagedriver.vpool == vdisk.vpool:
                    service_storagedriver = storagedriver
            if service_storagedriver is None:
                raise ValueError('No storagedriver found')
            # Set the number of tlogs to threshold so catchup can be tested
            mds_key = '{0}:{1}'.format(service.storagerouter.ip, service.ports[0])
            MDSClient.set_catchup(mds_key, vdisk.volume_id, default_tlog_behind)
            MDSClient.set_catchup_hook(mds_key, vdisk.volume_id, lambda: self.simulate_volumedriver_going_down(service_storagedriver))
            catch_up_list.append((mds_key, vdisk.volume_id, default_tlog_behind))

        self._prepare_catchup(structure)

        catch_up_1 = MDSCatchUp(vdisk.guid)
        with self.assertRaises(RuntimeError):
            catch_up_1.catch_up(async=False)
        # No catchup should have happened
        for mds_key, volume_id, tlogs_start in catch_up_list:
            current_tlogs = MDSClient.get_tlogs_behind(mds_key, volume_id)
            self.assertEqual(tlogs_start, current_tlogs, 'No catchup should have been invoked')
            # Clear the exception
            MDSClient.set_catchup_hook(mds_key, vdisk.volume_id, lambda: None)
        # Next iteration should not fail
        catch_up_2 = MDSCatchUp(vdisk.guid)
        catch_up_2.catch_up(async=False)
        for mds_key, volume_id, tlogs in catch_up_list:
            current_tlogs = MDSClient.get_tlogs_behind(mds_key, volume_id)
            self.assertEqual(current_tlogs, 0, 'catchup should have been invoked')
        self.assertFalse(self.sort_contexts(catch_up_1._relevant_contexts) == self.sort_contexts(catch_up_2._relevant_contexts),'Contexts should have changed')

    def test_contexts(self):
        """
        Test if the contexts are properly fetched
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1],
             'storagerouters': [1, 2, 3],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3)],  # <id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 1), (3, 2), (4, 3)]}  # (<id>, <storagedriver_id>)
        )
        mds_services = structure['mds_services']
        vdisks = set()
        for mds_service in mds_services.itervalues():
            # Capacity is set to 10 vdisks. creating only 1 for every mds will set the load to 10%
            vdisks.update(DalHelper.create_vdisks_for_mds_service(amount=1, start_id=len(vdisks) + 1, mds_service=mds_service).values())
        self._prepare_catchup(structure)
        for vdisk in vdisks:
            # Nothing is overloaded so a disk should have 2 mdses set
            MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)  # Set the safety
            catch_up = MDSCatchUp(vdisk.guid)
            # Relevant contexts is all worker contexts mixed together with the storadrivers for the vdisk vpool where the MDSes are located
            relevant_contexts = len(vdisk.info['metadata_backend_config']) * len(structure['storagerouters'].keys())
            self.assertEqual(len(catch_up._relevant_contexts), relevant_contexts)

    def test_skipping_already_registered(self):
        """
        Validate race condition handling
        - Validate that already queued items won't be scrubbed again
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'domains': [1, 2],
             'storagerouters': [1, 2, 3, 4, 5, 6],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4)],  # <id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 1), (3, 2), (4, 3), (5, 4)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': [(1, 1, 1, False), (2, 1, 2, True), (3, 2, 2, False), (4, 2, 1, True), (5, 3, 1, False), (6, 3, 2, True),
                                       (7, 4, 2, False), (8, 4, 1, True), (9, 5, 1, False), (10, 6, 2, False), (11, 6, 1, True)]}  # (<id>, <storagerouter_id>, <domain_id>, <backup>)
        )
        mds_services = structure['mds_services']
        # Capacity is set to 10 vdisks. Creating only 1 disk for this mds will set the load to 10%
        vdisk = DalHelper.create_vdisks_for_mds_service(amount=1, start_id=1, mds_service=mds_services.values()[0]).values()[0]
        # Nothing is overloaded so a disk should have 2 mdses set
        MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)  # Set the safety
        # Set number of tlogs to replay above the threshold
        services_by_socket = MDSCatchUp.map_mds_services_by_socket(vdisk)
        already_registered_logs = []
        for index, socket_config in enumerate(vdisk.info['metadata_backend_config']):
            if index == 0:
                continue
            socket = '{0}:{1}'.format(socket_config['ip'], socket_config['port'])
            service = services_by_socket.get(socket)
            # Set the number of tlogs to threshold so catchup can be tested
            MDSClient.set_catchup('{0}:{1}'.format(service.storagerouter.ip, service.ports[0]), vdisk.volume_id, 1000)
            already_registered_logs.append('MDS Service {0} at {1}:{2} is already being caught up'.format(service.name, service.storagerouter.ip, service.ports[0]))

        self._prepare_catchup(structure)

        # Register vdisk beforehand
        catch_up = MDSCatchUp(vdisk.guid)
        registration_data = catch_up._relevant_contexts[0]
        self.persistent.set(catch_up.mds_key, [registration_data])
        with self.assertLogs(level=logging.DEBUG) as logging_watcher:
            catch_up.catch_up(async=False)
        logs = logging_watcher.get_message_severity_map()
        catch_up_logs = [catch_up._format_message(log) for log in already_registered_logs]
        for log in catch_up_logs:
            self.assertIn(log, logs)

    def test_skipping_already_registered_async(self):
        """
        Validate race condition handling while being async
        - Validate that already queued items won't be scrubbed again
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'domains': [1, 2],
             'storagerouters': [1, 2, 3, 4, 5, 6],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4)],  # <id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 1), (3, 2), (4, 3), (5, 4)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': [(1, 1, 1, False), (2, 1, 2, True), (3, 2, 2, False), (4, 2, 1, True), (5, 3, 1, False), (6, 3, 2, True),
                                       (7, 4, 2, False), (8, 4, 1, True), (9, 5, 1, False), (10, 6, 2, False), (11, 6, 1, True)]}  # (<id>, <storagerouter_id>, <domain_id>, <backup>)
        )
        mds_services = structure['mds_services']
        # Capacity is set to 10 vdisks. Creating only 1 disk for this mds will set the load to 10%
        vdisk = DalHelper.create_vdisks_for_mds_service(amount=1, start_id=1, mds_service=mds_services.values()[0]).values()[0]
        # Nothing is overloaded so a disk should have 2 mdses set
        MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)  # Set the safety
        # Set number of tlogs to replay above the threshold
        services_by_socket = MDSCatchUp.map_mds_services_by_socket(vdisk)
        already_registered_logs = []
        for index, socket_config in enumerate(vdisk.info['metadata_backend_config']):
            if index == 0:
                continue
            socket = '{0}:{1}'.format(socket_config['ip'], socket_config['port'])
            service = services_by_socket.get(socket)
            # Set the number of tlogs to threshold so catchup can be tested
            MDSClient.set_catchup('{0}:{1}'.format(service.storagerouter.ip, service.ports[0]), vdisk.volume_id, 1000)
            already_registered_logs.append('MDS Service {0} at {1}:{2} is already being caught up'.format(service.name, service.storagerouter.ip, service.ports[0]))

        self._prepare_catchup(structure)

        # Register vdisk beforehand
        catch_up = MDSCatchUp(vdisk.guid)
        registration_data = catch_up._relevant_contexts[0]
        self.persistent.set(catch_up.mds_key, [registration_data])
        with self.assertLogs(level=logging.DEBUG) as logging_watcher:
            catch_up.catch_up()
            catch_up.wait()
        logs = logging_watcher.get_message_severity_map()
        catch_up_logs = [catch_up._format_message(log) for log in already_registered_logs]
        for log in catch_up_logs:
            self.assertIn(log, logs)

    def test_volumedriver_invalidation_async(self):
        """
        Validates if out-of-date information is handled properly async
        - Check for re-acquiring leftover locks because volumedriver died
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'domains': [1, 2],
             'storagerouters': [1, 2, 3, 4, 5, 6],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4)],  # <id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 1), (3, 2), (4, 3), (5, 4)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': [(1, 1, 1, False), (2, 1, 2, True), (3, 2, 2, False), (4, 2, 1, True),
                                       (5, 3, 1, False), (6, 3, 2, True),
                                       (7, 4, 2, False), (8, 4, 1, True), (9, 5, 1, False), (10, 6, 2, False),
                                       (11, 6, 1, True)]}  # (<id>, <storagerouter_id>, <domain_id>, <backup>)
        )
        mds_services = structure['mds_services']
        # Capacity is set to 10 vdisks. Creating only 1 disk for this mds will set the load to 10%
        vdisk = DalHelper.create_vdisks_for_mds_service(amount=1, start_id=1, mds_service=mds_services.values()[0]).values()[0]
        # Nothing is overloaded so a disk should have 2 mdses set
        MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)  # Set the safety
        # Set number of tlogs to replay above the threshold
        services_by_socket = MDSCatchUp.map_mds_services_by_socket(vdisk)
        catch_up_list = []
        default_tlog_behind = 1000
        for index, socket_config in enumerate(vdisk.info['metadata_backend_config']):
            if index == 0:
                continue
            socket = '{0}:{1}'.format(socket_config['ip'], socket_config['port'])
            service = services_by_socket.get(socket)
            service_storagedriver = None
            for storagedriver in service.storagerouter.storagedrivers:
                if storagedriver.vpool == vdisk.vpool:
                    service_storagedriver = storagedriver
            if service_storagedriver is None:
                raise ValueError('No storagedriver found')
            # Set the number of tlogs to threshold so catchup can be tested
            mds_key = '{0}:{1}'.format(service.storagerouter.ip, service.ports[0])
            MDSClient.set_catchup(mds_key, vdisk.volume_id, default_tlog_behind)
            MDSClient.set_catchup_hook(mds_key, vdisk.volume_id, lambda: self.simulate_volumedriver_going_down(service_storagedriver))
            catch_up_list.append((mds_key, vdisk.volume_id, default_tlog_behind))

        self._prepare_catchup(structure)

        catch_up_1 = MDSCatchUp(vdisk.guid)
        catch_up_1.catch_up()
        with self.assertRaises(RuntimeError):
            catch_up_1.wait()
        # No catchup should have happened
        for mds_key, volume_id, tlogs_start in catch_up_list:
            current_tlogs = MDSClient.get_tlogs_behind(mds_key, volume_id)
            self.assertEqual(tlogs_start, current_tlogs, 'No catchup should have been invoked')
            # Clear the exception
            MDSClient.set_catchup_hook(mds_key, vdisk.volume_id, lambda: None)
        # Next iteration should not fail
        catch_up_2 = MDSCatchUp(vdisk.guid)
        catch_up_2.catch_up()
        catch_up_2.wait()
        for mds_key, volume_id, tlogs in catch_up_list:
            current_tlogs = MDSClient.get_tlogs_behind(mds_key, volume_id)
            self.assertEqual(current_tlogs, 0, 'catchup should have been invoked')
        self.assertFalse(self.sort_contexts(catch_up_1._relevant_contexts) == self.sort_contexts(catch_up_2._relevant_contexts),'Contexts should have changed')

    def test_worker_invalidation_async(self):
        """
        Validates if out-of-date information is handled properly
        - Check for re-acquiring leftover locks because the workers died
        """
        structure = DalHelper.build_dal_structure(
            {'vpools': [1, 2],
             'domains': [1, 2],
             'storagerouters': [1, 2, 3, 4, 5, 6],
             'storagedrivers': [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4)],  # <id>, <vpool_id>, <storagerouter_id>)
             'mds_services': [(1, 1), (2, 1), (3, 2), (4, 3), (5, 4)],  # (<id>, <storagedriver_id>)
             'storagerouter_domains': [(1, 1, 1, False), (2, 1, 2, True), (3, 2, 2, False), (4, 2, 1, True),
                                       (5, 3, 1, False), (6, 3, 2, True),
                                       (7, 4, 2, False), (8, 4, 1, True), (9, 5, 1, False), (10, 6, 2, False),
                                       (11, 6, 1, True)]}  # (<id>, <storagerouter_id>, <domain_id>, <backup>)
        )
        mds_services = structure['mds_services']
        # Capacity is set to 10 vdisks. Creating only 1 disk for this mds will set the load to 10%
        vdisk = DalHelper.create_vdisks_for_mds_service(amount=1, start_id=1, mds_service=mds_services.values()[0]).values()[0]
        # Nothing is overloaded so a disk should have 2 mdses set
        MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)  # Set the safety
        # Set number of tlogs to replay above the threshold
        services_by_socket = MDSCatchUp.map_mds_services_by_socket(vdisk)
        catch_up_list = []
        default_tlog_behind = 1000
        for index, socket_config in enumerate(vdisk.info['metadata_backend_config']):
            if index == 0:
                continue
            socket = '{0}:{1}'.format(socket_config['ip'], socket_config['port'])
            service = services_by_socket.get(socket)
            # Set the number of tlogs to threshold so catchup can be tested
            mds_key = '{0}:{1}'.format(service.storagerouter.ip, service.ports[0])
            MDSClient.set_catchup(mds_key, vdisk.volume_id, default_tlog_behind)
            MDSClient.set_catchup_hook(mds_key, vdisk.volume_id, self.simulate_worker_failure)
            catch_up_list.append((mds_key, vdisk.volume_id, default_tlog_behind))

        self._prepare_catchup(structure)

        catch_up_1 = MDSCatchUp(vdisk.guid)
        catch_up_thread = gevent.spawn(self.catch_up_worker, catch_up_1)
        gevent.sleep(0)  # Start the gevent scheduling
        catch_up_thread.join()  # Wait for the worker lost to raise
        MDSCatchUp.reset_cache()
        # No catchup should have happened
        for mds_key, volume_id, tlogs_start in catch_up_list:
            current_tlogs = MDSClient.get_tlogs_behind(mds_key, volume_id)
            self.assertEqual(tlogs_start, current_tlogs, 'No catchup should have been invoked')
            # Clear the exception
            MDSClient.set_catchup_hook(mds_key, vdisk.volume_id, lambda: None)

        # Next iteration should not fail
        with self.assertLogs(level=logging.DEBUG) as logging_watcher:
            catch_up_2 = MDSCatchUp(vdisk.guid)
            catch_up_2.catch_up(async=False)
        logs = logging_watcher.get_message_severity_map()
        no_longer_relevant_logs = [log for log in logs if log.endswith('on the next save as it is no longer relevant')]
        self.assertEqual(len(no_longer_relevant_logs), 1, 'Items should have been discarded as the state was no longer relevant')
        for mds_key, volume_id, tlogs in catch_up_list:
            current_tlogs = MDSClient.get_tlogs_behind(mds_key, volume_id)
            self.assertEqual(current_tlogs, 0, 'catchup should have been invoked')
        self.assertNotEqual(self.sort_contexts(catch_up_1._relevant_contexts), self.sort_contexts(catch_up_2._relevant_contexts), 'Contexts should have changed')

    @staticmethod
    def simulate_worker_failure(*args, **kwargs):
        # type: (StorageRouter) -> None
        """
        - Send event to kill the process. After catching the event:
            - Restarts the mocked worker
        """
        _ = args, kwargs
        service_name = 'ovs-workers'
        service_manager = ServiceFactory.get_manager()
        client = SSHClient(System.get_my_storagerouter(), 'root')
        service_manager.restart_service(service_name, client)

        raise WorkerLossException('Simulated worker failure')

    @staticmethod
    def catch_up_worker(catch_up_instance):
        # type: (MDSCatchUp) -> None
        """
        Catchup that will be run in a separate process
        """
        catch_up_instance.catch_up(async=False)

    @staticmethod
    def simulate_volumedriver_going_down(std):
        # type: (StorageDriver) -> None
        """
        - Restarts the mocked volumedriver
        - Aborts the current catch ups
        """
        # Restart the volumedriver
        volumedriver_service_name = 'ovs-volumedriver_{0}'.format(std.vpool.name)
        service_manager = ServiceFactory.get_manager()
        client = SSHClient(std.storagerouter, 'root')
        service_manager.restart_service(volumedriver_service_name, client)
        # Raise an exception
        raise RuntimeError('Simulated storagedriver kill. Catch up could not proceed')
