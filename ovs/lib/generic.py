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
GenericTaskController module
"""
import os
import copy
import json
import time
import uuid
from datetime import datetime, timedelta
from Queue import Empty, Queue
from threading import Thread
from time import mktime
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.servicelist import ServiceList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.generic.filemutex import file_mutex
from ovs.extensions.generic.logger import Logger
from ovs_extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.lib.helpers.decorators import ovs_task
from ovs.lib.helpers.toolbox import Toolbox, Schedule
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.vdisk import VDiskController


class GenericController(object):
    """
    This controller contains all generic task code. These tasks can be
    executed at certain intervals and should be self-containing
    """
    _logger = Logger('lib')

    @staticmethod
    @ovs_task(name='ovs.generic.snapshot_all_vdisks', schedule=Schedule(minute='0', hour='*'), ensure_single_info={'mode': 'DEFAULT', 'extra_task_names': ['ovs.generic.delete_snapshots']})
    def snapshot_all_vdisks():
        """
        Snapshots all vDisks
        """
        GenericController._logger.info('[SSA] started')
        success = []
        fail = []
        for vdisk in VDiskList.get_vdisks():
            if vdisk.is_vtemplate is True:
                continue
            try:
                metadata = {'label': '',
                            'is_consistent': False,
                            'timestamp': str(int(time.time())),
                            'is_automatic': True,
                            'is_sticky': False}
                VDiskController.create_snapshot(vdisk_guid=vdisk.guid,
                                                metadata=metadata)
                success.append(vdisk.guid)
            except Exception:
                GenericController._logger.exception('Error taking snapshot for vDisk {0}'.format(vdisk.guid))
                fail.append(vdisk.guid)
        GenericController._logger.info('[SSA] Snapshot has been taken for {0} vDisks, {1} failed.'.format(len(success), len(fail)))
        return success, fail

    @staticmethod
    @ovs_task(name='ovs.generic.delete_snapshots', schedule=Schedule(minute='1', hour='2'), ensure_single_info={'mode': 'DEFAULT'})
    def delete_snapshots(timestamp=None):
        """
        Delete snapshots & scrubbing policy

        Implemented delete snapshot policy:
        < 1d | 1d bucket | 1 | best of bucket   | 1d
        < 1w | 1d bucket | 6 | oldest of bucket | 7d = 1w
        < 1m | 1w bucket | 3 | oldest of bucket | 4w = 1m
        > 1m | delete

        :param timestamp: Timestamp to determine whether snapshots should be kept or not, if none provided, current time will be used
        :type timestamp: float

        :return: None
        """
        GenericController._logger.info('Delete snapshots started')

        day = timedelta(1)
        week = day * 7

        def make_timestamp(offset):
            """
            Create an integer based timestamp
            :param offset: Offset in days
            :return: Timestamp
            """
            return int(mktime((base - offset).timetuple()))

        # Calculate bucket structure
        if timestamp is None:
            timestamp = time.time()
        base = datetime.fromtimestamp(timestamp).date() - day
        buckets = []
        # Buckets first 7 days: [0-1[, [1-2[, [2-3[, [3-4[, [4-5[, [5-6[, [6-7[
        for i in xrange(0, 7):
            buckets.append({'start': make_timestamp(day * i),
                            'end': make_timestamp(day * (i + 1)),
                            'type': '1d',
                            'snapshots': []})
        # Week buckets next 3 weeks: [7-14[, [14-21[, [21-28[
        for i in xrange(1, 4):
            buckets.append({'start': make_timestamp(week * i),
                            'end': make_timestamp(week * (i + 1)),
                            'type': '1w',
                            'snapshots': []})
        buckets.append({'start': make_timestamp(week * 4),
                        'end': 0,
                        'type': 'rest',
                        'snapshots': []})

        # Get a list of all snapshots that are used as parents for clones
        parent_snapshots = set([vd.parentsnapshot for vd in VDiskList.get_with_parent_snaphots()])

        # Place all snapshots in bucket_chains
        bucket_chains = []
        for vdisk in VDiskList.get_vdisks():
            if vdisk.info['object_type'] in ['BASE']:
                bucket_chain = copy.deepcopy(buckets)
                for snapshot in vdisk.snapshots:
                    if snapshot.get('is_sticky') is True:
                        continue
                    if snapshot['guid'] in parent_snapshots:
                        GenericController._logger.info('Not deleting snapshot {0} because it has clones'.format(snapshot['guid']))
                        continue
                    timestamp = int(snapshot['timestamp'])
                    for bucket in bucket_chain:
                        if bucket['start'] >= timestamp > bucket['end']:
                            bucket['snapshots'].append({'timestamp': timestamp,
                                                        'snapshot_id': snapshot['guid'],
                                                        'vdisk_guid': vdisk.guid,
                                                        'is_consistent': snapshot['is_consistent']})
                bucket_chains.append(bucket_chain)

        # Clean out the snapshot bucket_chains, we delete the snapshots we want to keep
        # And we'll remove all snapshots that remain in the buckets
        for bucket_chain in bucket_chains:
            first = True
            for bucket in bucket_chain:
                if first is True:
                    best = None
                    for snapshot in bucket['snapshots']:
                        if best is None:
                            best = snapshot
                        # Consistent is better than inconsistent
                        elif snapshot['is_consistent'] and not best['is_consistent']:
                            best = snapshot
                        # Newer (larger timestamp) is better than older snapshots
                        elif snapshot['is_consistent'] == best['is_consistent'] and \
                                snapshot['timestamp'] > best['timestamp']:
                            best = snapshot
                    bucket['snapshots'] = [s for s in bucket['snapshots'] if
                                           s['timestamp'] != best['timestamp']]
                    first = False
                elif bucket['end'] > 0:
                    oldest = None
                    for snapshot in bucket['snapshots']:
                        if oldest is None:
                            oldest = snapshot
                        # Older (smaller timestamp) is the one we want to keep
                        elif snapshot['timestamp'] < oldest['timestamp']:
                            oldest = snapshot
                    bucket['snapshots'] = [s for s in bucket['snapshots'] if
                                           s['timestamp'] != oldest['timestamp']]

        # Delete obsolete snapshots
        for bucket_chain in bucket_chains:
            for bucket in bucket_chain:
                for snapshot in bucket['snapshots']:
                    VDiskController.delete_snapshot(vdisk_guid=snapshot['vdisk_guid'],
                                                    snapshot_id=snapshot['snapshot_id'])
        GenericController._logger.info('Delete snapshots finished')

    @staticmethod
    @ovs_task(name='ovs.generic.execute_scrub', schedule=Schedule(minute='0', hour='3'), ensure_single_info={'mode': 'DEDUPED'})
    def execute_scrub(vpool_guids=None, vdisk_guids=None, storagerouter_guid=None):
        """
        Divide the scrub work among all StorageRouters with a SCRUB partition
        :param vpool_guids: Guids of the vPools that need to be scrubbed completely
        :type vpool_guids: list
        :param vdisk_guids: Guids of the vDisks that need to be scrubbed
        :type vdisk_guids: list
        :param storagerouter_guid: Guid of the StorageRouter to execute the scrub work on
        :type storagerouter_guid: str
        :return: None
        :rtype: NoneType
        """
        if vpool_guids is not None and not isinstance(vpool_guids, list):
            raise ValueError('vpool_guids should be a list')
        if vdisk_guids is not None and not isinstance(vdisk_guids, list):
            raise ValueError('vdisk_guids should be a list')
        if storagerouter_guid is not None and not isinstance(storagerouter_guid, basestring):
            raise ValueError('storagerouter_guid should be a str')

        GenericController._logger.info('Scrubber - Started')
        scrub_locations = []
        storagerouters = StorageRouterList.get_storagerouters() if storagerouter_guid is None else [StorageRouter(storagerouter_guid)]
        for storage_router in storagerouters:
            scrub_partitions = storage_router.partition_config.get(DiskPartition.ROLES.SCRUB, [])
            if len(scrub_partitions) == 0:
                continue

            try:
                SSHClient(endpoint=storage_router, username='root')
                for partition_guid in scrub_partitions:
                    partition = DiskPartition(partition_guid)
                    GenericController._logger.info('Scrubber - Storage Router {0} has {1} partition at {2}'.format(storage_router.ip, DiskPartition.ROLES.SCRUB, partition.folder))
                    scrub_locations.append({'scrub_path': str(partition.folder),
                                            'partition_guid': partition.guid,
                                            'storage_router': storage_router})
            except UnableToConnectException:
                GenericController._logger.warning('Scrubber - Storage Router {0} is not reachable'.format(storage_router.ip))

        if len(scrub_locations) == 0:
            raise ValueError('No scrub locations found, cannot scrub')

        vpool_vdisk_map = {}
        if vpool_guids is None and vdisk_guids is None:
            vpool_vdisk_map = dict((vpool, list(vpool.vdisks)) for vpool in VPoolList.get_vpools())
        else:
            if vpool_guids is not None:
                for vpool_guid in set(vpool_guids):
                    vpool = VPool(vpool_guid)
                    vpool_vdisk_map[vpool] = list(vpool.vdisks)
            if vdisk_guids is not None:
                for vdisk_guid in set(vdisk_guids):
                    vdisk = VDisk(vdisk_guid)
                    if vdisk.vpool not in vpool_vdisk_map:
                        vpool_vdisk_map[vdisk.vpool] = []
                    if vdisk not in vpool_vdisk_map[vdisk.vpool]:
                        vpool_vdisk_map[vdisk.vpool].append(vdisk)

        number_of_vpools = len(vpool_vdisk_map)
        if number_of_vpools >= 6:
            max_stacks_per_vpool = 1
        elif number_of_vpools >= 3:
            max_stacks_per_vpool = 2
        else:
            max_stacks_per_vpool = 5

        threads = []
        counter = 0
        error_messages = []
        for vp, vdisks in vpool_vdisk_map.iteritems():
            # Verify amount of vDisks on vPool
            GenericController._logger.info('Scrubber - vPool {0} - Checking scrub work'.format(vp.name))
            if len(vdisks) == 0:
                GenericController._logger.info('Scrubber - vPool {0} - No scrub work'.format(vp.name))
                continue

            # Fill queue with all vDisks for current vPool
            vpool_queue = Queue()
            for vd in vdisks:
                if vd.is_vtemplate is True:
                    GenericController._logger.info('Scrubber - vPool {0} - vDisk {1} {2} - Is a template, not scrubbing'.format(vp.name, vd.guid, vd.name))
                    continue
                vd.invalidate_dynamics('storagedriver_id')
                if not vd.storagedriver_id:
                    GenericController._logger.warning('Scrubber - vPool {0} - vDisk {1} {2} - No StorageDriver ID found'.format(vp.name, vd.guid, vd.name))
                    continue
                vpool_queue.put(vd.guid)

            stacks_to_spawn = min(max_stacks_per_vpool, len(scrub_locations))
            GenericController._logger.info('Scrubber - vPool {0} - Spawning {1} stack{2}'.format(vp.name, stacks_to_spawn, '' if stacks_to_spawn == 1 else 's'))
            for _ in xrange(stacks_to_spawn):
                scrub_target = scrub_locations[counter % len(scrub_locations)]
                stack = Thread(target=GenericController._deploy_stack_and_scrub,
                               args=(vpool_queue, vp, scrub_target, error_messages))
                stack.start()
                threads.append(stack)
                counter += 1

        for thread in threads:
            thread.join()

        if len(error_messages) > 0:
            raise Exception('Errors occurred while scrubbing:\n  - {0}'.format('\n  - '.join(error_messages)))

    @staticmethod
    def _execute_scrub(queue, vpool, scrub_info, scrub_dir, error_messages):
        def _verify_mds_config(current_vdisk):
            current_vdisk.invalidate_dynamics('info')
            vdisk_configs = current_vdisk.info['metadata_backend_config']
            if len(vdisk_configs) == 0:
                raise RuntimeError('Could not load MDS configuration')
            return vdisk_configs

        storagerouter = scrub_info['storage_router']
        partition_guid = scrub_info['partition_guid']
        volatile_client = VolatileFactory.get_client()
        backend_config_key = 'ovs/vpools/{0}/proxies/scrub/backend_config_{1}'.format(vpool.guid, partition_guid)
        try:
            # Empty the queue with vDisks to scrub
            with remote(storagerouter.ip, [VDisk]) as rem:
                while True:
                    vdisk = None
                    vdisk_guid = queue.get(False)  # Raises Empty Exception when queue is empty, so breaking the while True loop
                    volatile_key = 'ovs_scrubbing_vdisk_{0}'.format(vdisk_guid)
                    try:
                        # Check MDS master is local. Trigger MDS handover if necessary
                        vdisk = rem.VDisk(vdisk_guid)
                        GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - Started scrubbing at location {3}'.format(vpool.name, storagerouter.name, vdisk.name, scrub_dir))
                        configs = _verify_mds_config(current_vdisk=vdisk)
                        storagedriver = StorageDriverList.get_by_storagedriver_id(vdisk.storagedriver_id)
                        if configs[0].get('ip') != storagedriver.storagerouter.ip:
                            GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - MDS master is not local, trigger handover'.format(vpool.name, storagerouter.name, vdisk.name))
                            MDSServiceController.ensure_safety(vdisk_guid=vdisk_guid)  # Do not use a remote VDisk instance here
                            configs = _verify_mds_config(current_vdisk=vdisk)
                            if configs[0].get('ip') != storagedriver.storagerouter.ip:
                                GenericController._logger.warning('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - Skipping because master MDS still not local'.format(vpool.name, storagerouter.name, vdisk.name))
                                continue

                        # Check if vDisk is already being scrubbed
                        if volatile_client.add(key=volatile_key, value=volatile_key, time=24 * 60 * 60) is False:
                            GenericController._logger.warning('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - Skipping because vDisk is already being scrubbed'.format(vpool.name, storagerouter.name, vdisk.name))
                            continue

                        # Do the actual scrubbing
                        with vdisk.storagedriver_client.make_locked_client(str(vdisk.volume_id)) as locked_client:
                            GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - Retrieve and apply scrub work'.format(vpool.name, storagerouter.name, vdisk.name))
                            work_units = locked_client.get_scrubbing_workunits()
                            for work_unit in work_units:
                                res = locked_client.scrub(work_unit=work_unit,
                                                          scratch_dir=scrub_dir,
                                                          log_sinks=[Logger.get_sink_path(source='scrubber_{0}'.format(vpool.name),
                                                                                          forced_target_type=Logger.TARGET_TYPE_FILE)],
                                                          backend_config=Configuration.get_configuration_path(backend_config_key))
                                locked_client.apply_scrubbing_result(scrubbing_work_result=res)
                            if work_units:
                                GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - {3} work units successfully applied'.format(vpool.name, storagerouter.name, vdisk.name, len(work_units)))
                            else:
                                GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - No scrubbing required'.format(vpool.name, storagerouter.name, vdisk.name))
                    except Exception:
                        if vdisk is None:
                            message = 'Scrubber - vPool {0} - StorageRouter {1} - vDisk with guid {2} could not be found'.format(vpool.name, storagerouter.name, vdisk_guid)
                        else:
                            message = 'Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - Scrubbing failed'.format(vpool.name, storagerouter.name, vdisk.name)
                        error_messages.append(message)
                        GenericController._logger.exception(message)
                    finally:
                        # Remove vDisk from volatile memory
                        volatile_client.delete(volatile_key)

        except Empty:  # Raised when all items have been fetched from the queue
            GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Queue completely processed'.format(vpool.name, storagerouter.name))
        except Exception:
            message = 'Scrubber - vPool {0} - StorageRouter {1} - Scrubbing failed'.format(vpool.name, storagerouter.name)
            error_messages.append(message)
            GenericController._logger.exception(message)

    @staticmethod
    def _deploy_stack_and_scrub(queue, vpool, scrub_info, error_messages):
        """
        Executes scrub work for a given vDisk queue and vPool, based on scrub_info
        :param queue: a Queue with vDisk guids that need to be scrubbed (they should only be member of a single vPool)
        :type queue: Queue
        :param vpool: the vPool object of the vDisks
        :type vpool: VPool
        :param scrub_info: A dict containing scrub information:
                           `scrub_path` with the path where to scrub
                           `storage_router` with the StorageRouter that needs to do the work
        :type scrub_info: dict
        :param error_messages: A list of error messages to be filled (by reference)
        :type error_messages: list
        :return: None
        :rtype: NoneType
        """
        if len(vpool.storagedrivers) == 0 or not vpool.storagedrivers[0].storagedriver_id:
            error_messages.append('vPool {0} does not have any valid StorageDrivers configured'.format(vpool.name))
            return

        service_manager = ServiceFactory.get_manager()
        client = None
        lock_time = 5 * 60
        random_uuid = uuid.uuid4()
        storagerouter = scrub_info['storage_router']
        partition_guid = scrub_info['partition_guid']
        alba_proxy_service = 'ovs-albaproxy_{0}_scrub'.format(random_uuid)
        scrub_directory = '{0}/scrub_work_{1}'.format(scrub_info['scrub_path'], random_uuid)
        scrub_config_key = 'ovs/vpools/{0}/proxies/scrub/scrub_config_{1}'.format(vpool.guid, partition_guid)
        backend_config_key = 'ovs/vpools/{0}/proxies/scrub/backend_config_{1}'.format(vpool.guid, partition_guid)

        # Deploy a proxy
        try:
            with file_mutex(name='ovs_albaproxy_scrub', wait=lock_time):
                GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Deploying ALBA proxy {2}'.format(vpool.name, storagerouter.name, alba_proxy_service))
                client = SSHClient(storagerouter, 'root')
                client.dir_create(scrub_directory)
                client.dir_chmod(scrub_directory, 0777)  # Celery task executed by 'ovs' user and should be able to write in it
                if service_manager.has_service(name=alba_proxy_service, client=client) is True and service_manager.get_service_status(name=alba_proxy_service, client=client) == 'active':
                    GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Re-using existing proxy service {2}'.format(vpool.name, storagerouter.name, alba_proxy_service))
                    scrub_config = Configuration.get(scrub_config_key)
                else:
                    machine_id = System.get_my_machine_id(client)
                    port_range = Configuration.get('/ovs/framework/hosts/{0}/ports|storagedriver'.format(machine_id))
                    with volatile_mutex('deploy_proxy_for_scrub_{0}'.format(storagerouter.guid), wait=30):
                        port = System.get_free_ports(selected_range=port_range, nr=1, client=client)[0]
                    scrub_config = Configuration.get('ovs/vpools/{0}/proxies/scrub/generic_scrub'.format(vpool.guid))
                    scrub_config['port'] = port
                    scrub_config['transport'] = 'tcp'
                    Configuration.set(scrub_config_key, json.dumps(scrub_config, indent=4), raw=True)

                    params = {'VPOOL_NAME': vpool.name,
                              'LOG_SINK': Logger.get_sink_path(alba_proxy_service),
                              'CONFIG_PATH': Configuration.get_configuration_path(scrub_config_key)}
                    service_manager.add_service(name='ovs-albaproxy', params=params, client=client, target_name=alba_proxy_service)
                    service_manager.start_service(name=alba_proxy_service, client=client)
                    GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Deployed ALBA proxy {2}'.format(vpool.name, storagerouter.name, alba_proxy_service))

                backend_config = Configuration.get('ovs/vpools/{0}/hosts/{1}/config'.format(vpool.guid, vpool.storagedrivers[0].storagedriver_id))['backend_connection_manager']
                if backend_config.get('backend_type') != 'MULTI':
                    backend_config['alba_connection_host'] = '127.0.0.1'
                    backend_config['alba_connection_port'] = scrub_config['port']
                else:
                    for value in backend_config.itervalues():
                        if isinstance(value, dict):
                            value['alba_connection_host'] = '127.0.0.1'
                            value['alba_connection_port'] = scrub_config['port']
                # Copy backend connection manager information in separate key
                Configuration.set(backend_config_key, json.dumps({"backend_connection_manager": backend_config}, indent=4), raw=True)
        except Exception:
            message = 'Scrubber - vPool {0} - StorageRouter {1} - An error occurred deploying ALBA proxy {2}'.format(vpool.name, storagerouter.name, alba_proxy_service)
            error_messages.append(message)
            GenericController._logger.exception(message)
            if client is not None and service_manager.has_service(name=alba_proxy_service, client=client) is True:
                if service_manager.get_service_status(name=alba_proxy_service, client=client) == 'active':
                    service_manager.stop_service(name=alba_proxy_service, client=client)
                service_manager.remove_service(name=alba_proxy_service, client=client)
            if Configuration.exists(scrub_config_key):
                Configuration.delete(scrub_config_key)

        # Execute the actual scrubbing
        threads = []
        threads_key = '/ovs/framework/hosts/{0}/config|scrub_stack_threads'.format(storagerouter.machine_id)
        amount_threads = Configuration.get(key=threads_key) if Configuration.exists(key=threads_key) else 2
        if not isinstance(amount_threads, int):
            error_messages.append('Amount of threads to spawn must be an integer for StorageRouter with ID {0}'.format(storagerouter.machine_id))
            return

        amount_threads = max(amount_threads, 1)  # Make sure amount_threads is at least 1
        amount_threads = min(min(queue.qsize(), amount_threads), 20)  # Make sure amount threads is max 20
        GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Spawning {2} threads for proxy service {3}'.format(vpool.name, storagerouter.name, amount_threads, alba_proxy_service))
        for index in range(amount_threads):
            thread = Thread(name='execute_scrub_{0}_{1}_{2}'.format(vpool.guid, partition_guid, index),
                            target=GenericController._execute_scrub,
                            args=(queue, vpool, scrub_info, scrub_directory, error_messages))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

        # Delete the proxy again
        try:
            with file_mutex(name='ovs_albaproxy_scrub', wait=lock_time):
                GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Removing service {2}'.format(vpool.name, storagerouter.name, alba_proxy_service))
                client = SSHClient(storagerouter, 'root')
                client.dir_delete(scrub_directory)
                if service_manager.has_service(alba_proxy_service, client=client):
                    service_manager.stop_service(alba_proxy_service, client=client)
                    service_manager.remove_service(alba_proxy_service, client=client)
                if Configuration.exists(scrub_config_key):
                    Configuration.delete(scrub_config_key)
                GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Removed service {2}'.format(vpool.name, storagerouter.name, alba_proxy_service))
        except Exception:
            message = 'Scrubber - vPool {0} - StorageRouter {1} - Removing service {2} failed'.format(vpool.name, storagerouter.name, alba_proxy_service)
            error_messages.append(message)
            GenericController._logger.exception(message)

    @staticmethod
    @ovs_task(name='ovs.generic.collapse_arakoon', schedule=Schedule(minute='10', hour='0,2,4,6,8,10,12,14,16,18,20,22'), ensure_single_info={'mode': 'DEFAULT'})
    def collapse_arakoon():
        """
        Collapse Arakoon's Tlogs
        :return: None
        """
        from ovs_extensions.generic.toolbox import ExtensionsToolbox

        GenericController._logger.info('Arakoon collapse started')
        cluster_info = []
        storagerouters = StorageRouterList.get_storagerouters()
        if os.environ.get('RUNNING_UNITTESTS') != 'True':
            cluster_info = [('cacc', storagerouters[0])]

        cluster_names = []
        for service in ServiceList.get_services():
            if service.is_internal is True and service.type.name in (ServiceType.SERVICE_TYPES.ARAKOON,
                                                                     ServiceType.SERVICE_TYPES.NS_MGR,
                                                                     ServiceType.SERVICE_TYPES.ALBA_MGR):
                cluster = ExtensionsToolbox.remove_prefix(service.name, 'arakoon-')
                if cluster in cluster_names and cluster not in ['cacc', 'unittest-cacc']:
                    continue
                cluster_names.append(cluster)
                cluster_info.append((cluster, service.storagerouter))
        workload = {}
        cluster_config_map = {}
        for cluster, storagerouter in cluster_info:
            GenericController._logger.debug('  Collecting info for cluster {0}'.format(cluster))
            ip = storagerouter.ip if cluster in ['cacc', 'unittest-cacc'] else None
            try:
                config = ArakoonClusterConfig(cluster_id=cluster, source_ip=ip)
                cluster_config_map[cluster] = config
            except:
                GenericController._logger.exception('  Retrieving cluster information on {0} for {1} failed'.format(storagerouter.ip, cluster))
                continue
            for node in config.nodes:
                if node.ip not in workload:
                    workload[node.ip] = {'node_id': node.name,
                                         'clusters': []}
                workload[node.ip]['clusters'].append((cluster, ip))
        for storagerouter in storagerouters:
            try:
                if storagerouter.ip not in workload:
                    continue
                node_workload = workload[storagerouter.ip]
                client = SSHClient(storagerouter)
                for cluster, ip in node_workload['clusters']:
                    try:
                        GenericController._logger.debug('  Collapsing cluster {0} on {1}'.format(cluster, storagerouter.ip))
                        client.run(['arakoon', '--collapse-local', node_workload['node_id'], '2', '-config', cluster_config_map[cluster].external_config_path])
                        GenericController._logger.debug('  Collapsing cluster {0} on {1} completed'.format(cluster, storagerouter.ip))
                    except:
                        GenericController._logger.exception('  Collapsing cluster {0} on {1} failed'.format(cluster, storagerouter.ip))
            except UnableToConnectException:
                GenericController._logger.error('  Could not collapse any cluster on {0} (not reachable)'.format(storagerouter.name))
        GenericController._logger.info('Arakoon collapse finished')

    @staticmethod
    @ovs_task(name='ovs.generic.refresh_package_information', schedule=Schedule(minute='10', hour='*'), ensure_single_info={'mode': 'DEDUPED'})
    def refresh_package_information():
        """
        Retrieve and store the package information of all StorageRouters
        :return: None
        """
        GenericController._logger.info('Updating package information')
        threads = []
        information = {}
        all_storagerouters = StorageRouterList.get_storagerouters()
        for storagerouter in all_storagerouters:
            information[storagerouter.ip] = {}
            for fct in Toolbox.fetch_hooks('update', 'get_package_info_multi'):
                try:
                    # We make use of these clients in Threads --> cached = False
                    client = SSHClient(endpoint=storagerouter, username='root', cached=False)
                except UnableToConnectException:
                    information[storagerouter.ip]['errors'] = ['StorageRouter {0} is inaccessible'.format(storagerouter.name)]
                    break
                thread = Thread(target=fct,
                                args=(client, information))
                thread.start()
                threads.append(thread)

        for fct in Toolbox.fetch_hooks('update', 'get_package_info_single'):
            thread = Thread(target=fct,
                            args=(information,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        errors = []
        copy_information = copy.deepcopy(information)
        for ip, info in information.iteritems():
            if len(info.get('errors', [])) > 0:
                errors.extend(['{0}: {1}'.format(ip, error) for error in info['errors']])
                copy_information.pop(ip)

        for storagerouter in all_storagerouters:
            info = copy_information.get(storagerouter.ip, {})
            if 'errors' in info:
                info.pop('errors')
            storagerouter.package_information = info
            storagerouter.save()

        if len(errors) > 0:
            errors = [str(error) for error in set(errors)]
            raise Exception(' - {0}'.format('\n - '.join(errors)))

    @staticmethod
    @ovs_task(name='ovs.generic.run_backend_domain_hooks')
    def run_backend_domain_hooks(backend_guid):
        """
        Run hooks when the Backend Domains have been updated
        :param backend_guid: Guid of the Backend to update
        :type backend_guid: str
        :return: None
        """
        for fct in Toolbox.fetch_hooks('backend', 'domains-update'):
            fct(backend_guid=backend_guid)
