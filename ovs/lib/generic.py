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
from datetime import datetime, timedelta
from Queue import Empty, Queue
from threading import Thread
from time import mktime
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.lists.servicelist import ServiceList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonClusterConfig
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.filemutex import file_mutex
from ovs.extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs.extensions.services.service import ServiceManager
from ovs.lib.helpers.decorators import ovs_task
from ovs.lib.helpers.toolbox import Toolbox, Schedule
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.vdisk import VDiskController
from ovs.log.log_handler import LogHandler


class GenericController(object):
    """
    This controller contains all generic task code. These tasks can be
    executed at certain intervals and should be self-containing
    """
    _logger = LogHandler.get('lib', name='generic tasks')

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
    @ovs_task(name='ovs.generic.execute_scrub', schedule=Schedule(minute='0', hour='3'), ensure_single_info={'mode': 'DEFAULT'})
    def execute_scrub():
        """
        Retrieve and execute scrub work
        :return: None
        """
        GenericController._logger.info('Scrubber - Started')
        vpools = VPoolList.get_vpools()
        error_messages = []
        scrub_locations = []
        for storage_router in StorageRouterList.get_storagerouters():
            scrub_partitions = storage_router.partition_config.get(DiskPartition.ROLES.SCRUB, [])
            if len(scrub_partitions) == 0:
                continue
            if len(scrub_partitions) > 1:
                raise RuntimeError('Multiple {0} partitions defined for StorageRouter {1}'.format(DiskPartition.ROLES.SCRUB, storage_router.name))

            partition = DiskPartition(scrub_partitions[0])
            GenericController._logger.info('Scrubber - Storage Router {0:<15} has {1} partition at {2}'.format(storage_router.ip, DiskPartition.ROLES.SCRUB, partition.folder))
            try:
                SSHClient(storage_router, 'root')
                scrub_locations.append({'scrub_path': str(partition.folder),
                                        'storage_router': storage_router})
            except UnableToConnectException:
                GenericController._logger.warning('Scrubber - Storage Router {0:<15} is not reachable'.format(storage_router.ip))

        if len(scrub_locations) == 0:
            raise ValueError('No scrub locations found, cannot scrub')

        number_of_vpools = len(vpools)
        if number_of_vpools >= 6:
            max_threads_per_vpool = 1
        elif number_of_vpools >= 3:
            max_threads_per_vpool = 2
        else:
            max_threads_per_vpool = 5

        threads = []
        counter = 0
        for vp in vpools:
            # Verify amount of vDisks on vPool
            GenericController._logger.info('Scrubber - vPool {0} - Checking scrub work'.format(vp.name))
            if len(vp.vdisks) == 0:
                GenericController._logger.info('Scrubber - vPool {0} - No scrub work'.format(vp.name))
                continue

            # Fill queue with all vDisks for current vPool
            vpool_queue = Queue()
            for vd in vp.vdisks:
                if vd.is_vtemplate is True:
                    GenericController._logger.info('Scrubber - vPool {0} - vDisk {1} {2} - Is a template, not scrubbing'.format(vp.name, vd.guid, vd.name))
                    continue
                vd.invalidate_dynamics('storagedriver_id')
                if not vd.storagedriver_id:
                    GenericController._logger.warning('Scrubber - vPool {0} - vDisk {1} {2} - No StorageDriver ID found'.format(vp.name, vd.guid, vd.name))
                    continue
                vpool_queue.put(vd.guid)

            # Copy backend connection manager information in separate key
            threads_to_spawn = min(max_threads_per_vpool, len(scrub_locations))
            GenericController._logger.info('Scrubber - vPool {0} - Spawning {1} thread{2}'.format(vp.name, threads_to_spawn, '' if threads_to_spawn == 1 else 's'))
            for _ in range(threads_to_spawn):
                scrub_target = scrub_locations[counter % len(scrub_locations)]
                thread = Thread(target=GenericController.execute_scrub_work,
                                name='scrub_{0}_{1}'.format(vp.guid, scrub_target['storage_router'].guid),
                                args=(vpool_queue, vp, scrub_target, error_messages))
                thread.start()
                threads.append(thread)
                counter += 1

        for thread in threads:
            thread.join()

        if len(error_messages) > 0:
            raise Exception('Errors occurred while scrubbing:\n  - {0}'.format('\n  - '.join(error_messages)))

    @staticmethod
    def execute_scrub_work(queue, vpool, scrub_info, error_messages):
        """
        Executes scrub work for a given vDisk queue and vPool, based on scrub_info
        :param queue: a Queue with vDisk guids that need to be scrubbed (they should only be member of a single vPool)
        :type queue: Queue
        :param vpool: the vPool object of the vDisks
        :type vpool: VPool
        :param scrub_info: A dict containing scrub information: `scrub_path` with the path where to scrub and `storage_router` with the StorageRouter
                           that needs to do the work
        :type scrub_info: dict
        :param error_messages: A list of error messages to be filled
        :type error_messages: list
        :return: a list of error messages
        :rtype: list
        """

        def _verify_mds_config(current_vdisk):
            current_vdisk.invalidate_dynamics('info')
            vdisk_configs = current_vdisk.info['metadata_backend_config']
            if len(vdisk_configs) == 0:
                raise RuntimeError('Could not load MDS configuration')
            return vdisk_configs

        client = None
        lock_time = 5 * 60
        storagerouter = scrub_info['storage_router']
        scrub_directory = '{0}/scrub_work_{1}_{2}'.format(scrub_info['scrub_path'], vpool.name, storagerouter.name)
        scrub_config_key = 'ovs/vpools/{0}/proxies/scrub/scrub_config_{1}'.format(vpool.guid, storagerouter.guid)
        backend_config_key = 'ovs/vpools/{0}/proxies/scrub/backend_config_{1}'.format(vpool.guid, storagerouter.guid)
        alba_proxy_service = 'ovs-albaproxy_{0}_{1}_scrub'.format(vpool.name, storagerouter.name)

        # Deploy a proxy
        try:
            with file_mutex(name='ovs_albaproxy_scrub', wait=lock_time):
                GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Deploying ALBA proxy {2}'.format(vpool.name, storagerouter.name, alba_proxy_service))
                client = SSHClient(storagerouter, 'root')
                client.dir_create(scrub_directory)
                client.dir_chmod(scrub_directory, 0777)  # Celery task executed by 'ovs' user and should be able to write in it
                if ServiceManager.has_service(name=alba_proxy_service, client=client) is True and ServiceManager.get_service_status(name=alba_proxy_service, client=client) is True:
                    GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Re-using existing proxy service {2}'.format(vpool.name, storagerouter.name, alba_proxy_service))
                    scrub_config = Configuration.get(scrub_config_key)
                else:
                    machine_id = System.get_my_machine_id(client)
                    port_range = Configuration.get('/ovs/framework/hosts/{0}/ports|storagedriver'.format(machine_id))
                    port = System.get_free_ports(selected_range=port_range, nr=1, client=client)[0]
                    # Scrub config
                    # {u'albamgr_cfg_url': u'arakoon://config/ovs/vpools/71e2f717-f270-4a41-bbb0-d4c8c084d43e/proxies/64759516-3471-4321-b912-fb424568fc5b/config/abm?ini=%2Fopt%2FOpenvStorage%2Fconfig%2Farakoon_cacc.ini',
                    #  u'fragment_cache': [u'none'],
                    #  u'ips': [u'127.0.0.1'],
                    #  u'log_level': u'info',
                    #  u'manifest_cache_size': 17179869184,
                    #  u'port': 0,
                    #  u'transport': u'tcp'}

                    # Backend config
                    # {u'backend_type': u'MULTI',
                    #  u'backend_interface_retries_on_error': 5,
                    #  u'backend_interface_retry_backoff_multiplier': 2.0,
                    #  u'backend_interface_retry_interval_secs': 1,
                    #  u'0': {u'alba_connection_host': u'10.100.193.155',
                    #         u'alba_connection_port': 26204,
                    #         u'alba_connection_preset': u'preset',
                    #         u'alba_connection_timeout': 15,
                    #         u'alba_connection_transport': u'TCP',
                    #         u'backend_type': u'ALBA'}}
                    scrub_config = Configuration.get('ovs/vpools/{0}/proxies/scrub/generic_scrub'.format(vpool.guid))
                    scrub_config['port'] = port
                    scrub_config['transport'] = 'tcp'
                    Configuration.set(scrub_config_key, json.dumps(scrub_config, indent=4), raw=True)

                    params = {'VPOOL_NAME': vpool.name,
                              'LOG_SINK': LogHandler.get_sink_path('alba_proxy'),
                              'CONFIG_PATH': Configuration.get_configuration_path(scrub_config_key)}
                    ServiceManager.add_service(name='ovs-albaproxy', params=params, client=client, target_name=alba_proxy_service)
                    ServiceManager.start_service(name=alba_proxy_service, client=client)
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
                Configuration.set(backend_config_key, json.dumps({"backend_connection_manager": backend_config}, indent=4), raw=True)
        except Exception:
            message = 'Scrubber - vPool {0} - StorageRouter {1} - An error occurred deploying ALBA proxy {2}'.format(vpool.name, storagerouter.name, alba_proxy_service)
            error_messages.append(message)
            GenericController._logger.exception(message)
            if client is not None and ServiceManager.has_service(name=alba_proxy_service, client=client) is True:
                if ServiceManager.get_service_status(name=alba_proxy_service, client=client) is True:
                    ServiceManager.stop_service(name=alba_proxy_service, client=client)
                ServiceManager.remove_service(name=alba_proxy_service, client=client)
            if Configuration.exists(scrub_config_key):
                Configuration.delete(scrub_config_key)

        try:
            # Empty the queue with vDisks to scrub
            with remote(storagerouter.ip, [VDisk]) as rem:
                while True:
                    vdisk = None
                    vdisk_guid = queue.get(False)
                    try:
                        # Check MDS master is local. Trigger MDS handover if necessary
                        vdisk = rem.VDisk(vdisk_guid)
                        GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - Started scrubbing at location {3}'.format(vpool.name, storagerouter.name, vdisk.name, scrub_directory))
                        configs = _verify_mds_config(current_vdisk=vdisk)
                        storagedriver = StorageDriverList.get_by_storagedriver_id(vdisk.storagedriver_id)
                        if configs[0].get('ip') != storagedriver.storagerouter.ip:
                            GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - MDS master is not local, trigger handover'.format(vpool.name, storagerouter.name, vdisk.name))
                            MDSServiceController.ensure_safety(VDisk(vdisk_guid))  # Do not use a remote VDisk instance here
                            configs = _verify_mds_config(current_vdisk=vdisk)
                            if configs[0].get('ip') != storagedriver.storagerouter.ip:
                                GenericController._logger.warning('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - Skipping because master MDS still not local'.format(vpool.name, storagerouter.name, vdisk.name))
                                continue

                        # Do the actual scrubbing
                        with vdisk.storagedriver_client.make_locked_client(str(vdisk.volume_id)) as locked_client:
                            GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - Retrieve and apply scrub work'.format(vpool.name, storagerouter.name, vdisk.name))
                            work_units = locked_client.get_scrubbing_workunits()
                            for work_unit in work_units:
                                res = locked_client.scrub(work_unit=work_unit,
                                                          scratch_dir=scrub_directory,
                                                          log_sinks=[LogHandler.get_sink_path('scrubber', allow_override=True, forced_target_type='file')],
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

        except Empty:  # Raised when all items have been fetched from the queue
            GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Queue completely processed'.format(vpool.name, storagerouter.name))
        except Exception:
            message = 'Scrubber - vPool {0} - StorageRouter {1} - Scrubbing failed'.format(vpool.name, storagerouter.name)
            error_messages.append(message)
            GenericController._logger.exception(message)

        # Delete the proxy again
        try:
            with file_mutex(name='ovs_albaproxy_scrub', wait=lock_time):
                GenericController._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Removing service {2}'.format(vpool.name, storagerouter.name, alba_proxy_service))
                client = SSHClient(storagerouter, 'root')
                client.dir_delete(scrub_directory)
                if ServiceManager.has_service(alba_proxy_service, client=client):
                    ServiceManager.stop_service(alba_proxy_service, client=client)
                    ServiceManager.remove_service(alba_proxy_service, client=client)
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
        from ovs.extensions.generic.toolbox import ExtensionsToolbox

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
                config = ArakoonClusterConfig(cluster, source_ip=ip)
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
            for function in Toolbox.fetch_hooks('update', 'get_package_info_multi'):
                try:
                    # We make use of these clients in Threads --> cached = False
                    client = SSHClient(endpoint=storagerouter, username='root', cached=False)
                except UnableToConnectException:
                    information[storagerouter.ip]['errors'] = ['StorageRouter {0} is inaccessible'.format(storagerouter.name)]
                    break
                thread = Thread(target=function,
                                args=(client, information))
                thread.start()
                threads.append(thread)

        for function in Toolbox.fetch_hooks('update', 'get_package_info_single'):
            thread = Thread(target=function,
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
        for function in Toolbox.fetch_hooks('backend', 'domains-update'):
            function(backend_guid=backend_guid)
