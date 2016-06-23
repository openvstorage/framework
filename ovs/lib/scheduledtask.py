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
ScheduledTaskController module
"""

import copy
import time
from celery.schedules import crontab
from ConfigParser import RawConfigParser
from datetime import datetime
from datetime import timedelta
from ovs.celery_run import celery
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.lists.servicelist import ServiceList
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonClusterConfig
from ovs.extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonAdmin, ArakoonClientConfig
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.sshclient import UnableToConnectException
from ovs.extensions.generic.system import System
from ovs.extensions.services.service import ServiceManager
from ovs.lib.helpers.celery_toolbox import CeleryToolbox
from ovs.lib.helpers.decorators import ensure_single
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.vdisk import VDiskController
from ovs.lib.vmachine import VMachineController
from ovs.log.log_handler import LogHandler
from StringIO import StringIO
from time import mktime

SCRUBBER_LOGFILE_LOCATION = '/var/log/upstart/ovs-scrubber.log'


class ScheduledTaskController(object):
    """
    This controller contains all scheduled task code. These tasks can be
    executed at certain intervals and should be self-containing
    """
    _logger = LogHandler.get('lib', name='scheduled tasks')

    @staticmethod
    @celery.task(name='ovs.scheduled.snapshot_all_vms', schedule=crontab(minute='0', hour='2-22'))
    @ensure_single(task_name='ovs.scheduled.snapshot_all_vms', extra_task_names=['ovs.scheduled.delete_snapshots'])
    def snapshot_all_vms():
        """
        Snapshots all VMachines
        """
        ScheduledTaskController._logger.info('[SSA] started')
        success = []
        fail = []
        machines = VMachineList.get_customer_vmachines()
        for machine in machines:
            try:
                VMachineController.snapshot(machineguid=machine.guid,
                                            label='',
                                            is_consistent=False,
                                            is_automatic=True,
                                            is_sticky=False)
                success.append(machine.guid)
            except:
                fail.append(machine.guid)
        ScheduledTaskController._logger.info('[SSA] Snapshot has been taken for {0} vMachines, {1} failed.'.format(len(success), len(fail)))

    @staticmethod
    @celery.task(name='ovs.scheduled.delete_snapshots', schedule=crontab(minute='1', hour='2'))
    @ensure_single(task_name='ovs.scheduled.delete_snapshots')
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
        ScheduledTaskController._logger.info('Delete snapshots started')

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

        # Place all snapshots in bucket_chains
        bucket_chains = []
        for vmachine in VMachineList.get_customer_vmachines():
            if any(vd.info['object_type'] in ['BASE'] for vd in vmachine.vdisks):
                bucket_chain = copy.deepcopy(buckets)
                for snapshot in vmachine.snapshots:
                    if snapshot.get('is_sticky') is True:
                        continue
                    timestamp = int(snapshot['timestamp'])
                    for bucket in bucket_chain:
                        if bucket['start'] >= timestamp > bucket['end']:
                            for diskguid, snapshotguid in snapshot['snapshots'].iteritems():
                                if len(VDiskList.get_by_parentsnapshot(snapshotguid)) > 0:
                                    continue
                                bucket['snapshots'].append({'timestamp': timestamp,
                                                            'snapshotid': snapshotguid,
                                                            'diskguid': diskguid,
                                                            'is_consistent': snapshot['is_consistent']})
                bucket_chains.append(bucket_chain)

        for vdisk in VDiskList.get_without_vmachine():
            if vdisk.info['object_type'] in ['BASE']:
                bucket_chain = copy.deepcopy(buckets)
                for snapshot in vdisk.snapshots:
                    if snapshot.get('is_sticky') is True:
                        continue
                    if len(VDiskList.get_by_parentsnapshot(snapshot['guid'])) > 0:
                        continue
                    timestamp = int(snapshot['timestamp'])
                    for bucket in bucket_chain:
                        if bucket['start'] >= timestamp > bucket['end']:
                            bucket['snapshots'].append({'timestamp': timestamp,
                                                        'snapshotid': snapshot['guid'],
                                                        'diskguid': vdisk.guid,
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
                    VDiskController.delete_snapshot(diskguid=snapshot['diskguid'],
                                                    snapshotid=snapshot['snapshotid'])
        ScheduledTaskController._logger.info('Delete snapshots finished')

    @staticmethod
    @celery.task(name='ovs.scheduled.gather_scrub_work', schedule=crontab(minute='0', hour='3'))
    @ensure_single(task_name='ovs.scheduled.gather_scrub_work')
    def gather_scrub_work():
        """
        Retrieve and execute scrub work
        :return: None
        """
        ScheduledTaskController._logger.info('Gather Scrub - Started')

        scrub_locations = {}
        for storage_driver in StorageDriverList.get_storagedrivers():
            for partition in storage_driver.partitions:
                if DiskPartition.ROLES.SCRUB == partition.role:
                    ScheduledTaskController._logger.info('Gather Scrub - Storage Router {0:<15} has SCRUB partition at {1}'.format(storage_driver.storagerouter.ip, partition.path))
                    if storage_driver.storagerouter not in scrub_locations:
                        try:
                            sshclient = SSHClient(storage_driver.storagerouter)
                            # Use ServiceManager(sshclient) to make sure ovs-workers are actually running
                            if ServiceManager.get_service_status('workers', sshclient) is False:
                                ScheduledTaskController._logger.warning('Gather Scrub - Storage Router {0:<15} - workers are not running'.format(storage_driver.storagerouter.ip))
                            else:
                                scrub_locations[storage_driver.storagerouter] = str(partition.path)
                        except UnableToConnectException:
                            ScheduledTaskController._logger.warning('Gather Scrub - Storage Router {0:<15} is not reachable'.format(storage_driver.storagerouter.ip))

        if len(scrub_locations) == 0:
            raise RuntimeError('No scrub locations found')

        vdisk_guids = set()
        for vmachine in VMachineList.get_customer_vmachines():
            for vdisk in vmachine.vdisks:
                if vdisk.info['object_type'] == 'BASE':
                    vdisk_guids.add(vdisk.guid)
        for vdisk in VDiskList.get_without_vmachine():
            if vdisk.info['object_type'] == 'BASE':
                vdisk_guids.add(vdisk.guid)

        if len(vdisk_guids) == 0:
            ScheduledTaskController._logger.info('Gather Scrub - No scrub work needed'.format(len(vdisk_guids)))
            return

        ScheduledTaskController._logger.info('Gather Scrub - Checking {0} volumes for scrub work'.format(len(vdisk_guids)))
        local_machineid = System.get_my_machine_id()
        local_storage_router = None
        local_scrub_location = None
        local_vdisks_to_scrub = []
        result_set = {}
        storage_router_list = []
        scrub_map = {}

        for index, scrub_info in enumerate(scrub_locations.items()):
            start_index = index * len(vdisk_guids) / len(scrub_locations)
            end_index = (index + 1) * len(vdisk_guids) / len(scrub_locations)
            storage_router = scrub_info[0]
            vdisk_guids_to_scrub = list(vdisk_guids)[start_index:end_index]
            local = storage_router.machine_id == local_machineid
            ScheduledTaskController._logger.info('Gather Scrub - Storage Router {0:<15} ({1}) - Scrubbing {2} virtual disks'.format(storage_router.ip, 'local' if local is True else 'remote', len(vdisk_guids_to_scrub)))

            if local is True:
                local_storage_router = storage_router
                local_scrub_location = scrub_info[1]
                local_vdisks_to_scrub = vdisk_guids_to_scrub
            else:
                result_set[storage_router.ip] = ScheduledTaskController._execute_scrub_work.s(scrub_location=scrub_info[1],
                                                                                              vdisk_guids=vdisk_guids_to_scrub).apply_async(routing_key='sr.{0}'.format(storage_router.machine_id))
                storage_router_list.append(storage_router)
                scrub_map[storage_router.ip] = vdisk_guids_to_scrub

        # Remote tasks have been launched, now start the local task and then wait for remote tasks to finish
        processed_guids = []
        if local_scrub_location is not None and len(local_vdisks_to_scrub) > 0:
            try:
                processed_guids = ScheduledTaskController._execute_scrub_work(scrub_location=local_scrub_location,
                                                                              vdisk_guids=local_vdisks_to_scrub)
            except Exception as ex:
                ScheduledTaskController._logger.error('Gather Scrub - Storage Router {0:<15} - Scrubbing failed with error:\n - {1}'.format(local_storage_router.ip, ex))

        all_results, failed_nodes = CeleryToolbox.manage_running_tasks(result_set,
                                                                       timesleep=60)  # Check every 60 seconds if tasks are still running

        for ip, result in all_results.iteritems():
            if isinstance(result, list):
                processed_guids.extend(result)
            else:
                ScheduledTaskController._logger.error('Gather Scrub - Storage Router {0:<15} - Scrubbing failed with error:\n - {1}'.format(ip, result))

        result_set = {}
        for failed_node in failed_nodes:
            ScheduledTaskController._logger.warning('Scrubbing failed on node {0}. Will reschedule on another node.'.format(failed_node))
            vdisk_guids_to_scrub = scrub_map[failed_node]
            rescheduled_work = False
            for storage_router, scrub_location in scrub_locations.items():
                if storage_router.ip not in failed_nodes:
                    if storage_router.machine_id != local_machineid:
                        ScheduledTaskController._logger.info('Rescheduled scrub work from node {0} to node {1}.'.format(failed_node, storage_router.ip))
                        result_set[storage_router.ip] = ScheduledTaskController._execute_scrub_work.s(scrub_location=scrub_location,
                                                                                                      vdisk_guids=vdisk_guids_to_scrub).apply_async(
                            routing_key='sr.{0}'.format(storage_router.machine_id)
                        )
                        storage_router_list.append(storage_router)
                        rescheduled_work = True
                        break
            if rescheduled_work is False:
                if local_scrub_location is not None:
                    try:
                        processed_guids.extend(ScheduledTaskController._execute_scrub_work(scrub_location=local_scrub_location,
                                                                                           vdisk_guids=vdisk_guids_to_scrub))
                    except Exception as ex:
                        ScheduledTaskController._logger.error(
                            'Gather Scrub - Storage Router Local - Scrubbing failed with error:\n - {0}'.format(ex))
                else:
                    ScheduledTaskController._logger.warning('No nodes left to reschedule work from node {0}'.format(failed_node))

        if len(result_set) > 0:
            all_results2, failed_nodes = CeleryToolbox.manage_running_tasks(result_set,
                                                                            timesleep=60)  # Check every 60 seconds if tasks are still running

            for ip, result in all_results2.iteritems():
                if isinstance(result, list):
                    processed_guids.extend(result)
                else:
                    ScheduledTaskController._logger.error('Gather Scrub - Storage Router {0:<15} - Scrubbing failed with error:\n - {1}'.format(ip, result))

        if len(set(processed_guids)) != len(vdisk_guids) or set(processed_guids).difference(vdisk_guids):
            raise RuntimeError('Scrubbing failed for 1 or more storagerouters')
        ScheduledTaskController._logger.info('Gather Scrub - Finished')

    @staticmethod
    @celery.task(name='ovs.scheduled.execute_scrub_work')
    def _execute_scrub_work(scrub_location, vdisk_guids):
        def _verify_mds_config(current_vdisk):
            current_vdisk.invalidate_dynamics(['info'])
            vdisk_configs = current_vdisk.info['metadata_backend_config']
            if len(vdisk_configs) == 0:
                raise RuntimeError('Could not load MDS configuration')
            return vdisk_configs

        ScheduledTaskController._logger.info('Execute Scrub - Started')
        ScheduledTaskController._logger.info('Execute Scrub - Scrub location - {0}'.format(scrub_location))
        total = len(vdisk_guids)
        skipped = 0
        storagedrivers = {}
        failures = []
        for vdisk_guid in vdisk_guids:
            vdisk = VDisk(vdisk_guid)
            try:
                # Load the vDisk's StorageDriver
                ScheduledTaskController._logger.info('Execute Scrub - Virtual disk {0} - {1} - Started'.format(vdisk.guid, vdisk.name))
                vdisk.invalidate_dynamics(['storagedriver_id'])
                if vdisk.storagedriver_id not in storagedrivers:
                    storagedrivers[vdisk.storagedriver_id] = StorageDriverList.get_by_storagedriver_id(vdisk.storagedriver_id)
                storagedriver = storagedrivers[vdisk.storagedriver_id]

                # Load the vDisk's MDS configuration
                configs = _verify_mds_config(current_vdisk=vdisk)

                # Check MDS master is local. Trigger MDS handover if necessary
                if configs[0].get('ip') != storagedriver.storagerouter.ip:
                    ScheduledTaskController._logger.debug('Execute Scrub - Virtual disk {0} - {1} - MDS master is not local, trigger handover'.format(vdisk.guid, vdisk.name))
                    MDSServiceController.ensure_safety(vdisk)
                    configs = _verify_mds_config(current_vdisk=vdisk)
                    if configs[0].get('ip') != storagedriver.storagerouter.ip:
                        skipped += 1
                        ScheduledTaskController._logger.info('Execute Scrub - Virtual disk {0} - {1} - Skipping because master MDS still not local'.format(vdisk.guid, vdisk.name))
                        continue
                with vdisk.storagedriver_client.make_locked_client(str(vdisk.volume_id)) as locked_client:
                    ScheduledTaskController._logger.info('Execute Scrub - Virtual disk {0} - {1} - Retrieve and apply scrub work'.format(vdisk.guid, vdisk.name))
                    work_units = locked_client.get_scrubbing_workunits()
                    for work_unit in work_units:
                        scrubbing_result = locked_client.scrub(work_unit, scrub_location, log_sinks=[SCRUBBER_LOGFILE_LOCATION])
                        locked_client.apply_scrubbing_result(scrubbing_result)
                    if work_units:
                        ScheduledTaskController._logger.info('Execute Scrub - Virtual disk {0} - {1} - Scrub successfully applied'.format(vdisk.guid, vdisk.name))
                    else:
                        ScheduledTaskController._logger.info('Execute Scrub - Virtual disk {0} - {1} - No scrubbing required'.format(vdisk.guid, vdisk.name))
            except Exception as ex:
                failures.append('Failed scrubbing work unit for volume {0} with guid {1}: {2}'.format(vdisk.name, vdisk.guid, ex))

        failed = len(failures)
        ScheduledTaskController._logger.info('Execute Scrub - Finished - Success: {0} - Failed: {1} - Skipped: {2}'.format((total - failed - skipped), failed, skipped))
        if failed > 0:
            raise Exception('\n - '.join(failures))
        return vdisk_guids

    @staticmethod
    @celery.task(name='ovs.scheduled.collapse_arakoon', schedule=crontab(minute='10', hour='0,2,4,6,8,10,12,14,16,18,20,22'))
    @ensure_single(task_name='ovs.scheduled.collapse_arakoon')
    def collapse_arakoon():
        """
        Collapse Arakoon's Tlogs
        :return: None
        """
        ScheduledTaskController._logger.info('Starting arakoon collapse')
        arakoon_clusters = []
        for service in ServiceList.get_services():
            if service.is_internal is True and \
               service.type.name in (ServiceType.SERVICE_TYPES.ARAKOON,
                                     ServiceType.SERVICE_TYPES.NS_MGR,
                                     ServiceType.SERVICE_TYPES.ALBA_MGR):
                arakoon_clusters.append(service.name.replace('arakoon-', ''))

        for cluster in arakoon_clusters:
            ScheduledTaskController._logger.info('  Collapsing cluster {0}'.format(cluster))
            contents = EtcdConfiguration.get(ArakoonClusterConfig.ETCD_CONFIG_KEY.format(cluster), raw=True)
            parser = RawConfigParser()
            parser.readfp(StringIO(contents))
            nodes = {}
            for node in parser.get('global', 'cluster').split(','):
                node = node.strip()
                nodes[node] = ([str(parser.get(node, 'ip'))], int(parser.get(node, 'client_port')))
            config = ArakoonClientConfig(str(cluster), nodes)
            for node in nodes.keys():
                ScheduledTaskController._logger.info('    Collapsing node: {0}'.format(node))
                client = ArakoonAdmin(config)
                try:
                    client.collapse(str(node), 2)
                except:
                    ScheduledTaskController._logger.exception('Error during collapsing cluster {0} node {1}'.format(cluster, node))

        ScheduledTaskController._logger.info('Arakoon collapse finished')
