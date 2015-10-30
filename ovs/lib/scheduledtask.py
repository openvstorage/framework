# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
ScheduledTaskController module
"""

import copy
import time
import os
import traceback
from celery.schedules import crontab
from datetime import datetime, timedelta
from ovs.celery_run import celery
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagementEx
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.system import System
from ovs.lib.vmachine import VMachineController
from ovs.lib.vdisk import VDiskController
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.helpers.decorators import ensure_single
from ovs.log.logHandler import LogHandler
from time import mktime
from volumedriver.storagerouter.storagerouterclient import Scrubber
from volumedriver.storagerouter import storagerouterclient

_storagedriver_scrubber = Scrubber()
logger = LogHandler.get('lib', name='scheduled tasks')
storagerouterclient.Logger.setupLogging(LogHandler.load_path('storagerouterclient'))
storagerouterclient.Logger.enableLogging()


class ScheduledTaskController(object):
    """
    This controller contains all scheduled task code. These tasks can be
    executed at certain intervals and should be self-containing
    """

    @staticmethod
    @celery.task(name='ovs.scheduled.snapshotall', bind=True, schedule=crontab(minute='0', hour='2-22'))
    @ensure_single(['ovs.scheduled.snapshotall', 'ovs.scheduled.deletescrubsnapshots'])
    def snapshot_all_vms():
        """
        Snapshots all VMachines
        """
        logger.info('[SSA] started')
        success = []
        fail = []
        machines = VMachineList.get_customer_vmachines()
        for machine in machines:
            try:
                VMachineController.snapshot(machineguid=machine.guid,
                                            label='',
                                            is_consistent=False,
                                            is_automatic=True)
                success.append(machine.guid)
            except:
                fail.append(machine.guid)
        logger.info('[SSA] {0} vMachines were snapshotted, {1} failed.'.format(len(success), len(fail)))

    @staticmethod
    @celery.task(name='ovs.scheduled.deletescrubsnapshots', bind=True, schedule=crontab(minute='0', hour='2'))
    @ensure_single(['ovs.scheduled.deletescrubsnapshots'])
    def deletescrubsnapshots(timestamp=None):
        """
        Delete snapshots & scrubbing policy

        Implemented delete snapshot policy:
        < 1d | 1d bucket | 1 | best of bucket   | 1d
        < 1w | 1d bucket | 6 | oldest of bucket | 7d = 1w
        < 1m | 1w bucket | 3 | oldest of bucket | 4w = 1m
        > 1m | delete
        """

        logger.info('Delete snapshots started')

        day = timedelta(1)
        week = day * 7

        def make_timestamp(offset):
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
                    timestamp = int(snapshot['timestamp'])
                    for bucket in bucket_chain:
                        if bucket['start'] >= timestamp > bucket['end']:
                            for diskguid, snapshotguid in snapshot['snapshots'].iteritems():
                                bucket['snapshots'].append({'timestamp': timestamp,
                                                            'snapshotid': snapshotguid,
                                                            'diskguid': diskguid,
                                                            'is_consistent': snapshot['is_consistent']})
                bucket_chains.append(bucket_chain)

        for vdisk in VDiskList.get_without_vmachine():
            if vdisk.info['object_type'] in ['BASE']:
                bucket_chain = copy.deepcopy(buckets)
                for snapshot in vdisk.snapshots:
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

        logger.info('Delete snapshots finished')
        logger.info('Scrubbing started')

        vdisks = []
        for vmachine in VMachineList.get_customer_vmachines():
            for vdisk in vmachine.vdisks:
                if vdisk.info['object_type'] in ['BASE'] and len(vdisk.child_vdisks) == 0:
                    vdisks.append(vdisk)
        for vdisk in VDiskList.get_without_vmachine():
            if vdisk.info['object_type'] in ['BASE'] and len(vdisk.child_vdisks) == 0:
                vdisks.append(vdisk)

        total = 0
        failed = 0
        skipped = 0
        storagedrivers = {}
        scrub_location = None
        storagerouter = System.get_my_storagerouter()
        for disk in storagerouter.disks:
            for partition in disk.partitions:
                if DiskPartition.ROLES.SCRUB in partition.roles:
                    scrub_location = partition.folder
        if scrub_location is None:
            raise RuntimeError('No scrub location on StorageRouter {0}'.format(storagerouter.name))
        for vdisk in vdisks:
            try:
                total += 1
                # Load the vDisk's StorageDriver
                vdisk.invalidate_dynamics(['info', 'storagedriver_id'])
                if vdisk.storagedriver_id not in storagedrivers:
                    storagedrivers[vdisk.storagedriver_id] = StorageDriverList.get_by_storagedriver_id(vdisk.storagedriver_id)
                storagedriver = storagedrivers[vdisk.storagedriver_id]
                # Load the vDisk's MDS configuration
                vdisk.invalidate_dynamics(['info'])
                configs = vdisk.info['metadata_backend_config']
                if len(configs) == 0:
                    raise RuntimeError('Could not load MDS configuration')
                if configs[0]['ip'] != storagedriver.storagerouter.ip:
                    # The MDS master is not local. Trigger an MDS handover and try again
                    logger.debug('MDS for volume {0} is not local. Trigger handover'.format(vdisk.volume_id))
                    MDSServiceController.ensure_safety(vdisk)
                    vdisk.invalidate_dynamics(['info'])
                    configs = vdisk.info['metadata_backend_config']
                    if len(configs) == 0:
                        raise RuntimeError('Could not load MDS configuration')
                    if configs[0]['ip'] != storagedriver.storagerouter.ip:
                        skipped += 1
                        logger.info('Skipping scrubbing work unit for volume {0}: MDS master is not local'.format(vdisk.volume_id))
                        continue
                work_units = vdisk.storagedriver_client.get_scrubbing_workunits(str(vdisk.volume_id))
                for work_unit in work_units:
                    scrubbing_result = _storagedriver_scrubber.scrub(work_unit, str(scrub_location))
                    vdisk.storagedriver_client.apply_scrubbing_result(scrubbing_result)
            except Exception, ex:
                failed += 1
                logger.info('Failed scrubbing work unit for volume {0}: {1}'.format(vdisk.volume_id, ex))

        logger.info('Scrubbing finished. {0} volumes: {1} success, {2} failed, {3} skipped.'.format(total, (total - failed - skipped), failed, skipped))

    @staticmethod
    @celery.task(name='ovs.scheduled.collapse_arakoon', bind=True, schedule=crontab(minute='30', hour='0'))
    @ensure_single(['ovs.scheduled.collapse_arakoon'])
    def collapse_arakoon():
        logger.info('Starting arakoon collapse')
        arakoon_dir = os.path.join(Configuration.get('ovs.core.cfgdir'), 'arakoon')
        arakoon_clusters = map(lambda directory: os.path.basename(directory.rstrip(os.path.sep)),
                               os.walk(arakoon_dir).next()[1])
        for cluster in arakoon_clusters:
            logger.info('  Collapsing cluster: {}'.format(cluster))
            cluster_instance = ArakoonManagementEx().getCluster(cluster)
            for node in cluster_instance.listNodes():
                logger.info('    Collapsing node: {}'.format(node))
                try:
                    cluster_instance.remoteCollapse(node, 2)  # Keep 2 tlogs
                except Exception as e:
                    logger.info('Error during collapsing cluster {} node {}: {}\n{}'.format(cluster,
                                                                                            node,
                                                                                            str(e),
                                                                                            traceback.format_exc()))
        logger.info('Arakoon collapse finished')
