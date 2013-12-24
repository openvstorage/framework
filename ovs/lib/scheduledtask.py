# license see http://www.openvstorage.com/licenses/opensource/

"""
ScheduledTaskController module
"""

from celery import group
from celery.task.control import inspect
import copy
import time
import os
import traceback
from time import mktime
from datetime import datetime
from ovs.plugin.provider.configuration import Configuration
from ovs.celery import celery
from ovs.celery import loghandler
from ovs.lib.vmachine import VMachineController
from ovs.lib.vdisk import VDiskController
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient
from volumedriver.scrubber.scrubber import Scrubber

_vsr_client = VolumeStorageRouterClient().load()
_vsr_scrubber = Scrubber()


def ensure_single(tasknames):
    """
    Decorator ensuring a new task cannot be started in case a certain task is
    running, scheduled or reserved.

    The task using this decorator on, must be a bound task (with bind=True argument). Keep also in
    mind that validation will be executed by the worker itself, so if the task is scheduled on
    a worker currently processing a "duplicate" task, it will only get validated after the first
    one completes, which will result in the fact that the task will execute normally.

    @param tasknames: list of names to check
    @type tasknames: list
    """
    def wrap(function):
        """
        Wrapper function
        """
        def wrapped(self=None, *args, **kwargs):
            """
            Wrapped function
            """
            if not hasattr(self, 'request'):
                raise RuntimeError('The decorator ensure_single can only be applied to bound tasks (with bind=True argument)')
            task_id = self.request.id

            def can_run():
                """
                Checks whether a task is running/scheduled/reserved.
                The check is eecuted in stages, as querying the inspector is a slow call.
                """
                if tasknames:
                    inspector = inspect()
                    active = inspector.active()
                    for taskname in tasknames:
                        for worker in active.values():
                            for task in worker:
                                if task['id'] != task_id and taskname == task['name']:
                                    return False
                    scheduled = inspector.scheduled()
                    for taskname in tasknames:
                        for worker in scheduled.values():
                            for task in worker:
                                if task['id'] != task_id and taskname == task['name']:
                                    return False
                    reserved = inspector.reserved()
                    for taskname in tasknames:
                        for worker in reserved.values():
                            for task in worker:
                                if task['id'] != task_id and taskname == task['name']:
                                    return False
                return True

            if can_run():
                return function(*args, **kwargs)
            else:
                return None

        return wrapped
    return wrap


class ScheduledTaskController(object):
    """
    This controller contains all scheduled task code. These tasks can be
    executed at certain intervals and should be self-containing
    """

    @staticmethod
    @celery.task(name='ovs.scheduled.snapshotall', bind=True)
    @ensure_single(['ovs.scheduled.snapshotall', 'ovs.scheduled.deletescrubsnapshots'])
    def snapshot_all_vms(*args, **kwargs):
        """
        Snapshots all VMachines
        """

        _ = (args, kwargs)
        loghandler.logger.info('[SSA] started')
        tasks = []
        machines = VMachineList.get_vmachines()
        for machine in machines:
            tasks.append(VMachineController.snapshot.s(machineguid=machine.guid,
                                                       label='',
                                                       is_consistent=False))
        workflow = group(task for task in tasks)
        loghandler.logger.info('[SSA] %d disk snapshots launched' % len(tasks))
        return workflow()

    @staticmethod
    @celery.task(name='ovs.scheduled.deletescrubsnapshots', bind=True)
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

        loghandler.logger.info('Delete snapshots started')

        day = 60 * 60 * 24
        week = day * 7

        # Calculate bucket structure
        if timestamp is None:
            timestamp = time.time()
        offset = int(mktime(datetime.fromtimestamp(timestamp).date().timetuple())) - day
        buckets = []
        # Buckets first 7 days: [0-1[, [1-2[, [2-3[, [3-4[, [4-5[, [5-6[, [6-7[
        for i in xrange(0, 7):
            buckets.append({'start': offset - (day * i),
                            'end': offset - (day * (i + 1)),
                            'type': '1d',
                            'snapshots': []})
        # Week buckets next 3 weeks: [7-14[, [14-21[, [21-28[
        for i in xrange(1, 4):
            buckets.append({'start': offset - (week * i),
                            'end': offset - (week * (i + 1)),
                            'type': '1w',
                            'snapshots': []})
        buckets.append({'start': offset - (week * 4),
                        'end': 0,
                        'type': 'rest',
                        'snapshots': []})

        # Place all snapshots in bucket_chains
        bucket_chains = []
        for vmachine in VMachineList.get_customer_vmachines():
            if any(vd.info['volume_type'] in ['BASE', 'CLONE'] for vd in vmachine.vdisks):
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
            if vdisk.info['volume_type'] in ['BASE', 'CLONE']:
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

        loghandler.logger.info('Delete snapshots finished')
        loghandler.logger.info('Scrubbing started')

        vdisks = []
        for vmachine in VMachineList.get_customer_vmachines():
            for vdisk in vmachine.vdisks:
                if vdisk.info['volume_type'] in ['BASE', 'CLONE']:
                    vdisks.append(vdisk)
        for vdisk in VDiskList.get_without_vmachine():
            if vdisk.info['volume_type'] in ['BASE', 'CLONE']:
                vdisks.append(vdisk)

        total = 0
        failed = 0
        for vdisk in vdisks:
            work_units = _vsr_client.get_scrubbing_workunits(str(vdisk.volumeid))
            for work_unit in work_units:
                try:
                    total += 1
                    scrubbing_result = _vsr_scrubber.scrub(
                        work_unit,
                        Configuration.get('ovs.core.tempfs.mountpoint')
                    )
                    _vsr_client.apply_scrubbing_result(scrubbing_result)
                except:
                    failed += 1
                    loghandler.logger.info('Failed scrubbing work unit for volume {}'.format(
                        vdisk.volumeid
                    ))

        loghandler.logger.info('Scrubbing finished. {} out of {} items failed.'.format(
            failed, total
        ))

    @staticmethod
    @celery.task(name='ovs.scheduled.collapse_arakoon', bind=True)
    @ensure_single(['ovs.scheduled.collapse_arakoon'])
    def collapse_arakoon():
        loghandler.logger.info('Starting arakoon collapse')
        arakoon_dir = os.path.join(Configuration.get('ovs.core.cfgdir'), 'arakoon')
        arakoon_clusters = map(lambda directory: os.path.basename(directory.rstrip(os.path.sep)),
                               os.walk(arakoon_dir).next()[1])
        for cluster in arakoon_clusters:
            loghandler.logger.info('  Collapsing cluster: {}'.format(cluster))
            cluster_instance = ArakoonManagement().getCluster(cluster)
            for node in cluster_instance.listNodes():
                loghandler.logger.info('    Collapsing node: {}'.format(node))
                try:
                    cluster_instance.remoteCollapse(node, 2)  # Keep 2 tlogs
                except Exception as e:
                    loghandler.logger.info(
                        'Error during collapsing cluster {} node {}: {}\n{}'.format(
                            cluster, node, str(e), traceback.format_exc()
                        )
                    )
        loghandler.logger.info('Arakoon collapse finished')
