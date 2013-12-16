# license see http://www.openvstorage.com/licenses/opensource/

"""
ScheduledTaskController module
"""

from celery import group
import copy
import time
from time import mktime
from datetime import datetime

from ovs.celery import celery
from ovs.celery import loghandler
from ovs.lib.vmachine import VMachineController
from ovs.lib.vdisk import VDiskController
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.lists.vdisklist import VDiskList


class ScheduledTaskController(object):
    """
    This controller contains all scheduled task code. These tasks can be
    executed at certain intervals and should be self-containing
    """

    @celery.task(name='ovs.scheduled.snapshotall')
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
    @celery.task(name='ovs.scheduled.deletesnapshots')
    def deletesnapshots(timestamp=None, debug=False):
        """
        Delete snapshots policy

        Implemented policy:
        < 1d | 1d bucket | 1 | best of bucket   | 1d
        < 1w | 1d bucket | 6 | oldest of bucket | 7d = 1w
        < 1m | 1w bucket | 3 | oldest of bucket | 4w = 1m
        > 1m | delete
        """

        loghandler.logger.info('[DS] started')

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

        if debug:
            print '=============================================='
            print 'original list'
            for bucket_chain in bucket_chains:
                print '=============================================='
                for bucket in bucket_chain:
                    print '{} - {} ({}): {}'.format(
                        datetime.fromtimestamp(bucket['start']).strftime('%Y-%m-%d'),
                        datetime.fromtimestamp(bucket['end']).strftime('%Y-%m-%d'),
                        bucket['type'],
                        ', '.join([str(s['timestamp']) + '[{}]'.format('S' if s['is_consistent'] else ' ') for s in bucket['snapshots']])
                    )

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

        if debug:
            print '=============================================='
            print 'cleaned list'
            for bucket_chain in bucket_chains:
                print '=============================================='
                for bucket in bucket_chain:
                    print '{} - {} ({}): {}'.format(
                        datetime.fromtimestamp(bucket['start']).strftime('%Y-%m-%d'),
                        datetime.fromtimestamp(bucket['end']).strftime('%Y-%m-%d'),
                        bucket['type'],
                        ', '.join([str(s['timestamp']) + '[{}]'.format('S' if s['is_consistent'] else ' ') for s in bucket['snapshots']])
                    )

        # Delete obsolete snapshots
        for bucket_chain in bucket_chains:
            for bucket in bucket_chain:
                for snapshot in bucket['snapshots']:
                    VDiskController.delete_snapshot(diskguid=snapshot['diskguid'],
                                                    snapshotid=snapshot['snapshotid'])
