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
import time
from datetime import datetime, timedelta
from threading import Thread
from time import mktime
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.lists.servicelist import ServiceList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.lib.helpers.decorators import ovs_task
from ovs.lib.helpers.generic.scrubber import Scrubber
from ovs.lib.helpers.toolbox import Toolbox, Schedule
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
        day_timedelta = timedelta(1)

        class Snapshot(object):
            def __init__(self, timestamp, snapshot_id, vdisk_guid, is_consistent):
                self.timestamp = timestamp
                self.snapshot_id = snapshot_id
                self.vdisk_guid = vdisk_guid
                self.consistent = is_consistent

            def __str__(self):
                return 'Snapshot for vDisk {0}'.format(self.vdisk_guid)

        class Bucket(object):
            def __init__(self, start, end, type=None):
                self.start = start
                self.end = end
                self.type = type or ''
                self.snapshots = []

            def __str__(self):
                return 'Bucket (start: {0}, end: {1}, type: {2}) with {3}'.format(self.start, self.end, self.type, self.snapshots)

        def make_timestamp(offset):
            """
            Create an integer based timestamp
            :param offset: Offset in days
            :return: Timestamp
            """
            return int(mktime((base - offset).timetuple()))

        def _calculate_bucket_structure_for_vdisk(vdisk_path):
            """
            Path in configuration management where the config is located for deleting snapshots of given vdisk path.
            Located in ovs/framework/scheduling/retention_policy/{0}.format(vdisk_guid).
            Should look like this:
            [{'nr_of_snapshots': 24, 'nr_of_days': 1},
            {'nr_of_snapshots': 6,  'nr_of_days': 6},
            {'nr_of_snapshots': 3,  'nr_of_days': 21}])
            More periods in time can be given.
            The passed number of snapshots is an absolute number of snapshots and is evenly distributed across the number of days passed in the interval.
            This way, this config will result in storing
            one snapshot per hour the first day
            one snapshot per day the rest of the week
            one snapshot per week the rest of the month
            one older snapshot snapshot will always be stored for an interval older then the longest interval passed in the config
            :param vdisk_path: ovs/framework/scheduling/retention_policy/{0}.format(vdisk_guid)
            :return:
            """
            buckets = []
            policies = Configuration.get(vdisk_path, default=[{'nr_of_snapshots': 24, 'nr_of_days': 1},    # One per hour
                                                              {'nr_of_snapshots': 6,  'nr_of_days': 6},    # one per day for rest of the week
                                                              {'nr_of_snapshots': 3,  'nr_of_days': 21}])  # one per week for the rest of the month

            total_length = 0
            offset = total_length * day_timedelta
            for policy in policies:
                number_of_days = policy.get('nr_of_days', 1)
                number_of_snapshots = policy.get('nr_of_snapshots', number_of_days * 24)
                snapshot_timedelta = number_of_days * day_timedelta / number_of_snapshots
                for i in xrange(0, number_of_snapshots):
                    buckets.append(Bucket(start=make_timestamp(offset + snapshot_timedelta * i), end=make_timestamp(offset + snapshot_timedelta * (i + 1))))
                total_length += number_of_days
                offset = total_length * day_timedelta
            buckets.append(Bucket(start=make_timestamp(total_length * day_timedelta), end=0, type='rest'))
            return buckets

        if timestamp is None:
            timestamp = time.time()
        base = datetime.fromtimestamp(timestamp).date() - day_timedelta

        # Get a list of all snapshots that are used as parents for clones
        parent_snapshots = set([vd.parentsnapshot for vd in VDiskList.get_with_parent_snaphots()])

        # Place all snapshots in bucket_chains
        bucket_chains = []
        for vdisk in VDiskList.get_vdisks():
            path = 'ovs/framework/scheduling/retention_policy/{0}'.format(vdisk.guid)
            buckets = _calculate_bucket_structure_for_vdisk(path)

            if vdisk.info['object_type'] in ['BASE']:
                bucket_chain = copy.deepcopy(buckets)
                for vdisk_snapshot in vdisk.snapshots:  # type: Dict
                    if vdisk_snapshot.get('is_sticky'):
                        continue
                    if vdisk_snapshot['guid'] in parent_snapshots:
                        GenericController._logger.info('Not deleting snapshot {0} because it has clones'.format(vdisk_snapshot['guid']))
                        continue
                    timestamp = int(vdisk_snapshot['timestamp'])
                    for bucket in bucket_chain:
                        if bucket.start >= timestamp > bucket.end:
                            bucket.snapshots.append(Snapshot(timestamp, vdisk_snapshot['guid'], vdisk.guid, vdisk_snapshot['is_consistent']))
                bucket_chains.append(bucket_chain)

        # Clean out the snapshot bucket_chains, we delete the snapshots we want to keep
        # And we'll remove all snapshots that remain in the buckets
        for bucket_chain in bucket_chains:
            first = True
            for bucket in bucket_chain:
                if first is True:
                    best = None
                    for snapshot in bucket.snapshots:
                        if best is None:
                            best = snapshot
                        # Consistent is better than inconsistent
                        elif snapshot.consistent and not best.consistent:
                            best = snapshot
                        # Newer (larger timestamp) is better than older snapshots
                        elif snapshot.consistent == best.consistent and snapshot.timestamp > best.timestamp:
                            best = snapshot
                    bucket.snapshots = [s for s in bucket.snapshots if s.timestamp != best.timestamp]
                    first = False
                elif bucket.end > 0:
                    oldest = None
                    for snapshot in bucket.snapshots:
                        if oldest is None:
                            oldest = snapshot
                        # Older (smaller timestamp) is the one we want to keep
                        elif snapshot.timestamp < oldest.timestamp:
                            oldest = snapshot
                    bucket.snapshots = [s for s in bucket.snapshots if s.timestamp != oldest.timestamp]

        # Delete obsolete snapshots
        for bucket_chain in bucket_chains:
            for bucket in bucket_chain:
                for snapshot in bucket.snapshots:
                    VDiskController.delete_snapshot(vdisk_guid=snapshot.vdisk_guid,
                                                    snapshot_id=snapshot.snapshot_id)
        GenericController._logger.info('Delete snapshots finished')



    @staticmethod
    @ovs_task(name='ovs.generic.execute_scrub', schedule=Schedule(minute='0', hour='3'), ensure_single_info={'mode': 'DEDUPED'})
    def execute_scrub(vpool_guids=None, vdisk_guids=None, storagerouter_guid=None, manual=False):
        """
        Divide the scrub work among all StorageRouters with a SCRUB partition
        :param vpool_guids: Guids of the vPools that need to be scrubbed completely
        :type vpool_guids: list
        :param vdisk_guids: Guids of the vDisks that need to be scrubbed
        :type vdisk_guids: list
        :param storagerouter_guid: Guid of the StorageRouter to execute the scrub work on
        :type storagerouter_guid: str
        :param manual: Indicator whether the execute_scrub is called manually or as scheduled task (automatically)
        :type manual: bool
        :return: None
        :rtype: NoneType
        """
        # GenericController.execute_scrub.request.id gets the current celery task id (None if executed directly)
        # Fetching the task_id with the hasattr because Unit testing does not execute the wrapper (No celery task but a normal function being called)
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            task_id = 'unittest'
        else:
            task_id = GenericController.execute_scrub.request.id if hasattr(GenericController.execute_scrub, 'request') else None
        scrubber = Scrubber(vpool_guids, vdisk_guids, storagerouter_guid, manual=manual, task_id=task_id)
        return scrubber.execute_scrubbing()

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
