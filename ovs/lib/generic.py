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
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.sshclient import NotAuthenticatedException, SSHClient, UnableToConnectException
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.packages.packagefactory import PackageFactory
from ovs.lib.helpers.decorators import ovs_task
from ovs.lib.helpers.generic.scrubber import Scrubber
from ovs.lib.helpers.toolbox import Toolbox, Schedule
from ovs.lib.helpers.storagedriver.installer import StorageDriverInstaller
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
    @ovs_task(name='ovs.generic.refresh_package_information', schedule=Schedule(minute='10', hour='*'), ensure_single_info={'mode': 'DEFAULT'})
    def refresh_package_information():
        """
        Retrieve and store the package information of all StorageRouters
        :return: None
        """
        GenericController._logger.info('Updating package information')

        client_map = {}
        prerequisites = []
        package_info_cluster = {}
        all_storagerouters = StorageRouterList.get_storagerouters()
        all_storagerouters.sort(key=lambda sr: ExtensionsToolbox.advanced_sort(element=sr.ip, separator='.'))
        for storagerouter in all_storagerouters:
            package_info_cluster[storagerouter.ip] = {}
            try:
                # We make use of these clients in Threads --> cached = False
                client_map[storagerouter] = SSHClient(endpoint=storagerouter, username='root', cached=False)
            except (NotAuthenticatedException, UnableToConnectException):
                GenericController._logger.warning('StorageRouter {0} is inaccessible'.format(storagerouter.ip))
                prerequisites.append(['node_down', storagerouter.name])
                package_info_cluster[storagerouter.ip]['errors'] = ['StorageRouter {0} is inaccessible'.format(storagerouter.name)]

        # Retrieve for each StorageRouter in the cluster the installed and candidate versions of related packages
        # This also validates whether all required packages have been installed
        GenericController._logger.debug('Retrieving package information for the cluster')
        threads = []
        for storagerouter, client in client_map.iteritems():
            for fct in Toolbox.fetch_hooks(component='update', sub_component='get_package_update_info_cluster'):
                thread = Thread(target=fct, args=(client, package_info_cluster))
                thread.start()
                threads.append(thread)

        for thread in threads:
            thread.join()

        # Retrieve the related downtime / service restart information
        GenericController._logger.debug('Retrieving update information for the cluster')
        update_info_cluster = {}
        for storagerouter, client in client_map.iteritems():
            update_info_cluster[storagerouter.ip] = {'errors': package_info_cluster[storagerouter.ip].get('errors', [])}
            for fct in Toolbox.fetch_hooks(component='update', sub_component='get_update_info_cluster'):
                fct(client, update_info_cluster, package_info_cluster[storagerouter.ip])

        # Retrieve the update information for plugins (eg: ALBA, iSCSI)
        GenericController._logger.debug('Retrieving package and update information for the plugins')
        threads = []
        update_info_plugin = {}
        for fct in Toolbox.fetch_hooks('update', 'get_update_info_plugin'):
            thread = Thread(target=fct, args=(update_info_plugin, ))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        # Add the prerequisites
        if len(prerequisites) > 0:
            for ip, component_info in update_info_cluster.iteritems():
                if PackageFactory.COMP_FWK in component_info:
                    component_info[PackageFactory.COMP_FWK]['prerequisites'].extend(prerequisites)

        # Store information in model and collect errors for OVS cluster
        errors = set()
        for storagerouter in all_storagerouters:
            GenericController._logger.debug('Storing update information for StorageRouter {0}'.format(storagerouter.ip))
            update_info = update_info_cluster.get(storagerouter.ip, {})

            # Remove the errors from the update information
            sr_errors = update_info.pop('errors', [])
            if len(sr_errors) > 0:
                errors.update(['{0}: {1}'.format(storagerouter.ip, error) for error in sr_errors])
                update_info = {}  # If any error occurred, we store no update information for this StorageRouter

            # Remove the components without updates from the update information
            update_info_copy = copy.deepcopy(update_info)
            for component, info in update_info_copy.iteritems():
                if len(info['packages']) == 0:
                    update_info.pop(component)

            # Store the update information
            storagerouter.package_information = update_info
            storagerouter.save()

        # Collect errors for plugins
        for ip, plugin_errors in update_info_plugin.iteritems():
            if len(plugin_errors) > 0:
                errors.update(['{0}: {1}'.format(ip, error) for error in plugin_errors])

        if len(errors) > 0:
            raise Exception('\n - {0}'.format('\n - '.join(errors)))
        GenericController._logger.info('Finished updating package information')

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
