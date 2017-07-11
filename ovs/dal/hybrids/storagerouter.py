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
StorageRouter module
"""

import re
import time
from distutils.version import LooseVersion
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Dynamic, Property
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.log.log_handler import LogHandler


class StorageRouter(DataObject):
    """
    A StorageRouter represents the Open vStorage software stack, any (v)machine on which it is installed
    """
    _logger = LogHandler.get('dal', name='hybrid')

    __properties = [Property('name', str, unique=True, doc='Name of the Storage Router.'),
                    Property('description', str, mandatory=False, doc='Description of the Storage Router.'),
                    Property('machine_id', str, unique=True, mandatory=False, indexed=True, doc='The hardware identifier of the Storage Router'),
                    Property('ip', str, unique=True, indexed=True, doc='IP Address of the Storage Router, if available'),
                    Property('heartbeats', dict, default={}, doc='Heartbeat information of various monitors'),
                    Property('node_type', ['MASTER', 'EXTRA'], default='EXTRA', doc='Indicates the node\'s type'),
                    Property('rdma_capable', bool, doc='Is this Storage Router RDMA capable'),
                    Property('last_heartbeat', float, mandatory=False, doc='When was the last (external) heartbeat send/received'),
                    Property('package_information', dict, mandatory=False, default={}, doc='Information about installed packages and potential available new versions')]
    __relations = []
    __dynamics = [Dynamic('statistics', dict, 4),
                  Dynamic('vpools_guids', list, 15),
                  Dynamic('vdisks_guids', list, 15),
                  Dynamic('status', str, 10),
                  Dynamic('partition_config', dict, 3600),
                  Dynamic('regular_domains', list, 60),
                  Dynamic('recovery_domains', list, 60),
                  Dynamic('features', dict, 3600)]

    def _statistics(self, dynamic):
        """
        Aggregates the Statistics (IOPS, Bandwidth, ...) of each vDisk.
        """
        from ovs.dal.hybrids.vdisk import VDisk
        statistics = {}
        for storagedriver in self.storagedrivers:
            for key, value in storagedriver.fetch_statistics().iteritems():
                if isinstance(value, dict):
                    if key not in statistics:
                        statistics[key] = {}
                        for subkey, subvalue in value.iteritems():
                            if subkey not in statistics[key]:
                                statistics[key][subkey] = 0
                            statistics[key][subkey] += subvalue
                else:
                    if key not in statistics:
                        statistics[key] = 0
                    statistics[key] += value
        statistics['timestamp'] = time.time()
        VDisk.calculate_delta(self._key, dynamic, statistics)
        return statistics

    def _vdisks_guids(self):
        """
        Gets the vDisk guids served by this StorageRouter.
        """
        from ovs.dal.lists.vdisklist import VDiskList
        volume_ids = []
        vpools = set()
        storagedriver_ids = []
        for storagedriver in self.storagedrivers:
            vpools.add(storagedriver.vpool)
            storagedriver_ids.append(storagedriver.storagedriver_id)
        for vpool in vpools:
            for entry in vpool.objectregistry_client.get_all_registrations():
                if entry.node_id() in storagedriver_ids:
                    volume_ids.append(entry.object_id())
        return VDiskList.get_in_volume_ids(volume_ids).guids

    def _vpools_guids(self):
        """
        Gets the vPool guids linked to this StorageRouter (trough StorageDriver)
        """
        vpool_guids = set()
        for storagedriver in self.storagedrivers:
            vpool_guids.add(storagedriver.vpool_guid)
        return list(vpool_guids)

    def _status(self):
        """
        Calculates the current Storage Router status based on various heartbeats
        """
        pointer = 0
        statusses = ['OK', 'WARNING', 'FAILURE']
        current_time = time.time()
        if self.heartbeats is not None:
            process_delay = abs(self.heartbeats.get('process', 0) - current_time)
            if process_delay > 60 * 5:
                pointer = max(pointer, 2)
            else:
                delay = abs(self.heartbeats.get('celery', 0) - current_time)
                if delay > 60 * 5:
                    pointer = max(pointer, 2)
                elif delay > 60 * 2:
                    pointer = max(pointer, 1)
        for disk in self.disks:
            if disk.state == 'MISSING':
                pointer = max(pointer, 2)
            for partition in disk.partitions:
                if partition.state == 'MISSING':
                    pointer = max(pointer, 2)
        return statusses[pointer]

    def _partition_config(self):
        """
        Returns a dict with all partition information of a given storagerouter
        """
        from ovs.dal.hybrids.diskpartition import DiskPartition
        dataset = dict((role, []) for role in DiskPartition.ROLES)
        for disk in self.disks:
            for partition in disk.partitions:
                for role in partition.roles:
                    dataset[role].append(partition.guid)
        return dataset

    def _regular_domains(self):
        """
        Returns a list of domain guids with backup flag False
        :return: List of domain guids
        """
        return [junction.domain_guid for junction in self.domains if junction.backup is False]

    def _recovery_domains(self):
        """
        Returns a list of domain guids with backup flag True
        :return: List of domain guids
        """
        return [junction.domain_guid for junction in self.domains if junction.backup is True]

    def _features(self):
        """
        Returns information about installed/available features
        :return: Dictionary containing edition and available features per component
        """
        try:
            enterprise = 'enterprise'
            community = 'community'
            client = SSHClient(self, username='root')
            enterprise_regex = re.compile('^(?P<edition>ee-)?(?P<version>.*)$')

            version = client.run("volumedriver_fs --version | grep version: | awk '{print $2}'", allow_insecure=True, allow_nonzero=True)
            volumedriver_version = enterprise_regex.match(version).groupdict()
            volumedriver_edition = enterprise if volumedriver_version['edition'] == 'ee-' else community
            volumedriver_version_lv = LooseVersion(volumedriver_version['version'])
            volumedriver_features = [feature for feature, version
                                     in {'directory_unlink': ('6.15.0', None)}.iteritems()
                                     if volumedriver_version_lv >= LooseVersion(version[0])
                                     and (version[1] is None or version[1] == volumedriver_edition)]

            version = client.run("alba version --terse", allow_insecure=True, allow_nonzero=True)
            alba_version = enterprise_regex.match(version).groupdict()
            alba_edition = enterprise if alba_version['edition'] == 'ee-' else community
            alba_version_lv = LooseVersion(alba_version['version'])
            alba_features = [feature for feature, version
                             in {'cache-quota': ('1.4.4', enterprise),
                                 'block-cache': ('1.4.0', enterprise)}.iteritems()
                             if alba_version_lv >= LooseVersion(version[0])
                             and (version[1] is None or version[1] == alba_edition)]

            return {'volumedriver': {'edition': volumedriver_edition,
                                     'features': volumedriver_features},
                    'alba': {'edition': alba_edition,
                             'features': alba_features}}
        except UnableToConnectException:
            pass
        except Exception:
            StorageRouter._logger.exception('Could not load feature information')
        return None
