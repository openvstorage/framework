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
StorageDriver module
"""

import copy
import time
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation, Dynamic
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storageserver.storagedriver import StorageDriverClient, StorageDriverConfiguration


class StorageDriver(DataObject):
    """
    The StorageDriver class represents a Storage Driver. A Storage Driver is an application
    on a Storage Router to which the vDisks connect. The Storage Driver is the gateway to the Storage Backend.
    """
    DISTANCES = DataObject.enumerator('Distance', {'NEAR': 0, 'FAR': 10000, 'INFINITE': 20000})

    __properties = [Property('name', str, doc='Name of the Storage Driver.'),
                    Property('description', str, mandatory=False, doc='Description of the Storage Driver.'),
                    Property('ports', dict, doc='Ports on which the Storage Driver is listening (management, xmlrpc, dtl, edge).'),
                    Property('cluster_ip', str, doc='IP address on which the Storage Driver is listening.'),
                    Property('storage_ip', str, doc='IP address on which the vpool is shared to hypervisor'),
                    Property('storagedriver_id', str, unique=True, indexed=True, doc='ID of the Storage Driver as known by the Storage Drivers.'),
                    Property('mountpoint', str, doc='Mountpoint from which the Storage Driver serves data'),
                    Property('startup_counter', int, default=0, doc='StorageDriver startup counter')]
    __relations = [Relation('vpool', VPool, 'storagedrivers'),
                   Relation('storagerouter', StorageRouter, 'storagedrivers')]
    __dynamics = [Dynamic('status', str, 30),
                  Dynamic('statistics', dict, 4),
                  Dynamic('edge_clients', list, 30),
                  Dynamic('vdisks_guids', list, 15),
                  Dynamic('proxy_summary', dict, 15),
                  Dynamic('vpool_backend_info', dict, 60),
                  Dynamic('cluster_node_config', dict, 3600),
                  Dynamic('global_write_buffer', int, 60)]

    def _status(self):
        """
        Fetches the Status of the Storage Driver.
        """
        _ = self
        return None

    def _statistics(self, dynamic):
        """
        Aggregates the Statistics (IOPS, Bandwidth, ...) of the vDisks connected to the Storage Driver.
        """
        from ovs.dal.hybrids.vdisk import VDisk
        statistics = {}
        for key, value in self.fetch_statistics().iteritems():
            statistics[key] = value
        statistics['timestamp'] = time.time()
        VDisk.calculate_delta(self._key, dynamic, statistics)
        return statistics

    def _edge_clients(self):
        """
        Retrieves all edge clients
        """
        clients = []
        try:
            for item in self.vpool.storagedriver_client.list_client_connections(str(self.storagedriver_id), req_timeout_secs=2):
                clients.append({'key': '{0}:{1}'.format(item.ip, item.port),
                                'object_id': item.object_id,
                                'ip': item.ip,
                                'port': item.port,
                                'server_ip': self.storage_ip,
                                'server_port': self.ports['edge']})
        except Exception:
            StorageDriver._logger.exception('Error loading edge clients from {0}'.format(self.storagedriver_id))
        clients.sort(key=lambda e: (e['ip'], e['port']))
        return clients

    def _vdisks_guids(self):
        """
        Gets the vDisk guids served by this StorageDriver.
        """
        from ovs.dal.lists.vdisklist import VDiskList
        volume_ids = []
        for entry in self.vpool.objectregistry_client.get_all_registrations():
            if entry.node_id() == self.storagedriver_id:
                volume_ids.append(entry.object_id())
        return VDiskList.get_in_volume_ids(volume_ids).guids

    def fetch_statistics(self):
        """
        Loads statistics from this vDisk - returns unprocessed data
        """
        # Load data from volumedriver
        sdstats = StorageDriverClient.EMPTY_STATISTICS()
        if self.storagedriver_id and self.vpool:
            try:
                sdstats = self.vpool.storagedriver_client.statistics_node(str(self.storagedriver_id), req_timeout_secs=2)
            except Exception as ex:
                StorageDriver._logger.error('Error loading statistics_node from {0}: {1}'.format(self.storagedriver_id, ex))
        # Load volumedriver data in dictionary
        return VDisk.extract_statistics(sdstats, None if len(self.vpool.vdisks) == 0 else self.vpool.vdisks[0])

    def _vpool_backend_info(self):
        """
        Retrieve some additional information about the vPool to be shown in the GUI
        Size of the global write buffer for this Storage Driver, the accelerated backend info, connection info and caching strategy
        :return: Information about vPool and accelerated Backend
        :rtype: dict
        """
        vpool_backend_info = {'backend': copy.deepcopy(self.vpool.metadata['backend']),
                              'caching_info': {StorageDriverConfiguration.CACHE_BLOCK: {'read': False,
                                                                                        'write': False,
                                                                                        'quota': None,
                                                                                        'backend_info': None},  # Will contain connection info if it wouldn't be None
                                               StorageDriverConfiguration.CACHE_FRAGMENT: {'read': False,
                                                                                           'write': False,
                                                                                           'quota': None,
                                                                                           'backend_info': None}}}
        if 'caching_info' not in self.vpool.metadata:
            self._logger.critical('Metadata structure has not been updated yet')
            return vpool_backend_info
        if self.storagerouter_guid not in self.vpool.metadata['caching_info']:
            # No caching configured
            return vpool_backend_info
        for cache_type, cache_data in vpool_backend_info['caching_info'].iteritems():
            caching_info = self.vpool.metadata['caching_info'][self.storagerouter_guid][cache_type]
            # Update the cache data matching the keys currently specified in cache_data
            cache_data.update((k, caching_info[k]) for k in cache_data.viewkeys() & caching_info.viewkeys())
            # Possible set backend_info to None to match this view
            if caching_info['is_backend'] is False:
                cache_data['backend_info'] = None
        # Add global write buffer
        vpool_backend_info['global_write_buffer'] = self.global_write_buffer
        return vpool_backend_info

    def _cluster_node_config(self):
        """
        Prepares a ClusterNodeConfig dict for the StorageDriver process
        """
        from ovs.extensions.generic.configuration import Configuration
        rdma = Configuration.get('/ovs/framework/rdma')
        distance_map = {}
        primary_domains = []
        secondary_domains = []
        for junction in self.storagerouter.domains:
            if junction.backup is False:
                primary_domains.append(junction.domain_guid)
            else:
                secondary_domains.append(junction.domain_guid)
        for sd in self.vpool.storagedrivers:
            if sd.guid == self.guid:
                continue
            if len(primary_domains) == 0:
                distance_map[str(sd.storagedriver_id)] = StorageDriver.DISTANCES.NEAR
            else:
                distance = StorageDriver.DISTANCES.INFINITE
                for junction in sd.storagerouter.domains:
                    if junction.backup is False:
                        if junction.domain_guid in primary_domains:
                            distance = min(distance, StorageDriver.DISTANCES.NEAR)
                            break  # We can break here since we reached the minimum distance
                        elif junction.domain_guid in secondary_domains:
                            distance = min(distance, StorageDriver.DISTANCES.FAR)
                distance_map[str(sd.storagedriver_id)] = distance
        return {'vrouter_id': self.storagedriver_id,
                'host': self.storage_ip,
                'message_port': self.ports['management'],
                'xmlrpc_host': self.cluster_ip,
                'xmlrpc_port': self.ports['xmlrpc'],
                'failovercache_host': self.storage_ip,
                'failovercache_port': self.ports['dtl'],
                'network_server_uri': '{0}://{1}:{2}'.format('rdma' if rdma else 'tcp',
                                                             self.storage_ip,
                                                             self.ports['edge']),
                'node_distance_map': distance_map}

    def _proxy_summary(self):
        """
        Returns a summary of the proxies of this StorageDriver
        :return: summary of the proxies
        :rtype: dict
        """
        proxy_info = {'red': 0,
                      'orange': 0,
                      'green': 0}
        summary = {'proxies': proxy_info}

        try:
            service_manager = ServiceFactory.get_manager()
            client = SSHClient(self.storagerouter)
        except Exception:
            self._logger.exception('Unable to retrieve necessary clients')
        else:
            for alba_proxy in self.alba_proxies:
                try:
                    service_status = service_manager.get_service_status(alba_proxy.service.name, client)
                except Exception:
                    # A ValueError can occur when the services are still being deployed (the model will be updated before the actual deployment)
                    self._logger.exception('Unable to retrieve the service status for service {0} of StorageDriver {1}'.format(alba_proxy.service.name, self.guid))
                    proxy_info['red'] += 1
                    continue
                if service_status == 'active':
                    proxy_info['green'] += 1
                elif service_status == 'inactive':
                    proxy_info['orange'] += 1
                else:
                    proxy_info['red'] += 1
        finally:
            return summary

    def _global_write_buffer(self):
        """
        Return the global write buffer for available for a StorageDriver
        :return: Calculated global write buffer
        :rtype: int
        """
        # Avoid circular import
        from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition

        global_write_buffer = 0
        for partition in self.partitions:
            if partition.role == DiskPartition.ROLES.WRITE and partition.sub_role == StorageDriverPartition.SUBROLE.SCO:
                global_write_buffer += partition.size
        return global_write_buffer
