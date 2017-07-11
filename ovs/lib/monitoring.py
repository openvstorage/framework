# Copyright (C) 2017 iNuron NV
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
MonitoringController module
"""
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs_extensions.api.client import OVSClient
from ovs_extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.lib.helpers.decorators import ovs_task
from ovs.lib.helpers.toolbox import Schedule
from ovs.log.log_handler import LogHandler


class MonitoringController(object):
    """
    A controller that can execute various quality/monitoring checks
    """
    _logger = LogHandler.get('lib', name='ovs-monitoring')

    @staticmethod
    def test_ssh_connectivity():
        """
        Validates whether all nodes can SSH into eachother
        """
        MonitoringController._logger.info('Starting SSH connectivity test...')
        ips = [sr.ip for sr in StorageRouterList.get_storagerouters()]
        for ip in ips:
            for primary_username in ['root', 'ovs']:
                try:
                    with remote(ip, [SSHClient], username=primary_username) as rem:
                        for local_ip in ips:
                            for username in ['root', 'ovs']:
                                message = '* Connection from {0}@{1} to {2}@{3}... {{0}}'.format(primary_username, ip, username, local_ip)
                                try:
                                    c = rem.SSHClient(local_ip, username=username)
                                    assert c.run(['whoami']).strip() == username
                                    message = message.format('OK')
                                    logger = MonitoringController._logger.info
                                except Exception as ex:
                                    message = message.format(ex.message)
                                    logger = MonitoringController._logger.error
                                logger(message)
                except Exception as ex:
                    MonitoringController._logger.error('* Could not connect to {0}@{1}: {2}'.format(primary_username, ip, ex.message))
        MonitoringController._logger.info('Finished')

    @staticmethod
    @ovs_task(name='ovs.monitoring.verify_vdisk_cache_quota', schedule=Schedule(minute='15', hour='*'), ensure_single_info={'mode': 'DEFAULT'})
    def verify_vdisk_cache_quota():
        """
        Validates whether the caching quota is reaching its limits or has surpassed it
        Each vDisk can consume a part of the total fragment caching capacity
        """
        MonitoringController._logger.info('Starting vDisk caching quota verification...')
        alba_guid_size_map = {}
        for storagedriver in StorageDriverList.get_storagedrivers():
            storagedriver.invalidate_dynamics(['vpool_backend_info', 'vdisks_guids'])

            for cache_type in [['cache_quota_fc', 'backend_info', 'connection_info', 'fragment'],
                               ['cache_quota_bc', 'block_cache_backend_info', 'block_cache_connection_info', 'block']]:
                cache_quota = storagedriver.vpool_backend_info[cache_type[0]]
                backend_info = storagedriver.vpool_backend_info[cache_type[1]]
                connection_info = storagedriver.vpool_backend_info[cache_type[2]]
                if backend_info is None or connection_info is None:
                    continue

                alba_backend_name = backend_info['name']
                alba_backend_host = connection_info['host']
                alba_backend_guid = backend_info['alba_backend_guid']
                if alba_backend_guid not in alba_guid_size_map:
                    ovs_client = OVSClient(ip=alba_backend_host,
                                           port=connection_info['port'],
                                           credentials=(connection_info['client_id'], connection_info['client_secret']),
                                           version=2,
                                           cache_store=VolatileFactory.get_client())
                    try:
                        alba_guid_size_map[alba_backend_guid] = {'name': alba_backend_name,
                                                                 'backend_ip': alba_backend_host,
                                                                 'total_size': ovs_client.get('/alba/backends/{0}/'.format(alba_backend_guid), params={'contents': 'usages'})['usages']['size'],
                                                                 'requested_size': 0}
                    except Exception:
                        MonitoringController._logger.exception('Failed to retrieve ALBA Backend info for {0} on host {1}'.format(alba_backend_name, alba_backend_host))
                        continue

                for vdisk_guid in storagedriver.vdisks_guids:
                    vdisk = VDisk(vdisk_guid)
                    vdisk_cq = vdisk.cache_quota.get(cache_type[3]) if vdisk.cache_quota is not None else None
                    if vdisk_cq is None:
                        alba_guid_size_map[alba_backend_guid]['requested_size'] += cache_quota if cache_quota is not None else 0
                    else:
                        alba_guid_size_map[alba_backend_guid]['requested_size'] += vdisk_cq

        local_ips = [sr.ip for sr in StorageRouterList.get_storagerouters()]
        for alba_backend_info in alba_guid_size_map.itervalues():
            name = alba_backend_info['name']
            backend_ip = alba_backend_info['backend_ip']

            location = 'local'
            remote_msg = ''
            if backend_ip not in local_ips:
                location = 'remote'
                remote_msg = ' (on remote IP {0})'.format(backend_ip)

            percentage = alba_backend_info['requested_size'] / alba_backend_info['total_size'] * 100
            if percentage > 100:
                MonitoringController._logger.error('OVS_WARNING: Over-allocation for vDisk caching quota on {0} ALBA Backend {1}{2}. Unexpected behavior might occur'.format(location, name, remote_msg))
            elif percentage > 70:
                MonitoringController._logger.warning('OVS_WARNING: vDisk caching quota on {0} ALBA Backend {1} is at {2:.1f}%{3}'.format(location, name, percentage, remote_msg))
        MonitoringController._logger.info('Finished vDisk cache quota verification')
