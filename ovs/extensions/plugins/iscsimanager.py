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
Generic module for calling the iSCSI Manager
"""
import os
import json
import time
import base64
import inspect
import logging
import requests
from ovs.log.log_handler import LogHandler
try:
    from requests.packages.urllib3 import disable_warnings
except ImportError:
    try:
        reload(requests)  # Required for 2.6 > 2.7 upgrade (new requests.packages module)
    except ImportError:
        pass  # So, this reload fails because of some FileNodeWarning that can't be found. But it did reload. Yay.
    from requests.packages.urllib3 import disable_warnings
from requests.packages.urllib3.exceptions import InsecurePlatformWarning
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.packages.urllib3.exceptions import SNIMissingWarning
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)


class InvalidCredentialsError(RuntimeError):
    """
    Invalid credentials error
    """
    pass


class NotFoundError(RuntimeError):
    """
    Method not found error
    """
    pass


class ISCSIManagerClient(object):
    """
    iSCSI Manager Client
    """

    disable_warnings(InsecurePlatformWarning)
    disable_warnings(InsecureRequestWarning)
    disable_warnings(SNIMissingWarning)

    test_results = {}
    test_exceptions = {}

    def __init__(self, node):
        self._logger = LogHandler.get('extensions', name='iscsi-manager-client')
        self.node = node
        self.timeout = 10
        self._unittest_mode = os.environ.get('RUNNING_UNITTESTS') == 'True'
        self._log_min_duration = 1

    def _call(self, method, url, data=None, timeout=None, clean=False):
        if self._unittest_mode is True:
            curframe = inspect.currentframe()
            calframe = inspect.getouterframes(curframe, 2)
            exception = ISCSIManagerClient.test_exceptions.get(self.node, {}).get(calframe[1][3])
            if exception is not None:
                raise exception
            return ISCSIManagerClient.test_results[self.node][calframe[1][3]]

        if timeout is None:
            timeout = self.timeout

        # Refresh
        self._base_url = 'https://{0}:{1}'.format(self.node.ip, self.node.port)
        self._base_headers = {'Authorization': 'Basic {0}'.format(base64.b64encode('{0}:{1}'.format(self.node.username, self.node.password)).strip())}

        start = time.time()
        kwargs = {'url': '{0}/{1}'.format(self._base_url, url),
                  'headers': self._base_headers,
                  'verify': False,
                  'timeout': timeout}
        if data is not None:
            kwargs['data'] = data
        response = method(**kwargs)
        if response.status_code == 404:
            raise NotFoundError('URL not found: {0}'.format(kwargs['url']))
        try:
            data = response.json()
        except:
            raise RuntimeError(response.content)
        internal_duration = data['_duration']
        if data.get('_success', True) is False:
            error_message = data.get('_error', 'Unknown exception: {0}'.format(data))
            if error_message == 'Invalid credentials':
                raise InvalidCredentialsError(error_message)
            raise RuntimeError(error_message)
        if clean is True:
            def _clean(_dict):
                for _key in _dict.keys():
                    if _key.startswith('_'):
                        del _dict[_key]
                    elif isinstance(_dict[_key], dict):
                        _clean(_dict[_key])
            _clean(data)
        duration = time.time() - start
        if duration > self._log_min_duration:
            self._logger.info('Request "{0}" took {1:.2f} seconds (internal duration {2:.2f} seconds)'.format(inspect.stack()[1][3], duration, internal_duration))
        return data

    def get_metadata(self):
        """
        Gets metadata from the node
        :return: The ID of the node
        :rtype: dict
        """
        return self._call(method=requests.get, url='', clean=True)

    def target_list(self):
        """
        List all known targets
        :return: A list of all Targets
        :rtype: dict
        """
        return self._call(method=requests.get, url='targets', clean=True)

    def target_create(self, name):
        """
        Create a target
        :param name: Name to give the new target
        :type name: str
        :return: Information about the created Target
        :rtype: dict
        """
        return self._call(method=requests.post, url='targets', data={'name': name}, clean=True)

    def target_delete(self, target_id, force=False):
        """
        Delete a target
        :param target_id: ID of the target to delete
        :type target_id: int
        :param force: Forcefully remove the Target
        :type force: bool
        :return: Metadata about the performed call
        :rtype: dict
        """
        return self._call(method=requests.delete, url='targets/{0}'.format(target_id), data={'force': force})

    def target_update(self, target_id, **kwargs):
        """
        Update values of the Target
        :param target_id: ID of the Target to update
        :type target_id: int
        :return: Metadata about the performed call
        :rtype: dict
        """
        return self._call(method=requests.patch, url='targets/{0}'.format(target_id), data={'parameters': json.dumps(kwargs)})

    def connection_list(self, target_id):
        """
        List all active connections to the specified Target
        :param target_id: ID of the Target
        :type target_id: int
        :return: Overview of the active connections
        :rtype: dict
        """
        return self._call(method=requests.get, url='targets/{0}/connections'.format(target_id), clean=True)

    def connection_delete(self, target_id, session_id, connection_id):
        """
        Delete an active connection and related ACL (if any, to prevent automatic reconnect)
        :param target_id: ID of the Target
        :type target_id: int
        :param session_id: ID of the session
        :type session_id: int
        :param connection_id: ID of the connection
        :type connection_id: int
        :return: Metadata about the performed call
        :rtype: dict
        """
        return self._call(method=requests.delete, url='targets/{0}/connection/delete'.format(target_id), data={'session_id': session_id, 'connection_id': connection_id})

    def lun_create(self, target_id, vdisk_location):
        """
        Create a LUN on the specified Target
        :param target_id: ID of the Target on which to create the LUN
        :type target_id: int
        :param vdisk_location: Absolute file path of the vDisk to expose
        :type vdisk_location: str
        :return: None
        :rtype: NoneType
        """
        return self._call(method=requests.post, url='targets/{0}/lun'.format(target_id), data={'vdisk_location': vdisk_location}, clean=True)

    def lun_delete(self, target_id, lun_id=1, force=False):
        """
        Delete a LUN from a Target
        :param target_id: ID of the Target on which to remove a LUN
        :type target_id: int
        :param lun_id: ID of the LUN to remove from the Target
        :type lun_id: int
        :param force: Forcefully delete the LUN
        :type force: bool
        :return: None
        :rtype: NoneType
        """
        return self._call(method=requests.delete, url='targets/{0}/lun/{1}'.format(target_id, lun_id), data={'force': force})

    def lun_list(self, target_id):
        """
        List all LUNs of the specified Target
        :param target_id: ID of the Target to retrieve the LUNs from
        :type target_id: int
        :return: Information about the LUNs
        :rtype: dict
        """
        return self._call(method=requests.get, url='targets/{0}/lun'.format(target_id), clean=True)

    def acl_create(self, target_id, ip_range='ALL'):
        """
        Create an ACL on the specified Target
        :param target_id: ID of the Target on which to add the ACL
        :type target_id: int
        :param ip_range: IP or IP range indicating who is allowed access to the Target ('ALL' for anyone)
        :type ip_range: str
        :return: None
        :rtype: NoneType
        """
        return self._call(method=requests.post, url='targets/{0}/acl'.format(target_id), data={'ip_range': ip_range})

    def acl_delete(self, target_id, ip_range='ALL', force=False):
        """
        Delete an ACL from the specified Target
        :param target_id: ID of the Target on which to remove an ACL
        :type target_id: int
        :param ip_range: IP or IP range indicating who is no longer allowed access to the Target ('ALL' for no one)
        :param force: Forcefully delete the ACL
        :type force: bool
        :return: None
        :rtype: NoneType
        """
        return self._call(method=requests.delete, url='targets/{0}/acl/delete'.format(target_id), data={'ip_range': ip_range, 'force': force})

    def acl_list(self, target_id):
        """
        List all ACLs configured on the specified Target
        :param target_id: ID of the Target
        :type target_id: int
        :return: Overview of the configured ACLs
        :rtype: dict
        """
        return self._call(method=requests.get, url='targets/{0}/acl'.format(target_id), clean=True)

    # def get_logs(self):
    #     """
    #     Retrieve the logs from the node
    #     """
    #     return self._call(requests.get, 'collect_logs', timeout=60)
    #
    # def get_disks(self):
    #     """
    #     Gets the node's disk states
    #     """
    #     return self._call(requests.get, 'disks', clean=True)
    #
    # def get_disk(self, disk_id):
    #     """
    #     Gets one of the node's disk's state
    #     :param disk_id: Identifier of the disk
    #     :type disk_id: str
    #     """
    #     return self._call(requests.get, 'disks/{0}'.format(disk_id), clean=True)
    #
    # def add_disk(self, disk_id):
    #     """
    #     Adds a disk
    #     :param disk_id: Identifier of the disk
    #     :type disk_id: str
    #     """
    #     return self._call(requests.post, 'disks/{0}/add'.format(disk_id), timeout=300)
    #
    # def remove_disk(self, disk_id, partition_aliases=None):
    #     """
    #     Removes a disk
    #     :param disk_id: Identifier of the disk
    #     :type disk_id: str
    #     :param partition_aliases: Aliases of the partition of the disk (required for missing disks)
    #     :type partition_aliases: list
    #     """
    #     if partition_aliases is None:
    #         partition_aliases = []
    #     return self._call(requests.post, 'disks/{0}/delete'.format(disk_id), timeout=60, data={'partition_aliases': json.dumps(partition_aliases)})
    #
    # def restart_disk(self, disk_id):
    #     """
    #     Restarts a disk
    #     :param disk_id: Identifier of the disk
    #     :type disk_id: str
    #     """
    #     return self._call(requests.post, 'disks/{0}/restart'.format(disk_id), timeout=60)
    #
    # def get_asds(self):
    #     """
    #     Loads all asds (grouped by disk)
    #     """
    #     return self._call(requests.get, 'asds', clean=True)
    #
    # def get_asds_for_disk(self, disk_id):
    #     """
    #     Loads all asds from a given disk
    #     :param disk_id: The disk identifier for which to load the asds
    #     :type disk_id: str
    #     """
    #     return self._call(requests.get, 'disks/{0}/asds'.format(disk_id), clean=True)
    #
    # def get_claimed_asds(self, disk_id):
    #     """
    #     Retrieve all ASDs claimed by any Backend for the specified disk
    #     """
    #     try:
    #         asd_info = self._call(requests.get, 'disks/{0}/get_claimed_asds'.format(disk_id), clean=True, timeout=60)
    #         asd_info['call_exists'] = True
    #         return asd_info
    #     except NotFoundError:
    #         return {'call_exists': False}
    #
    # def add_asd(self, disk_id):
    #     """
    #     Adds an ASD to a disk
    #     :param disk_id: Identifier of the disk
    #     :type disk_id: str
    #     """
    #     return self._call(requests.post, 'disks/{0}/asds'.format(disk_id), timeout=30)
    #
    # def restart_asd(self, disk_id, asd_id):
    #     """
    #     Restarts an ASD
    #     :param disk_id: Disk identifier
    #     :type disk_id: str
    #     :param asd_id: AsdID from the ASD to be restarted
    #     :type asd_id: str
    #     """
    #     return self._call(requests.post, 'disks/{0}/asds/{1}/restart'.format(disk_id, asd_id), timeout=30)
    #
    # def delete_asd(self, disk_id, asd_id):
    #     """
    #     Deletes an ASD from a Disk
    #     :param disk_id: Disk identifier
    #     :type disk_id: str
    #     :param asd_id: AsdID from the ASD to be removed
    #     :type asd_id: str
    #     """
    #     return self._call(requests.post, 'disks/{0}/asds/{1}/delete'.format(disk_id, asd_id), timeout=60)
    #
    # def list_asd_services(self):
    #     """
    #     Retrieve the ASD service names and their currently running version
    #     """
    #     return self._call(requests.get, 'asds/services', timeout=60, clean=True)['services']
    #
    # def get_package_information(self):
    #     """
    #     Retrieve the package information for this ALBA node
    #     :return: Latest available version and services which require a restart
    #     """
    #     # For backwards compatibility we first attempt to retrieve using the newest API
    #     try:
    #         return self._call(requests.get, 'update/package_information', timeout=120, clean=True)
    #     except NotFoundError:
    #         update_info = self._call(requests.get, 'update/information', timeout=120, clean=True)
    #         if update_info['version']:
    #             return {'alba': {'openvstorage-sdm': {'candidate': update_info['version'],
    #                                                   'installed': update_info['installed'],
    #                                                   'services_to_restart': []}}}
    #         return {}
    #
    # def execute_update(self, package_name):
    #     """
    #     Execute an update
    #     :return: None
    #     """
    #     try:
    #         return self._call(requests.post, 'update/install/{0}'.format(package_name), timeout=300)
    #     except NotFoundError:
    #         # Backwards compatibility
    #         status = self._call(requests.post, 'update/execute/started', timeout=300).get('status', 'done')
    #         if status != 'done':
    #             counter = 0
    #             max_counter = 12
    #             while counter < max_counter:
    #                 status = self._call(requests.post, 'update/execute/{0}'.format(status), timeout=300).get('status', 'done')
    #                 if status == 'done':
    #                     break
    #                 time.sleep(10)
    #                 counter += 1
    #             if counter == max_counter:
    #                 raise Exception('Failed to update SDM')
    #
    # def restart_services(self):
    #     """
    #     Restart the alba-asd-<ID> services
    #     :return: None
    #     """
    #     return self._call(requests.post, 'update/restart_services')
    #
    # def add_maintenance_service(self, name, alba_backend_guid, abm_name):
    #     """
    #     Add service to asd manager
    #     :param name: Name of the service
    #     :param alba_backend_guid: The guid of the AlbaBackend
    #     :param abm_name: The name of the ABM
    #     :return: result
    #     """
    #     return self._call(requests.post, 'maintenance/{0}/add'.format(name),
    #                       data={'alba_backend_guid': alba_backend_guid,
    #                             'abm_name': abm_name})
    #
    # def remove_maintenance_service(self, name):
    #     """
    #     Remove service from asd manager
    #     :param name: name
    #     :return: result
    #     """
    #     return self._call(requests.post, 'maintenance/{0}/remove'.format(name))
    #
    # def list_maintenance_services(self):
    #     """
    #     Retrieve configured maintenance services from asd manager
    #     :return: dict of services
    #     """
    #     return self._call(requests.get, 'maintenance', clean=True)['services']
    #
    # def get_service_status(self, name):
    #     """
    #     Retrieve the status of the service specified
    #     :param name: Name of the service to check
    #     :type name: str
    #     :return: Status of the service
    #     :rtype: str
    #     """
    #     return self._call(requests.get, 'service_status/{0}'.format(name))['status'][1]
