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
SSHClient module
Used for remote or local command execution
"""
import time
from ovs.dal.helpers import Descriptor
# Do not remove unused imports, they are imported elsewhere from here
# noinspection PyUnresolvedReferences
from ovs_extensions.constants import is_unittest_mode
from ovs_extensions.generic.sshclient import SSHClient as _SSHClient, UnableToConnectException,\
    NotAuthenticatedException, CalledProcessTimeout, TimeOutException


class SSHClient(_SSHClient):
    """
    Remote/local client
    """
    HEARTBEAT_TIMEOUT = 300

    def __init__(self, endpoint, username='ovs', password=None, cached=True, timeout=None):
        """
        Initializes an SSHClient
        :param endpoint: Ip address to connect to / storagerouter object
        :type endpoint: basestring / ovs.dal.hybrids.storagerouter.StorageRouter
        :param username: Name of the user to connect as
        :type username: str
        :param password: Password to authenticate the user as. Can be None when ssh keys are in place.
        :type password: str
        :param cached: Cache this SSHClient instance
        :type cached: bool
        :param timeout: An optional timeout (in seconds) for the TCP connect
        :type timeout: float
        """
        from ovs.dal.hybrids.storagerouter import StorageRouter
        storagerouter = None
        if Descriptor.isinstance(endpoint, StorageRouter):
            # Refresh the object before checking its attributes
            storagerouter = StorageRouter(endpoint.guid)
            endpoint = storagerouter.ip

        unittest_mode = is_unittest_mode()

        if storagerouter is not None and unittest_mode is False:
            self._check_storagerouter(storagerouter, True)

        super(SSHClient, self).__init__(endpoint=endpoint,
                                        username=username,
                                        password=password,
                                        cached=cached,
                                        timeout=timeout)

    @staticmethod
    def get_clients(endpoints, user_names=None, raise_error=False):
        """
        Retrieve a list of SSHClient instances
        :param endpoints: List of StorageRouters or IPs
        :type endpoints: list
        :param user_names: User names for which to establish the connections
        :type user_names: list
        :param raise_error: Raise exception if any endpoint is unreachable
        :type raise_error: bool
        :return: Dictionary with SSHClients for the specified endpoints and user names
        :rtype: dict
        """
        if user_names is None or len(user_names) == 0:
            user_names = [None]

        clients = {'offline': []}
        for endpoint in endpoints:
            for user_name in user_names:
                try:
                    client = SSHClient(endpoint=endpoint, username=user_name)
                    if endpoint not in clients:
                        clients[endpoint] = {}
                    clients[endpoint][client.username] = client
                except (UnableToConnectException, NotAuthenticatedException):
                    clients['offline'].append(endpoint)
                    if raise_error is True:
                        raise
                    break
        return clients

    @classmethod
    def _check_storagerouter(cls, storagerouter, refresh=True):
        """
        Checks the heartbeat data of the storagerouter
        :param storagerouter: Storagerouter object
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :return: None
        :rtype: NoneType
        """
        process_heartbeat = storagerouter.heartbeats.get('process')
        if process_heartbeat is not None:
            if time.time() - process_heartbeat > cls.HEARTBEAT_TIMEOUT:
                if refresh is True:
                    from ovs.dal.hybrids.storagerouter import StorageRouter
                    # Check with a fresher instance of the storagerouter
                    # (not using discard as the callee might have done some other stuff to this object)
                    storagerouter = StorageRouter(storagerouter.guid)
                    return cls._check_storagerouter(storagerouter, False)
                message = 'StorageRouter {0} process heartbeat > {1}s'.format(storagerouter.ip, cls.HEARTBEAT_TIMEOUT)
                raise UnableToConnectException(message)
