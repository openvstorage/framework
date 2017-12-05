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
import os
import time
from ovs.dal.helpers import Descriptor
# Do not remove unused imports, they are imported elsewhere from here
# noinspection PyUnresolvedReferences
from ovs_extensions.generic.sshclient import SSHClient as _SSHClient, UnableToConnectException, NotAuthenticatedException, CalledProcessTimeout, TimeOutException


class SSHClient(_SSHClient):
    """
    Remote/local client
    """

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

        unittest_mode = os.environ.get('RUNNING_UNITTESTS') == 'True'

        if storagerouter is not None and unittest_mode is False:
            process_heartbeat = storagerouter.heartbeats.get('process')
            if process_heartbeat is not None:
                if time.time() - process_heartbeat > 300:
                    message = 'StorageRouter {0} process heartbeat > 300s'.format(storagerouter.ip)
                    raise UnableToConnectException(message)

        super(SSHClient, self).__init__(endpoint=endpoint,
                                        username=username,
                                        password=password,
                                        cached=cached,
                                        timeout=timeout)
