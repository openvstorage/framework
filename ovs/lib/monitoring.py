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
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient
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
        ips = [sr.ip for sr in StorageRouterList.get_storagerouters()]
        for ip in ips:
            for primary_username in ['root', 'ovs']:
                try:
                    with remote(ip, [SSHClient], username=primary_username) as rem:
                        for local_ip in ips:
                            for username in ['root', 'ovs']:
                                message = 'Connection from {0}@{1} to {2}@{3}... {{0}}'.format(primary_username, ip, username, local_ip)
                                try:
                                    c = rem.SSHClient(local_ip, username=username)
                                    assert c.run(['whoami']).strip() == username
                                    message = message.format('OK')
                                    logger = MonitoringController._logger.info
                                except Exception as ex:
                                    message = message.format(ex)
                                    logger = MonitoringController._logger.error
                                logger(message)
                except Exception as ex:
                    MonitoringController._logger.error('Could not connect to {0}@{1}: {2}'.format(primary_username, ip, ex))
