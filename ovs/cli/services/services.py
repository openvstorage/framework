# Copyright (C) 2019 iNuron NV
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

import re
import click
from ovs_extensions.cli.commands import OVSCommand


def framework_stop(host=None):
    """
    Stops all framework services on this node, all nodes or on specified host
    :param host: None, 'all' or ip
    :return:
    """
    from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
    from ovs.extensions.services.servicefactory import ServiceFactory

    storagerouter_list = _get_storagerouter_list(host)
    print 'Stopping...'
    for storagerouter in storagerouter_list:
        try:
            client = SSHClient(storagerouter, username='root')
            ServiceFactory.get_manager().stop_service('watcher-framework', client)
        except UnableToConnectException:
            print '{0} on {1}... failed (Node unreachable)'.format('Stopping', storagerouter.name)
            continue
    print 'Done'


def framework_start(host=None):
    """
    Starts all framework services on this node, all nodes or on specified host
    :param host: None, all or ip
    :return:
    """
    from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
    from ovs.extensions.services.servicefactory import ServiceFactory

    storagerouter_list = _get_storagerouter_list(host)
    print 'Starting...'
    for storagerouter in storagerouter_list:
        try:
            client = SSHClient(storagerouter, username='root')
            ServiceFactory.get_manager().start_service('watcher-framework', client)
        except UnableToConnectException:
            print '{0} on {1}... failed (Node unreachable)'.format('Starting', storagerouter.name)
            continue

    print 'Done'


def _get_storagerouter_list(host):
    from ovs.dal.lists.storagerouterlist import StorageRouterList
    from ovs.lib.helpers.toolbox import Toolbox
    from ovs.extensions.generic.system import System

    storagerouter_list = []
    if not host:
        storagerouter_list = [System.get_my_storagerouter()]
    else:
        if re.match(Toolbox.regex_ip, host):
            sr = StorageRouterList.get_by_ip(host)
            storagerouter_list = [sr]
        if host == 'all':
            storagerouter_list = sorted(StorageRouterList.get_storagerouters(), key=lambda k: k.name)
        else:
            print 'Invalid argument given. `Host` should be `all|<IP>`'
    return storagerouter_list
