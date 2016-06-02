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

import time
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.lib.storagerouter import StorageRouterController
from ovs.extensions.generic.system import System
from ovs.log.log_handler import LogHandler
from subprocess import check_output, CalledProcessError


class HeartBeat(object):
    """
    Heartbeat class
    """
    ARP_TIMEOUT = 30

    def __init__(self):
        """
        Dummy init
        """
        raise Exception('Heartbeat class cannot be instantiated')

    @staticmethod
    def pulse():
        """
        Update the heartbeats for the Current Routers
        :return: None
        """
        logger = LogHandler.get('extensions', name='heartbeat')
        machine_id = System.get_my_machine_id()
        current_time = int(time.time())
        storagerouter = StorageRouterList.get_by_machine_id(machine_id)
        StorageRouterController.ping.s(storagerouter.guid, current_time).apply_async(routing_key='sr.{0}'.format(machine_id))

        routers = StorageRouterList.get_storagerouters()
        for node in routers:
            if node.machine_id == machine_id:
                node.heartbeats['process'] = current_time
            else:
                try:
                    # check timeout of other nodes and clear arp cache
                    if node.heartbeats and 'process' in node.heartbeats:
                        if current_time - node.heartbeats['process'] >= HeartBeat.ARP_TIMEOUT:
                            check_output("/usr/sbin/arp -d {0}".format(node.name), shell=True)
                except CalledProcessError:
                    logger.exception('Error clearing ARP cache')
            node.save()

if __name__ == '__main__':
    HeartBeat.pulse()
