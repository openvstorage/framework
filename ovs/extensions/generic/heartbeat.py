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
import logging
from ovs.extensions.log import configure_logging
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.generic.system import System
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.lib.storagerouter import StorageRouterController
from subprocess import check_output, CalledProcessError


class HeartBeat(object):
    """
    Heartbeat class
    Put into crontab after installing openvstorage-core (see /etc/cron.d/openvstorage-core)
    """
    ARP_TIMEOUT = 30

    logger = logging.getLogger('ovs.heartbeat')

    @classmethod
    def pulse(cls):
        """
        Update the heartbeats for the Current Routers
        :return: None
        """
        storagerouter = System.get_my_storagerouter()
        current_time = int(time.time())
        with volatile_mutex('storagerouter_heartbeat_{0}'.format(storagerouter.guid)):
            node_save = StorageRouter(storagerouter.guid)
            node_save.heartbeats['process'] = current_time
            node_save.save()
        StorageRouterController.ping.s(storagerouter.guid, current_time).apply_async(routing_key='sr.{0}'.format(storagerouter.machine_id))

        # Disabled because it no longer serves any purpose
        # cls.clear_arp_cache(current_time)

    @classmethod
    def clear_arp_cache(cls, current_time):
        """
        Clear the ARP cache on this node for all other nodes in the cluster
        :param current_time: Current timestamp to use
        :type current_time: int
        :return: None
        :rtype: NoneType
        """
        for storagerouter in StorageRouterList.get_storagerouters():
            if storagerouter == System.get_my_storagerouter():
                continue
            cls.clear_arp_cache_for_node(storagerouter, current_time)

    @classmethod
    def clear_arp_cache_for_node(cls, storagerouter, current_time):
        """
        Clear the ARP cache
        Implemented for faster detections that a host is down
        Unsure if relevant anymore. Here for documentation purposes
        :param storagerouter: Node to clear ARP cache for
        :type storagerouter: StorageRouter
        :param current_time: Current timestamp to use
        :type current_time: int
        :return: None
        :rtype: NoneType
        """
        current_time = current_time
        try:
            # Check timeout of other nodes and clear arp cache
            if storagerouter.heartbeats and 'process' in storagerouter.heartbeats and current_time - storagerouter.heartbeats['process'] >= cls.ARP_TIMEOUT:
                check_output("/usr/sbin/arp -d '{0}'".format(storagerouter.name.replace(r"'", r"'\''")), shell=True)
        except CalledProcessError:
            cls.logger.exception('Error clearing ARP cache')


if __name__ == '__main__':
    configure_logging()
    HeartBeat.pulse()
