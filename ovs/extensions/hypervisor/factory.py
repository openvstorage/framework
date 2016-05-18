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
Hypervisor/ManagementCenter factory module
"""

from ovs.extensions.generic.filemutex import file_mutex


class Factory(object):
    """
    Factory class provides functionality to get abstracted hypervisor
    """

    hypervisors = {}
    mgmtcenters = {}

    @staticmethod
    def get(pmachine):
        """
        Returns the appropriate hypervisor client class for a given PMachine
        """
        hvtype = pmachine.hvtype
        ip = pmachine.ip
        username = pmachine.username
        password = pmachine.password
        key = '{0}_{1}'.format(ip, username)
        if key not in Factory.hypervisors:
            mutex = file_mutex('hypervisor_{0}'.format(key))
            try:
                mutex.acquire(30)
                if key not in Factory.hypervisors:
                    if hvtype == 'VMWARE':
                        from hypervisors.vmware import VMware
                        hypervisor = VMware(ip, username, password)
                    elif hvtype == 'KVM':
                        from hypervisors.kvm import KVM
                        hypervisor = KVM(ip, username, password)
                    else:
                        raise NotImplementedError('Hypervisor {0} is not yet supported'.format(hvtype))
                    Factory.hypervisors[key] = hypervisor
            finally:
                mutex.release()
        return Factory.hypervisors[key]

    @staticmethod
    def get_mgmtcenter(pmachine=None, mgmt_center=None):
        """
        @param pmachine: pmachine hybrid from DAL
        @param mgmt_center: mgmtcenter hybrid from DAL
        Returns the appropriate sdk client for the management center of the node
        """
        if not ((pmachine is None) ^ (mgmt_center is None)):
            raise ValueError('Either a pMachine or a Management center should be passed')
        if pmachine is not None:
            mgmt_center = pmachine.mgmtcenter
            if mgmt_center is None:
                return None

        mgmtcenter_type = mgmt_center.type
        ip = mgmt_center.ip
        username = mgmt_center.username
        password = mgmt_center.password
        key = '{0}_{1}'.format(ip, username)
        if key not in Factory.mgmtcenters:
            mutex = file_mutex('mgmtcenter_{0}'.format(key))
            try:
                mutex.acquire(30)
                if key not in Factory.mgmtcenters:
                    if mgmtcenter_type == 'VCENTER':
                        from mgmtcenters.vcenter import VCenter
                        mgmtcenter = VCenter(ip, username, password)
                    elif mgmtcenter_type == 'OPENSTACK':
                        from mgmtcenters.openstack import OpenStack
                        mgmtcenter = OpenStack(ip, username, password)
                    else:
                        raise NotImplementedError('Management center for {0} is not yet supported'.format(mgmtcenter_type))
                    Factory.mgmtcenters[key] = mgmtcenter
            finally:
                mutex.release()
        return Factory.mgmtcenters[key]
