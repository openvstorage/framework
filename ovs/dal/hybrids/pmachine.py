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
PMachine module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation, Dynamic
from ovs.dal.hybrids.mgmtcenter import MgmtCenter
from ovs.extensions.hypervisor.factory import Factory


class PMachine(DataObject):
    """
    The PMachine class represents a pMachine. A pMachine is the physical machine
    running the Hypervisor.
    """
    __properties = {Property('name', str, doc='Name of the pMachine.'),
                    Property('description', str, mandatory=False, doc='Description of the pMachine.'),
                    Property('username', str, doc='Username of the pMachine.'),
                    Property('password', str, mandatory=False, doc='Password of the pMachine.'),
                    Property('ip', str, doc='IP address of the pMachine.'),
                    Property('hvtype', ['HYPERV', 'VMWARE', 'XEN', 'KVM'], doc='Hypervisor type running on the pMachine.'),
                    Property('hypervisor_id', str, mandatory=False, doc='Hypervisor id - primary key on Management Center')}
    __relations = [Relation('mgmtcenter', MgmtCenter, 'pmachines', mandatory=False)]
    __dynamics = [Dynamic('host_status', str, 60)]

    def _host_status(self):
        """
        Returns the host status as reported by the management center (e.g. vCenter Server)
        """
        mgmtcentersdk = Factory.get_mgmtcenter(self)
        if mgmtcentersdk:
            if self.hypervisor_id:
                return mgmtcentersdk.get_host_status_by_pk(self.hypervisor_id)
            if self.ip:
                return mgmtcentersdk.get_host_status_by_ip(self.ip)
        return 'UNKNOWN'
