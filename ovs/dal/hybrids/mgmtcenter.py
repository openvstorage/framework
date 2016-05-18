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
Management center module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Dynamic
from ovs.extensions.hypervisor.factory import Factory


class MgmtCenter(DataObject):
    """
    The MgmtCenter represents a management center (e.g. vCenter Server for VMware)
    """
    __properties = [Property('name', str, doc='Name of the Management Center.'),
                    Property('description', str, mandatory=False, doc='Description of the Management Center.'),
                    Property('username', str, doc='Username of the Management Center.'),
                    Property('password', str, doc='Password of the Management Center.'),
                    Property('ip', str, doc='IP address of the Management Center.'),
                    Property('port', int, doc='Port of the Management Center.'),
                    Property('type', ['VCENTER', 'OPENSTACK'], doc='Management Center type.'),
                    Property('metadata', dict, default=dict(), doc='Management Center specific metadata')]  # to avoid adding custom properties for every value
    __relations = []
    __dynamics = [Dynamic('hosts', dict, 60)]

    def _hosts(self):
        """
        Returns all hosts (not only those known to OVS) managed by the Management center
        """
        mgmt_center = Factory.get_mgmtcenter(mgmt_center=self)
        if mgmt_center is not None:
            return mgmt_center.get_hosts()
        else:
            return {}
