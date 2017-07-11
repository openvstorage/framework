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
Backend module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation, Dynamic
from ovs.dal.hybrids.backendtype import BackendType


class Backend(DataObject):
    """
    A Backend represents an instance of the supported backend types that has been setup with the OVS GUI
    """
    STATUSES = DataObject.enumerator('Status', ['INSTALLING', 'RUNNING', 'FAILURE', 'WARNING', 'DELETING'])
    
    __properties = [Property('name', str, unique=True, doc='Name of the Backend.'),
                    Property('status', STATUSES.keys(), default='INSTALLING', doc='State of the backend')]
    __relations = [Relation('backend_type', BackendType, 'backends', doc='Type of the backend.')]
    __dynamics = [Dynamic('linked_guid', str, 3600),
                  Dynamic('available', bool, 60),
                  Dynamic('regular_domains', list, 60),
                  Dynamic('access_rights', dict, 3600),
                  Dynamic('live_status', str, 30)]

    def _linked_guid(self):
        """
        Returns the GUID of the detail object that's linked to this particular backend. This depends on the backend type.
        This requires that the backlink from that object to this object is named <backend_type>_backend and is a
        one-to-one relation
        """
        if self.backend_type.has_plugin is False:
            return None
        return getattr(self, '{0}_backend_guid'.format(self.backend_type.code))

    def _available(self):
        """
        Returns True if the backend can be used
        """
        if self.backend_type.has_plugin is False:
            return False
        linked_backend = getattr(self, '{0}_backend'.format(self.backend_type.code))
        if linked_backend is not None:
            return linked_backend.available
        return False

    def _regular_domains(self):
        """
        Returns a list of domain guids
        :return: List of domain guids
        """
        return [junction.domain_guid for junction in self.domains]

    def _access_rights(self):
        """
        A condensed extract from the user_rights and client_rights
        :return: dict
        """
        data = {'users': {},
                'clients': {}}
        for user_right in self.user_rights:
            data['users'][user_right.user_guid] = user_right.grant
        for client_right in self.client_rights:
            data['clients'][client_right.client_guid] = client_right.grant
        return data

    def _live_status(self):
        """
        Retrieve the actual status from the Backend
        :return: Status reported by the plugin
        """
        if self.backend_type.has_plugin is False:
            return 'running'

        linked_backend = getattr(self, '{0}_backend'.format(self.backend_type.code))
        if linked_backend is not None:
            return linked_backend.live_status
        return 'running'
