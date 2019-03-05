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
from ovs.dal.dataobject.attributes import Property, Dynamic, Relation, RelationGuid, RelationTypes
# Typing import
# noinspection PyUnreachableCode
if False:
    from ovs.dal.hybrids.backendtype import BackendType


class Backend(DataObject):
    """
    A Backend represents an instance of the supported backend types that has been setup with the OVS GUI
    """
    __slots__ = ('STATUSES', 'name', 'status',
                 'linked_guid', 'available', 'regular_domains', 'access_rights', 'live_status',
                 'backend_type', 'backend_type_guid', 'backends', 'backends_guids')
    STATUSES = DataObject.enumerator('Status', ['INSTALLING', 'RUNNING', 'FAILURE', 'WARNING', 'DELETING'])

    name = Property(str, unique=True, doc='Name of the backend')
    status = Property(STATUSES.keys(), default='INSTALLING', doc='State of the backend')

    linked_guid = Dynamic(str, 3600)
    available = Dynamic(bool, 60)
    regular_domains = Dynamic(list, 60)
    access_rights = Dynamic(dict, 3600)
    live_status = Dynamic(str, 30)

    backend_type = Relation('BackendType', doc='Type of the backend')
    backend_type_guid = RelationGuid(backend_type)

    domains = Relation('BackendDomain', relation_type=RelationTypes.MANYTOONE, doc='Associated domains')
    domains_guids = RelationGuid(domains)

    def _linked_guid(self):
        """
        Returns the GUID of the detail object that's linked to this particular backend. This depends on the backend type.
        This requires that the backlink from that object to this object is named <backend_type>_backend and is a
        one-to-one relation
        """
        backend_type = self.backend_type  # type: BackendType
        if not backend_type.has_plugin:
            return None
        return getattr(self, '{0}_backend_guid'.format(backend_type.code))

    def _available(self):
        """
        Returns True if the backend can be used
        """
        backend_type = self.backend_type  # type: BackendType
        if backend_type.has_plugin is False:
            return False
        linked_backend = getattr(self, '{0}_backend'.format(backend_type.code))
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
