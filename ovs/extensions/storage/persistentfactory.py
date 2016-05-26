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
Generic persistent factory.
"""
import os
from ovs.extensions.db.etcd.configuration import EtcdConfiguration


class PersistentFactory(object):
    """
    The PersistentFactory will generate certain default clients.
    """

    @staticmethod
    def get_client(client_type=None):
        """
        Returns a persistent storage client
        :param client_type: Type of store client
        """
        if not hasattr(PersistentFactory, 'store') or PersistentFactory.store is None:
            if os.environ.get('RUNNING_UNITTESTS') == 'True':
                client_type = 'dummy'

            if client_type is None:
                client_type = EtcdConfiguration.get('/ovs/framework/stores|persistent')

            PersistentFactory.store = None
            if client_type in ['pyrakoon', 'arakoon']:
                from ovs.extensions.storage.persistent.pyrakoonstore import PyrakoonStore
                PersistentFactory.store = PyrakoonStore(str(EtcdConfiguration.get('/ovs/framework/arakoon_clusters|ovsdb')))
            if client_type == 'dummy':
                from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
                PersistentFactory.store = DummyPersistentStore()

        if PersistentFactory.store is None:
            raise RuntimeError('Invalid client_type specified')
        return PersistentFactory.store
