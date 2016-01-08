# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Generic persistent factory.
"""
from ovs.extensions.generic.etcdconfig import EtcdConfiguration
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('extensions', name='persistent factory')


class PersistentFactory(object):
    """
    The PersistentFactory will generate certain default clients.
    """

    @staticmethod
    def get_client(client_type=None):
        """
        Returns a persistent storage client
        """

        if not hasattr(PersistentFactory, 'store') or PersistentFactory.store is None:
            if client_type is None:
                client_type = EtcdConfiguration.get('/ovs/framework/stores|persistent')

            PersistentFactory.store = None
            if client_type == 'pyrakoon':
                from ovs.extensions.storage.persistent.pyrakoonstore import PyrakoonStore
                PersistentFactory.store = PyrakoonStore('ovsdb')
            if client_type == 'arakoon':
                from ovs.extensions.storage.persistent.arakoonstore import ArakoonStore
                PersistentFactory.store = ArakoonStore('ovsdb')
            if client_type == 'default':
                from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
                PersistentFactory.store = DummyPersistentStore()

        if PersistentFactory.store is None:
            raise RuntimeError('Invalid client_type specified')
        return PersistentFactory.store
