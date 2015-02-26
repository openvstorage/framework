# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Mockups module
"""


class SRClient():
    """
    Mocks the SRClient
    """

    def __init__(self):
        """
        Dummy init method
        """
        pass

    @staticmethod
    def info(volume_id):
        """
        Return fake info
        """
        _ = volume_id
        return type('Info', (), {})()

    @staticmethod
    def list_snapshots(volume_id):
        """
        Return fake info
        """
        _ = volume_id
        return []


class StorageDriverClient():
    """
    Mocks the StorageDriverClient
    """

    stat_counters = ['backend_data_read', 'backend_data_written',
                     'backend_read_operations', 'backend_write_operations',
                     'cluster_cache_hits', 'cluster_cache_misses', 'data_read',
                     'data_written', 'metadata_store_hits', 'metadata_store_misses',
                     'read_operations', 'sco_cache_hits', 'sco_cache_misses',
                     'write_operations']
    stat_sums = {'operations': ['write_operations', 'read_operations'],
                 'cache_hits': ['sco_cache_hits', 'cluster_cache_hits'],
                 'data_transferred': ['data_written', 'data_read']}
    stat_keys = stat_counters + stat_sums.keys()

    def __init__(self):
        """
        Dummy init method
        """
        pass

    @staticmethod
    def empty_statistics():
        """
        Returns a fake empty object
        """

        class Statistics():
            """ Dummy class """

            def __init__(self):
                """ Dummy init """
                pass

            @property
            def backend_data_read(self):
                """ Dummy property """
                return 0

            @property
            def backend_data_written(self):
                """ Dummy property """
                return 0

            @property
            def backend_read_operations(self):
                """ Dummy property """
                return 0

            @property
            def backend_write_operations(self):
                """ Dummy property """
                return 0

            @property
            def cluster_cache_hits(self):
                """ Dummy property """
                return 0

            @property
            def cluster_cache_misses(self):
                """ Dummy property """
                return 0

            @property
            def data_read(self):
                """ Dummy property """
                return 0

            @property
            def data_written(self):
                """ Dummy property """
                return 0

            @property
            def metadata_store_hits(self):
                """ Dummy property """
                return 0

            @property
            def metadata_store_misses(self):
                """ Dummy property """
                return 0

            @property
            def read_operations(self):
                """ Dummy property """
                return 0

            @property
            def sco_cache_hits(self):
                """ Dummy property """
                return 0

            @property
            def sco_cache_misses(self):
                """ Dummy property """
                return 0

            @property
            def write_operations(self):
                """ Dummy property """
                return 0

        return Statistics()

    @staticmethod
    def empty_info():
        """
        Returns a fake empty object
        """
        return type('Info', (), {})()

    @staticmethod
    def load():
        """
        Returns the mocked SRClient
        """
        return SRClient()


class MDSClient():
    """
    Mocks the MDSClient
    """

    def __init__(self, service):
        """
        Dummy init method
        """
        self.service = service


class MetadataServerClient():
    """
    Mocks the MetadataServerClient
    """

    mds_data = {}

    def __init__(self):
        """
        Dummy init method
        """
        pass

    @staticmethod
    def load(service):
        """
        Returns the mocked MDSClient
        """
        return MDSClient(service)


class StorageDriver():
    """
    Mocks the StorageDriver
    """
    StorageDriverClient = StorageDriverClient
    MetadataServerClient = MetadataServerClient

    def __init__(self):
        """
        Dummy init method
        """
        pass


class Loader():
    """
    Mocks loader class
    """

    def __init__(self):
        """
        Dummy init method
        """
        pass

    @staticmethod
    def load(module):
        """
        Always returns 'unittest'
        """
        _ = module
        return 'unittest'


class LoaderModule():
    """
    Mocks dependency loader module
    """

    Loader = Loader

    def __init__(self):
        """
        Dummy init method
        """
        pass


class Hypervisor():
    """
    Mocks a hypervisor client
    """

    def __init__(self):
        """
        Dummy init method
        """
        pass

    def get_state(self, vmid):
        """
        Always returns running
        """
        _ = self, vmid
        return 'RUNNING'


class Factory():
    """
    Mocks hypervisor factory
    """

    def __init__(self):
        """
        Dummy init method
        """
        pass

    @staticmethod
    def get(pmachine):
        """
        Dummy
        """
        _ = pmachine
        return None

    @staticmethod
    def get_mgmtcenter(pmachine=None, mgmt_center=None):
        """
        Dummy
        """
        _ = pmachine, mgmt_center
        return None


class FactoryModule():
    """
    Mocks hypervisor factory
    """

    Factory = Factory

    def __init__(self):
        """
        Dummy init method
        """
        pass
