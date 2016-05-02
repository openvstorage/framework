# Copyright 2016 iNuron NV
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


class Snapshot(object):
    """
    Dummy snapshot class
    """

    def __init__(self, metadata):
        """
        Init method
        """
        self.metadata = metadata
        self.stored = 0
        self.in_backend = True


class SRClient(object):
    """
    Mocks the SRClient
    """
    client_type = 'MOCK_OK'

    def __init__(self, client_type):
        """
        Dummy init method
        """
        SRClient.client_type = client_type

    @staticmethod
    def list_snapshots(volume_id):
        """
        Return fake info
        """
        snapshots = StorageDriverClient.snapshots.get(volume_id, {})
        return snapshots.keys()

    @staticmethod
    def create_snapshot(volume_id, snapshot_id, metadata):
        """
        Create snapshot mockup
        """
        snapshots = StorageDriverClient.snapshots.get(volume_id, {})
        snapshots[snapshot_id] = Snapshot(metadata)
        StorageDriverClient.snapshots[volume_id] = snapshots

    @staticmethod
    def info_snapshot(volume_id, guid):
        """
        Info snapshot mockup
        """
        return StorageDriverClient.snapshots[volume_id][guid]

    @staticmethod
    def delete_snapshot(volume_id, guid):
        """
        Delete snapshot mockup
        """
        del StorageDriverClient.snapshots[volume_id][guid]

    @staticmethod
    def info_volume(volume_id):
        """
        Info volume mockup
        """
        return type('Info', (), {'object_type': property(lambda s: 'BASE'),
                                 'metadata_backend_config': property(lambda s: StorageDriverClient.metadata_backend_config.get(volume_id)),
                                 'vrouter_id': property(lambda s: StorageDriverClient.vrouter_id.get(volume_id))})()

    @staticmethod
    def get_scrubbing_workunits(volume_id):
        """
        Get scrubbing workload mockup
        """
        _ = volume_id
        return []

    @staticmethod
    def update_metadata_backend_config(volume_id, metadata_backend_config):
        """
        Stores the given config
        """
        StorageDriverClient.metadata_backend_config[volume_id] = metadata_backend_config

    @staticmethod
    def create_clone_from_template(target_path, metadata_backend_config, parent_volume_id, node_id):
        """
        Create clone from template
        """
        _ = target_path, metadata_backend_config, parent_volume_id, node_id
        if SRClient.client_type == 'MOCK_BAD':
            raise RuntimeError('Backend Error in SRClient')


class StorageDriverClient(object):
    """
    Mocks the StorageDriverClient
    """

    snapshots = {}
    metadata_backend_config = {}
    catch_up = {}
    vrouter_id = {}
    client_type = 'MOCK_OK'

    def __init__(self):
        """
        Dummy init method
        """
        pass

    @staticmethod
    def use_bad_client():
        """
        Use bad client
        """
        StorageDriverClient.client_type = 'MOCK_BAD'

    @staticmethod
    def use_good_client():
        """
        Use good client
        """
        StorageDriverClient.client_type = 'MOCK_OK'

    @staticmethod
    def load(vpool):
        """
        Returns the mocked SRClient
        """
        _ = vpool
        return SRClient(StorageDriverClient.client_type)

    @staticmethod
    def clean():
        """
        Restore empty settings
        """
        StorageDriverClient.snapshots = {}
        StorageDriverClient.metadata_backend_config = {}
        StorageDriverClient.catch_up = {}
        StorageDriverClient.vrouter_id = {}
        StorageDriverClient.client_type = 'MOCK_OK'

    @staticmethod
    def empty_info():
        """
        Returns an empty info object
        """
        return type('Info', (), {'object_type': property(lambda s: 'BASE'),
                                 'metadata_backend_config': property(lambda s: None),
                                 'vrouter_id': property(lambda s: None)})()
    EMPTY_INFO = empty_info


class MDSClient(object):
    """
    Mocks the MDSClient
    """

    def __init__(self, service):
        """
        Dummy init method
        """
        self.service = service

    def catch_up(self, volume_id, dry_run):
        """
        Dummy catchup
        """
        _ = self, dry_run
        return StorageDriverClient.catch_up[volume_id]

    def create_namespace(self, volume_id):
        """
        Dummy create namespace method
        """
        _ = self, volume_id


class MetadataServerClient(object):
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


class StorageDriverConfiguration(object):
    """
    Mocks the StorageDriverConfiguration
    """

    def __init__(self):
        """
        Dummy init method
        """
        pass


class StorageDriverModule(object):
    """
    Mocks the StorageDriver
    """
    StorageDriverClient = StorageDriverClient
    MetadataServerClient = MetadataServerClient
    StorageDriverConfiguration = StorageDriverConfiguration

    def __init__(self):
        """
        Dummy init method
        """
        pass

    @staticmethod
    def use_bad_client():
        """
        Use bad client
        """
        StorageDriverClient.use_bad_client()

    @staticmethod
    def use_good_client():
        """
        Use good client
        """
        StorageDriverClient.use_good_client()

    @staticmethod
    def clean():
        """
        Restore empty settings
        """
        StorageDriverClient.clean()


class SSHClient(object):
    """
    SSHClient class
    """
    def __init__(self, endpoint, username='ovs', password=None):
        """
        Dummy init method
        """
        _ = endpoint, username, password


class UnableToConnectException(object):
    """
    Custom exception thrown when client cannot connect to remote side
    """
    pass


class SSHClientModule(object):
    """
    Mocks the SSHClient
    """
    SSHClient = SSHClient
    UnableToConnectException = UnableToConnectException
