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


class Snapshot():
    """
    Dummy snapshot class
    """

    def __init__(self, metadata):
        """
        Init method
        """
        self.metadata = metadata
        self.stored = 0


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


class StorageDriverClient():
    """
    Mocks the StorageDriverClient
    """

    snapshots = {}
    metadata_backend_config = {}
    catch_up = {}
    vrouter_id = {}

    def __init__(self):
        """
        Dummy init method
        """
        pass

    @staticmethod
    def load(vpool):
        """
        Returns the mocked SRClient
        """
        _ = vpool
        return SRClient()

    @staticmethod
    def empty_info():
        """
        Returns an empty info object
        """
        return type('Info', (), {'object_type': property(lambda s: 'BASE'),
                                 'metadata_backend_config': property(lambda s: None),
                                 'vrouter_id': property(lambda s: None)})()


class MDSClient():
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


class StorageDriverConfiguration():
    """
    Mocks the StorageDriverConfiguration
    """

    def __init__(self):
        """
        Dummy init method
        """
        pass


class StorageDriverModule():
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
