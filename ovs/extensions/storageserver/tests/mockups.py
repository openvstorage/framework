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
Mock wrapper class for the storagedriver client
"""


class MockStorageRouterClient(object):
    """
    Storage Router Client Mock class
    """
    catch_up = {}
    metadata_backend_config = {}
    snapshots = {}
    vrouter_id = {}

    def __init__(self, vpool_guid, arakoon_contacts):
        """
        Init method
        """
        _ = vpool_guid
        _ = arakoon_contacts

    @staticmethod
    def clean():
        """
        Clean everything up from previous runs
        """
        MockStorageRouterClient.catch_up = {}
        MockStorageRouterClient.metadata_backend_config = {}
        MockStorageRouterClient.snapshots = {}
        MockStorageRouterClient.vrouter_id = {}

    @staticmethod
    def create_snapshot(volume_id, snapshot_id, metadata):
        """
        Create snapshot mockup
        """
        snapshots = MockStorageRouterClient.snapshots.get(volume_id, {})
        snapshots[snapshot_id] = Snapshot(metadata)
        MockStorageRouterClient.snapshots[volume_id] = snapshots

    @staticmethod
    def delete_snapshot(volume_id, snapshot_id):
        """
        Delete snapshot mockup
        """
        del MockStorageRouterClient.snapshots[volume_id][snapshot_id]

    @staticmethod
    def info_snapshot(volume_id, snapshot_id):
        """
        Info snapshot mockup
        """
        return MockStorageRouterClient.snapshots[volume_id][snapshot_id]

    @staticmethod
    def list_snapshots(volume_id):
        """
        Return fake info
        """
        return MockStorageRouterClient.snapshots.get(volume_id, {}).keys()

    @staticmethod
    def info_volume(volume_id):
        """
        Info volume mockup
        """
        return type('Info', (), {'object_type': property(lambda s: 'BASE'),
                                 'metadata_backend_config': property(lambda s: MockStorageRouterClient.metadata_backend_config.get(volume_id)),
                                 'vrouter_id': property(lambda s: MockStorageRouterClient.vrouter_id.get(volume_id))})()

    @staticmethod
    def update_metadata_backend_config(volume_id, metadata_backend_config):
        """
        Stores the given config
        """
        MockStorageRouterClient.metadata_backend_config[volume_id] = metadata_backend_config

    @staticmethod
    def empty_info():
        """
        Returns an empty info object
        """
        return type('Info', (), {'object_type': property(lambda s: 'BASE'),
                                 'metadata_backend_config': property(lambda s: None),
                                 'vrouter_id': property(lambda s: None)})()
    EMPTY_INFO = empty_info


class MockMetadataServerClient(object):
    """
    Mocks the Metadata Server Client
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
        return MockStorageRouterClient.catch_up[volume_id]

    def create_namespace(self, volume_id):
        """
        Dummy create namespace method
        """
        _ = self, volume_id


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
