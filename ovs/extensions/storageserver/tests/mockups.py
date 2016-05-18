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
