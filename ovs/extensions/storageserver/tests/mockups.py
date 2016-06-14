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
from volumedriver.storagerouter.storagerouterclient import DTLConfigMode


class MockStorageRouterClient(object):
    """
    Storage Router Client Mock class
    """
    catch_up = {}
    dtl_config_cache = {}
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
        MockStorageRouterClient.dtl_config_cache = {}
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

    @staticmethod
    def get_dtl_config(volume_id):
        """
        Retrieve a fake DTL configuration
        """
        return MockStorageRouterClient.dtl_config_cache.get(volume_id)

    @staticmethod
    def get_dtl_config_mode(volume_id):
        """
        Retrieve a fake DTL configuration mode
        """
        if volume_id in MockStorageRouterClient.dtl_config_cache:
            return MockStorageRouterClient.dtl_config_cache['volume_id'].dtl_config_mode
        return DTLConfigMode.AUTOMATIC

    @staticmethod
    def set_manual_dtl_config(volume_id, config):
        """
        Set a fake DTL configuration
        """
        if config is None:
            dtl_config = DTLConfig(host='null', mode='no_sync', port=None)
        else:
            dtl_config = DTLConfig(host=config.host, mode=config.mode, port=config.port)
        MockStorageRouterClient.dtl_config_cache[volume_id] = dtl_config

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


class DTLConfig(object):
    """
    Dummy DTL configuration class
    """
    def __init__(self, host, mode, port):
        """
        Init method
        """
        self.host = host
        self.port = port
        self.dtl_mode = mode
        self.dtl_config_mode = DTLConfigMode.MANUAL
