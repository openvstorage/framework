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
import copy
import uuid
import pickle
from volumedriver.storagerouter.storagerouterclient import DTLConfigMode


class MockStorageRouterClient(object):
    """
    Storage Router Client Mock class
    """
    catch_up = {}
    dtl_config_cache = {}
    metadata_backend_config = {}
    object_type = {}
    snapshots = {}
    synced = True
    volumes = {}
    vrouter_id = {}

    def __init__(self, vpool_guid, arakoon_contacts):
        """
        Init method
        """
        _ = arakoon_contacts
        self.vpool_guid = vpool_guid

    @staticmethod
    def clean():
        """
        Clean everything up from previous runs
        """
        MockStorageRouterClient.catch_up = {}
        MockStorageRouterClient.dtl_config_cache = {}
        MockStorageRouterClient.metadata_backend_config = {}
        MockStorageRouterClient.object_type = {}
        MockStorageRouterClient.snapshots = {}
        MockStorageRouterClient.synced = True
        MockStorageRouterClient.volumes = {}
        MockStorageRouterClient.vrouter_id = {}

    @staticmethod
    def create_clone(target_path, metadata_backend_config, parent_volume_id, parent_snapshot_id, node_id):
        """
        Create a mocked clone
        """
        _ = target_path, metadata_backend_config, parent_volume_id, parent_snapshot_id, node_id
        volume_id = str(uuid.uuid4())
        MockStorageRouterClient.vrouter_id[volume_id] = node_id
        return volume_id

    @staticmethod
    def create_clone_from_template(target_path, metadata_backend_config, parent_volume_id, node_id):
        """
        Create a vDisk from a vTemplate
        """
        _ = target_path, metadata_backend_config, parent_volume_id, node_id
        volume_id = str(uuid.uuid4())
        MockStorageRouterClient.vrouter_id[volume_id] = node_id
        return volume_id

    @staticmethod
    def create_snapshot(volume_id, snapshot_id, metadata):
        """
        Create snapshot mockup
        """
        snapshots = MockStorageRouterClient.snapshots.get(volume_id, {})
        snapshots[snapshot_id] = Snapshot(metadata)
        MockStorageRouterClient.snapshots[volume_id] = snapshots

    @staticmethod
    def create_volume(target_path, metadata_backend_config, volume_size, node_id):
        """
        Create a mocked volume
        """
        from ovs.dal.lists.storagedriverlist import StorageDriverList

        _ = target_path, metadata_backend_config, volume_size
        volume_id = str(uuid.uuid4())
        storagedriver = StorageDriverList.get_by_storagedriver_id(node_id)
        if storagedriver is None:
            raise ValueError('Failed to retrieve storagedriver with ID {0}'.format(node_id))
        vpool = storagedriver.vpool
        MockStorageRouterClient.vrouter_id[volume_id] = node_id
        if vpool.guid not in MockStorageRouterClient.volumes:
            MockStorageRouterClient.volumes[vpool.guid] = []
        MockStorageRouterClient.volumes[vpool.guid].append(volume_id)
        return volume_id

    @staticmethod
    def delete_snapshot(volume_id, snapshot_id):
        """
        Delete snapshot mockup
        """
        del MockStorageRouterClient.snapshots[volume_id][snapshot_id]

    @staticmethod
    def empty_info():
        """
        Returns an empty info object
        """
        return type('Info', (), {'cluster_multiplier': 0,
                                 'lba_size': 0,
                                 'metadata_backend_config': property(lambda s: None),
                                 'object_type': property(lambda s: 'BASE'),
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
            return MockStorageRouterClient.dtl_config_cache[volume_id].dtl_config_mode
        return DTLConfigMode.AUTOMATIC

    @staticmethod
    def get_metadata_cache_capacity(volume_id):
        """
        Retrieve the metadata cache capacity for volume
        """
        _ = volume_id
        return 10240

    @staticmethod
    def get_readcache_behaviour(volume_id):
        """
        Retrieve the read cache behaviour for volume
        """
        _ = volume_id
        return None  # Means the vPool global value is used

    @staticmethod
    def get_readcache_limit(volume_id):
        """
        Retrieve the read cache limit for volume
        """
        _ = volume_id
        return None  # Means the vPool global value is used

    @staticmethod
    def get_readcache_mode(volume_id):
        """
        Retrieve the read cache mode for volume
        """
        _ = volume_id
        return None  # Means the vPool global value is used

    @staticmethod
    def get_sco_cache_max_non_disposable_factor(volume_id):
        """
        Retrieve the SCO cache multiplier for a volume
        """
        _ = volume_id
        return None  # Means the vPool global value is used

    @staticmethod
    def get_sco_multiplier(volume_id):
        """
        Retrieve the SCO multiplier for volume
        """
        _ = volume_id
        return 1024

    @staticmethod
    def get_tlog_multiplier(volume_id):
        """
        Retrieve the TLOG multiplier for volume
        """
        _ = volume_id
        return 16

    @staticmethod
    def info_snapshot(volume_id, snapshot_id):
        """
        Info snapshot mockup
        """
        return MockStorageRouterClient.snapshots[volume_id][snapshot_id]

    @staticmethod
    def info_volume(volume_id):
        """
        Info volume mockup
        """
        return type('Info', (), {'cluster_multiplier': property(lambda s: 8),
                                 'lba_size': property(lambda s: 512),
                                 'metadata_backend_config': property(lambda s: MockStorageRouterClient.metadata_backend_config.get(volume_id)),
                                 'object_type': property(lambda s: MockStorageRouterClient.object_type.get(volume_id, 'BASE')),
                                 'vrouter_id': property(lambda s: MockStorageRouterClient.vrouter_id.get(volume_id))})()

    @staticmethod
    def is_volume_synced_up_to_snapshot(volume_id, snapshot_id):
        """
        Is volume synced up to specified snapshot mockup
        """
        _ = volume_id, snapshot_id
        snapshot = MockStorageRouterClient.snapshots.get(volume_id, {}).get(snapshot_id)
        if snapshot is not None:
            if MockStorageRouterClient.synced is False:
                return False
            return snapshot.in_backend
        return True

    @staticmethod
    def list_snapshots(volume_id):
        """
        Return fake info
        """
        return MockStorageRouterClient.snapshots.get(volume_id, {}).keys()

    def list_volumes(self):
        """
        Return a list of volumes
        """
        return MockStorageRouterClient.volumes.get(self.vpool_guid)

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

    @staticmethod
    def set_metadata_cache_capacity(volume_id, num_pages):
        """
        Set the metadata cache capacity
        """
        _ = volume_id, num_pages

    @staticmethod
    def set_volume_as_template(volume_id):
        """
        Set a volume as a template
        """
        # Converting to template results in only latest snapshot to be kept
        timestamp = 0
        newest_snap_id = None
        for snap_id, snap_info in MockStorageRouterClient.snapshots.get(volume_id, {}).iteritems():
            metadata = pickle.loads(snap_info.metadata)
            if metadata['timestamp'] > timestamp:
                timestamp = metadata['timestamp']
                newest_snap_id = snap_id
        if newest_snap_id is not None:
            for snap_id in copy.deepcopy(MockStorageRouterClient.snapshots.get(volume_id, {})).iterkeys():
                if snap_id != newest_snap_id:
                    MockStorageRouterClient.snapshots[volume_id].pop(snap_id)
        MockStorageRouterClient.object_type[volume_id] = 'TEMPLATE'

    @staticmethod
    def unlink(volume_id):
        """
        Delete a volume
        """
        for vpool_guid, volume_ids in MockStorageRouterClient.volumes.iteritems():
            if volume_id in volume_ids:
                MockStorageRouterClient.volumes[vpool_guid].remove(volume_id)
                break

    @staticmethod
    def update_metadata_backend_config(volume_id, metadata_backend_config):
        """
        Stores the given config
        """
        MockStorageRouterClient.metadata_backend_config[volume_id] = metadata_backend_config

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
        metadata_dict = pickle.loads(metadata)
        self.metadata = metadata
        self.stored = 0
        self.in_backend = metadata_dict.get('in_backend', True)


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
        self.mode = mode
        self.dtl_config_mode = DTLConfigMode.MANUAL
