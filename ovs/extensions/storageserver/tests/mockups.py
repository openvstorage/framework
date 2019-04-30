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
import json
import uuid
import pickle
import threading
from volumedriver.storagerouter.storagerouterclient import DTLConfigMode, ObjectNotFoundException


class VolumeRestartInProgressException(Exception):
    """ VolumeRestartInProgressException mockup for backwards compatibility """
    pass


class LocalStorageRouterClient(object):
    """
    Local Storage Router Client mock class
    """
    configurations = {}

    def __init__(self, path):
        """
        Init method
        """
        self.path = path

    def server_revision(self, req_timeout_secs=None):
        """
        Returns a fake revision
        """
        _ = req_timeout_secs
        exception = StorageRouterClient.exceptions.get('server_revision', {}).get(self.path)
        if exception is not None:
            raise exception
        return 0

    def update_configuration(self, path, req_timeout_secs=None):
        """
        Update configuration mock
        """
        _ = req_timeout_secs
        from ovs.extensions.generic.configuration import Configuration
        if path != self.path:
            raise RuntimeError('Unexpected path passed. Not an issue, but unexpected. This (unittest) code might need to be adapted.')
        main_key = Configuration.extract_key_from_path(path)
        current_content = LocalStorageRouterClient.configurations.get(main_key, {})
        new_content = json.loads(Configuration.get(main_key, raw=True))
        changes = []
        for section_key, section in new_content.iteritems():
            current_section = current_content.get(section_key, {})
            for key, value in section.iteritems():
                current_value = current_section.get(key)
                if current_section.get(key) != value:
                    changes.append({'param_name': key,
                                    'old_value': current_value,
                                    'new_value': value})
        LocalStorageRouterClient.configurations[main_key] = new_content
        return changes


class StorageRouterClient(object):
    """
    Storage Router Client mock class
    """
    _config_cache = {}
    _dtl_config_cache = {}
    _metadata_backend_config = {}
    _snapshots = {}
    delete_snapshot_callbacks = {}
    volumes = {}
    exceptions = {}
    vrouter_id = {}
    object_type = {}
    failover_mode = {}
    mds_recording = []
    node_config_recordings = []
    synced = True

    def __init__(self, vpool_guid, arakoon_contacts):
        """
        Init method
        """
        _ = arakoon_contacts
        self.vpool_guid = vpool_guid
        for item in [StorageRouterClient.volumes,
                     StorageRouterClient.vrouter_id,
                     StorageRouterClient.object_type,
                     StorageRouterClient.failover_mode,
                     StorageRouterClient._config_cache,
                     StorageRouterClient._dtl_config_cache,
                     StorageRouterClient._metadata_backend_config,
                     StorageRouterClient._snapshots]:
            if self.vpool_guid not in item:
                item[self.vpool_guid] = {}

    @staticmethod
    def _clean(vpool_guid=None, volume_id=None):
        """
        Clean everything up from previous runs
        """
        StorageRouterClient.synced = True
        for item in [StorageRouterClient.volumes,
                     StorageRouterClient.vrouter_id,
                     StorageRouterClient.object_type,
                     StorageRouterClient.failover_mode,
                     StorageRouterClient._config_cache,
                     StorageRouterClient._dtl_config_cache,
                     StorageRouterClient._metadata_backend_config,
                     StorageRouterClient._snapshots]:
            for this_vpool_guid in item.keys():
                if vpool_guid is None or vpool_guid == this_vpool_guid:
                    if volume_id is not None:
                        if volume_id in item[this_vpool_guid]:
                            del item[this_vpool_guid][volume_id]
                    else:
                        item[this_vpool_guid] = {}

    def create_clone(self, target_path, metadata_backend_config, parent_volume_id, parent_snapshot_id, node_id, req_timeout_secs=None):
        """
        Create a mocked clone
        """
        _ = parent_snapshot_id, req_timeout_secs
        if parent_volume_id not in StorageRouterClient.volumes[self.vpool_guid]:
            raise RuntimeError('Could not find volume {0}'.format(parent_volume_id))
        volume_size = StorageRouterClient.volumes[self.vpool_guid][parent_volume_id]['volume_size']
        return self.create_volume(target_path, metadata_backend_config, volume_size, node_id)

    def create_clone_from_template(self, target_path, metadata_backend_config, parent_volume_id, node_id, req_timeout_secs=None):
        """
        Create a vDisk from a vTemplate
        """
        _ = req_timeout_secs
        if parent_volume_id not in StorageRouterClient.volumes[self.vpool_guid]:
            raise RuntimeError('Could not find volume {0}'.format(parent_volume_id))
        parent_volume = StorageRouterClient.volumes[self.vpool_guid][parent_volume_id]
        if StorageRouterClient.object_type[self.vpool_guid].get(parent_volume_id, 'BASE') != 'TEMPLATE':
            raise ValueError('Can only clone from a template')
        return self.create_volume(target_path, metadata_backend_config, parent_volume['volume_size'], node_id)

    def create_snapshot(self, volume_id, snapshot_id, metadata, req_timeout_secs=None):
        """
        Create snapshot mockup
        """
        _ = req_timeout_secs
        snapshots = StorageRouterClient._snapshots[self.vpool_guid].get(volume_id, {})
        snapshots[snapshot_id] = Snapshot(metadata)
        StorageRouterClient._snapshots[self.vpool_guid][volume_id] = snapshots

    def _set_snapshot_in_backend(self, volume_id, snapshot_id, in_backend):
        """
        Sets a snapshot in/out backend
        """
        if volume_id not in StorageRouterClient._snapshots[self.vpool_guid]:
            raise RuntimeError('Could not find volume {0}'.format(volume_id))
        StorageRouterClient._snapshots[self.vpool_guid][volume_id][snapshot_id].in_backend = in_backend

    def create_volume(self, target_path, metadata_backend_config, volume_size, node_id, req_timeout_secs=None):
        """
        Create a mocked volume
        """
        _ = req_timeout_secs
        from ovs.dal.lists.storagedriverlist import StorageDriverList
        volume_id = str(uuid.uuid4())
        storagedriver = StorageDriverList.get_by_storagedriver_id(node_id)
        if storagedriver is None:
            raise ValueError('Failed to retrieve storagedriver with ID {0}'.format(node_id))
        StorageRouterClient.vrouter_id[self.vpool_guid][volume_id] = node_id
        StorageRouterClient._metadata_backend_config[self.vpool_guid][volume_id] = metadata_backend_config
        StorageRouterClient.volumes[self.vpool_guid][volume_id] = {'volume_id': volume_id,
                                                                   'volume_size': volume_size,
                                                                   'target_path': target_path}
        return volume_id

    def _set_object_type(self, volume_id, object_type):
        """
        Sets the apparent object type
        """
        if volume_id not in StorageRouterClient.object_type[self.vpool_guid]:
            raise RuntimeError('Could not find volume {0}'.format(volume_id))
        StorageRouterClient.object_type[self.vpool_guid][volume_id] = object_type

    def delete_snapshot(self, volume_id, snapshot_id, req_timeout_secs=None):
        """
        Delete snapshot mockup
        """
        _ = req_timeout_secs
        callback = self.delete_snapshot_callbacks.get(volume_id, {}).get(snapshot_id)
        if callback:
            callback()
        del StorageRouterClient._snapshots[self.vpool_guid][volume_id][snapshot_id]

    def empty_info(self, req_timeout_secs=None):
        """
        Returns an empty info object
        """
        _ = self, req_timeout_secs
        return type('Info', (), {'halted': False,
                                 'lba_size': 0,
                                 'vrouter_id': property(lambda s: None),
                                 'object_type': property(lambda s: 'BASE'),
                                 'volume_size': 0,
                                 'failover_mode': property(lambda s: 'OK_STANDALONE'),
                                 'cluster_multiplier': 0,
                                 'metadata_backend_config': property(lambda s: None)})()

    def get_dtl_config(self, volume_id, req_timeout_secs=None):
        """
        Retrieve a fake DTL configuration
        """
        _ = req_timeout_secs
        return StorageRouterClient._dtl_config_cache[self.vpool_guid].get(volume_id)

    def get_dtl_config_mode(self, volume_id, req_timeout_secs=None):
        """
        Retrieve a fake DTL configuration mode
        """
        _ = req_timeout_secs
        if volume_id in StorageRouterClient._dtl_config_cache[self.vpool_guid]:
            if StorageRouterClient._dtl_config_cache[self.vpool_guid][volume_id] is None:
                return DTLConfigMode.MANUAL
            return StorageRouterClient._dtl_config_cache[self.vpool_guid][volume_id].dtl_config_mode
        return DTLConfigMode.AUTOMATIC

    def get_metadata_cache_capacity(self, volume_id, req_timeout_secs=None):
        """
        Retrieve the metadata cache capacity for volume
        """
        _ = self, req_timeout_secs
        return StorageRouterClient._config_cache.get(self.vpool_guid, {}).get(volume_id, {}).get('metadata_cache_capacity', 8192)

    def get_sco_cache_max_non_disposable_factor(self, volume_id, req_timeout_secs=None):
        """
        Retrieve the SCO cache multiplier for a volume
        """
        _ = self, req_timeout_secs
        return StorageRouterClient._config_cache.get(self.vpool_guid, {}).get(volume_id, {}).get('sco_cache_non_disposable_factor', 12)

    def get_sco_multiplier(self, volume_id, req_timeout_secs=None):
        """
        Retrieve the SCO multiplier for volume
        """
        _ = self, req_timeout_secs
        return StorageRouterClient._config_cache.get(self.vpool_guid, {}).get(volume_id, {}).get('sco_multiplier', 1024)

    def get_tlog_multiplier(self, volume_id, req_timeout_secs=None):
        """
        Retrieve the TLOG multiplier for volume
        """
        _ = self, req_timeout_secs
        return StorageRouterClient._config_cache.get(self.vpool_guid, {}).get(volume_id, {}).get('tlog_multiplier', 16)

    def info_snapshot(self, volume_id, snapshot_id, req_timeout_secs=None):
        """
        Info snapshot mockup
        """
        _ = req_timeout_secs
        return StorageRouterClient._snapshots[self.vpool_guid][volume_id][snapshot_id]

    def info_volume(self, volume_id, req_timeout_secs=None):
        """
        Info volume mockup
        """
        _ = req_timeout_secs
        volume_size = StorageRouterClient.volumes[self.vpool_guid].get(volume_id, {}).get('volume_size', 0)
        return type('Info', (), {'halted': property(lambda s: False),
                                 'lba_size': property(lambda s: 512),
                                 'vrouter_id': property(lambda s: StorageRouterClient.vrouter_id[self.vpool_guid].get(volume_id)),
                                 'volume_size': property(lambda s: volume_size),
                                 'object_type': property(lambda s: StorageRouterClient.object_type[self.vpool_guid].get(volume_id, 'BASE')),
                                 'failover_mode': property(lambda s: StorageRouterClient.failover_mode[self.vpool_guid].get(volume_id, 'OK_STANDALONE')),
                                 'cluster_multiplier': property(lambda s: 8),
                                 'metadata_backend_config': property(lambda s: StorageRouterClient._metadata_backend_config[self.vpool_guid].get(volume_id))})()

    def is_volume_synced_up_to_snapshot(self, volume_id, snapshot_id, req_timeout_secs=None):
        """
        Is volume synced up to specified snapshot mockup
        """
        _ = req_timeout_secs
        snapshot = StorageRouterClient._snapshots[self.vpool_guid].get(volume_id, {}).get(snapshot_id)
        if snapshot is not None:
            if StorageRouterClient.synced is False:
                return False
            return snapshot.in_backend
        return True

    def list_snapshots(self, volume_id, req_timeout_secs=None):
        """
        Return fake info
        """
        _ = req_timeout_secs
        return StorageRouterClient._snapshots[self.vpool_guid].get(volume_id, {}).keys()

    def list_volumes(self, req_timeout_secs=None):
        """
        Return a list of volumes
        """
        _ = req_timeout_secs
        return [volume_id for volume_id in StorageRouterClient.volumes[self.vpool_guid].iterkeys()]

    def make_locked_client(self, volume_id, req_timeout_secs=None, update_interval_secs=None):
        """
        Retrieve a mocked locked client connection for the specified volume
        """
        _ = self, req_timeout_secs, update_interval_secs
        return LockedClient(volume_id)

    def set_manual_dtl_config(self, volume_id, config, req_timeout_secs=None):
        """
        Set a fake DTL configuration
        """
        _ = req_timeout_secs
        if config is None:
            StorageRouterClient.failover_mode[self.vpool_guid][volume_id] = 'OK_STANDALONE'
            dtl_config = None
        else:
            StorageRouterClient.failover_mode[self.vpool_guid][volume_id] = 'OK_SYNC'
            dtl_config = DTLConfig(host=config.host, mode=config.mode, port=config.port)
        StorageRouterClient._dtl_config_cache[self.vpool_guid][volume_id] = dtl_config

    def set_metadata_cache_capacity(self, volume_id, num_pages, req_timeout_secs=None):
        """
        Set the metadata cache capacity for volume
        """
        _ = req_timeout_secs
        if self.vpool_guid not in StorageRouterClient._config_cache:
            StorageRouterClient._config_cache[self.vpool_guid] = {}
        if volume_id not in StorageRouterClient._config_cache[self.vpool_guid]:
            StorageRouterClient._config_cache[self.vpool_guid][volume_id] = {}
        StorageRouterClient._config_cache[self.vpool_guid][volume_id]['metadata_cache_capacity'] = num_pages

    def set_sco_cache_max_non_disposable_factor(self, volume_id, factor, req_timeout_secs=None):
        """
        Retrieve the SCO cache multiplier for a volume
        """
        _ = req_timeout_secs
        if self.vpool_guid not in StorageRouterClient._config_cache:
            StorageRouterClient._config_cache[self.vpool_guid] = {}
        if volume_id not in StorageRouterClient._config_cache[self.vpool_guid]:
            StorageRouterClient._config_cache[self.vpool_guid][volume_id] = {}
        StorageRouterClient._config_cache[self.vpool_guid][volume_id]['sco_cache_non_disposable_factor'] = factor

    def set_sco_multiplier(self, volume_id, multiplier, req_timeout_secs=None):
        """
        Set the SCO multiplier for volume
        """
        _ = req_timeout_secs
        if self.vpool_guid not in StorageRouterClient._config_cache:
            StorageRouterClient._config_cache[self.vpool_guid] = {}
        if volume_id not in StorageRouterClient._config_cache[self.vpool_guid]:
            StorageRouterClient._config_cache[self.vpool_guid][volume_id] = {}
        StorageRouterClient._config_cache[self.vpool_guid][volume_id]['sco_multiplier'] = multiplier

    def set_tlog_multiplier(self, volume_id, multiplier, req_timeout_secs=None):
        """
        Retrieve the TLOG multiplier for volume
        """
        _ = req_timeout_secs
        if self.vpool_guid not in StorageRouterClient._config_cache:
            StorageRouterClient._config_cache[self.vpool_guid] = {}
        if volume_id not in StorageRouterClient._config_cache[self.vpool_guid]:
            StorageRouterClient._config_cache[self.vpool_guid][volume_id] = {}
        StorageRouterClient._config_cache[self.vpool_guid][volume_id]['tlog_multiplier'] = multiplier

    def set_volume_as_template(self, volume_id, req_timeout_secs=None):
        """
        Set a volume as a template
        """
        _ = req_timeout_secs
        # Converting to template results in only latest snapshot to be kept
        timestamp = 0
        newest_snap_id = None
        for snap_id, snap_info in StorageRouterClient._snapshots[self.vpool_guid].get(volume_id, {}).iteritems():
            metadata = pickle.loads(snap_info.metadata)
            if metadata['timestamp'] > timestamp:
                timestamp = metadata['timestamp']
                newest_snap_id = snap_id
        if newest_snap_id is not None:
            for snap_id in copy.deepcopy(StorageRouterClient._snapshots[self.vpool_guid].get(volume_id, {})).iterkeys():
                if snap_id != newest_snap_id:
                    StorageRouterClient._snapshots[self.vpool_guid][volume_id].pop(snap_id)
        StorageRouterClient.object_type[self.vpool_guid][volume_id] = 'TEMPLATE'

    def unlink(self, devicename, req_timeout_secs=None):
        """
        Delete a volume
        """
        _ = req_timeout_secs
        for volume_id, volume_info in StorageRouterClient.volumes[self.vpool_guid].iteritems():
            if volume_info['target_path'] == devicename:
                StorageRouterClient._clean(self.vpool_guid, volume_id)
                break

    def update_metadata_backend_config(self, volume_id, metadata_backend_config, req_timeout_secs=None):
        """
        Stores the given config
        """
        _ = req_timeout_secs
        from ovs.extensions.storageserver.storagedriver import MetadataServerClient
        StorageRouterClient._metadata_backend_config[self.vpool_guid][volume_id] = metadata_backend_config
        config_stream = []
        configs = metadata_backend_config.node_configs()
        if len(configs) == 0:
            raise RuntimeError('At least one config should be passed')
        master = configs[0]
        for config in configs:
            config_stream.append('{0}:{1}'.format(config.address(), config.port()))
        StorageRouterClient.mds_recording.append(config_stream)
        client = MDSClient(master)
        client.set_role(volume_id, MetadataServerClient.MDS_ROLE.MASTER, _internal=True)

    def migrate(self, volume_id, node_id, force_restart, req_timeout_secs=None):
        """
        Dummy migrate method
        """
        _ = force_restart, req_timeout_secs
        from ovs.dal.lists.storagedriverlist import StorageDriverList

        storagedriver = StorageDriverList.get_by_storagedriver_id(node_id)
        if storagedriver is None:
            raise ValueError('Failed to retrieve storagedriver with ID {0}'.format(node_id))
        StorageRouterClient.vrouter_id[self.vpool_guid][volume_id] = node_id

    def update_cluster_node_configs(self, vrouter_id, req_timeout_secs=None):
        """
        Dummy update_cluster_node_configs
        """
        _ = self, req_timeout_secs
        StorageRouterClient.node_config_recordings.append(vrouter_id)

    EMPTY_INFO = empty_info


class ObjectRegistryClient(object):
    """
    Mocks the ObjectRegistryClient
    """

    def __init__(self, vrouter_cluster_id, arakoon_cluster_id, arakoon_node_configs):
        """
        Initializes a Mocked ObjectRegistryClient
        """
        self.vpool_guid = vrouter_cluster_id
        _ = arakoon_cluster_id, arakoon_node_configs

    def get_all_registrations(self):
        """
        Retrieve all Object Registration objects for all volumes
        """
        registrations = []
        for volume_id in StorageRouterClient.volumes[self.vpool_guid].iterkeys():
            registrations.append(ObjectRegistration(
                StorageRouterClient.vrouter_id[self.vpool_guid][volume_id],
                volume_id,
                StorageRouterClient.object_type[self.vpool_guid].get(volume_id, 'BASE')
            ))
        return registrations

    def find(self, volume_id):
        """
        Find Object Registration based on volume ID
        """
        volumes = StorageRouterClient.volumes[self.vpool_guid]
        if volume_id in volumes:
            return ObjectRegistration(
                StorageRouterClient.vrouter_id[self.vpool_guid][volume_id],
                volume_id,
                StorageRouterClient.object_type[self.vpool_guid].get(volume_id, 'BASE')
            )
        return None


class FileSystemMetaDataClient(object):
    """
    Mocks the FileSystemMetaDataClient
    """

    def __init__(self, vrouter_cluster_id, arakoon_cluster_id, arakoon_node_configs):
        """
        Initializes a Mocked FileSystemMetaDataClient
        """
        self.vpool_guid = vrouter_cluster_id
        _ = arakoon_cluster_id, arakoon_node_configs

    def lookup(self, volume_id):
        """
        Gets the devicename corresponding with a given Volume ID
        """
        if volume_id not in StorageRouterClient.volumes[self.vpool_guid]:
            raise ObjectNotFoundException(volume_id)
        return StorageRouterClient.volumes[self.vpool_guid][volume_id]['target_path']


class MDSClient(object):
    """
    Mocks the Metadata Server Client
    """
    _catchup = {}
    _catchup_hook = {}
    _roles = {}

    def __init__(self, mds_node_config, timeout_secs=20, key=None):
        """
        Dummy init method
        """
        _ = timeout_secs
        if key is None:
            self.key = '{0}:{1}'.format(mds_node_config.address(), mds_node_config.port())
        else:
            self.key = key

    @staticmethod
    def _clean():
        """
        Clean everything up from previous runs
        """
        MDSClient._catchup = {}
        MDSClient._roles = {}

    def catch_up(self, nspace, dry_run):
        """
        Dummy catchup
        """
        if self.key not in MDSClient._catchup:
            MDSClient._catchup[self.key] = {}
        if nspace not in MDSClient._catchup[self.key]:
            raise RuntimeError('Namespace does not exist')
        if dry_run is False:
            if self.key in MDSClient._catchup_hook:
                if nspace in MDSClient._catchup_hook[self.key]:
                    MDSClient._catchup_hook[self.key][nspace]()
            MDSClient._catchup[self.key][nspace] = 0
        return MDSClient._catchup[self.key][nspace]

    @staticmethod
    def get_tlogs_behind(key, nspace):
        if key not in MDSClient._catchup:
            MDSClient._catchup[key] = {}
        if nspace not in MDSClient._catchup[key]:
            raise RuntimeError('Namespace does not exist')
        return MDSClient._catchup[key][nspace]

    def create_namespace(self, nspace):
        """
        Dummy create namespace method
        """
        if self.key not in MDSClient._catchup:
            MDSClient._catchup[self.key] = {}
        MDSClient._catchup[self.key][nspace] = 0
        if self.key not in MDSClient._roles:
            MDSClient._roles[self.key] = {}
        MDSClient._roles[self.key][nspace] = None

    def list_namespaces(self):
        """
        Dummy list namespaces method
        """
        if self.key not in MDSClient._catchup:
            return []
        return MDSClient._catchup[self.key].keys()

    def remove_namespace(self, nspace):
        """
        Dummy remove namespace method
        """
        if self.key not in MDSClient._catchup:
            MDSClient._catchup[self.key] = {}
        if nspace in MDSClient._catchup[self.key]:
            del MDSClient._catchup[self.key][nspace]
        else:
            raise RuntimeError('Namespace does not exist')
        if self.key not in MDSClient._roles:
            MDSClient._roles[self.key] = {}
        if nspace in MDSClient._roles[self.key]:
            del MDSClient._roles[self.key][nspace]
        else:
            raise RuntimeError('Namespace does not exist')

    def _has_namespace(self, volume_id):
        """
        Indicates a namespace exist
        """
        return volume_id in MDSClient._catchup.get(self.key, {}) and volume_id in MDSClient._roles.get(self.key, {})

    def set_role(self, nspace, role, _internal=False):
        """
        Dummy set role method
        """
        if self.key not in MDSClient._catchup:
            MDSClient._roles[self.key] = {}
        if nspace not in MDSClient._roles[self.key]:
            raise RuntimeError('Namespace does not exist')
        MDSClient._roles[self.key][nspace] = role
        StorageRouterClient.mds_recording.append('{0}: {1} ({2})'.format(self.key, role, 'I' if _internal is True else 'E'))

    def get_role(self, nspace):
        """
        Gets the role for a volume
        """
        if self.key not in MDSClient._catchup:
            MDSClient._roles[self.key] = {}
        if nspace not in MDSClient._roles[self.key]:
            raise RuntimeError('Namespace does not exist')
        return MDSClient._roles[self.key][nspace]

    @staticmethod
    def set_catchup(key, volume_id, tlogs):
        """
        Sets the fake catchup work
        """
        if key not in MDSClient._catchup:
            MDSClient._catchup[key] = {}
        MDSClient._catchup[key][volume_id] = tlogs

    @staticmethod
    def set_catchup_hook(key, volume_id, callback):
        """
        Set a callback hook for the catchup
        """
        if key not in MDSClient._catchup_hook:
            MDSClient._catchup_hook[key] = {}
        MDSClient._catchup_hook[key][volume_id] = callback


class LockedClient(object):
    """
    The locked client class which is used in vdisk.storagedriver_client.make_locked_client
    """
    thread_names = []
    scrub_controller = {}

    def __init__(self, volume_id):
        self.volume_id = volume_id

    def __enter__(self):
        return self

    def __exit__(self, nspace, update_interval_secs, grace_period_secs):
        pass

    def get_scrubbing_workunits(self):
        """
        Retrieve the amount of scrub work to be done
        """
        return LockedClient.scrub_controller['volumes'][self.volume_id]['scrub_work']

    def scrub(self, *args, **kwargs):
        """
        Scrub mock
        """
        _ = args, kwargs
        return len(LockedClient.scrub_controller['volumes'][self.volume_id]['scrub_work'])

    def apply_scrubbing_result(self, scrubbing_work_result):
        """
        Apply scrubbing result
        """
        _ = scrubbing_work_result
        LockedClient.scrub_controller['waiter'].wait()
        thread_name = threading.current_thread().getName()
        possible_thread_names = LockedClient.scrub_controller['possible_threads']
        if possible_thread_names is not None:
            assert thread_name in possible_thread_names
        if thread_name in LockedClient.thread_names:
            LockedClient.thread_names.remove(thread_name)
        if LockedClient.scrub_controller['volumes'][self.volume_id]['success'] is False:
            raise Exception('Raising exception while scrubbing')
        LockedClient.scrub_controller['volumes'][self.volume_id]['scrub_work'] = []


class Snapshot(object):
    """
    Dummy snapshot class
    """
    def __init__(self, metadata):
        """
        Init method
        """
        mandatory_keys = ['label', 'timestamp', 'is_consistent']
        optional_keys = {'in_backend', 'is_sticky', 'machineguid', 'is_automatic'}
        metadata_dict = pickle.loads(metadata)
        copy_metadata = copy.deepcopy(metadata_dict)

        for key in mandatory_keys:
            if key not in copy_metadata:
                raise ValueError('Expected key {0} to be provided to create a snapshot'.format(key))
            copy_metadata.pop(key)

        difference = set(copy_metadata.keys()).difference(optional_keys)
        if difference:
            raise ValueError('Unexpected keys provided to create a snapshot: {0}'.format(difference))

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

    def __repr__(self):
        """
        Representation
        """
        return json.dumps({'host': self.host,
                           'port': self.port,
                           'mode': self.mode,
                           'dtl_config_mode': self.dtl_config_mode})


class ObjectRegistration(object):
    """
    Mocked ObjectRegistration
    """
    def __init__(self, node_id, object_id, object_type):
        self._node_id = node_id
        self._object_id = object_id
        self._object_type = object_type

    def node_id(self):
        """
        Node ID
        """
        return self._node_id

    def object_id(self):
        """
        Object ID
        """
        return self._object_id

    def object_type(self):
        """
        Object Type
        """
        return self._object_type


class ArakoonNodeConfig(object):
    """
    Mocked ArakoonNodeConfig
    """
    def __init__(self, *args, **kwargs):
        _ = args, kwargs


class ClusterRegistry(object):
    """
    Mocked ClusterRegistry
    """
    _registry = {}

    def __init__(self, vfs_cluster_id, ara_cluster_id, ara_node_configs, ara_timeout_secs=None):
        _ = ara_cluster_id, ara_node_configs, ara_timeout_secs
        self._cluster_id = vfs_cluster_id
        if self._cluster_id not in ClusterRegistry._registry:
            ClusterRegistry._registry[self._cluster_id] = {'node_configs': []}

    def get_node_configs(self):
        """
        Retrieve the node configurations
        :return: Dictionary with the node configurations
        :rtype: dict
        """
        return ClusterRegistry._registry[self._cluster_id]['node_configs']

    def set_node_configs(self, configs):
        """
        Set the node configurations
        :param configs: Configuration to set in the registry
        :type configs: dict
        :return: None
        """
        ClusterRegistry._registry[self._cluster_id]['node_configs'] = configs
