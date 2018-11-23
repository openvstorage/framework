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

from .base import BaseStorageDriverConfig


class FilesystemConfig(BaseStorageDriverConfig):
    """
    Filesystem container of the storagedriver config
    """
    def __init__(self, fs_virtual_disk_format, fs_dtl_host=None, fs_dtl_port=None, fs_dtl_mode=None, fs_ignore_sync=None, fs_max_open_files=None, fs_cache_dentries=None, fs_raw_disk_suffix=None,
                 fs_dtl_config_mode=None, fs_file_event_rules=None, fs_enable_shm_interface=None, fs_metadata_backend_type=None,
                 fs_enable_network_interface=None, fs_metadata_backend_mds_nodes=None, fs_metadata_backend_mds_timeout_secs=None, fs_metadata_backend_arakoon_cluster_id=None,
                 fs_metadata_backend_arakoon_cluster_nodes=None, fs_metadata_backend_mds_slave_max_tlogs_behind=None, fs_metadata_backend_mds_apply_relocations_to_slaves=None, *args, **kwargs):
        """
        Initiate the config of the volumedriver_fs: filesystem
        :param fs_virtual_disk_format: virtual disk format (raw)
        :param fs_dtl_host: DTL host
        :param fs_dtl_port: DTL port
        :param fs_dtl_mode: DTL mode : Asynchronous | Synchronous
        :param fs_ignore_sync: ignore sync requests - AT THE POTENTIAL EXPENSE OF DATA LOSS
        :param fs_max_open_files: Maximum number of open files, is set using rlimit() on startup
        :param fs_cache_dentries: whether to cache directory entries locally
        :param fs_raw_disk_suffix: Suffix to use when creating clones if fs_virtual_disk_format=raw
        :param fs_dtl_config_mode: Configuration mode : Automatic | Manual
        :param fs_file_event_rules: an array of filesystem event rules, each consisting of a "path_regex" and an array of "fs_calls"
        :param fs_enable_shm_interface:  Whether to enable the SHM interface
        :param fs_metadata_backend_type: Type of metadata backend to use for volumes created via the filesystem interface
        :param fs_enable_network_interface: Whether to enable the network interface
        :param fs_metadata_backend_mds_nodes: an array of MDS node configurations for the volume metadata, each containing host and port
        :param fs_metadata_backend_mds_timeout_secs: timeout (in seconds) for calls to MDS servers
        :param fs_metadata_backend_arakoon_cluster_id: Arakoon cluster identifier for the volume metadata
        :param fs_metadata_backend_arakoon_cluster_nodes: an array of arakoon cluster node configurations for the volume metadata, each containing node_id, host and port
        :param fs_metadata_backend_mds_slave_max_tlogs_behind: max number of TLogs a slave is allowed to run behind to still permit a failover to it
        :param fs_metadata_backend_mds_apply_relocations_to_slaves: a bool indicating whether to apply relocations to slave MDS tables
        """
        self.fs_dtl_host = fs_dtl_host
        self.fs_dtl_port = fs_dtl_port
        self.fs_dtl_mode = fs_dtl_mode
        self.fs_ignore_sync = fs_ignore_sync
        self.fs_max_open_files = fs_max_open_files
        self.fs_cache_dentries = fs_cache_dentries
        self.fs_raw_disk_suffix = fs_raw_disk_suffix
        self.fs_dtl_config_mode = fs_dtl_config_mode
        self.fs_file_event_rules = fs_file_event_rules
        self.fs_virtual_disk_format = fs_virtual_disk_format
        self.fs_enable_shm_interface = fs_enable_shm_interface
        self.fs_metadata_backend_type = fs_metadata_backend_type
        self.fs_enable_network_interface = fs_enable_network_interface
        self.fs_metadata_backend_mds_nodes = fs_metadata_backend_mds_nodes
        self.fs_metadata_backend_mds_timeout_secs = fs_metadata_backend_mds_timeout_secs
        self.fs_metadata_backend_arakoon_cluster_id = fs_metadata_backend_arakoon_cluster_id
        self.fs_metadata_backend_arakoon_cluster_nodes = fs_metadata_backend_arakoon_cluster_nodes
        self.fs_metadata_backend_mds_slave_max_tlogs_behind = fs_metadata_backend_mds_slave_max_tlogs_behind
        self.fs_metadata_backend_mds_apply_relocations_to_slaves = fs_metadata_backend_mds_apply_relocations_to_slaves
