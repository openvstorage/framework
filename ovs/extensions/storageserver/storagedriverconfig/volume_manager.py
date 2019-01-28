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


class VolumeManagerConfig(BaseStorageDriverConfig):
    """
    Volumemanager config container of the storagedriver config
    """

    component_identifier = 'volume_manager'

    def __init__(self, metadata_path, tlog_path, clean_interval=1, default_cluster_size=None, dtl_throttle_usecs=4000, non_disposable_scos_factor=None,
                 number_of_scos_in_tlog=None, read_cache_default_behaviour=None, read_cache_default_mode=None, sap_persist_interval=None,
                 sco_written_to_backend_action=None, required_tlog_freespace=None, required_meta_freespace=None, freespace_check_interval=None,
                 metadata_cache_capacity=None, metadata_mds_slave_max_tlogs_behind=None, arakoon_metadata_sequence_size=None, open_scos_per_volume=None,
                 dtl_busy_loop_usecs=None,  dtl_queue_depth=None, dtl_write_trigger=None, dtl_request_timeout_ms=None,
                 dtl_connect_timeout_ms=None, dtl_check_interval_in_seconds=None, *args,**kwargs):
        """
        Initiate the volumedriverfs config: volumemanager
        :param metadata_path: Directory, where to create subdirectories in for volume metadata storage
        :param tlog_path: Directory, where to create subdirectories for volume tlogs
        :param clean_interval: Interval between runs of scocache cleanups, in seconds. scocache_cleanup_trigger / clean_interval should be larger than the aggregated write speed to the scocache. Defaults to 1
        :param default_cluster_size:  size of a cluster in bytes
        :param non_disposable_scos_factor:  Factor to multiply number_of_scos_in_tlog with to determine the amount of non-disposable data permitted per volume
        :param number_of_scos_in_tlog: The number of SCOs that trigger a tlog rollover
        :param read_cache_default_behaviour: Default read cache behaviour, should be CacheOnWrite, CacheOnRead or NoCache
        :param read_cache_default_mode: Default read cache mode, should be ContentBased or LocationBased
        :param sap_persist_interval: Interval between writing SAP data, in seconds
        :param sco_written_to_backend_action: Default SCO cache behaviour (SetDisposable, SetDisposableAndPurgeFromPageCache, PurgeFromSCOCache)
        :param required_tlog_freespace:  Required free space in the tlog directory
        :param required_meta_freespace:  Required free space in the metadata directory
        :param freespace_check_interval: Interval between checks of required freespace parameters, in seconds
        :param metadata_cache_capacity: number of metadata pages to keep cached
        :param metadata_mds_slave_max_tlogs_behind: max number of TLogs a slave is allowed to run behind to still permit a failover to it
        :param arakoon_metadata_sequence_size: Size of Arakoon sequences used to send metadata pages to Arakoon
        :param open_scos_per_volume:  Number of open SCOs per volume
        :param dtl_throttle_usecs: Timeout for retrying writes to the DTL. Defaults to 4000
        :param dtl_queue_depth:  Size of the queue of entries to be sent to the DTL
        :param dtl_write_trigger: Trigger to start writing entries in the foc queue to the backend
        :param dtl_request_timeout_ms:  Timeout for DTL requests
        :param dtl_connect_timeout_ms: Timeout for connection attempts to the DTL - 0: wait forever / the OS to signal errors
        :param dtl_check_interval_in_seconds: Interval between checks of the DTL state of volumes
        """
        self.clean_interval = clean_interval
        self.metadata_path = metadata_path
        self.tlog_path = tlog_path
        self.default_cluster_size = default_cluster_size
        self.non_disposable_scos_factor = non_disposable_scos_factor
        self.number_of_scos_in_tlog = number_of_scos_in_tlog
        self.read_cache_default_behaviour = read_cache_default_behaviour
        self.read_cache_default_mode = read_cache_default_mode
        self.sap_persist_interval = sap_persist_interval
        self.sco_written_to_backend_action = sco_written_to_backend_action
        self.required_tlog_freespace = required_tlog_freespace
        self.required_meta_freespace = required_meta_freespace
        self.freespace_check_interval = freespace_check_interval
        self.metadata_cache_capacity = metadata_cache_capacity
        self.metadata_mds_slave_max_tlogs_behind = metadata_mds_slave_max_tlogs_behind
        self.arakoon_metadata_sequence_size = arakoon_metadata_sequence_size
        self.open_scos_per_volume = open_scos_per_volume
        self.dtl_throttle_usecs = dtl_throttle_usecs
        self.dtl_busy_loop_usecs = dtl_busy_loop_usecs
        self.dtl_check_interval_in_seconds = dtl_check_interval_in_seconds
        self.dtl_connect_timeout_ms = dtl_connect_timeout_ms
        self.dtl_queue_depth = dtl_queue_depth
        self.dtl_request_timeout_ms = dtl_request_timeout_ms
        self.dtl_write_trigger = dtl_write_trigger
