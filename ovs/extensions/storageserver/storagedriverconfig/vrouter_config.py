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

from ovs.extensions.storageserver.storagedriverconfig.generic_config import GenericConfig


class VRouterConfig(GenericConfig):
    """
    VRouterconfig container of the storagedriver config
    """
    def __init__(self,vrouter_id, vrouter_max_workers=None, vrouter_min_workers=None, vrouter_use_fencing=None,
                 vrouter_sco_multiplier=None, vrouter_routing_retries=None, vrouter_redirect_retries=None, vrouter_local_io_retries=None, vrouter_keepalive_retries=None,
                 vrouter_send_sync_response=None, vrouter_migrate_timeout_ms=None, vrouter_keepalive_time_secs=None, vrouter_file_read_threshold=None,
                 vrouter_redirect_timeout_ms=None, vrouter_file_write_threshold=None, vrouter_volume_read_threshold=None, vrouter_volume_write_threshold=None,
                 vrouter_backend_sync_timeout_ms=None, vrouter_registry_cache_capacity=None, vrouter_keepalive_interval_secs=None,
                 vrouter_local_io_sleep_before_retry_usecs=None, vrouter_check_local_volume_potential_period=None, *args, **kwargs):
        """

        :param vrouter_id: the vrouter_id of this node of the cluster. Must be one of the vrouter_id's s specified in the vrouter_cluster_nodes section
        :param vrouter_max_workers: maximum number of worker threads to handle redirected requests
        :param vrouter_min_workers: minimal number of worker threads to handle redirected requests
        :param vrouter_use_fencing: whether to use fencing support if it is available
        :param vrouter_sco_multiplier: number of clusters in a sco
        :param vrouter_routing_retries: number of times the routing shall be retried in case the volume is not found (exponential backoff inbetween retries!)
        :param vrouter_redirect_retries: number of retries to after a redirect timed out
        :param vrouter_local_io_retries: number of retry attempts for requests that failed with a retryable error
        :param vrouter_keepalive_retries: number of unacknowledged probes before considering the other side dead
        :param vrouter_send_sync_response: whether to send extended response data on sync requests
        :param vrouter_migrate_timeout_ms: timeout for migration requests in milliseconds (in addition to remote backend sync timeout!)
        :param vrouter_keepalive_time_secs: time between two keepalive probe cycles in seconds (0 switches keepalive off)
        :param vrouter_file_read_threshold: number of remote read requests before auto-migrating a file - 0 turns it off
        :param vrouter_redirect_timeout_ms: timeout for redirected requests in milliseconds - 0 turns it off
        :param vrouter_file_write_threshold: number of remote write requests before auto-migrating a file - 0 turns it off
        :param vrouter_volume_read_threshold: number of remote read requests before auto-migrating a volume - 0 turns it off
        :param vrouter_volume_write_threshold: number of remote write requests before auto-migrating a volume - 0 turns it off
        :param vrouter_backend_sync_timeout_ms: timeout for remote backend syncs (during migration) - 0 turns it off
        :param vrouter_registry_cache_capacity: number of ObjectRegistrations to keep cached
        :param vrouter_keepalive_interval_secs: time (seconds) between probes of a cycle if the previous one was unacknowledged
        :param vrouter_local_io_sleep_before_retry_usecs: delay (microseconds) before rerrying a request that failed with a retryable error
        :param vrouter_check_local_volume_potential_period: how often to recheck the local volume potential during migration
        """
        self.vrouter_id = vrouter_id
        self.vrouter_max_workers = vrouter_max_workers
        self.vrouter_min_workers = vrouter_min_workers
        self.vrouter_use_fencing = vrouter_use_fencing
        self.vrouter_sco_multiplier = vrouter_sco_multiplier
        self.vrouter_routing_retries = vrouter_routing_retries
        self.vrouter_redirect_retries = vrouter_redirect_retries
        self.vrouter_local_io_retries = vrouter_local_io_retries
        self.vrouter_keepalive_retries = vrouter_keepalive_retries
        self.vrouter_send_sync_response = vrouter_send_sync_response
        self.vrouter_migrate_timeout_ms = vrouter_migrate_timeout_ms
        self.vrouter_keepalive_time_secs = vrouter_keepalive_time_secs
        self.vrouter_file_read_threshold = vrouter_file_read_threshold
        self.vrouter_redirect_timeout_ms = vrouter_redirect_timeout_ms
        self.vrouter_file_write_threshold = vrouter_file_write_threshold
        self.vrouter_volume_read_threshold = vrouter_volume_read_threshold
        self.vrouter_volume_write_threshold = vrouter_volume_write_threshold
        self.vrouter_backend_sync_timeout_ms = vrouter_backend_sync_timeout_ms
        self.vrouter_registry_cache_capacity = vrouter_registry_cache_capacity
        self.vrouter_keepalive_interval_secs = vrouter_keepalive_interval_secs
        self.vrouter_local_io_sleep_before_retry_usecs = vrouter_local_io_sleep_before_retry_usecs
        self.vrouter_check_local_volume_potential_period = vrouter_check_local_volume_potential_period
