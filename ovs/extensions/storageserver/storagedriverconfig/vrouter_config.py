from ovs.extensions.storageserver.storagedriverconfig.generic_config import GenericConfig


class VRouterConfig(GenericConfig):
    def __init__(self,vrouter_id, vregistry_arakoon_cluster_id, vrouter_max_workers=None, vrouter_min_workers=None, vrouter_use_fencing=None,
                 vrouter_sco_multiplier=None, vrouter_routing_retries=None, vrouter_redirect_retries=None, vrouter_local_io_retries=None, vrouter_keepalive_retries=None,
                 vrouter_send_sync_response=None, vrouter_migrate_timeout_ms=None, vrouter_keepalive_time_secs=None, vrouter_file_read_threshold=None,
                 vrouter_redirect_timeout_ms=None, vrouter_file_write_threshold=None, vrouter_volume_read_threshold=None, vrouter_volume_write_threshold=None,
                 vrouter_backend_sync_timeout_ms=None, vrouter_registry_cache_capacity=None, vrouter_keepalive_interval_secs=None,
                 vrouter_local_io_sleep_before_retry_usecs=None, vrouter_check_local_volume_potential_period=None, **kwargs):
        self.vrouter_id = vrouter_id
        self.vregistry_arakoon_cluster_id = vregistry_arakoon_cluster_id
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
