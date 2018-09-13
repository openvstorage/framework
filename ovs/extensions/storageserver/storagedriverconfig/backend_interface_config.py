from ovs.extensions.storageserver.storagedriverconfig.generic_config import GenericConfig

class BackendInterfaceConfig(GenericConfig):
    def __init__(self, backend_connection_pool_capacity=None, backend_interface_retries_on_error=None, backend_interface_retry_interval_secs=None, backend_interface_retry_interval_max_secs=None,
                 backend_connection_pool_blacklist_secs=None, backend_interface_retry_backoff_multiplier=None, backend_interface_partial_read_retries_on_error=None,
                 backend_interface_partial_read_timeout_msecs=None, backend_interface_partial_read_timeout_max_msecs=None, backend_interface_partial_read_timeout_multiplier=None, backend_interface_partial_read_retry_interval_msecs=None,
                 backend_interface_partial_read_retry_interval_max_msecs=None, backend_interface_partial_read_retry_backoff_multiplier=None, **kwargs):
        """
        Initiate the volumedriverfs config: backend_interface
        :param backend_connection_pool_capacity: Capacity of the connection pool maintained by the BackendConnectionManager
        :param backend_interface_retries_on_error: How many times to retry a failed backend operation
        :param backend_interface_retry_interval_secs: delay before retrying a failed backend operation in seconds
        :param backend_interface_retry_interval_max_secs:  max delay before retrying a failed backend operation in seconds
        :param backend_connection_pool_blacklist_secs: Duration (in seconds) in which to skip a connection pool after an error
        :param backend_interface_retry_backoff_multiplier: multiplier for the retry interval on each subsequent retry
        :param backend_interface_partial_read_timeout_msecs: timeout for a partial read operation (milliseconds)
        :param backend_interface_partial_read_retries_on_error: How many times to retry a failed partial read operation
        :param backend_interface_partial_read_timeout_max_msecs:  max timeout for a partial read operation on retry (milliseconds)
        :param backend_interface_partial_read_timeout_multiplier: multiplier for the partial read timeout on each subsequent retry
        :param backend_interface_partial_read_retry_interval_msecs: delay before retrying a failed partial read in milliseconds
        :param backend_interface_partial_read_retry_interval_max_msecs: max delay before retrying a failed partial read in milliseconds
        :param backend_interface_partial_read_retry_backoff_multiplier: multiplier for the retry interval on each subsequent retry (< 0 -> backend_interface_retry_backoff_multiplier is used)
        :param kwargs:
        """
        self.backend_connection_pool_capacity = backend_connection_pool_capacity
        self.backend_interface_retries_on_error = backend_interface_retries_on_error
        self.backend_interface_retry_interval_secs = backend_interface_retry_interval_secs
        self.backend_connection_pool_blacklist_secs = backend_connection_pool_blacklist_secs
        self.backend_interface_retry_interval_max_secs = backend_interface_retry_interval_secs
        self.backend_interface_retry_interval_max_secs = backend_interface_retry_interval_max_secs
        self.backend_interface_retry_backoff_multiplier = backend_interface_retry_backoff_multiplier
        self.backend_interface_partial_read_timeout_msecs = backend_interface_partial_read_timeout_msecs
        self.backend_interface_partial_read_retries_on_error = backend_interface_partial_read_retries_on_error
        self.backend_interface_partial_read_timeout_max_msecs = backend_interface_partial_read_timeout_max_msecs
        self.backend_interface_partial_read_timeout_multiplier = backend_interface_partial_read_timeout_multiplier
        self.backend_interface_partial_read_retry_interval_msecs = backend_interface_partial_read_retry_interval_msecs
        self.backend_interface_partial_read_retry_interval_max_msecs = backend_interface_partial_read_retry_interval_max_msecs
        self.backend_interface_partial_read_retry_backoff_multiplier = backend_interface_partial_read_retry_backoff_multiplier