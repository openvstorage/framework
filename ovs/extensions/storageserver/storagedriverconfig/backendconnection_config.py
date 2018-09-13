from ovs.extensions.storageserver.storagedriverconfig.generic_config import GenericConfig

class BackendConnectionConfig(GenericConfig):
    def __init__(self, alba_connection_host=None, alba_connection_port=None, alba_connection_preset=None, alba_connection_timeout=None, alba_connection_use_rora=None,
                 alba_connection_transport=None, alba_connection_rora_timeout_msecs=None, alba_connection_rora_manifest_cache_capacity=None, alba_connection_asd_connection_pool_capacity=None,
                 backend_type=None, **kwargs):
        """
        Initiate the volumedriverfs config: Backend_connection

        :param alba_connection_host: When backend_type is ALBA: the ALBA host to connect to, otherwise ignored
        :param alba_connection_port: When backend_type is ALBA: The ALBA port to connect to, otherwise ignored
        :param alba_connection_preset: When backend_type is ALBA: the ALBA preset to use for new namespaces
        :param alba_connection_timeout: The timeout for the ALBA proxy, in seconds
        :param alba_connection_use_rora: Whether to enable Read Optimized RDMA ASD (RORA) support
        :param alba_connection_transport: When backend_type is ALBA: the ALBA connection to use: TCP (default) or RDMA
        :param alba_connection_rora_timeout_msecs: Timeout for RORA (fast path) partial reads (milliseconds)
        :param alba_connection_rora_manifest_cache_capacity: Capacity of the RORA fetcher's manifest cache
        :param alba_connection_asd_connection_pool_capacity: connection pool (per ASD) capacity
        :param backend_type: Type of backend connection one of ALBA, LOCAL, MULTI or S3, the other parameters in this section are only used when their correct backendtype is set
        :param kwargs:
        """
        self.alba_connection_host = alba_connection_host
        self.alba_connection_port = alba_connection_port
        self.alba_connection_preset = alba_connection_preset
        self.alba_connection_timeout = alba_connection_timeout
        self.alba_connection_use_rora = alba_connection_use_rora
        self.alba_connection_transport = alba_connection_transport
        self.alba_connection_rora_timeout_msecs = alba_connection_rora_timeout_msecs
        self.alba_connection_rora_manifest_cache_capacity = alba_connection_rora_manifest_cache_capacity
        self.alba_connection_asd_connection_pool_capacity = alba_connection_asd_connection_pool_capacity
        self.backend_type = backend_type

