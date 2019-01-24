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

import re
from .base import BaseStorageDriverConfig
from ovs.dal.hybrids.vpool import VPool
from ovs_extensions.constants.storagedriver import VOLDRV_DTL_TRANSPORT_TCP
from ovs_extensions.generic.toolbox import ExtensionsToolbox


class BackendConnectionManager(BaseStorageDriverConfig):
    """
    Backendinterface container of the storagedriver config
    """

    component_identifier = 'backend_connection_manager'

    def __init__(self, vpool, storage_ip, alba_proxies,
                 local_connection_path=None, backend_connection_pool_capacity=None, backend_interface_retries_on_error=None, backend_interface_retry_interval_secs=1, backend_interface_retry_interval_max_secs=None,
                 backend_connection_pool_blacklist_secs=None, backend_interface_retry_backoff_multiplier=2.0, backend_interface_partial_read_retries_on_error=5,
                 backend_interface_partial_read_timeout_msecs=None, backend_interface_partial_read_timeout_max_msecs=None, backend_interface_partial_read_timeout_multiplier=None, backend_interface_partial_read_retry_interval_msecs=None,
                 backend_interface_partial_read_retry_interval_max_msecs=None, backend_interface_partial_read_retry_backoff_multiplier=None,  backend_type='MULTI', *args, **kwargs):
        """
        Initiate the volumedriverfs config: backend_interface
        :param local_connection_path: When backend_type is LOCAL: path to use as LOCAL backend, otherwise ignored
        :param backend_connection_pool_capacity: Capacity of the connection pool maintained by the BackendConnectionManager
        :param backend_interface_retries_on_error: How many times to retry a failed backend operation
        :param backend_interface_retry_interval_secs: delay before retrying a failed backend operation in seconds. Defaults to 1
        :param backend_interface_retry_interval_max_secs:  max delay before retrying a failed backend operation in seconds
        :param backend_connection_pool_blacklist_secs: Duration (in seconds) in which to skip a connection pool after an error
        :param backend_interface_retry_backoff_multiplier: multiplier for the retry interval on each subsequent retry. Defaults to 2.0
        :param backend_interface_partial_read_timeout_msecs: timeout for a partial read operation (milliseconds)
        :param backend_interface_partial_read_retries_on_error: How many times to retry a failed partial read operation. Defaults to 5
        :param backend_interface_partial_read_timeout_max_msecs:  max timeout for a partial read operation on retry (milliseconds)
        :param backend_interface_partial_read_timeout_multiplier: multiplier for the partial read timeout on each subsequent retry
        :param backend_interface_partial_read_retry_interval_msecs: delay before retrying a failed partial read in milliseconds
        :param backend_interface_partial_read_retry_interval_max_msecs: max delay before retrying a failed partial read in milliseconds
        :param backend_interface_partial_read_retry_backoff_multiplier: multiplier for the retry interval on each subsequent retry (< 0 -> backend_interface_retry_backoff_multiplier is used)
        :param backend_type: Type of backend connection one of ALBA, LOCAL, MULTI or S3, the other parameters in this section are only used when their correct backendtype is set. Defaults to MULTI

        """
        self.backend_type = backend_type
        if self.backend_type == 'LOCAL' and local_connection_path is None:
            raise RuntimeError('Local_connection_path needs to be provided if backendtype is LOCAL')

        if not isinstance(self._vpool, VPool):
            raise RuntimeError('Backendconnection config needs a VPool instance to construct the storagedriverconfig')
        if not re.match(storage_ip, ExtensionsToolbox.regex_ip):
            raise RuntimeError('Backendconnection config needs a valid IP to construct its storagedriverconfig')

        self._vpool = vpool
        self._storage_ip = storage_ip
        self._alba_proxies = alba_proxies

        self.local_connection_path = local_connection_path
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

        self.alba_connection_config = AlbaConnectionConfig(**kwargs)

    def to_dict(self):
        # type: () -> Dict[str, any]
        """
        Convert the current config set to a dictionary
        :return: Dict
        :rtype: Dict[str, any]
        """

        config = super(BackendConnectionManager, self).to_dict()  # Instantiate known config so far

        for index, proxy in enumerate(sorted(self._alba_proxies, key=lambda k: k.service.ports[0])):
            config[str(index)] = AlbaConnectionConfig(alba_connection_host=self._storage_ip,
                                                      alba_connection_port=proxy.service.ports[0],
                                                      alba_connection_preset=self._vpool.metadata['backend']['backend_info']['preset']).to_dict()
        return config


class AlbaConnectionConfig(BaseStorageDriverConfig):

    component_identifier = 'backend_connection_manager'

    def __init__(self, alba_connection_host=None, alba_connection_port=None, alba_connection_preset=None, alba_connection_timeout=30,
                 alba_connection_use_rora=True, alba_connection_transport=VOLDRV_DTL_TRANSPORT_TCP, alba_connection_rora_timeout_msecs=50,
                 alba_connection_rora_manifest_cache_capacity=25000, alba_connection_asd_connection_pool_capacity=10,
                 *args, **kwargs):
        """
        :param alba_connection_host: When backend_type is ALBA: the ALBA host to connect to, otherwise ignored
        :param alba_connection_port: When backend_type is ALBA: The ALBA port to connect to, otherwise ignored
        :param alba_connection_preset: When backend_type is ALBA: the ALBA preset to use for new namespaces
        :param alba_connection_timeout: The timeout for the ALBA proxy, in seconds
        :param alba_connection_use_rora: Whether to enable Read Optimized RDMA ASD (RORA) support
        :param alba_connection_transport: When backend_type is ALBA: the ALBA connection to use: TCP (default) or RDMA
        :param alba_connection_rora_timeout_msecs: Timeout for RORA (fast path) partial reads (milliseconds)
        :param alba_connection_rora_manifest_cache_capacity: Capacity of the RORA fetcher's manifest cache
        :param alba_connection_asd_connection_pool_capacity: connection pool (per ASD) capacity
        """
        self.backend_type = 'ALBA'
        self.alba_connection_host = alba_connection_host
        self.alba_connection_port = alba_connection_port
        self.alba_connection_preset = alba_connection_preset
        self.alba_connection_timeout = alba_connection_timeout
        self.alba_connection_use_rora = alba_connection_use_rora
        self.alba_connection_transport = alba_connection_transport
        self.alba_connection_rora_timeout_msecs = alba_connection_rora_timeout_msecs
        self.alba_connection_rora_manifest_cache_capacity = alba_connection_rora_manifest_cache_capacity
        self.alba_connection_asd_connection_pool_capacity = alba_connection_asd_connection_pool_capacity


class S3ConnectionConfig(object):

    component_identifier = 'backend_connection_manager'

    def __init__(self, s3_connection_host=None, s3_connection_port=None, s3_connection_use_ssl=None, s3_connection_flavour=None, s3_connection_username=None,
                 s3_connection_password=None, s3_connection_ssl_cert_file=None, s3_connection_ssl_verify_host=None, s3_connection_verbose_logging=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config: s3 manager
        :param s3_connection_host: When backend_type is S3: the S3 host to connect to, otherwise ignored
        :param s3_connection_port: When backend_type is S3: the S3 port to connect to, otherwise ignored
        :param s3_connection_use_ssl: When backend_type is S3: whether to use SSL to encrypt the connection
        :param s3_connection_flavour: S3 backend flavour: S3 (default), GCS, WALRUS or SWIFT
        :param s3_connection_username: When backend_type is S3: the S3 username, otherwise ignored
        :param s3_connection_password: When backend_type is S3: the S3 password
        :param s3_connection_ssl_cert_file: When backend_type is S3: path to a file holding the SSL certificate
        :param s3_connection_ssl_verify_host: When backend_type is S3: whether to verify the SSL certificate's subject against the host
        :param s3_connection_verbose_logging: When backend_type is S3: whether to do verbose logging
        """
        self.s3_connection_host = s3_connection_host
        self.s3_connection_port = s3_connection_port
        self.s3_connection_use_ssl = s3_connection_use_ssl
        self.s3_connection_flavour = s3_connection_flavour
        self.s3_connection_username = s3_connection_username
        self.s3_connection_password = s3_connection_password
        self.s3_connection_ssl_cert_file = s3_connection_ssl_cert_file
        self.s3_connection_ssl_verify_host = s3_connection_ssl_verify_host
        self.s3_connection_verbose_logging = s3_connection_verbose_logging
