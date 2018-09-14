# Copyright (C) 2018 iNuron NV
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
StorageDriverConfig
"""
import json
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.storageserver.storagedriverconfig.backend_config import BackendConfig
from ovs.extensions.storageserver.storagedriverconfig.filesystem_config import FilesystemConfig
from ovs.extensions.storageserver.storagedriverconfig.generic_config import GenericConfig
from ovs.extensions.storageserver.storagedriverconfig.vrouter_config import VRouterConfig
from ovs.extensions.storageserver.storagedriverconfig.volume_manager_config import VolumeManagerConfig


class VRegistryConfig(GenericConfig):
    def __init__(self, vregistry_arakoon_cluster_id, vregistry_arakoon_cluster_nodes, vregistry_arakoon_timeout_ms=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config: vregistry configuration
        :param vregistry_arakoon_cluster_id: Arakoon cluster identifier for the volume registry
        :param vregistry_arakoon_cluster_nodes:  an array of arakoon cluster node configurations for the volume registry, each containing node_id, host and port
        :param vregistry_arakoon_timeout_ms: Arakoon client timeout in milliseconds for the volume registry
        :param args:
        :param kwargs:
        """
        self.vregistry_arakoon_cluster_id = vregistry_arakoon_cluster_id
        self.vregistry_arakoon_cluster_nodes = vregistry_arakoon_cluster_nodes
        self.vregistry_arakoon_timeout_ms = vregistry_arakoon_timeout_ms

class DtlConfig(GenericConfig):

    def __init__(self, dtl_path, dtl_transport=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config: dtl configuration
        :param dtl_path: path to the directory the DTL writes its data in
        :param dtl_transport: transport to use for the DTL (RDMA|TCP)
        :param kwargs:
        """
        self.dtl_path = dtl_path
        self.dtl_transport = dtl_transport

class DlsConfig(GenericConfig):
    def __init__(self, dls_type=None, dls_arakoon_timeout_ms=None, dls_arakoon_cluster_id=None, dls_arakoon_cluster_nodes=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config: dls configuration
        :param dls_type: Type of distributed lock store to use (default / currently only supported value: "Backend")
        :param dls_arakoon_timeout_ms: Arakoon client timeout in milliseconds for the distributed lock store
        :param dls_arakoon_cluster_id: Arakoon cluster identifier for the distributed lock store
        :param dls_arakoon_cluster_nodes:  an array of arakoon cluster node configurations for the distributed lock store, each containing node_id, host and port
        :param kwargs:
        """
        self.dls_type = dls_type
        self.dls_arakoon_timeout_ms = dls_arakoon_timeout_ms
        self.dls_arakoon_cluster_id = dls_arakoon_cluster_id
        self.dls_arakoon_cluster_nodes = dls_arakoon_cluster_nodes

class S3ConnectionConfig(object):
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
        :param kwargs:
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

class NetworkConfig(GenericConfig):
    def __init__(self, network_uri=None, network_xio_slab_config=None, network_workqueue_max_threads=None, network_snd_rcv_queue_depth=None,
                 network_max_neighbour_distance=None, network_workqueue_ctrl_max_threads=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config: network config manager
        :param network_uri: When backend_type is S3: whether to do verbose logging
        :param network_xio_slab_config: Accelio's mempool profile configuration, maximum number of slabs is XIO_MAX_SLABS_NR(=6)
        :param network_workqueue_max_threads: Maximum workqueue threads
        :param network_snd_rcv_queue_depth: Maximum tx/rx queued messages
        :param network_max_neighbour_distance: Hide nodes that have a distance >= this value from network clients
        :param network_workqueue_ctrl_max_threads: Maximum control path workqueue threads
        :param kwargs:
        """
        self.network_uri = network_uri
        self.network_xio_slab_config = network_xio_slab_config
        self.network_workqueue_max_threads = network_workqueue_max_threads
        self.network_snd_rcv_queue_depth = network_snd_rcv_queue_depth
        self.network_max_neighbour_distance =network_max_neighbour_distance
        self.network_workqueue_ctrl_max_threads = network_workqueue_ctrl_max_threads

class MdsConfig(GenericConfig):
    def __init__(self, mds_nodes=None, mds_db_type=None, mds_threads=None, mds_poll_secs=None, mds_cached_pages=None, mds_timeout_secs=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config: MDS config manager
        :param mds_nodes: an array of MDS node configurations each containing address, port, db_directory and scratch_directory
        :param mds_db_type: Type of database to use for metadata. Supported values: ROCKSDB
        :param mds_threads: Number of threads per node (0 -> autoconfiguration based on the number of available CPUs)
        :param mds_poll_secs: Poll interval for the backend check in seconds
        :param mds_cached_pages: Capacity of the metadata page cache per volume
        :param mds_timeout_secs: Timeout for network transfers - (0 -> no timeout!)
        :param kwargs:
        """
        self.mds_nodes = mds_nodes
        self.mds_db_type = mds_db_type
        self.mds_threads = mds_threads
        self.mds_poll_secs = mds_poll_secs
        self.mds_cached_pages = mds_cached_pages
        self.mds_timeout_secs = mds_timeout_secs

class EventsConfig(GenericConfig):
    def __init__(self, events_amqp_uris=None, events_amqp_exchange=None, events_amqp_routing_key=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config: events config manager
        :param events_amqp_uris: array of URIs, each consisting of an "amqp_uri" entry for a node of the AMQP cluster events shall be sent to
        :param events_amqp_exchange: AMQP exchange events will be sent to
        :param events_amqp_routing_key: AMQP routing key used for sending events
        :param kwargs:
        """
        self.events_amqp_uris = events_amqp_uris
        self.events_amqp_exchange = events_amqp_exchange
        self.events_amqp_routing_key = events_amqp_routing_key

class ScoCacheConfig(GenericConfig):
    def __init__(self, backoff_gap, trigger_gap, scocache_mount_points, *args, **kwargs):
        """
        Initiate the volumedriverfs config: events config manager
        :param backoff_gap: scocache-mountpoint freespace objective for scocache-cleaner
        :param trigger_gap: scocache-mountpoint freespace threshold below which scocache-cleaner is triggered
        :param scocache_mount_point: An array of directories and sizes to be used as scocache mount points
        :param kwargs:
        """
        self.trigger_gap = trigger_gap
        self.backoff_gap = backoff_gap
        self.scocache_mount_points = scocache_mount_points

class FiledriverConfig(GenericConfig):
    def __init__(self, fd_namespace, fd_cache_path, fd_extent_cache_capacity=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config:  filedriver config
        :param fd_namespace:  backend namespace to use for filedriver objects
        :param fd_cache_path:  cache for filedriver objects
        :param fd_extent_cache_capacity: number of extents the extent cache can hold
        """
        self.fd_namespace = fd_namespace
        self.fd_cache_path = fd_cache_path
        self.fd_extent_cache_capacity = fd_extent_cache_capacity

class ScrubManagerConfig(GenericConfig):
    def __init__(self, scrub_manager_interval=None, scrub_manager_sync_wait_secs=None, scrub_manager_max_parent_scrubs=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config:  filedriver config
        :param scrub_manager_interval: interval (in seconds) of the ScrubManager
        :param scrub_manager_sync_wait_secs:  number of seconds to wait for a scrub result application to be on the backend before giving up
        :param scrub_manager_max_parent_scrubs: max number of pending scrub replies for parents
        """
        self.scrub_manager_interval = scrub_manager_interval
        self.scrub_manager_sync_wait_secs = scrub_manager_sync_wait_secs
        self.scrub_manager_max_parent_scrubs = scrub_manager_max_parent_scrubs

class ContentCacheConfig(GenericConfig):
    def __init__(self, read_cache_serialization_path=None, serialize_read_cache=None, clustercache_mount_points=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config:  filedriver config
        :param read_cache_serialization_path: Directory to store the serialization of the Read Cache
        :param serialize_read_cache: Whether to serialize the readcache on exit or not
        :param clustercache_mount_points: An array of directories and sizes to be used as Read Cache mount points
        """
        self.read_cache_serialization_path = read_cache_serialization_path
        self.serialize_read_cache = serialize_read_cache
        self.clustercache_mount_points = clustercache_mount_points


class StorageDriverConfig(object):
    """
    Usage:
    Config classes with non-essential parameters may be left out as parameter.
    Classes with essential parameters are:
    filedriver_config, vregistry_config, vrouter_config, volume_manager_config, filesystem_config, dtl_config, scocache_config
    --------------------------------------------------------------------------
    vrouter_config = VRouterConfig(**my_dict)
    volume_manager_config = VolumeManagerConfig(**my_dict)
    filesystem_config = FilesystemConfig(**my_dict)
    dtl_config = DtlConfig(**my_dict)
    mds_config = MdsConfig(**my_dict)
    backend_config = BackendConfig(**my_dict)
    dls_config = DlsConfig(**my_dict)
    events_config = EventsConfig(**my_dict)
    network_config = NetworkConfig(**my_dict)
    vregistry_config = VRegistryConfig(**my_dict)
    scocache_config= ScoCacheConfig(*my_dict)
    filedriver_config = FiledriverConfig(**my_dict)
    contentcache_config = ContentCacheConfig(**my_dict)
    scrub_manager_config = ScrubManagerConfig(**my_dict)

    pprint.pprint(StorageDriverConfig(backend_config=backend_config,
                                      vrouter_config=vrouter_config,
                                      volume_manager_config=volume_manager_config,
                                      filesystem_config=filesystem_config,
                                      dtl_config=dtl_config,
                                      mds_config=mds_config,
                                      dls_config=dls_config,
                                      backend_interface_config=backend_config,
                                      events_config=events_config,
                                      network_config=network_config,
                                      vregistry_config=vregistry_config,
                                      scocache_config=scocache_config,
                                      filedriver_config=filedriver_config,
                                      contentcache_config=contentcache_config,
                                      scrub_manager_config=scrub_manager_config,
                                      **my_dict).get_config())
    """
    def __init__(self, vrouter_cluster_id,
                 filedriver_config, vregistry_config, vrouter_config, volume_manager_config, filesystem_config, dtl_config, scocache_config,
                 backend_config=None, mds_config=None, dls_config=None, events_config=None, network_config=None, contentcache_config=None, scrub_manager_config=None,
                 backend_type=None, num_threads=None, shm_region_size=None, stats_collector_destination=None, stats_collector_interval_secs=None,
                 asio_service_manager_threads=None, asio_service_manager_io_service_per_thread=None, fuse_min_workers=None, fuse_max_workers=None, bgc_threads=None,*args, **kwargs):

        self.vrouter_cluster_id = vrouter_cluster_id

        # Optional params

        self.backend_type = backend_type
        self.num_threads = num_threads
        self.shm_region_size = shm_region_size

        self.fuse_min_workers = fuse_min_workers
        self.fuse_max_workers = fuse_max_workers

        self.stats_collector_destination = stats_collector_destination
        self.stats_collector_interval_secs = stats_collector_interval_secs

        self.asio_service_manager_threads = asio_service_manager_threads
        self.asio_service_manager_io_service_per_thread = asio_service_manager_io_service_per_thread

        self.bgc_threads = bgc_threads

        # Seperate configs
        # Configs which require at least one mandatory param
        self.vrouter_config = vrouter_config
        self.vregistry_config = vregistry_config
        self.filedriver_config = filedriver_config
        self.volume_manager_config = volume_manager_config
        self.filesystem_config = filesystem_config
        self.dtl_config = dtl_config
        self.scocache_config = scocache_config

        # Configs that can be None
        self.backend_config = backend_config or BackendConfig()
        self.mds_config = mds_config or MdsConfig()
        self.dls_config = dls_config or DlsConfig()
        self.events_config = events_config or EventsConfig()
        self.network_config = network_config or NetworkConfig()
        self.contentcache_config = contentcache_config or ContentCacheConfig()
        self.scrub_manager_config = scrub_manager_config or ScrubManagerConfig()
        self._config = {}



    def get_config(self):
        """
        Get the config of the instantiated class
        :return: dict
        """
        self._config = {'asio_service_manager': {'asio_service_manager_io_service_per_thread': self.asio_service_manager_io_service_per_thread,
                                                 'asio_service_manager_threads': self.asio_service_manager_threads},
                        'backend_connection_manager': self.backend_config.get_config(),
                        'backend_garbage_collector': {'bgc_threads': self.bgc_threads},
                        'content_addressed_cache': self.contentcache_config.get_config(),
                        'distributed_lock_store': self.dls_config.get_config(),
                        'distributed_transaction_log': self.dtl_config.get_config(),
                        'event_publisher': self.events_config.get_config(),
                        'file_driver': self.filedriver_config.get_config(),
                        'filesystem': self.filesystem_config.get_config(),
                        'fuse': {'fuse_min_workers': self.fuse_min_workers,
                                 'fuse_max_workers': self.fuse_max_workers},
                        'metadata_server': self.mds_config.get_config(),
                        'network_interface': self.network_config.get_config(),
                        'threadpool_component': {'num_threads': self.num_threads},
                        'scocache': self.scocache_config.get_config(),
                        'scrub_manager': self.scrub_manager_config.get_config(),
                        'shm_interface': {'shm_region_size': self.shm_region_size},
                        'stats_collector': {'stats_collector_destination': self.stats_collector_destination,
                                            'stats_collector_interval_secs': self.stats_collector_interval_secs},
                        'volume_manager': self.volume_manager_config.get_config(),
                        'volume_registry': self.vregistry_config.get_config(),
                        'volume_router': self.vrouter_config.get_config(),
                        'volume_router_cluster': {'vrouter_cluster_id': self.vrouter_cluster_id}}
        return json.dumps(ExtensionsToolbox.filter_dict_for_none(self._config))
