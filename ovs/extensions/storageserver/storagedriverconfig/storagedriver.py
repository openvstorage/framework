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
from .base import BaseStorageDriverConfig
from .connection_manager import BackendConnectionManager
from .filesystem import FileSystemConfig
from .volume_router import VolumeRouterConfig
from .volume_manager import VolumeManagerConfig
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.generic.toolbox import ExtensionsToolbox

class VolumeRegistryConfig(BaseStorageDriverConfig):

    component_identifier = 'volume_registry'

    def __init__(self, vregistry_arakoon_cluster_id, vregistry_arakoon_cluster_nodes, vregistry_arakoon_timeout_ms=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config: vregistry configuration
        :param vregistry_arakoon_cluster_id: Arakoon cluster identifier for the volume registry
        :param vregistry_arakoon_cluster_nodes:  an array of arakoon cluster node configurations for the volume registry, each containing node_id, host and port
        :param vregistry_arakoon_timeout_ms: Arakoon client timeout in milliseconds for the volume registry
        """
        self.vregistry_arakoon_cluster_id = vregistry_arakoon_cluster_id
        self.vregistry_arakoon_cluster_nodes = vregistry_arakoon_cluster_nodes
        self.vregistry_arakoon_timeout_ms = vregistry_arakoon_timeout_ms


class DistributedTransactionLogConfig(BaseStorageDriverConfig):

    component_identifier = 'distributed_transaction_log'

    def __init__(self, dtl_path, dtl_transport=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config: dtl configuration
        :param dtl_path: path to the directory the DTL writes its data in
        :param dtl_transport: transport to use for the DTL (RDMA|TCP)
        """
        self.dtl_path = dtl_path
        self.dtl_transport = dtl_transport


class DistributedLockStoreConfig(BaseStorageDriverConfig):

    component_identifier = 'distributed_lock_store'

    def __init__(self, dls_type='Arakoon', dls_arakoon_timeout_ms=None, dls_arakoon_cluster_id=None, dls_arakoon_cluster_nodes=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config: dls configuration
        :param dls_type: Type of distributed lock store to use (default / currently only supported value: "Backend")
        :param dls_arakoon_timeout_ms: Arakoon client timeout in milliseconds for the distributed lock store
        :param dls_arakoon_cluster_id: Arakoon cluster identifier for the distributed lock store
        :param dls_arakoon_cluster_nodes:  an array of arakoon cluster node configurations for the distributed lock store, each containing node_id, host and port
        """
        self.dls_type = dls_type
        self.dls_arakoon_timeout_ms = dls_arakoon_timeout_ms
        self.dls_arakoon_cluster_id = dls_arakoon_cluster_id
        self.dls_arakoon_cluster_nodes = dls_arakoon_cluster_nodes


class NetworkInterfaceConfig(BaseStorageDriverConfig):

    component_identifier = 'network_interface'

    def __init__(self, network_uri=None, network_xio_slab_config=None, network_workqueue_max_threads=None, network_snd_rcv_queue_depth=None,
                 network_max_neighbour_distance=9999, network_workqueue_ctrl_max_threads=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config: network config manager
        :param network_uri: When backend_type is S3: whether to do verbose logging
        :param network_xio_slab_config: Accelio's mempool profile configuration, maximum number of slabs is XIO_MAX_SLABS_NR(=6)
        :param network_workqueue_max_threads: Maximum workqueue threads
        :param network_snd_rcv_queue_depth: Maximum tx/rx queued messages
        :param network_max_neighbour_distance: Hide nodes that have a distance >= this value from network clients
        :param network_workqueue_ctrl_max_threads: Maximum control path workqueue threads
        """
        self.network_uri = network_uri
        self.network_xio_slab_config = network_xio_slab_config
        self.network_workqueue_max_threads = network_workqueue_max_threads
        self.network_snd_rcv_queue_depth = network_snd_rcv_queue_depth
        self.network_max_neighbour_distance = network_max_neighbour_distance
        self.network_workqueue_ctrl_max_threads = network_workqueue_ctrl_max_threads


class MetadataServerConfig(BaseStorageDriverConfig):

    component_identifier = 'metadata_server'

    def __init__(self, mds_nodes=None, mds_db_type=None, mds_threads=None, mds_poll_secs=None, mds_cached_pages=None, mds_timeout_secs=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config: MDS config manager
        :param mds_nodes: an array of MDS node configurations each containing address, port, db_directory and scratch_directory
        :param mds_db_type: Type of database to use for metadata. Supported values: ROCKSDB
        :param mds_threads: Number of threads per node (0 -> autoconfiguration based on the number of available CPUs)
        :param mds_poll_secs: Poll interval for the backend check in seconds
        :param mds_cached_pages: Capacity of the metadata page cache per volume
        :param mds_timeout_secs: Timeout for network transfers - (0 -> no timeout!)
        """
        self.mds_nodes = mds_nodes
        self.mds_db_type = mds_db_type
        self.mds_threads = mds_threads
        self.mds_poll_secs = mds_poll_secs
        self.mds_cached_pages = mds_cached_pages
        self.mds_timeout_secs = mds_timeout_secs


class EventPublisherConfig(BaseStorageDriverConfig):

    component_identifier = 'event_publisher'

    def __init__(self, events_amqp_uris=None, events_amqp_exchange=None, events_amqp_routing_key=None, *args, **kwargs):
        """
        Initiate the volumedriverfs config: events config manager
        :param events_amqp_uris: array of URIs, each consisting of an "amqp_uri" entry for a node of the AMQP cluster events shall be sent to
        :param events_amqp_exchange: AMQP exchange events will be sent to
        :param events_amqp_routing_key: AMQP routing key used for sending events
        """

        if events_amqp_uris:
            self.events_amqp_uris = events_amqp_uris
        else:
            mq_user = Configuration.get('/ovs/framework/messagequeue|user')
            mq_protocol = Configuration.get('/ovs/framework/messagequeue|protocol')
            mq_password = Configuration.get('/ovs/framework/messagequeue|password')
            self.events_amqp_uris = [{'amqp_uri': '{0}://{1}:{2}@{3}:5672'.format(mq_protocol, mq_user, mq_password, sr.ip)} for sr in StorageRouterList.get_masters()]

        self.events_amqp_exchange = events_amqp_exchange
        self.events_amqp_routing_key = events_amqp_routing_key or Configuration.get('/ovs/framework/messagequeue|queues.storagedriver', default=None)


class ScoCacheConfig(BaseStorageDriverConfig):

    component_identifier = 'scocache'

    def __init__(self, backoff_gap, trigger_gap, scocache_mount_points, *args, **kwargs):
        """
        Initiate the volumedriverfs config: events config manager
        :param backoff_gap: scocache-mountpoint freespace objective for scocache-cleaner
        :param trigger_gap: scocache-mountpoint freespace threshold below which scocache-cleaner is triggered
        :param scocache_mount_point: An array of directories and sizes to be used as scocache mount points
        """
        self.trigger_gap = trigger_gap
        self.backoff_gap = backoff_gap
        self.scocache_mount_points = scocache_mount_points


class FileDriverConfig(BaseStorageDriverConfig):

    component_identifier = 'file_driver'

    def __init__(self, fd_namespace, fd_cache_path, fd_extent_cache_capacity=1024, *args, **kwargs):
        """
        Initiate the volumedriverfs config:  filedriver config
        :param fd_namespace:  backend namespace to use for filedriver objects
        :param fd_cache_path:  cache for filedriver objects
        :param fd_extent_cache_capacity: number of extents the extent cache can hold
        """
        self.fd_namespace = fd_namespace
        self.fd_cache_path = fd_cache_path
        self.fd_extent_cache_capacity = fd_extent_cache_capacity


class ScrubManagerConfig(BaseStorageDriverConfig):

    component_identifier = 'scrub_manager'

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


class ContentAddressedCacheConfig(BaseStorageDriverConfig):

    component_identifier = 'content_addressed_cache'

    def __init__(self, read_cache_serialization_path=None, serialize_read_cache=False, clustercache_mount_points=None, *args, **kwargs):
        """
        Initiate the content_addressed_cache config
        :param read_cache_serialization_path: Directory to store the serialization of the Read Cache
        :param serialize_read_cache: Whether to serialize the readcache on exit or not
        :param clustercache_mount_points: An array of directories and sizes to be used as Read Cache mount points
        """
        if read_cache_serialization_path is None:
            read_cache_serialization_path = []

        self.read_cache_serialization_path = read_cache_serialization_path
        self.serialize_read_cache = serialize_read_cache
        self.clustercache_mount_points = clustercache_mount_points


class StorageDriverConfig(BaseStorageDriverConfig):
    """
    Usage:
    Config classes with non-essential parameters may be left out as parameter.
    Classes with essential parameters are:
    filedriver_config, vregistry_config, vrouter_config, volume_manager_config, filesystem_config, dtl_config, scocache_config
    --------------------------------------------------------------------------
    vrouter_config = VRouterConfig(**my_dict)
    volume_manager_config = VolumeManagerConfig(**my_dict)
    filesystem_config = FileSystemConfig(**my_dict)
    dtl_config = DistributedTransactionLogConfig(**my_dict)
    mds_config = MetadataServerConfig(**my_dict)
    backend_config = BackendConfig(**my_dict)
    dls_config = DistributedLockStoreConfig(**my_dict)
    events_config = EventPublisherConfig(**my_dict)
    network_config = NetworkInterfaceConfig(**my_dict)
    vregistry_config = VolumeRegistryConfig(**my_dict)
    scocache_config= ScoCacheConfig(*my_dict)
    filedriver_config = FileDriverConfig(**my_dict)
    contentcache_config = ContentAddressedCacheConfig(**my_dict)
    scrub_manager_config = ScrubManagerConfig(**my_dict)

    Note that this object 'my_dict' must be a flat dict with all containing parameters. These parameters will then be formatted as needed for the volumedriver

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

    # Compilation of all other objects
    component_identifier = 'complete_storagedriver_config'

    def to_dict(self):
        # type: () -> Dict[str, any]
        """
        Get the complete overview as a dict
        :return: The complete overview as dict
        :rtype: Dict[str, any]
        """
        self._config = {u'asio_service_manager': {u'asio_service_manager_io_service_per_thread': self.asio_service_manager_io_service_per_thread,
                                                  u'asio_service_manager_threads': self.asio_service_manager_threads},
                        u'backend_connection_manager': self.backend_config.to_dict(),
                        u'backend_garbage_collector': {u'bgc_threads': self.bgc_threads},
                        u'content_addressed_cache': self.contentcache_config.to_dict(),
                        u'distributed_lock_store': self.dls_config.to_dict(),
                        u'distributed_transaction_log': self.dtl_config.to_dict(),
                        u'event_publisher': self.events_config.to_dict(),
                        u'file_driver': self.filedriver_config.to_dict(),
                        u'filesystem': self.filesystem_config.to_dict(),
                        u'fuse': {u'fuse_min_workers': self.fuse_min_workers,
                                 u'fuse_max_workers': self.fuse_max_workers},
                        u'metadata_server': self.mds_config.to_dict(),
                        u'network_interface': self.network_config.to_dict(),
                        u'threadpool_component': {u'num_threads': self.num_threads},
                        u'scocache': self.scocache_config.to_dict(),
                        u'scrub_manager': self.scrub_manager_config.to_dict(),
                        u'shm_interface': {u'shm_region_size': self.shm_region_size},
                        u'stats_collector': {u'stats_collector_destination': self.stats_collector_destination,
                                             u'stats_collector_interval_secs': self.stats_collector_interval_secs},
                        u'volume_manager': self.volume_manager_config.to_dict(),
                        u'volume_registry': self.vregistry_config.to_dict(),
                        u'volume_router': self.vrouter_config.to_dict(),
                        u'volume_router_cluster': {u'vrouter_cluster_id': self.vrouter_cluster_id}}
        return ExtensionsToolbox.filter_dict_for_none(self._config)

    def __init__(self,
                 vrouter_cluster_id,  # type: str
                 filedriver_config,  # type: FileDriverConfig
                 vregistry_config,  # type: VolumeRegistryConfig
                 vrouter_config,  # type: VolumeRouterConfig
                 volume_manager_config,  # type: VolumeManagerConfig
                 filesystem_config,  # type: FileSystemConfig
                 dtl_config,  # type: DistributedTransactionLogConfig
                 scocache_config,  # type: ScoCacheConfig
                 backend_config,  # type: BackendConnectionManager
                 mds_config=None,  # type: Optional[MetadataServerConfig]
                 dls_config=None,  # type: Optional[DistributedLockStoreConfig]
                 events_config=None,  # type: Optional[EventPublisherConfig]
                 network_config=None,  # type: Optional[NetworkInterfaceConfig]
                 contentcache_config=None,  # type: Optional[ContentAddressedCacheConfig]
                 scrub_manager_config=None,  # type: Optional[ScrubManagerConfig]
                 backend_type=None,  # type: Optional[str]
                 num_threads=16,  # type: Optional[int]
                 shm_region_size=None,  # type: Optional[int]
                 stats_collector_destination=None,  # type: Optional[str]
                 stats_collector_interval_secs=None,  # type: Optional[str]
                 asio_service_manager_threads=None,  # type: Optional[int]
                 asio_service_manager_io_service_per_thread=None,  # type: Optional[int]
                 fuse_min_workers=None,  # type: Optional[int]
                 fuse_max_workers=None,  # type: Optional[int]
                 bgc_threads=None,  # type: Optional[int]
                 *args, **kwargs):
        # type: (...) -> None
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

        # Separate configs
        # Configs which require at least one mandatory param
        self.vrouter_config = vrouter_config
        self.vregistry_config = vregistry_config
        self.filedriver_config = filedriver_config
        self.volume_manager_config = volume_manager_config
        self.filesystem_config = filesystem_config
        self.dtl_config = dtl_config
        self.scocache_config = scocache_config
        self.backend_config = backend_config

        # Configs that can be None
        self.mds_config = mds_config or MetadataServerConfig()  # type: MetadataServerConfig
        self.dls_config = dls_config or DistributedLockStoreConfig()  # type: DistributedLockStoreConfig
        self.events_config = events_config or EventPublisherConfig()  # type: NetworkInterfaceConfig
        self.network_config = network_config or NetworkInterfaceConfig()  # type: NetworkInterfaceConfig
        self.contentcache_config = contentcache_config or ContentAddressedCacheConfig()  # type: ContentAddressedCacheConfig
        self.scrub_manager_config = scrub_manager_config or ScrubManagerConfig()  # type: ScrubManagerConfig
        self._config = {}


    @staticmethod
    def from_dict(whole_config):

        def _fetch_partial_config(object):
            if object.component_identifier in whole_config:
                return object(**whole_config.get(object.component_identifier))
            else:
                return object()

        # Create mandatory objects
        vrouter_cluster_id = whole_config['volume_router_cluster']['vrouter_cluster_id']
        scocache_config = ScoCacheConfig(**whole_config[ScoCacheConfig.component_identifier])
        filedriver_config = FileDriverConfig(**whole_config[FileDriverConfig.component_identifier])
        filesystem_config = FileSystemConfig(**whole_config[FileSystemConfig.component_identifier])
        vrouter_config = VolumeRouterConfig(**whole_config[VolumeRouterConfig.component_identifier])
        vregistry_config = VolumeRegistryConfig(**whole_config[VolumeRegistryConfig.component_identifier])
        volume_manager_config = VolumeManagerConfig(**whole_config[VolumeManagerConfig.component_identifier])
        backend_config = BackendConnectionManager(**whole_config[BackendConnectionManager.component_identifier])
        dtl_config = DistributedTransactionLogConfig(**whole_config[DistributedTransactionLogConfig.component_identifier])

        # Create optional objects
        mds_config = _fetch_partial_config(MetadataServerConfig)
        events_config = _fetch_partial_config(EventPublisherConfig)
        scrub_manager_config = _fetch_partial_config(ScrubManagerConfig)
        network_config = _fetch_partial_config(NetworkInterfaceConfig)
        dls_config = _fetch_partial_config(DistributedLockStoreConfig)
        contentcache_config = _fetch_partial_config(ContentAddressedCacheConfig)


        # Fill created objects in in __init__ and use remaining config values to fill in other params of the __init__
        return StorageDriverConfig(vrouter_cluster_id=vrouter_cluster_id,
                                   dtl_config=dtl_config,
                                   filedriver_config=filedriver_config,
                                   filesystem_config=filesystem_config,
                                   dls_config=dls_config,
                                   vrouter_config=vrouter_config,
                                   scocache_config=scocache_config,
                                   vregistry_config=vregistry_config,
                                   volume_manager_config=volume_manager_config,
                                   backend_config=backend_config,
                                   mds_config=mds_config,
                                   events_config=events_config,
                                   scrub_manager_config=scrub_manager_config,
                                   network_config=network_config,
                                   contentcache_config=contentcache_config,**whole_config)


