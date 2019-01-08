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

from __future__ import absolute_import

from .base import BaseStorageDriverConfig
from .connection_manager import BackendConnectionManager, AlbaConnectionConfig, S3ConnectionConfig
from .filesystem import FileSystemConfig
from .storagedriver import VolumeRegistryConfig, DistributedTransactionLogConfig, DistributedLockStoreConfig,\
    NetworkInterfaceConfig, MetadataServerConfig, EventPublisherConfig, ScoCacheConfig, FileDriverConfig, \
    ScrubManagerConfig, ContentAddressedCacheConfig, StorageDriverConfig
from .volume_manager import VolumeManagerConfig
from .volume_router import VolumeRouterConfig

# Combine them into a bundle
__all__ = ['BaseStorageDriverConfig',
           'BackendConnectionManager', 'AlbaConnectionConfig', 'S3ConnectionConfig',
           'FileSystemConfig',
           'VolumeRegistryConfig', 'DistributedTransactionLogConfig', 'DistributedLockStoreConfig',
           'NetworkInterfaceConfig', 'MetadataServerConfig', 'EventPublisherConfig', 'ScoCacheConfig', 'FileDriverConfig',
           'ScrubManagerConfig', 'ContentAddressedCacheConfig', 'StorageDriverConfig',
           'VolumeManagerConfig',
           'VolumeRouterConfig']
