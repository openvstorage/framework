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

"""
Module for VDiskController
"""
import os
import re
import json
import math
import time
import uuid
import pickle
import random
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.hybrids.domain import Domain
from ovs.dal.hybrids.j_vdiskdomain import VDiskDomain
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs_extensions.generic.volatilemutex import NoLockAvailableException
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storageserver.storagedriver import DTLConfig, DTLConfigMode, LOG_LEVEL_MAPPING, MDSMetaDataBackendConfig, \
                                                       MDSNodeConfig, StorageDriverClient, StorageDriverConfiguration, VolumeRestartInProgressException
from ovs.lib.helpers.decorators import log, ovs_task
from ovs.lib.helpers.toolbox import Schedule, Toolbox
from ovs.lib.mdsservice import MDSServiceController
from volumedriver.storagerouter import storagerouterclient, VolumeDriverEvents_pb2


class VDiskController(object):
    """
    Contains all BLL regarding VDisks
    """
    _VOLDRV_EVENT_KEY = 'voldrv_event_vdisk_{0}'
    _logger = Logger('lib')
    _log_level = LOG_LEVEL_MAPPING[_logger.getEffectiveLevel()]

    # noinspection PyCallByClass,PyTypeChecker
    storagerouterclient.Logger.setupLogging(Logger.load_path('storagerouterclient'), _log_level)
    # noinspection PyArgumentList
    storagerouterclient.Logger.enableLogging()

    @staticmethod
    @ovs_task(name='ovs.vdisk.list_volumes')
    def list_volumes(vpool_guid=None):
        """
        List all known volumes on a specific vpool or on all
        :param vpool_guid: Guid of the vPool to list the volumes for
        :type vpool_guid: str
        :return: Volumes known by the vPool or all volumes if no vpool_guid is provided
        :rtype: list
        """
        if vpool_guid is not None:
            vpool = VPool(vpool_guid)
            response = vpool.storagedriver_client.list_volumes(req_timeout_secs=5)
        else:
            response = []
            for vpool in VPoolList.get_vpools():
                response.extend(vpool.storagedriver_client.list_volumes(req_timeout_secs=5))
        return response

    @staticmethod
    def clean_vdisk_from_model(vdisk):
        """
        Removes a vDisk from the model
        :param vdisk: The vDisk to be removed
        :type vdisk: ovs.dal.hybrids.vdisk.VDisk
        :return: None
        """
        VDiskController._logger.info('Cleaning vDisk {0}'.format(vdisk.name))
        for mds_service in vdisk.mds_services:
            mds_service.delete()
        for domain_junction in vdisk.domains_dtl:
            domain_junction.delete()
        vdisk.delete()

    @staticmethod
    def vdisk_checkup(vdisk):
        """
        Triggers a few (async) tasks to make sure the vDisk is in a healthy state.
        :param vdisk: The vDisk to check
        :type vdisk: ovs.dal.hybrids.vdisk.VDisk
        :return: None
        """
        try:
            VDiskController.dtl_checkup(vdisk_guid=vdisk.guid)
        except Exception:
            VDiskController._logger.exception('Error during vDisk checkup (DTL)')
        try:
            VDiskController._set_vdisk_metadata_pagecache_size(vdisk)
            MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)
        except Exception:
            VDiskController._logger.exception('Error during vDisk checkup')
            if vdisk.objectregistry_client.find(str(vdisk.volume_id)) is None:
                VDiskController._logger.warning('Volume {0} does not exist anymore.'.format(vdisk.volume_id))
                VDiskController.clean_vdisk_from_model(vdisk)
        command = None
        try:
            vpool = vdisk.vpool
            storagedriver = vpool.storagedrivers[0]
            storagerouter = storagedriver.storagerouter
            if 'cache-quota' in storagerouter.features['alba']['features']:
                proxy = storagedriver.alba_proxies[0]
                configuration = Configuration.get_configuration_path('/ovs/vpools/{0}/proxies/{1}/config/abm'.format(vpool.guid, proxy.guid))
                client = SSHClient(storagerouter)
                if vdisk.cache_quota is not None:
                    fcq = vdisk.cache_quota.get(VPool.CACHES.FRAGMENT)
                    bcq = vdisk.cache_quota.get(VPool.CACHES.BLOCK)
                else:
                    vdisk.invalidate_dynamics(['storagedriver_id', 'storagerouter_guid'])
                    metadata = vpool.metadata['backend']['caching_info'].get(vdisk.storagerouter_guid, {})
                    fcq = metadata.get('quota_fc')
                    bcq = metadata.get('quota_bc')
                if fcq is not None and fcq > 0:
                    fcq_action = 'Setting FCQ to {0}'.format(fcq)
                    fcq_command = ['--fragment-cache-quota', str(fcq)]
                else:
                    fcq_action = 'Clearing FCQ'
                    fcq_command = ['--clear-fragment-cache-quota']
                if 'block-cache' in storagerouter.features['alba']['features'] and bcq is not None and bcq > 0:
                    bcq_action = 'Setting BCQ to {0}'.format(bcq)
                    bcq_command = ['--block-cache-quota', str(bcq)]
                else:
                    bcq_action = 'Clearing BCQ'
                    bcq_command = ['--clear-block-cache-quota']
                command = ['alba', 'set-namespace-cache-quota', '--to-json', '--config', configuration] + fcq_command + bcq_command + [vdisk.volume_id]
                VDiskController._logger.debug('Cache Quota actions on vDisk {0}: {1}, {2}'.format(vdisk.name, fcq_action, bcq_action))
                raw_result = client.run(command)
                try:
                    results = json.loads(raw_result)
                except ValueError:
                    VDiskController._logger.debug('Could not parse result: {0}'.format(raw_result))
                    raise
                if results['success'] is False:
                    raise RuntimeError('Could not set Fragment/Block Cache Quota: {0}'.format(results))
        except Exception:
            if command is not None:
                VDiskController._logger.debug('Executed command: {0}'.format(command))
            VDiskController._logger.exception('Error when setting cache quotas')

    @staticmethod
    @ovs_task(name='ovs.vdisk.delete_from_voldrv')
    @log('VOLUMEDRIVER_TASK')
    def delete_from_voldrv(volume_id):
        """
        Delete a vDisk from model only since its been deleted on volumedriver
        Triggered by volumedriver messages on the queue
        :param volume_id: Volume ID of the vDisk
        :type volume_id: str
        :return: None
        """
        with volatile_mutex(VDiskController._VOLDRV_EVENT_KEY.format(volume_id), wait=20):
            vdisk = VDiskList.get_vdisk_by_volume_id(volume_id)
            if vdisk is not None:
                for _function in Toolbox.fetch_hooks('vdisk_removal', 'before_volume_remove'):
                    try:
                        _function(vdisk.guid)
                    except RuntimeError:
                        VDiskController._logger.exception('Executing hook {0} failed'.format(_function.__name__))
                VDiskController.clean_vdisk_from_model(vdisk)
            else:
                VDiskController._logger.info('Volume {0} does not exist'.format(volume_id))

    @staticmethod
    @ovs_task(name='ovs.vdisk.delete')
    def delete(vdisk_guid):
        """
        Delete a vDisk through API
        :param vdisk_guid: Guid of the vDisk to delete
        :type vdisk_guid: str
        :return: None
        """
        vdisk = VDisk(vdisk_guid)
        if len(vdisk.child_vdisks) > 0:
            raise RuntimeError('vDisk {0} has clones, cannot delete'.format(vdisk.name))
        vdisk.invalidate_dynamics('storagerouter_guid')
        storagerouter = StorageRouter(vdisk.storagerouter_guid)
        if 'directory_unlink' in storagerouter.features['volumedriver']['features']:
            first = True
            devicename_parts = vdisk.devicename.strip('/').split('/')
            for index in reversed(range(len(devicename_parts))):
                try:
                    path = '/{0}'.format('/'.join(devicename_parts[:index + 1]))
                    vdisk.storagedriver_client.unlink(path)
                    first = False
                except RuntimeError:
                    if first is True:  # Longest path should always succeed, so raise if not
                        raise
        else:
            vdisk.storagedriver_client.unlink(str(vdisk.devicename))
        VDiskController.delete_from_voldrv(vdisk.volume_id)

    @staticmethod
    @ovs_task(name='ovs.vdisk.extend')
    def extend(vdisk_guid, volume_size):
        """
        Extend a vDisk through API
        :param vdisk_guid: Guid of the vDisk to extend
        :type vdisk_guid: str
        :param volume_size: New size in bytes
        :type volume_size: int
        :return: None
        """
        vdisk = VDisk(vdisk_guid)
        if volume_size > 64 * 1024 ** 4:
            raise ValueError('Maximum volume size of 64TiB exceeded')
        if volume_size < vdisk.size:
            raise ValueError('Shrinking is not possible')
        VDiskController._logger.info('Extending vDisk {0} to {1}B'.format(vdisk.name, volume_size))
        try:
            vdisk.storagedriver_client.truncate(object_id=str(vdisk.volume_id),
                                                new_size='{0}B'.format(volume_size))
        except Exception:
            VDiskController._logger.exception('Extending vDisk {0} failed because volume is not running'.format(vdisk.name))
            raise Exception('Volume {0} is not running, cannot extend'.format(vdisk.name))
        vdisk.size = volume_size
        vdisk.save()
        VDiskController._logger.info('Extended vDisk {0} to {1}B'.format(vdisk.name, volume_size))
        VDiskController._logger.info('Running "after_volume_extend" hooks for vDisk {0}'.format(vdisk.guid))
        for _function in Toolbox.fetch_hooks('vdisk_extend', 'after_volume_extend'):
            try:
                _function(vdisk.guid)
            except RuntimeError:
                VDiskController._logger.exception('Executing hook {0} failed'.format(_function.__name__))

    @staticmethod
    @ovs_task(name='ovs.vdisk.resize_from_voldrv')
    @log('VOLUMEDRIVER_TASK')
    def resize_from_voldrv(volume_id, volume_size, volume_path, storagedriver_id):
        """
        Resize a vDisk
        Triggered by volumedriver messages on the queue
        :param volume_id: volume ID of the vDisk
        :type volume_id: str
        :param volume_size: Size of the volume
        :type volume_size: int
        :param volume_path: Path on hypervisor to the volume
        :type volume_path: str
        :param storagedriver_id: ID of the storagedriver serving the volume to resize
        :type storagedriver_id: str
        :return: None
        """
        storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
        vpool = storagedriver.vpool
        with volatile_mutex(VDiskController._VOLDRV_EVENT_KEY.format(volume_id), wait=30):
            if vpool.objectregistry_client.find(str(volume_id)) is None:
                VDiskController._logger.warning('Ignoring resize_from_voldrv event for non-existing volume {0}'.format(volume_id))
                return
            vdisk = VDiskList.get_vdisk_by_volume_id(volume_id)

            if vdisk is None:
                vdisk = VDisk()
                vdisk.name = VDiskController.extract_volumename(volume_path)
            vdisk.devicename = volume_path
            vdisk.volume_id = volume_id
            vdisk.size = volume_size
            vdisk.vpool = storagedriver.vpool
            vdisk.metadata = {'lba_size': vdisk.info['lba_size'],
                              'cluster_multiplier': vdisk.info['cluster_multiplier']}
            vdisk.save()
            VDiskController.vdisk_checkup(vdisk)
            VDiskController._logger.info('Running "after_volume_extend" hooks for vDisk {0}'.format(vdisk.guid))
            for _function in Toolbox.fetch_hooks('vdisk_extend', 'after_volume_extend'):
                try:
                    _function(vdisk.guid)
                except RuntimeError:
                    VDiskController._logger.exception('Executing hook {0} failed'.format(_function.__name__))

    @staticmethod
    @ovs_task(name='ovs.vdisk.migrate_from_voldrv')
    @log('VOLUMEDRIVER_TASK')
    def migrate_from_voldrv(volume_id, new_owner_id):
        """
        Triggered when volume has changed owner (Clean migration or stolen due to other reason)
        Triggered by volumedriver messages
        :param volume_id: Volume ID of the vDisk
        :type volume_id: unicode
        :param new_owner_id: ID of the storage driver the volume migrated to
        :type new_owner_id: unicode
        :return: None
        """
        sd = StorageDriverList.get_by_storagedriver_id(storagedriver_id=new_owner_id)
        vdisk = VDiskList.get_vdisk_by_volume_id(volume_id=volume_id)
        if vdisk is not None:
            VDiskController._logger.info('Migration - Guid {0} - ID {1} - Detected migration for vDisk {2}'.format(vdisk.guid, vdisk.volume_id, vdisk.name))
            if sd is not None:
                VDiskController._logger.info('Migration - Guid {0} - ID {1} - Storage Router {2} is the new owner of vDisk {3}'.format(vdisk.guid, vdisk.volume_id, sd.storagerouter.name, vdisk.name))
            MDSServiceController.mds_checkup()
            VDiskController.dtl_checkup(vdisk_guid=vdisk.guid)

    @staticmethod
    @ovs_task(name='ovs.vdisk.rename_from_voldrv')
    def rename_from_voldrv(old_path, new_path, storagedriver_id):
        """
        Processes a rename event from the volumedriver. At this point we only expect folder renames. These folders
        might contain vDisks. Although the vDisk's .raw file cannot be moved/renamed, the folders can.
        :param old_path: The old path (prefix) that is renamed
        :type old_path: str
        :param new_path: The new path (prefix) of that folder
        :type new_path: str
        :param storagedriver_id: The StorageDriver's ID that executed the rename
        :type storagedriver_id: str
        :return: None
        :rtype: NoneType
        """
        from ovs_extensions.generic.toolbox import ExtensionsToolbox

        old_path = '/{0}/'.format(old_path.strip('/'))
        new_path = '/{0}/'.format(new_path.strip('/'))
        storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
        vpool = storagedriver.vpool
        VDiskController._logger.debug('Processing rename on {0} from {1} to {2}'.format(vpool.name, old_path, new_path))
        _hooked_functions = Toolbox.fetch_hooks('vdisk_rename', 'after_volume_rename')
        for vdisk in vpool.vdisks:
            devicename = vdisk.devicename
            if devicename.startswith(old_path):
                volume_id = vdisk.volume_id
                with volatile_mutex(VDiskController._VOLDRV_EVENT_KEY.format(volume_id), wait=30):
                    vdisk.discard()
                    devicename = vdisk.devicename
                    if devicename.startswith(old_path):
                        vdisk.devicename = '{0}{1}'.format(new_path, ExtensionsToolbox.remove_prefix(devicename, old_path))
                        vdisk.save()
                        VDiskController._logger.info('Renamed device name from {0} to {1} on vDisk {2}'.format(devicename, vdisk.devicename, vdisk.guid))
                        VDiskController._logger.info('Running "after_volume_rename" hooks for vDisk {0}'.format(vdisk.guid))
                        for _function in _hooked_functions:
                            try:
                                _function(vdisk.guid)
                            except RuntimeError:
                                VDiskController._logger.exception('Executing hook {0} failed'.format(_function.__name__))

    @staticmethod
    @ovs_task(name='ovs.vdisk.clone')
    def clone(vdisk_guid, name, snapshot_id=None, storagerouter_guid=None, pagecache_ratio=None, cache_quota=None):
        """
        Clone a vDisk
        :param vdisk_guid: Guid of the vDisk to clone
        :type vdisk_guid: str
        :param name: Name of the new clone (can be a path or a user friendly name)
        :type name: str
        :param snapshot_id: ID of the snapshot to clone from
        :type snapshot_id: str
        :param storagerouter_guid: Guid of the StorageRouter
        :type storagerouter_guid: str
        :param pagecache_ratio: Ratio of the page cache size (compared to a 100% cache)
        :type pagecache_ratio: float
        :param cache_quota: Max disk space the new clone can consume for caching (both fragment as block) purposes (in Bytes)
        :type cache_quota: dict
        :return: Information about the cloned volume
        :rtype: dict
        """
        # Validations
        vdisk = VDisk(vdisk_guid)
        devicename = VDiskController.clean_devicename(name)
        if VDiskList.get_by_devicename_and_vpool(devicename, vdisk.vpool) is not None:
            raise RuntimeError('A vDisk with this name already exists on vPool {0}'.format(vdisk.vpool.name))
        if storagerouter_guid is not None:
            storagedrivers = [sd for sd in vdisk.vpool.storagedrivers if sd.storagerouter_guid == storagerouter_guid]
            if len(storagedrivers) == 0:
                raise RuntimeError('Could not use the given StorageRouter: {0}'.format(storagerouter_guid))
            storagedriver = storagedrivers[0]
        else:
            storagedriver = StorageDriverList.get_by_storagedriver_id(vdisk.storagedriver_id)
            if storagedriver is None:
                raise RuntimeError('Could not find StorageDriver with ID {0}'.format(vdisk.storagedriver_id))

        if pagecache_ratio is not None:
            if not 0.0 < pagecache_ratio <= 1:
                raise RuntimeError('Parameter pagecache_ratio must be 0 < x <= 1')
        if cache_quota is not None:
            for quota_type in VPool.CACHES.values():
                quota = cache_quota.get(quota_type)
                if quota is not None:
                    if not 0.1 * 1024.0 ** 3 <= quota <= 1024 ** 4:
                        raise ValueError('Parameter cache_quota must be between 0.1 GiB and 1024 GiB')

        mds_service = MDSServiceController.get_preferred_mds(storagedriver.storagerouter, vdisk.vpool)[0]
        if mds_service is None:
            raise RuntimeError('Could not find a MDS service')

        # Create new snapshot if required
        if snapshot_id is None:
            timestamp = str(int(time.time()))
            metadata = {'label': '',
                        'is_consistent': False,
                        'timestamp': timestamp,
                        'is_automatic': True}
            snapshot_id = VDiskController.create_snapshot(vdisk_guid=vdisk_guid, metadata=metadata)
            try:
                VDiskController._wait_for_snapshot_to_be_synced_to_backend(vdisk_guid=vdisk.guid, snapshot_id=snapshot_id)
            except RuntimeError:
                try:
                    VDiskController.delete_snapshot(vdisk_guid=vdisk_guid, snapshot_id=snapshot_id)
                except Exception:
                    pass
                raise RuntimeError('Could not find created snapshot in time')

        # Verify if snapshot is synced to backend
        else:
            VDiskController._wait_for_snapshot_to_be_synced_to_backend(vdisk_guid=vdisk.guid, snapshot_id=snapshot_id)

        # Configure StorageDriver
        try:
            VDiskController._logger.info('Clone snapshot {0} of vDisk {1} to location {2}'.format(snapshot_id, vdisk.name, devicename))
            # noinspection PyArgumentList
            backend_config = MDSMetaDataBackendConfig([MDSNodeConfig(address=str(mds_service.service.storagerouter.ip),
                                                                     port=mds_service.service.ports[0])])
            volume_id = vdisk.storagedriver_client.create_clone(target_path=devicename,
                                                                metadata_backend_config=backend_config,
                                                                parent_volume_id=str(vdisk.volume_id),
                                                                parent_snapshot_id=str(snapshot_id),
                                                                node_id=str(storagedriver.storagedriver_id))
        except Exception as ex:
            VDiskController._logger.error('Cloning snapshot to new vDisk {0} failed: {1}'.format(name, str(ex)))
            raise

        try:
            VDiskController._logger.debug('Scheduling a backend sync for clone with ID {0}'.format(volume_id))
            vdisk.storagedriver_client.schedule_backend_sync(volume_id=volume_id,
                                                             req_timeout_secs=10)
        except Exception:
            # If this would fail, it doesn't matter because this is only a workaround for this: https://github.com/openvstorage/volumedriver/issues/148
            VDiskController._logger.exception('Scheduling backend sync for clone {0} failed'.format(volume_id))

        with volatile_mutex(VDiskController._VOLDRV_EVENT_KEY.format(volume_id), wait=30):
            new_vdisk = VDiskList.get_vdisk_by_volume_id(volume_id)
            if new_vdisk is None:
                new_vdisk = VDisk()
                new_vdisk.size = vdisk.size
                new_vdisk.vpool = vdisk.vpool
                new_vdisk.volume_id = volume_id
                new_vdisk.devicename = devicename
                new_vdisk.description = name
                new_vdisk.cache_quota = vdisk.cache_quota if cache_quota is None else cache_quota
            new_vdisk.name = name
            new_vdisk.parent_vdisk = vdisk
            new_vdisk.parentsnapshot = snapshot_id
            new_vdisk.pagecache_ratio = pagecache_ratio if pagecache_ratio is not None else vdisk.pagecache_ratio
            new_vdisk.save()
            VDiskController.vdisk_checkup(new_vdisk)

        return {'vdisk_guid': new_vdisk.guid,
                'name': new_vdisk.name,
                'backingdevice': devicename}

    @staticmethod
    def list_snapshot_ids(vdisk):
        """
        Retrieve the snapshot IDs for a given vDisk
        :param vdisk: vDisk to retrieve the snapshot IDs for
        :type vdisk: ovs.dal.hybrids.vdisk.VDisk
        :return: The snapshot IDs for the given vDisk
        :rtype: list
        """
        volume_id = str(vdisk.volume_id)
        try:
            return vdisk.storagedriver_client.list_snapshots(volume_id, req_timeout_secs=10)
        except VolumeRestartInProgressException:
            time.sleep(0.5)
            return vdisk.storagedriver_client.list_snapshots(volume_id, req_timeout_secs=10)

    @staticmethod
    @ovs_task(name='ovs.vdisk.create_snapshot')
    def create_snapshot(vdisk_guid, metadata):
        """
        Create a vDisk snapshot
        :param vdisk_guid: Guid of the vDisk
        :type vdisk_guid: str
        :param metadata: Dictionary of metadata
        :type metadata: dict
        :return: ID of the newly created snapshot
        :rtype: str
        """
        if not isinstance(metadata, dict):
            raise ValueError('Expected metadata as dict, got {0} instead'.format(type(metadata)))
        result = VDiskController.create_snapshots([vdisk_guid], metadata)
        vdisk_result = result[vdisk_guid]
        if vdisk_result[0] is False:
            raise RuntimeError(vdisk_result[1])
        return vdisk_result[1]

    @staticmethod
    @ovs_task(name='ovs.vdisk.create_snapshots')
    def create_snapshots(vdisk_guids, metadata):
        """
        Create vDisk snapshots
        :param vdisk_guids: Guid of the vDisks
        :type vdisk_guids: list
        :param metadata: Dictionary of metadata
        :type metadata: dict
        :return: ID of the newly created snapshot
        :rtype: dict
        """
        if not isinstance(metadata, dict):
            raise ValueError('Expected metadata as dict, got {0} instead'.format(type(metadata)))
        consistent = metadata.get('is_consistent', False)
        metadata = pickle.dumps(metadata)
        results = {}
        for guid in vdisk_guids:
            try:
                vdisk = VDisk(guid)
                snapshot_ids = VDiskController.list_snapshot_ids(vdisk=vdisk)
                if len(snapshot_ids) > 0:
                    if VDiskController.is_volume_synced_up_to_snapshot(vdisk_guid=vdisk.guid, snapshot_id=snapshot_ids[-1]) is False:  # Most recent last in list
                        results[guid] = [False, 'Previously created snapshot did not make it to the backend yet']
                        continue

                VDiskController._logger.info('Create {0} snapshot for vDisk {1}'.format('consistent' if consistent is True else 'inconsistent', vdisk.name))
                snapshot_id = str(uuid.uuid4())
                vdisk.storagedriver_client.create_snapshot(volume_id=str(vdisk.volume_id),
                                                           snapshot_id=str(snapshot_id),
                                                           metadata=metadata,
                                                           req_timeout_secs=10)
                vdisk.invalidate_dynamics(['snapshots', 'snapshot_ids'])
                results[guid] = [True, snapshot_id]
            except Exception as ex:
                results[guid] = [False, ex.message]
        return results

    @staticmethod
    @ovs_task(name='ovs.vdisk.delete_snapshot')
    def delete_snapshot(vdisk_guid, snapshot_id):
        """
        Delete a vDisk snapshot
        :param vdisk_guid: Guid of the vDisk
        :type vdisk_guid: str
        :param snapshot_id: ID of the snapshot
        :type snapshot_id: str
        :return: None
        """
        result = VDiskController.delete_snapshots({vdisk_guid: snapshot_id})
        vdisk_result = result[vdisk_guid]
        if vdisk_result[0] is False:
            raise RuntimeError(vdisk_result[1])

    @staticmethod
    @ovs_task(name='ovs.vdisk.delete_snapshots')
    def delete_snapshots(snapshot_mapping):
        """
        Delete vDisk snapshots
        :param snapshot_mapping: Mapping of VDisk guid and Snapshot ID(s)
        :type snapshot_mapping: dict
        :return: Information about the deleted snapshots, whether they succeeded or not
        :rtype: dict
        """
        results = {}
        for vdisk_guid, snapshot_ids in snapshot_mapping.iteritems():
            backwards_compat = False  # Snapshot_mapping and return value of this function used to be different in older versions
            if not isinstance(snapshot_ids, list):
                snapshot_ids = [snapshot_ids]
                backwards_compat = True

            results[vdisk_guid] = {'success': True,
                                   'error': None,
                                   'results': {}}

            try:
                vdisk = VDisk(vdisk_guid)
            except Exception as ex:
                results[vdisk_guid].update({'success': False,
                                            'error': ex.message})
                if backwards_compat is True:
                    results[vdisk_guid] = [False, ex.message]
                continue

            for snapshot_id in set(snapshot_ids):
                try:
                    if snapshot_id not in VDiskController.list_snapshot_ids(vdisk=vdisk):
                        raise RuntimeError('Snapshot {0} does not belong to vDisk {1}'.format(snapshot_id, vdisk.name))

                    nr_clones = len(VDiskList.get_by_parentsnapshot(snapshot_id))
                    if nr_clones > 0:
                        raise RuntimeError('Snapshot {0} has {1} volume{2} cloned from it, cannot remove'.format(snapshot_id, nr_clones, '' if nr_clones == 1 else 's'))

                    VDiskController._logger.info('Deleting snapshot {0} from vDisk {1}'.format(snapshot_id, vdisk.name))
                    vdisk.storagedriver_client.delete_snapshot(volume_id=str(vdisk.volume_id),
                                                               snapshot_id=str(snapshot_id),
                                                               req_timeout_secs=10)
                    result = [True, snapshot_id]
                except Exception as ex:
                    result = [False, ex.message]
                results[vdisk_guid]['results'][snapshot_id] = result
                if result[0] is False:
                    results[vdisk_guid].update({'success': False,
                                                'error': 'One or more snapshots could not be removed'})
            vdisk.invalidate_dynamics(['snapshots', 'snapshot_ids'])
            if backwards_compat is True:
                results[vdisk_guid] = results[vdisk_guid]['results'][snapshot_ids[0]]
        return results

    @staticmethod
    @ovs_task(name='ovs.vdisk.set_as_template')
    def set_as_template(vdisk_guid):
        """
        Set a vDisk as template
        :param vdisk_guid: Guid of the vDisk
        :type vdisk_guid: str
        :return: None
        """
        vdisk = VDisk(vdisk_guid)
        if vdisk.is_vtemplate is True:
            VDiskController._logger.info('vDisk {0} has already been set as vTemplate'.format(vdisk.name))
            return
        if len(vdisk.child_vdisks) > 0:
            raise RuntimeError('vDisk {0} has clones, cannot convert to vTemplate'.format(vdisk.name))
        if vdisk.parent_vdisk_guid is not None:
            raise RuntimeError('vDisk {0} has been cloned, cannot convert to vTemplate'.format(vdisk.name))

        VDiskController._logger.info('Converting vDisk {0} into vTemplate'.format(vdisk.name))
        try:
            vdisk.storagedriver_client.set_volume_as_template(str(vdisk.volume_id), req_timeout_secs=30)
        except Exception:
            VDiskController._logger.exception('Failed to convert vDisk {0} into vTemplate'.format(vdisk.name))
            raise Exception('Converting vDisk {0} into vTemplate failed'.format(vdisk.name))
        vdisk.invalidate_dynamics(['is_vtemplate', 'info', 'snapshots', 'snapshot_ids'])

    @staticmethod
    def _move(vdisk_guid, target_storagerouter_guid, force=False):
        try:
            vdisk = VDisk(vdisk_guid)
        except ObjectNotFoundException:
            VDiskController._logger.exception('No valid VDisk has been found with provided guid {0}'.format(vdisk_guid))
            raise
        storagedriver = None
        try:
            storagerouter = StorageRouter(target_storagerouter_guid)
        except ObjectNotFoundException:
            VDiskController._logger.exception('No valid StorageRouter has been found with provided guid {0}'.format(target_storagerouter_guid))
            raise

        for sd in storagerouter.storagedrivers:
            if sd.vpool == vdisk.vpool:
                storagedriver = sd
                break

        if storagedriver is None:
            err_msg = 'Failed to find the matching StorageDriver for vdisk {0}'.format(vdisk.name)
            VDiskController._logger.exception(err_msg)
            raise RuntimeError(err_msg)

        VDiskController._logger.info('Starting moval of VDisk {0}'.format(vdisk_guid))
        try:
            vdisk.storagedriver_client.migrate(object_id=str(vdisk.volume_id),
                                               node_id=str(storagedriver.storagedriver_id),
                                               force_restart=force)
        except Exception:
            err_msg = 'Failed to move vDisk {0}'.format(vdisk.name)
            VDiskController._logger.exception(err_msg)
            raise Exception(err_msg)

        try:
            MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)
            VDiskController.dtl_checkup.delay(vdisk_guid=vdisk.guid)
        except Exception:
            VDiskController._logger.exception('Executing post-migrate actions failed for vDisk {0}'.format(vdisk.name))

    @staticmethod
    @ovs_task(name='ovs.vdisk.move')
    def move(vdisk_guid, target_storagerouter_guid, force=False):
        """
        Move a vDisk to the specified StorageRouter
        :param vdisk_guid: Guid of the vDisk to move
        :type vdisk_guid: str
        :param target_storagerouter_guid: Guid of the StorageRouter to move the vDisk to
        :type target_storagerouter_guid: str
        :param force: Indicates whether to force the migration or not (forcing can lead to data loss)
        :type force: bool
        :return: None
        """
        VDiskController._move(vdisk_guid, target_storagerouter_guid, force=force)

    @staticmethod
    @ovs_task(name='ovs.vdisk.move_multiple')
    def move_multiple(vdisk_guids, target_storagerouter_guid, force=False):
        """
        Move list of vDisks to the specified StorageRouter
        :param vdisk_guids: Guids of the vDisk to move
        :type vdisk_guids: list
        :param target_storagerouter_guid: Guid of the StorageRouter to move the vDisk to
        :type target_storagerouter_guid: str
        :param force: Indicates whether to force the migration or not (forcing can lead to data loss)
        :type force: bool
        :return: None
        """
        for vdisk_guid in vdisk_guids:
            VDiskController._move(vdisk_guid, target_storagerouter_guid, force)

    @staticmethod
    @ovs_task(name='ovs.vdisk.rollback')
    def rollback(vdisk_guid, timestamp):
        """
        Rolls back a vDisk based on a given vDisk snapshot timestamp
        :param vdisk_guid: Guid of the vDisk to rollback
        :type vdisk_guid: str
        :param timestamp: Timestamp of the snapshot to rollback from
        :type timestamp: str
        :return: True
        :rtype: bool
        """
        vdisk = VDisk(vdisk_guid)
        snapshots = [snap for snap in vdisk.snapshots if snap['timestamp'] == timestamp]
        if not snapshots:
            raise ValueError('No snapshot found for timestamp {0}'.format(timestamp))
        snapshotguid = snapshots[0]['guid']
        VDiskController._wait_for_snapshot_to_be_synced_to_backend(vdisk_guid=vdisk.guid, snapshot_id=snapshotguid)
        vdisk.storagedriver_client.rollback_volume(str(vdisk.volume_id), str(snapshotguid))
        vdisk.invalidate_dynamics(['snapshots'])
        return True

    @staticmethod
    @ovs_task(name='ovs.vdisk.create_from_template')
    def create_from_template(vdisk_guid, name, storagerouter_guid=None, pagecache_ratio=None, cache_quota=None):
        """
        Create a vDisk from a template
        :param vdisk_guid: Guid of the vDisk
        :type vdisk_guid: str
        :param name: Name of the newly created vDisk (can be a filename or a user friendly name)
        :type name: str
        :param storagerouter_guid: Guid of the Storage Router on which the vDisk should be started
        :type storagerouter_guid: str
        :param pagecache_ratio: Ratio of the page cache size (compared to a 100% cache)
        :type pagecache_ratio: float
        :param cache_quota: Max disk space the new volume can consume for caching purposes (in Bytes)
        :type cache_quota: dict
        :return: Information about the new volume (vdisk_guid, name, backingdevice)
        :rtype: dict
        """
        vdisk = VDisk(vdisk_guid)
        # Validations
        if not vdisk.is_vtemplate:
            raise RuntimeError('The given vDisk is not a vTemplate')
        devicename = VDiskController.clean_devicename(name)
        if VDiskList.get_by_devicename_and_vpool(devicename, vdisk.vpool) is not None:
            raise RuntimeError('A vDisk with this name already exists on vPool {0}'.format(vdisk.vpool.name))
        if storagerouter_guid is not None:
            storagedrivers = [sd for sd in vdisk.vpool.storagedrivers if sd.storagerouter_guid == storagerouter_guid]
            if len(storagedrivers) == 0:
                raise RuntimeError('Could not use the given StorageRouter: {0}'.format(storagerouter_guid))
            storagedriver = storagedrivers[0]
        else:
            storagedriver = StorageDriverList.get_by_storagedriver_id(vdisk.storagedriver_id)
            if storagedriver is None:
                raise RuntimeError('Could not find StorageDriver with ID {0}'.format(vdisk.storagedriver_id))

        if pagecache_ratio is not None:
            if not 0.0 < pagecache_ratio <= 1:
                raise RuntimeError('Parameter pagecache_ratio must be 0 < x <= 1')
        if cache_quota is not None:
            for quota_type in VPool.CACHES.values():
                quota = cache_quota.get(quota_type)
                if quota is not None:
                    if not 0.1 * 1024.0 ** 3 <= quota <= 1024 ** 4:
                        raise ValueError('Parameter cache_quota must be between 0.1 GiB and 1024 GiB')

        mds_service = MDSServiceController.get_preferred_mds(storagedriver.storagerouter, vdisk.vpool)[0]
        if mds_service is None:
            raise RuntimeError('Could not find a MDS service')

        VDiskController._logger.info('Create vDisk from vTemplate {0} to new vDisk {1} to location {2}'.format(vdisk.name, name, devicename))
        try:
            # noinspection PyArgumentList
            backend_config = MDSNodeConfig(address=str(mds_service.service.storagerouter.ip),
                                           port=mds_service.service.ports[0])
            volume_id = vdisk.storagedriver_client.create_clone_from_template(target_path=devicename,
                                                                              metadata_backend_config=MDSMetaDataBackendConfig([backend_config]),
                                                                              parent_volume_id=str(vdisk.volume_id),
                                                                              node_id=str(storagedriver.storagedriver_id))
        except Exception as ex:
            VDiskController._logger.error('Cloning vTemplate {0} failed: {1}'.format(vdisk.name, str(ex)))
            raise

        try:
            VDiskController._logger.debug('Scheduling a backend sync for clone from template with ID {0}'.format(volume_id))
            vdisk.storagedriver_client.schedule_backend_sync(volume_id=volume_id,
                                                             req_timeout_secs=10)
        except Exception:
            # If this would fail, it doesn't matter because this is only a workaround for this: https://github.com/openvstorage/volumedriver/issues/148
            VDiskController._logger.exception('Scheduling backend sync for clone from template {0} failed'.format(volume_id))

        with volatile_mutex(VDiskController._VOLDRV_EVENT_KEY.format(volume_id), wait=30):
            new_vdisk = VDiskList.get_vdisk_by_volume_id(volume_id)
            if new_vdisk is None:
                new_vdisk = VDisk()
                new_vdisk.size = vdisk.size
                new_vdisk.vpool = vdisk.vpool
                new_vdisk.volume_id = volume_id
                new_vdisk.devicename = devicename
                new_vdisk.description = name
                new_vdisk.cache_quota = vdisk.cache_quota if cache_quota is None else cache_quota
            new_vdisk.name = name
            new_vdisk.parent_vdisk = vdisk
            new_vdisk.pagecache_ratio = pagecache_ratio if pagecache_ratio is not None else vdisk.pagecache_ratio
            new_vdisk.save()
            VDiskController.vdisk_checkup(new_vdisk)

        return {'vdisk_guid': new_vdisk.guid,
                'name': new_vdisk.name,
                'backingdevice': devicename}

    @staticmethod
    @ovs_task(name='ovs.vdisk.create_new')
    def create_new(volume_name, volume_size, storagedriver_guid, pagecache_ratio=1.0, cache_quota=None):
        """
        Create a new vDisk/volume using hypervisor calls
        :param volume_name: Name of the vDisk (can be a filename or a user friendly name)
        :type volume_name: str
        :param volume_size: Size of the vDisk
        :type volume_size: int
        :param storagedriver_guid: Guid of the Storagedriver
        :type storagedriver_guid: str
        :param pagecache_ratio: Ratio of the page cache size (compared to a 100% cache)
        :type pagecache_ratio: float
        :param cache_quota: Max disk space the new volume can consume for caching purposes (in Bytes)
        :type cache_quota: dict
        :return: Guid of the new vDisk
        :rtype: str
        """
        # Validations
        storagedriver = StorageDriver(storagedriver_guid)
        devicename = VDiskController.clean_devicename(volume_name)
        vpool = storagedriver.vpool
        if VDiskList.get_by_devicename_and_vpool(devicename, vpool) is not None:
            raise RuntimeError('A vDisk with this name already exists on vPool {0}'.format(vpool.name))
        if volume_size > 64 * 1024 ** 4:
            raise ValueError('Maximum volume size of 64TiB exceeded')

        if not 0.0 < pagecache_ratio <= 1:
            raise RuntimeError('Parameter pagecache_ratio must be 0 < x <= 1')
        if cache_quota is not None:
            for quota_type in VPool.CACHES.values():
                quota = cache_quota.get(quota_type)
                if quota is not None:
                    if not 0.1 * 1024.0 ** 3 <= quota <= 1024 ** 4:
                        raise ValueError('Parameter cache_quota must be between 0.1 GiB and 1024 GiB')

        mds_service = MDSServiceController.get_preferred_mds(storagedriver.storagerouter, vpool)[0]
        if mds_service is None:
            raise RuntimeError('Could not find a MDS service')

        VDiskController._logger.info('Creating new empty vDisk {0} of {1} bytes'.format(volume_name, volume_size))
        try:
            # noinspection PyArgumentList
            backend_config = MDSMetaDataBackendConfig([MDSNodeConfig(address=str(mds_service.service.storagerouter.ip),
                                                                     port=mds_service.service.ports[0])])
            volume_id = vpool.storagedriver_client.create_volume(target_path=devicename,
                                                                 metadata_backend_config=backend_config,
                                                                 volume_size="{0}B".format(volume_size),
                                                                 node_id=str(storagedriver.storagedriver_id),
                                                                 req_timeout_secs=30)
        except Exception as ex:
            VDiskController._logger.error('Creating new vDisk {0} failed: {1}'.format(volume_name, str(ex)))
            raise

        with volatile_mutex(VDiskController._VOLDRV_EVENT_KEY.format(volume_id), wait=30):
            new_vdisk = VDiskList.get_vdisk_by_volume_id(volume_id)
            if new_vdisk is None:
                new_vdisk = VDisk()
                new_vdisk.size = volume_size
                new_vdisk.vpool = vpool
                new_vdisk.volume_id = volume_id
                new_vdisk.devicename = devicename
                new_vdisk.cache_quota = cache_quota
                new_vdisk.description = volume_name
            new_vdisk.name = volume_name
            new_vdisk.pagecache_ratio = pagecache_ratio
            new_vdisk.save()
            VDiskController.vdisk_checkup(new_vdisk)

        VDiskController._logger.info('Created volume. Location {0}'.format(devicename))
        return new_vdisk.guid

    @staticmethod
    @ovs_task(name='ovs.vdisk.get_config_params')
    def get_config_params(vdisk_guid):
        """
        Retrieve the configuration parameters for the given vDisk from the storagedriver.
        :param vdisk_guid: Guid of the vDisk to retrieve the configuration for
        :type vdisk_guid: str
        :return: Storage driver configuration information for the vDisk
        :rtype: dict
        """
        vdisk = VDisk(vdisk_guid)
        vpool = vdisk.vpool

        storagedriver_config = StorageDriverConfiguration(vpool.guid, vdisk.storagedriver_id)
        volume_manager = storagedriver_config.configuration.get('volume_manager', {})
        cluster_size = storagedriver_config.configuration.get('volume_manager', {}).get('default_cluster_size', 4096)

        volume_id = str(vdisk.volume_id)
        try:
            sco_size = vdisk.storagedriver_client.get_sco_multiplier(volume_id, req_timeout_secs=10) / 1024 * (cluster_size / 1024)
            dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id, req_timeout_secs=10)
            tlog_multiplier = vdisk.storagedriver_client.get_tlog_multiplier(volume_id, req_timeout_secs=10)
            non_disposable_sco_factor = vdisk.storagedriver_client.get_sco_cache_max_non_disposable_factor(volume_id, req_timeout_secs=10)
        except Exception:
            VDiskController._logger.exception('Failed to retrieve configuration parameters for vDisk {0}'.format(vdisk.name))
            raise Exception('Retrieving configuration for vDisk {0} failed'.format(vdisk.name))

        dtl_target = []
        if dtl_config is None:
            dtl_mode = 'no_sync'
        else:
            dtl_mode = StorageDriverClient.REVERSE_DTL_MODE_MAP[dtl_config.mode]
            dtl_target = [junction.domain_guid for junction in vdisk.domains_dtl]

        if tlog_multiplier is None:
            tlog_multiplier = volume_manager.get('number_of_scos_in_tlog', 20)
        if non_disposable_sco_factor is None:
            non_disposable_sco_factor = volume_manager.get('non_disposable_scos_factor', 12)

        cache_quota = vdisk.cache_quota
        if cache_quota is None:
            vdisk.invalidate_dynamics('storagerouter_guid')
            metadata = vpool.metadata['backend']['caching_info'].get(vdisk.storagerouter_guid, {})
            cache_quota = {VPool.CACHES.FRAGMENT: metadata.get('quota_fc'),
                           VPool.CACHES.BLOCK: metadata.get('quota_bc')}

        return {'sco_size': sco_size,
                'dtl_mode': dtl_mode,
                'dtl_target': dtl_target,
                'cache_quota': cache_quota,
                'write_buffer': int(tlog_multiplier * sco_size * non_disposable_sco_factor),
                'pagecache_ratio': vdisk.pagecache_ratio}

    @staticmethod
    @ovs_task(name='ovs.vdisk.set_config_params')
    def set_config_params(vdisk_guid, new_config_params):
        """
        Sets configuration parameters for a given vDisk.
        DTL allocation rules:
            - First priority to StorageRouters located in the vDisk's StorageRouter's Recovery Domain
            - Second priority to StorageRouters located in the vDisk's StorageRouter's Regular Domain
            - If Domains configured, but no StorageRouters are found matching any of the Domains on the vDisk's StorageRouter, a random SR in the same vPool is chosen
            - If no Domains configured on the vDisk StorageRouter, any other StorageRouter on which the vPool has been extended is chosen

        :param vdisk_guid: Guid of the vDisk to set the configuration parameters for
        :type vdisk_guid: str
        :param new_config_params: New configuration parameters
        :type new_config_params: dict
        :return: None
        """
        # Backwards compatibility
        new_config_params.pop('dedupe_mode', None)
        new_config_params.pop('cache_strategy', None)
        new_config_params.pop('readcache_limit', None)
        new_config_params.pop('metadata_cache_size', None)

        Toolbox.verify_required_params(verify_keys=True,
                                       actual_params=new_config_params,
                                       required_params={'dtl_mode': (str, StorageDriverClient.VDISK_DTL_MODE_MAP.keys(), False),
                                                        'sco_size': (int, StorageDriverClient.TLOG_MULTIPLIER_MAP.keys(), False),
                                                        'dtl_target': (list, Toolbox.regex_guid, False),
                                                        'cache_quota': (dict, {VPool.CACHES.FRAGMENT: (int, {'min': 1024 ** 3 / 10, 'max': 1024 ** 4}, False),
                                                                               VPool.CACHES.BLOCK: (int, {'min': 1024 ** 3 / 10, 'max': 1024 ** 4}, False)}, False),
                                                        'write_buffer': (int, {'min': 128, 'max': 10 * 1024}, False),
                                                        'pagecache_ratio': (float, {'min': 0, 'max': 1, 'exclude': [0]}, False)})

        errors = False
        vdisk = VDisk(vdisk_guid)
        vdisk.invalidate_dynamics('storagerouter_guid')
        if vdisk.storagerouter_guid is None:
            raise ValueError('VDisk {0} is not linked to a StorageRouter'.format(vdisk.name))

        volume_id = str(vdisk.volume_id)
        old_config_params = VDiskController.get_config_params(vdisk.guid)

        #################
        # Update SCO size
        # (This impacts TLOG multiplier which on its turn impacts write buffer)
        new_sco_size = new_config_params.pop('sco_size', 0)
        old_sco_size = old_config_params['sco_size']
        if new_sco_size > 0 and new_sco_size != old_sco_size:
            write_buffer = float(new_config_params['write_buffer']) if 'write_buffer' in new_config_params else float(old_config_params['write_buffer'])
            tlog_multiplier = StorageDriverClient.TLOG_MULTIPLIER_MAP[new_sco_size]
            sco_factor = write_buffer / tlog_multiplier / new_sco_size
            try:
                VDiskController._logger.info('Updating property sco_size on vDisk {0} to {1}'.format(vdisk.name, new_sco_size))
                vdisk.storagedriver_client.set_sco_multiplier(volume_id, new_sco_size / 4 * 1024, req_timeout_secs=10)
                vdisk.storagedriver_client.set_tlog_multiplier(volume_id, tlog_multiplier, req_timeout_secs=10)
                vdisk.storagedriver_client.set_sco_cache_max_non_disposable_factor(volume_id, sco_factor, req_timeout_secs=10)
                VDiskController._logger.info('Updated property sco_size')
            except Exception:
                VDiskController._logger.exception('Error updating "sco_size"')
                errors = True

        ############
        # Update DTL
        new_dtl_mode = new_config_params.pop('dtl_mode', None)
        old_dtl_mode = old_config_params['dtl_mode']
        new_dtl_targets = set(new_config_params.pop('dtl_target', []))  # Domain guids
        old_dtl_targets = set(old_config_params['dtl_target'])

        if new_dtl_mode is not None and new_dtl_mode == 'no_sync' and len(new_dtl_targets) > 0:
            raise ValueError('Invalid DTL settings specified')

        if new_dtl_mode is not None and (old_dtl_mode != new_dtl_mode or new_dtl_targets != old_dtl_targets):
            dtl_targets = []
            for domain_guid in new_dtl_targets:
                try:
                    dtl_targets.append(Domain(domain_guid))
                except ObjectNotFoundException:
                    raise ValueError('Non-existing Domain guid provided: {0}'.format(domain_guid))

            # Set manual DTL flag (Before 'set_manual_dtl_config' is called, because of DTL state transition event)
            vpool_config = vdisk.vpool.configuration
            orig_manual_flag = vdisk.has_manual_dtl
            Toolbox.verify_required_params(actual_params=vpool_config, required_params={'dtl_enabled': (bool, None)})

            manual = len(dtl_targets) > 0
            if vpool_config['dtl_enabled'] is False:
                if new_dtl_mode != 'no_sync':
                    manual = True
            else:
                if new_dtl_mode == 'no_sync':
                    manual = True
            vdisk.has_manual_dtl = manual
            vdisk.save()

            if new_dtl_mode == 'no_sync':
                VDiskController._logger.info('Disabling DTL for vDisk {0}'.format(vdisk.name))
                try:
                    vdisk.storagedriver_client.set_manual_dtl_config(volume_id, None, req_timeout_secs=10)
                    for junction in vdisk.domains_dtl:
                        junction.delete()
                    vdisk.invalidate_dynamics('dtl_status')
                    VDiskController._logger.info('Disabled DTL for vDisk {0}'.format(vdisk.name))
                except Exception:
                    VDiskController._logger.exception('Failed to disable DTL for vDisk {0}'.format(vdisk.name))
                    errors = True
            else:
                VDiskController._logger.info('Checking if reconfiguration is required based on new parameters for vDisk {0}'.format(vdisk.name))
                dtl_config = None
                importances = VDiskController._retrieve_possible_dtl_targets(vdisk=vdisk, dtl_targets=dtl_targets)
                for possible_storagerouters in importances:
                    if os.environ.get('RUNNING_UNITTESTS') == 'True':
                        possible_storagerouters.sort(key=lambda i: i.guid)
                    else:
                        random.shuffle(possible_storagerouters)

                    for storagerouter in possible_storagerouters:
                        for sd in sorted(storagerouter.storagedrivers, key=lambda i: i.guid):
                            if sd.vpool_guid != vdisk.vpool_guid:
                                continue
                            VDiskController._logger.info('Setting DTL to {0}:{1} for vDisk {2}'.format(sd.storage_ip, sd.ports['dtl'], vdisk.name))
                            dtl_config = DTLConfig(str(sd.storage_ip), sd.ports['dtl'], StorageDriverClient.VDISK_DTL_MODE_MAP[new_dtl_mode])
                            try:
                                vdisk.storagedriver_client.set_manual_dtl_config(volume_id, dtl_config, req_timeout_secs=10)
                                VDiskController._logger.info('Configured DTL to {0}:{1} for vDisk {2}'.format(sd.storage_ip, sd.ports['dtl'], vdisk.name))
                                vdisk.invalidate_dynamics(['dtl_status'])
                                break
                            except Exception:
                                VDiskController._logger.exception('Failed to update the current DTL configuration for vDisk {0}'.format(vdisk.name))
                                dtl_config = None

                        if dtl_config is not None:
                            break
                    if dtl_config is not None:
                        break

                if dtl_config is None:
                    VDiskController._logger.error('No possible StorageRouters found to configure DTL on for vDisk {0}'.format(vdisk.name))
                    errors = True
                else:
                    # Reset relations
                    VDiskController._logger.info('Successfully configured DTL to {0}:{1} for vDisk {2}'.format(dtl_config.host, dtl_config.port, vdisk.name))
                    for junction in vdisk.domains_dtl:
                        junction.delete()
                    for domain in dtl_targets:
                        vdisk_domain = VDiskDomain()
                        vdisk_domain.vdisk = vdisk
                        vdisk_domain.domain = domain
                        vdisk_domain.save()

            if errors is True:
                # Restore manual DTL flag
                vdisk.has_manual_dtl = orig_manual_flag
                vdisk.save()

        ###############
        # Update others
        for key in new_config_params:
            try:
                new_value = new_config_params[key]
                old_value = old_config_params[key]
                if new_value != old_value:
                    VDiskController._logger.info('Updating property {0} on vDisk {1} from to {2}'.format(key, vdisk.name, new_value))
                    if key == 'write_buffer':
                        tlog_multiplier = vdisk.storagedriver_client.get_tlog_multiplier(volume_id, req_timeout_secs=5) or StorageDriverClient.TLOG_MULTIPLIER_MAP[new_sco_size]
                        sco_factor = float(new_value) / tlog_multiplier / new_sco_size
                        vdisk.storagedriver_client.set_sco_cache_max_non_disposable_factor(volume_id, sco_factor, req_timeout_secs=10)
                    elif key == 'pagecache_ratio':
                        vdisk.pagecache_ratio = new_value
                        vdisk.save()
                        VDiskController._set_vdisk_metadata_pagecache_size(vdisk)
                    elif key == 'cache_quota':
                        vdisk.cache_quota = new_value
                        VDiskController.vdisk_checkup(vdisk)
                        vdisk.save()
                    else:
                        raise KeyError('Unsupported property provided: "{0}"'.format(key))
                    VDiskController._logger.info('Updated property {0}'.format(key))
            except Exception:
                VDiskController._logger.exception('Error updating "{0}"'.format(key))
                errors = True

        if errors is True:
            raise Exception('Failed to update the values for vDisk {0}'.format(vdisk.name))

    @staticmethod
    @ovs_task(name='ovs.vdisk.dtl_checkup', schedule=Schedule(minute='15', hour='0,4,8,12,16,20'), ensure_single_info={'mode': 'DEDUPED'})
    def dtl_checkup(vpool_guid=None, vdisk_guid=None, storagerouters_to_exclude=None):
        """
        Check DTL for all volumes, for all volumes of a vPool or for 1 specific volume
        DTL allocation rules:
            - First priority to StorageRouters located in the vDisk's StorageRouter's Recovery Domain
            - Second priority to StorageRouters located in the vDisk's StorageRouter's Regular Domain
            - If Domains configured, but no StorageRouters are found matching any of the Domains on the vDisk's StorageRouter, a random SR in the same vPool is chosen
            - If no Domains configured on the vDisk StorageRouter, any other StorageRouter on which the vPool has been extended is chosen

        :param vpool_guid: vPool to check the DTL configuration of all its vDisks
        :type vpool_guid: str
        :param vdisk_guid: vDisk to check its DTL configuration
        :type vdisk_guid: str
        :param storagerouters_to_exclude: Storage Router Guids to exclude from possible targets
        :type storagerouters_to_exclude: list
        :return: None
        :rtype: NoneType
        """
        service_manager = ServiceFactory.get_manager()

        if vpool_guid is not None and vdisk_guid is not None:
            raise ValueError('vPool and vDisk are mutually exclusive')
        if storagerouters_to_exclude is None:
            storagerouters_to_exclude = []

        VDiskController._logger.info('DTL checkup started')
        vdisk = None
        vpool = None
        if vdisk_guid is not None:
            try:
                vdisk = VDisk(vdisk_guid)
            except ObjectNotFoundException:
                VDiskController._logger.warning('    vDisk with guid {0} no longer available in model, skipping this iteration'.format(vdisk_guid))
                return
        if vpool_guid is not None:
            try:
                vpool = VPool(vpool_guid)
            except ObjectNotFoundException:
                VDiskController._logger.warning('    vPool with guid {0} no longer available in model, skipping this iteration'.format(vpool_guid))
                return

        errors_found = False
        root_client_map = {}
        vdisks = VDiskList.get_vdisks() if vdisk is None and vpool is None else vpool.vdisks if vpool is not None else [vdisk]
        iteration = 0
        while len(vdisks) > 0:
            time_to_wait_for_lock = iteration * 10 + 1
            iteration += 1
            if time_to_wait_for_lock > 40:
                VDiskController._logger.error('vDisks with guids {0} could not be checked'.format(', '.join([vdisk.guid for vdisk in vdisks])))
                errors_found = True
                break
            vdisks_copy = list(vdisks)
            for vdisk in vdisks_copy:
                try:
                    ####################
                    # GATHER INFORMATION
                    VDiskController._logger.info('    Verifying vDisk {0} with guid {1}'.format(vdisk.name, vdisk.guid))
                    vdisk.invalidate_dynamics(['storagedriver_client', 'storagerouter_guid'])
                    if vdisk.storagedriver_client is None:
                        vdisks.remove(vdisk)
                        VDiskController._logger.warning('    VDisk {0} with guid {1} does not have a storagedriver client'.format(vdisk.name, vdisk.guid))
                        continue

                    vpool = vdisk.vpool
                    lock_key = 'dtl_checkup_{0}'.format(vdisk.guid)
                    vpool_config = vpool.configuration
                    Toolbox.verify_required_params(required_params={'dtl_mode': (str, StorageDriverClient.VPOOL_DTL_MODE_MAP.keys()),
                                                                    'dtl_enabled': (bool, None),
                                                                    'dtl_config_mode': (str, [StorageDriverClient.VOLDRV_DTL_MANUAL_MODE, StorageDriverClient.VOLDRV_DTL_AUTOMATIC_MODE])},
                                                   actual_params=vpool_config)

                    volume_id = str(vdisk.volume_id)
                    dtl_vpool_enabled = vpool_config['dtl_enabled']
                    dtl_vpool_config_mode = vpool_config['dtl_config_mode']
                    try:
                        current_dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id, req_timeout_secs=5)
                        current_dtl_config_mode = vdisk.storagedriver_client.get_dtl_config_mode(volume_id, req_timeout_secs=5)
                    except Exception:
                        # Can occur when a volume has not been stolen yet from a dead node
                        VDiskController._logger.exception('    VDisk {0} with guid {1}: Failed to retrieve the DTL configuration from storage driver'.format(vdisk.name, vdisk.guid))
                        errors_found = True
                        vdisks.remove(vdisk)
                        continue

                    ##########################
                    # CHECKS FOR DISABLED DTLs
                    if dtl_vpool_enabled is False and current_dtl_config is None:
                        if current_dtl_config_mode == DTLConfigMode.AUTOMATIC:
                            VDiskController._logger.info('    DTL is globally disabled for vPool {0} with guid {1}. Setting to MANUAL mode for vDisk {2}'.format(vpool.name, vpool.guid, vdisk.name))
                            try:
                                with volatile_mutex(lock_key, wait=time_to_wait_for_lock):
                                    vdisk.storagedriver_client.set_manual_dtl_config(volume_id, None, req_timeout_secs=10)
                                    vdisk.invalidate_dynamics(['dtl_status'])
                            except NoLockAvailableException:
                                VDiskController._logger.info('    Could not acquire lock while trying to set MANUAL mode for vDisk {0}, continuing with next vDisk'.format(vdisk.name))
                                continue
                        vdisks.remove(vdisk)
                        continue
                    elif current_dtl_config_mode == DTLConfigMode.MANUAL and current_dtl_config is None and vdisk.has_manual_dtl is True:
                        VDiskController._logger.info('    DTL is disabled for vDisk {0} with guid {1}'.format(vdisk.name, vdisk.guid))
                        vdisks.remove(vdisk)
                        continue

                    ##########################
                    # DETERMINE CURRENT TARGET
                    current_target = None
                    if current_dtl_config is not None:
                        sds = [sd for sd in vpool.storagedrivers if sd.storage_ip == current_dtl_config.host]
                        if len(sds) > 0:
                            current_target = sds[0]

                    ###################
                    # VERIFY MANUAL DTL
                    importances = VDiskController._retrieve_possible_dtl_targets(vdisk=vdisk)
                    if vdisk.has_manual_dtl is True:
                        VDiskController._logger.info('    VDisk {0} with guid {1} has a manual DTL configuration'.format(vdisk.name, vdisk.guid))
                        if current_dtl_config is None:
                            VDiskController._logger.info('    VDisk {0} with guid {1} has a manually disabled DTL'.format(vdisk.name, vdisk.guid))
                            vdisks.remove(vdisk)
                            continue

                        correct = False
                        for possible_storagerouters in importances[:2]:  # Only allow current_target in primary or secondary for manual DTL
                            if len(possible_storagerouters) > 0:
                                if current_target.storagerouter in possible_storagerouters:
                                    correct = True
                                break
                        if correct is True:
                            VDiskController._logger.info('    VDisk {0} with guid {1} manual DTL configuration is valid'.format(vdisk.name, vdisk.guid))
                        else:
                            VDiskController._logger.warning('OVS_WARNING: VDisk {0} with guid {1} manual DTL configuration is no longer valid ({2})'.format(vdisk.name, vdisk.guid, current_dtl_config))
                        vdisks.remove(vdisk)
                        continue

                    ######################
                    # DETERMINE NEW TARGET
                    new_targets = []
                    for index, possible_storagerouters in enumerate(importances):
                        VDiskController._logger.info('    Checking {0} StorageRouters'.format('primary' if index == 0 else 'secondary' if index == 1 else 'all vPool related'))
                        for storagerouter in possible_storagerouters:
                            if storagerouter in storagerouters_to_exclude:
                                continue
                            if storagerouter not in root_client_map:
                                root_client_map[storagerouter] = None
                                try:
                                    root_client = SSHClient(endpoint=storagerouter, username='root')
                                    service_name = 'dtl_{0}'.format(vpool.name)
                                    if service_manager.has_service(service_name, client=root_client) is True and service_manager.get_service_status(service_name, client=root_client) == 'active':
                                        root_client_map[storagerouter] = root_client
                                    else:
                                        VDiskController._logger.warning('    DTL service on Storage Router with IP {0} is not reachable'.format(storagerouter.ip))
                                except UnableToConnectException:
                                    VDiskController._logger.warning('    Storage Router with IP {0} of vDisk {1} is not reachable'.format(storagerouter.ip, vdisk.name))
                            if root_client_map[storagerouter] is not None:
                                new_targets.append(storagerouter)
                        if len(new_targets) > 0:  # StorageRouters with highest possible priority found
                            break

                    #################################
                    # VERIFY RECONFIGURATION REQUIRED
                    reconfigure_required = False
                    if current_dtl_config is None:
                        VDiskController._logger.info('        No DTL configuration found, but there are Storage Routers available')
                        reconfigure_required = True
                    elif current_dtl_config_mode == DTLConfigMode.AUTOMATIC:
                        VDiskController._logger.info('        DTL configuration set to AUTOMATIC, switching to MANUAL')
                        reconfigure_required = True
                    elif dtl_vpool_config_mode == DTLConfigMode.MANUAL and dtl_vpool_enabled is True:
                        VDiskController._logger.info('        DTL configuration set to MANUAL, but static host provided ... overruling')
                        reconfigure_required = True
                    elif current_target is None:
                        VDiskController._logger.info('        DTL configuration set to MANUAL, but no StorageRouter found ... correcting')
                        reconfigure_required = True
                    elif current_target is not None and len(new_targets) == 0:
                        VDiskController._logger.info('        DTL configuration set to MANUAL, but no new StorageRouter found ... setting to STANDALONE')
                        reconfigure_required = True
                    elif current_target.storagerouter not in new_targets:
                        VDiskController._logger.info('        DTL configuration is not optimal, updating to new location')
                        reconfigure_required = True
                    elif current_dtl_config.port != current_target.ports['dtl']:
                        VDiskController._logger.info('        Configured port does not match expected port ({0} vs {1})'.format(current_dtl_config.port, current_target.ports['dtl']))
                        reconfigure_required = True

                    if reconfigure_required is False:
                        vdisks.remove(vdisk)
                        continue

                    #####################
                    # RECONFIGURE THE DTL
                    if len(new_targets) == 0:
                        dtl_config = None
                        VDiskController._logger.info('        DTL config that will be set -->  None')
                    else:
                        sds = [sd for sd in vpool.storagedrivers if sd.storagerouter == new_targets[0]]
                        if len(sds) == 0:
                            VDiskController._logger.error('Could not retrieve related storagedriver')
                            errors_found = True
                            vdisks.remove(vdisk)
                            continue

                        sd = sds[0]
                        dtl_ip = str(sd.storage_ip)
                        dtl_port = sd.ports['dtl']
                        dtl_mode = vpool_config['dtl_mode'] if current_dtl_config is None else StorageDriverClient.REVERSE_DTL_MODE_MAP[current_dtl_config.mode]
                        dtl_config = DTLConfig(dtl_ip, dtl_port, StorageDriverClient.VDISK_DTL_MODE_MAP[dtl_mode])
                        VDiskController._logger.info('        DTL config that will be set -->  Host: {0}, Port: {1}, Mode: {2}'.format(dtl_ip, dtl_port, dtl_mode))
                    try:
                        with volatile_mutex(lock_key, wait=time_to_wait_for_lock):
                            vdisk.storagedriver_client.set_manual_dtl_config(volume_id, dtl_config, req_timeout_secs=10)
                            vdisk.has_manual_dtl = False  # As soon as DTL checkup changes DTL settings, its no longer manual
                            vdisk.save()
                            vdisk.invalidate_dynamics(['dtl_status'])
                            vdisks.remove(vdisk)
                    except NoLockAvailableException:
                        VDiskController._logger.info('    Could not acquire lock, continuing with next vDisk')
                except Exception:
                    errors_found = True
                    VDiskController._logger.exception('Something went wrong configuring the DTL for vDisk {0} with guid {1}'.format(vdisk.name, vdisk.guid))
                    vdisks.remove(vdisk)

        if errors_found is True:
            VDiskController._logger.error('DTL checkup ended with errors')
            raise Exception('DTL checkup failed with errors. Please check logging for more information')
        VDiskController._logger.info('DTL checkup ended')

    @staticmethod
    @ovs_task(name='ovs.vdisk.dtl_state_transition')
    @log('VOLUMEDRIVER_TASK')
    def dtl_state_transition(volume_id, old_state, new_state, storagedriver_id):
        """
        Triggered by volumedriver when DTL state changes
        :param volume_id: ID of the volume
        :type volume_id: str
        :param old_state: Previous DTL status
        :type old_state: int
        :param new_state: New DTL status
        :type new_state: int
        :param storagedriver_id: ID of the storagedriver hosting the volume
        :type storagedriver_id: str
        :return: None
        """
        if new_state == VolumeDriverEvents_pb2.Degraded and old_state != VolumeDriverEvents_pb2.Standalone:
            vdisk = VDiskList.get_vdisk_by_volume_id(volume_id)
            if vdisk:
                VDiskController._logger.info('Degraded DTL detected for volume {0} with guid {1}'.format(vdisk.name, vdisk.guid))
                storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
                VDiskController.dtl_checkup(vdisk_guid=vdisk.guid,
                                            storagerouters_to_exclude=[storagedriver.storagerouter_guid],
                                            ensure_single_timeout=600)

    @staticmethod
    @ovs_task(name='ovs.vdisk.schedule_backend_sync')
    def schedule_backend_sync(vdisk_guid):
        """
        Schedule a backend sync on a vDisk
        :param vdisk_guid: Guid of vDisk to schedule a backend sync to
        :type vdisk_guid: str
        :return: TLogName associated with the data sent off to the backend
        :rtype: str
        """
        vdisk = VDisk(vdisk_guid)
        VDiskController._logger.info('Schedule backend sync for vdisk {0}'.format(vdisk.name))
        try:
            return vdisk.storagedriver_client.schedule_backend_sync(str(vdisk.volume_id))
        except Exception:
            VDiskController._logger.exception('Failed to schedule backend synchronization for vDisk {0}'.format(vdisk.name))
            raise Exception('Scheduling backend sync failed for vDisk {0}'.format(vdisk.name))

    @staticmethod
    @ovs_task(name='ovs.vdisk.is_volume_synced_up_to_tlog')
    def is_volume_synced_up_to_tlog(vdisk_guid, tlog_name):
        """
        Verify if a volume is synced up to a specific tlog
        :param vdisk_guid: Guid of vDisk to verify
        :type vdisk_guid: str
        :param tlog_name: Tlog_name to verify
        :type tlog_name: str
        :return: True or False
        :rtype: bool
        """
        vdisk = VDisk(vdisk_guid)
        try:
            return vdisk.storagedriver_client.is_volume_synced_up_to_tlog(str(vdisk.volume_id), str(tlog_name), req_timeout_secs=10)
        except Exception:
            VDiskController._logger.exception('Failed to verify whether vDisk {0} is synced up to tlog'.format(vdisk.name))
            raise Exception('Verifying if vDisk {0} is synced up to tlog failed'.format(vdisk.name))

    @staticmethod
    @ovs_task(name='ovs.vdisk.is_volume_synced_up_to_snapshot')
    def is_volume_synced_up_to_snapshot(vdisk_guid, snapshot_id):
        """
        Verify if a volume is synced up to a specific snapshot
        :param vdisk_guid: Guid of vDisk to verify
        :type vdisk_guid: str
        :param snapshot_id: Snapshot_id to verify
        :type snapshot_id: str
        :return: True or False
        :rtype: bool
        """
        vdisk = VDisk(vdisk_guid)
        try:
            return vdisk.storagedriver_client.is_volume_synced_up_to_snapshot(str(vdisk.volume_id), str(snapshot_id), req_timeout_secs=10)
        except Exception:
            VDiskController._logger.exception('Failed to verify whether vDisk {0} is synced up to snapshot'.format(vdisk.name))
            raise Exception('Verifying if vDisk {0} is synced up to snapshot failed'.format(vdisk.name))

    @staticmethod
    @ovs_task(name='ovs.vdisk.restart')
    def restart(vdisk_guid, force):
        """
        Restart the given vDisk
        :param vdisk_guid: The guid of the vDisk to restart
        :type vdisk_guid: str
        :param force: Force a restart at a possible cost of data loss
        :type force: bool
        :return: None
        :rtype: NoneType
        """
        vdisk = VDisk(vdisk_guid)
        vdisk.invalidate_dynamics('info')
        if vdisk.info['live_status'] == 'RUNNING':
            raise ValueError('Cannot restart a volume which is RUNNING')

        vdisk.storagedriver_client.restart_object(object_id=str(vdisk.volume_id),
                                                  force_restart=force,
                                                  req_timeout_secs=60)
        vdisk.invalidate_dynamics(['info', 'dtl_status'])

    @staticmethod
    @ovs_task(name='ovs.vdisk.sync_with_reality', schedule=Schedule(minute='30', hour='*'))
    def sync_with_reality(vpool_guid=None):
        """
        Syncs vDisks in the model with reality
        :param vpool_guid: Optional vPool guid. All vPools if omitted
        :type vpool_guid: str or None
        :return: None
        :rtype: NoneType
        """
        from ovs.extensions.storageserver.storagedriver import FeatureNotAvailableException

        if vpool_guid is None:
            vpools = VPoolList.get_vpools()
        else:
            vpools = [VPool(vpool_guid)]
        for vpool in vpools:
            vdisks = dict((str(vdisk.volume_id), vdisk) for vdisk in vpool.vdisks)
            for entry in vpool.objectregistry_client.get_all_registrations():
                volume_id = entry.object_id()
                if volume_id not in vdisks:
                    with volatile_mutex(VDiskController._VOLDRV_EVENT_KEY.format(volume_id), wait=30):
                        new_vdisk = VDiskList.get_vdisk_by_volume_id(volume_id)
                        if new_vdisk is None:
                            VDiskController._logger.info('OVS_WARNING: Adding vDisk to model. ID: {0}'.format(volume_id))
                            new_vdisk = VDisk()
                            new_vdisk.volume_id = volume_id
                            new_vdisk.vpool = vpool
                            try:
                                fsmetadata_client = new_vdisk.fsmetadata_client
                                devicename = fsmetadata_client.lookup(volume_id)
                                name = VDiskController.extract_volumename(devicename)
                            except FeatureNotAvailableException:
                                VDiskController._logger.exception('Could not load device name from StorageDriver')
                                devicename = '/{0}.raw'.format(volume_id)
                                name = volume_id
                            new_vdisk.name = name
                            new_vdisk.size = new_vdisk.info['volume_size']
                            new_vdisk.devicename = devicename
                            new_vdisk.description = name
                            new_vdisk.pagecache_ratio = 1.0
                            new_vdisk.metadata = {'lba_size': new_vdisk.info['lba_size'],
                                                  'cluster_multiplier': new_vdisk.info['cluster_multiplier']}
                            new_vdisk.save()
                            VDiskController.vdisk_checkup(new_vdisk)
                else:
                    del vdisks[volume_id]
            for volume_id, vdisk in vdisks.iteritems():
                with volatile_mutex(VDiskController._VOLDRV_EVENT_KEY.format(volume_id), wait=30):
                    if vpool.objectregistry_client.find(str(volume_id)) is None:
                        VDiskController._logger.info('OVS_WARNING: Removing vDisk from model. ID: {0} - Guid: {1}'.format(volume_id, vdisk.guid))
                        VDiskController.clean_vdisk_from_model(vdisk)

    @staticmethod
    def _wait_for_snapshot_to_be_synced_to_backend(vdisk_guid, snapshot_id):
        tries = 25  # 5 minutes
        while VDiskController.is_volume_synced_up_to_snapshot(vdisk_guid=vdisk_guid, snapshot_id=snapshot_id) is False and tries > 0:
            sleep_amount = 25 - tries
            time.sleep(sleep_amount)
            tries -= 1
            VDiskController._logger.info('Waiting for snapshot to be synced, waited {0} second{1}'.format(sleep_amount, '' if sleep_amount == 1 else 's'))
        if tries == 0:
            raise RuntimeError('Snapshot {0} of volume {1} still not synced to backend, not waiting any longer'.format(snapshot_id, vdisk_guid))

    @staticmethod
    def _set_vdisk_metadata_pagecache_size(vdisk):
        """
        Set metadata page cache size to configured ratio

        Terminology:
        cache_capacity (the value set to set_metadata_cache_capacity) is the "number of pages to cache"
        one page can cache "metadata_page_capacity" entries
        an entry addresses one cluster of a volume

        Example:
        A volume has a cluster_size of 4k (default) and a metadata_page_capacity of 32. A single page addresses 4k * 32 = 128k of a volume
        So if a volume's size is 256M, the cache should have a capacity (cache_capacity) of 1024 to be completely in memory

        Example 2:
        A volume has a size of 256M, and a cluster_size of 4k, and a metadata_page_capacity of 32
        If we want 10% of that volume to be cached, we need 256M / (4k * 32 = 128k) = 2048 => a cache_capacity of 205

        :param vdisk: Object vDisk
        :type vdisk: ovs.dal.hybrids.vdisk.VDisk
        :return: None
        :rtype: NoneType
        """
        service_manager = ServiceFactory.get_manager()
        if vdisk.vpool.metadata_store_bits is None:
            bits = None
            for storagedriver in vdisk.vpool.storagedrivers:
                entries = service_manager.extract_from_service_file(name='ovs-volumedriver_{0}'.format(vdisk.vpool.name),
                                                                    client=SSHClient(endpoint=storagedriver.storagerouter, username='root'),
                                                                    entries=['METADATASTORE_BITS='])
                if len(entries) == 1:
                    bits = entries[0].split('=')[-1]
                    bits = int(bits) if bits.isdigit() else 5
                    break
            vdisk.vpool.metadata_store_bits = bits
            vdisk.vpool.save()

        storagedriver_id = vdisk.storagedriver_id
        if vdisk.vpool.metadata_store_bits is None or storagedriver_id is None:
            VDiskController._logger.warning('OVS_WARNING: Failed to set the page cache size for vDisk {0}'.format(vdisk.name))
            return

        ratio = vdisk.pagecache_ratio
        storagedriver_config = StorageDriverConfiguration(vdisk.vpool_guid, storagedriver_id)
        cluster_size = storagedriver_config.configuration.get('volume_manager', {}).get('default_cluster_size', 4096)

        # noinspection PyTypeChecker
        metadata_page_size = float(2 ** vdisk.vpool.metadata_store_bits * cluster_size)
        cache_capacity = int(math.ceil(vdisk.size / metadata_page_size * ratio))

        max_cache_capacity = int(2 * 1024 ** 4 / metadata_page_size)
        cache_capacity = min(max_cache_capacity, cache_capacity)
        VDiskController._logger.info('Setting metadata page cache size for vdisk {0} to {1}'.format(vdisk.name, cache_capacity))
        vdisk.storagedriver_client.set_metadata_cache_capacity(str(vdisk.volume_id), cache_capacity, req_timeout_secs=10)

    @staticmethod
    def clean_devicename(name):
        """
        Clean a name into a usable filename
        :param name: Name of the device
        :type name: str
        :return: A cleaned device name
        :rtype: str
        """
        name = name.strip('/').replace(' ', '_')
        while '//' in name:
            name = name.replace('//', '/')
        name = re.compile('[^/\w\-.]+').sub('', name)
        filename = name.split('/')[-1]
        if len(filename) > 4 and filename[-4:] == '.raw':
            return '/{0}'.format(name)
        return '/{0}.raw'.format(name)

    @staticmethod
    def extract_volumename(devicename):
        """
        Extracts a reasonable volume name out of a given device name
        :param devicename: A raw device name of a volume (e.g. /foo/bar.raw)
        :type devicename: str
        :return: A cleaned up volume name (e.g. bar)
        """
        return devicename.rsplit('/', 1)[-1].rsplit('.', 1)[0]

    @staticmethod
    def _retrieve_possible_dtl_targets(vdisk, dtl_targets=None):
        """
        Retrieve all the StorageRouters which could serve as a possible DTL target location for the specified vDisk
        A list of lists is returned, with the order of lists as following:
            * 1st list: StorageRouters which have a Regular Domain identical to the Recovery Domain of the hosting StorageRouter of the vDisk
            * 2nd list: StorageRouters which have a Regular Domain identical to the Regular Domain of the hosting StorageRouter of the vDisk
            * 3rd list: StorageRouters on which the vPool of the vDisk has been extended to, except for the hosting StorageRouter
        """
        this_sr = StorageRouter(vdisk.storagerouter_guid)
        other_storagerouters = set([sd.storagerouter for sd in vdisk.vpool.storagedrivers if sd.storagerouter != this_sr])

        # Retrieve all StorageRouters linked to the Recovery Domains (primary) and Regular Domains (secondary) for the StorageRouter hosting this vDisk
        primary = set()
        secondary = set()
        for junction in this_sr.domains:
            if junction.backup is True:
                primary.update(set(StorageRouterList.get_primary_storagerouters_for_domain(junction.domain)))
            else:
                secondary.update(set(StorageRouterList.get_primary_storagerouters_for_domain(junction.domain)))
        primary = primary.intersection(other_storagerouters)
        secondary = secondary.difference(primary)
        secondary = secondary.intersection(other_storagerouters)

        domains = []
        if dtl_targets is None:  # Used by DTL checkup
            domains = [junction.domain for junction in vdisk.domains_dtl]
        elif len(dtl_targets) > 0:  # Used by get_set_config_params
            domains = dtl_targets

        if len(domains) > 0:
            manual_srs = set()
            for domain in domains:
                manual_srs.update(set(StorageRouterList.get_primary_storagerouters_for_domain(domain)))

            # Determine possibilities based on priority
            primary = manual_srs.intersection(primary)
            secondary = manual_srs.intersection(secondary)
            other_storagerouters = manual_srs.intersection(other_storagerouters)

        primary = list(primary)
        secondary = list(secondary)
        other_storagerouters = list(other_storagerouters)

        # Randomize the order of StorageRouters for the DTL checkup
        random.shuffle(primary)
        random.shuffle(secondary)
        random.shuffle(other_storagerouters)
        return [primary, secondary, other_storagerouters]
