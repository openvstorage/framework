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
import time
import uuid
import pickle
import random
from Queue import Queue
from ovs.celery_run import celery
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.domain import Domain
from ovs.dal.hybrids.j_vdiskdomain import VDiskDomain
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.domainlist import DomainList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.volatilemutex import NoLockAvailableException, volatile_mutex
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.storageserver.storagedriver import DTLConfig, DTLConfigMode, MDSMetaDataBackendConfig, MDSNodeConfig, \
                                                       StorageDriverClient, StorageDriverConfiguration, \
                                                       SRCObjectNotFoundException, FeatureNotAvailableException
from ovs.lib.helpers.decorators import ensure_single, log
from ovs.lib.helpers.toolbox import Schedule, Toolbox
from ovs.lib.mdsservice import MDSServiceController
from ovs.log.log_handler import LogHandler
from volumedriver.storagerouter import storagerouterclient, VolumeDriverEvents_pb2


class VDiskController(object):
    """
    Contains all BLL regarding VDisks
    """
    _logger = LogHandler.get('lib', name='vdisk')

    storagerouterclient.Logger.setupLogging(LogHandler.load_path('storagerouterclient'))
    # noinspection PyArgumentList
    storagerouterclient.Logger.enableLogging()

    @staticmethod
    @celery.task(name='ovs.vdisk.list_volumes')
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
        VDiskController.dtl_checkup.delay(vdisk_guid=vdisk.guid)
        try:
            VDiskController._set_vdisk_metadata_pagecache_size(vdisk)
            MDSServiceController.ensure_safety(vdisk)
        except Exception:
            VDiskController._logger.exception('Error during vDisk checkup')
            if vdisk.objectregistry_client.find(str(vdisk.volume_id)) is None:
                VDiskController._logger.warning('Volume {0} does not exist anymore.'.format(vdisk.volume_id))
                VDiskController.clean_vdisk_from_model(vdisk)

    @staticmethod
    @celery.task(name='ovs.vdisk.delete_from_voldrv')
    @log('VOLUMEDRIVER_TASK')
    def delete_from_voldrv(volume_id):
        """
        Delete a vDisk from model only since its been deleted on volumedriver
        Triggered by volumedriver messages on the queue
        :param volume_id: Volume ID of the vDisk
        :type volume_id: str
        :return: None
        """
        with volatile_mutex('voldrv_event_disk_{0}'.format(volume_id), wait=20):
            vdisk = VDiskList.get_vdisk_by_volume_id(volume_id)
            if vdisk is not None:
                VDiskController.clean_vdisk_from_model(vdisk)
            else:
                VDiskController._logger.info('Volume {0} does not exist'.format(volume_id))

    @staticmethod
    @celery.task(name='ovs.vdisk.delete')
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
        vdisk.storagedriver_client.unlink(str(vdisk.devicename))
        VDiskController.delete_from_voldrv(vdisk.volume_id)

    @staticmethod
    @celery.task(name='ovs.vdisk.extend')
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

    @staticmethod
    @celery.task(name='ovs.vdisk.resize_from_voldrv')
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
        with volatile_mutex('voldrv_event_disk_{0}'.format(volume_id), wait=30):
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

    @staticmethod
    @celery.task(name='ovs.vdisk.migrate_from_voldrv')
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
    @celery.task(name='ovs.vdisk.rename_from_voldrv')
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
        from ovs.extensions.generic.toolbox import ExtensionsToolbox

        old_path = '/{0}/'.format(old_path.strip('/'))
        new_path = '/{0}/'.format(new_path.strip('/'))
        storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
        vpool = storagedriver.vpool
        VDiskController._logger.debug('Processing rename on {0} from {1} to {2}'.format(vpool.name, old_path, new_path))
        for vdisk in vpool.vdisks:
            devicename = vdisk.devicename
            if devicename.startswith(old_path):
                volume_id = vdisk.volume_id
                with volatile_mutex('voldrv_event_disk_{0}'.format(volume_id), wait=30):
                    vdisk.discard()
                    devicename = vdisk.devicename
                    if devicename.startswith(old_path):
                        vdisk.devicename = '{0}{1}'.format(new_path, ExtensionsToolbox.remove_prefix(devicename, old_path))
                        vdisk.save()
                        VDiskController._logger.info('Renamed devicename from {0} to {1} on vDisk {2}'.format(devicename, vdisk.devicename, vdisk.guid))

    @staticmethod
    @celery.task(name='ovs.vdisk.clone')
    def clone(vdisk_guid, name, snapshot_id=None, storagerouter_guid=None, pagecache_ratio=None):
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
        :param pagecache_ratio: Ratio of the pagecache size (compared to a 100% cache)
        :type pagecache_ratio: float
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
            if not 0 < pagecache_ratio <= 1:
                raise RuntimeError('Parameter pagecache_ratio must be 0 < x <= 1')

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
                except:
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

        with volatile_mutex('voldrv_event_disk_{0}'.format(volume_id), wait=30):
            new_vdisk = VDiskList.get_vdisk_by_volume_id(volume_id)
            if new_vdisk is None:
                new_vdisk = VDisk()
                new_vdisk.volume_id = volume_id
                new_vdisk.size = vdisk.size
                new_vdisk.description = name
                new_vdisk.devicename = devicename
                new_vdisk.vpool = vdisk.vpool
            new_vdisk.pagecache_ratio = pagecache_ratio if pagecache_ratio is not None else vdisk.pagecache_ratio
            new_vdisk.name = name
            new_vdisk.parent_vdisk = vdisk
            new_vdisk.parentsnapshot = snapshot_id
            new_vdisk.save()
            VDiskController.vdisk_checkup(new_vdisk)

        return {'vdisk_guid': new_vdisk.guid,
                'name': new_vdisk.name,
                'backingdevice': devicename}

    @staticmethod
    @celery.task(name='ovs.vdisk.create_snapshot')
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
    @celery.task(name='ovs.vdisk.create_snapshots')
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
                VDiskController._logger.info('Create {0} snapshot for vDisk {1}'.format('consistent' if consistent is True else 'inconsistent', vdisk.name))
                snapshot_id = str(uuid.uuid4())
                vdisk.invalidate_dynamics(['snapshots'])
                if len(vdisk.snapshots) > 0:
                    most_recent_snap = sorted(vdisk.snapshots, key=lambda k: k['timestamp'], reverse=True)[0]  # Most recent first
                    if VDiskController.is_volume_synced_up_to_snapshot(vdisk_guid=vdisk.guid, snapshot_id=most_recent_snap['guid']) is False:
                        results[guid] = [False, 'Previously created snapshot did not make it to the backend yet']
                        continue
                vdisk.storagedriver_client.create_snapshot(volume_id=str(vdisk.volume_id),
                                                           snapshot_id=str(snapshot_id),
                                                           metadata=metadata,
                                                           req_timeout_secs=10)
                vdisk.invalidate_dynamics(['snapshots'])
                results[guid] = [True, snapshot_id]
            except Exception as ex:
                results[guid] = [False, ex.message]
        return results

    @staticmethod
    @celery.task(name='ovs.vdisk.delete_snapshot')
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
    @celery.task(name='ovs.vdisk.delete_snapshots')
    def delete_snapshots(snapshot_mapping):
        """
        Delete vDisk snapshots
        :param snapshot_mapping: Mapping of VDisk guid and Snapshot ID
        :type snapshot_mapping: dict
        :return: Information about the deleted snapshots, whether they succeeded or not
        :rtype: dict
        """
        results = {}
        for vdisk_guid, snapshot_id in snapshot_mapping.iteritems():
            try:
                vdisk = VDisk(vdisk_guid)
                vdisk.invalidate_dynamics(['snapshots'])
                if snapshot_id not in [snap['guid'] for snap in vdisk.snapshots]:
                    results[vdisk_guid] = [False, 'Snapshot {0} does not belong to vDisk {1}'.format(snapshot_id, vdisk.name)]
                    continue

                clones_of_snapshot = VDiskList.get_by_parentsnapshot(snapshot_id)
                if len(clones_of_snapshot) > 0:
                    results[vdisk_guid] = [False, 'Snapshot {0} has {1} volume{2} cloned from it, cannot remove'.format(snapshot_id, len(clones_of_snapshot), '' if len(clones_of_snapshot) == 1 else 's')]
                    continue

                VDiskController._logger.info('Deleting snapshot {0} from vDisk {1}'.format(snapshot_id, vdisk.name))
                vdisk.storagedriver_client.delete_snapshot(str(vdisk.volume_id), str(snapshot_id), req_timeout_secs=10)
                vdisk.invalidate_dynamics(['snapshots'])
                results[vdisk_guid] = [True, snapshot_id]
            except Exception as ex:
                results[vdisk_guid] = [False, ex.message]
        return results

    @staticmethod
    @celery.task(name='ovs.vdisk.set_as_template')
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
        vdisk.invalidate_dynamics(['is_vtemplate', 'info'])

    @staticmethod
    @celery.task(name='ovs.vdisk.move')
    def move(vdisk_guid, target_storagerouter_guid, force=False):
        """
        Move a vDisk to the specified StorageRouter
        :param vdisk_guid: Guid of the vDisk to move
        :type vdisk_guid: str
        :param target_storagerouter_guid: Guid of the StorageRouter to move the vDisk to
        :type target_storagerouter_guid: str
        :param force: Indicates whether to force the migration or not (forcing can lead to dataloss)
        :type force: bool
        :return: None
        """
        vdisk = VDisk(vdisk_guid)
        storagedriver = None
        storagerouter = StorageRouter(target_storagerouter_guid)

        for sd in storagerouter.storagedrivers:
            if sd.vpool == vdisk.vpool:
                storagedriver = sd
                break

        if storagedriver is None:
            raise RuntimeError('Failed to find the matching StorageDriver')

        try:
            vdisk.storagedriver_client.migrate(object_id=str(vdisk.volume_id),
                                               node_id=str(storagedriver.storagedriver_id),
                                               force_restart=force)
        except Exception:
            VDiskController._logger.exception('Failed to move vDisk {0}'.format(vdisk.name))
            raise Exception('Moving vDisk {0} failed'.format(vdisk.name))

        try:
            MDSServiceController.ensure_safety(vdisk=vdisk)
            VDiskController.dtl_checkup.delay(vdisk_guid=vdisk.guid)
        except:
            VDiskController._logger.exception('Executing post-migrate actions failed for vDisk {0}'.format(vdisk.name))

    @staticmethod
    @celery.task(name='ovs.vdisk.rollback')
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
    @celery.task(name='ovs.vdisk.create_from_template')
    def create_from_template(vdisk_guid, name, storagerouter_guid=None, pagecache_ratio=None):
        """
        Create a vDisk from a template
        :param vdisk_guid: Guid of the vDisk
        :type vdisk_guid: str
        :param name: Name of the newly created vDisk (can be a filename or a user friendly name)
        :type name: str
        :param storagerouter_guid: Guid of the Storage Router on which the vDisk should be started
        :type storagerouter_guid: str
        :param pagecache_ratio: Ratio of the pagecache size (compared to a 100% cache)
        :type pagecache_ratio: float
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
            if not 0 < pagecache_ratio <= 1:
                raise RuntimeError('Parameter pagecache_ratio must be 0 < x <= 1')

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
                                                                              node_id=str(storagedriver.storagedriver_id),
                                                                              req_timeout_secs=30)
        except Exception as ex:
            VDiskController._logger.error('Cloning vTemplate {0} failed: {1}'.format(vdisk.name, str(ex)))
            raise

        with volatile_mutex('voldrv_event_disk_{0}'.format(volume_id), wait=30):
            new_vdisk = VDiskList.get_vdisk_by_volume_id(volume_id)
            if new_vdisk is None:
                new_vdisk = VDisk()
                new_vdisk.volume_id = volume_id
                new_vdisk.size = vdisk.size
                new_vdisk.description = name
                new_vdisk.devicename = devicename
                new_vdisk.vpool = vdisk.vpool
            new_vdisk.pagecache_ratio = pagecache_ratio if pagecache_ratio is not None else vdisk.pagecache_ratio
            new_vdisk.name = name
            new_vdisk.parent_vdisk = vdisk
            new_vdisk.save()
            VDiskController.vdisk_checkup(new_vdisk)

        return {'vdisk_guid': new_vdisk.guid,
                'name': new_vdisk.name,
                'backingdevice': devicename}

    @staticmethod
    @celery.task(name='ovs.vdisk.create_new')
    def create_new(volume_name, volume_size, storagedriver_guid, pagecache_ratio=1.0):
        """
        Create a new vDisk/volume using hypervisor calls
        :param volume_name: Name of the vDisk (can be a filename or a user friendly name)
        :type volume_name: str
        :param volume_size: Size of the vDisk
        :type volume_size: int
        :param storagedriver_guid: Guid of the Storagedriver
        :type storagedriver_guid: str
        :param pagecache_ratio: Ratio of the pagecache size (compared to a 100% cache)
        :type pagecache_ratio: float
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

        if not 0 < pagecache_ratio <= 1:
            raise RuntimeError('Parameter pagecache_ratio must be 0 < x <= 1')

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

        with volatile_mutex('voldrv_event_disk_{0}'.format(volume_id), wait=30):
            new_vdisk = VDiskList.get_vdisk_by_volume_id(volume_id)
            if new_vdisk is None:
                new_vdisk = VDisk()
                new_vdisk.size = volume_size
                new_vdisk.vpool = vpool
                new_vdisk.devicename = devicename
                new_vdisk.description = volume_name
                new_vdisk.volume_id = volume_id
            new_vdisk.pagecache_ratio = pagecache_ratio
            new_vdisk.name = volume_name
            new_vdisk.save()
            VDiskController.vdisk_checkup(new_vdisk)

        VDiskController._logger.info('Created volume. Location {0}'.format(devicename))
        return new_vdisk.guid

    @staticmethod
    @celery.task(name='ovs.vdisk.get_config_params')
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

        storagedriver_config = StorageDriverConfiguration('storagedriver', vpool.guid, vdisk.storagedriver_id)
        storagedriver_config.load()
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

        return {'sco_size': sco_size,
                'dtl_mode': dtl_mode,
                'write_buffer': int(tlog_multiplier * sco_size * non_disposable_sco_factor),
                'dtl_target': dtl_target,
                'pagecache_ratio': vdisk.pagecache_ratio}

    @staticmethod
    @celery.task(name='ovs.vdisk.set_config_params')
    def set_config_params(vdisk_guid, new_config_params):
        """
        Sets configuration parameters for a given vDisk.
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

        required_params = {'dtl_mode': (str, StorageDriverClient.VDISK_DTL_MODE_MAP.keys()),
                           'sco_size': (int, StorageDriverClient.TLOG_MULTIPLIER_MAP.keys()),
                           'dtl_target': (list, Toolbox.regex_guid),
                           'write_buffer': (int, {'min': 128, 'max': 10 * 1024})}

        if new_config_params.get('pagecache_ratio') is not None:
            # noinspection PyTypeChecker
            required_params.update({'pagecache_ratio': (float, {'min': 0, 'max': 1})})

        Toolbox.verify_required_params(required_params, new_config_params)
        if 'pagecache_ratio' in new_config_params and new_config_params['pagecache_ratio'] == 0:
            raise RuntimeError('Parameter pagecache_ratio must be 0 < x <= 1')

        errors = False
        vdisk = VDisk(vdisk_guid)
        volume_id = str(vdisk.volume_id)
        old_config_params = VDiskController.get_config_params(vdisk.guid)

        # Update SCO size, because this impacts TLOG multiplier which on its turn impacts write buffer
        new_sco_size = new_config_params['sco_size']
        old_sco_size = old_config_params['sco_size']
        if new_sco_size != old_sco_size:
            write_buffer = float(new_config_params['write_buffer'])
            tlog_multiplier = StorageDriverClient.TLOG_MULTIPLIER_MAP[new_sco_size]
            sco_factor = write_buffer / tlog_multiplier / new_sco_size
            try:
                VDiskController._logger.info('Updating property sco_size on vDisk {0} to {1}'.format(vdisk.name, new_sco_size))
                vdisk.storagedriver_client.set_sco_multiplier(volume_id, new_sco_size / 4 * 1024, req_timeout_secs=10)
                vdisk.storagedriver_client.set_tlog_multiplier(volume_id, tlog_multiplier, req_timeout_secs=10)
                vdisk.storagedriver_client.set_sco_cache_max_non_disposable_factor(volume_id, sco_factor, req_timeout_secs=10)
                VDiskController._logger.info('Updated property sco_size')
            except Exception as ex:
                VDiskController._logger.error('Error updating "sco_size": {0}'.format(ex))
                errors = True

        # Check for DTL changes
        new_dtl_mode = new_config_params['dtl_mode']
        old_dtl_mode = old_config_params['dtl_mode']
        new_dtl_targets = set(new_config_params['dtl_target'])  # Domain guids
        old_dtl_targets = set(old_config_params['dtl_target'])

        if new_dtl_mode == 'no_sync':
            vdisk.has_manual_dtl = True
            vdisk.save()
            if old_dtl_mode != new_dtl_mode:
                VDiskController._logger.info('Disabling DTL for vDisk {0}'.format(vdisk.name))
                try:
                    vdisk.storagedriver_client.set_manual_dtl_config(volume_id, None, req_timeout_secs=10)
                except Exception:
                    VDiskController._logger.exception('Failed to disable DTL for vDisk {0}'.format(vdisk.name))
                    raise Exception('Disabling DTL for vDisk {0} failed'.format(vdisk.name))

                for junction in vdisk.domains_dtl:
                    junction.delete()
                vdisk.invalidate_dynamics(['dtl_status'])
        elif new_dtl_mode != old_dtl_mode or new_dtl_targets != old_dtl_targets:  # Mode is sync or async and targets changed or DTL mode changed
            VDiskController._logger.info('Checking if reconfiguration is required based on new parameters for vDisk {0}'.format(vdisk.name))
            storagerouter = StorageRouter(vdisk.storagerouter_guid)
            # No new DTL targets --> Check if we can find possible Storage Routers to configure DTL on
            if len(new_dtl_targets) == 0:
                vdisk.has_manual_dtl = False
                vdisk.save()

                if len([sr for sr in StorageRouterList.get_storagerouters() if sr.domains_guids]) == 0:  # No domains, so all Storage Routers are fine
                    possible_storagerouters = list(StorageRouterList.get_storagerouters())
                else:  # Find out which Storage Routers have a regular domain configured
                    possible_storagerouter_guids = set()
                    for domain in DomainList.get_domains():
                        if storagerouter.guid in domain.storage_router_layout['regular']:
                            if len(domain.storage_router_layout['regular']) > 1:
                                possible_storagerouter_guids.update(domain.storage_router_layout['regular'])
                        elif len(domain.storage_router_layout['regular']) > 0:
                            possible_storagerouter_guids.update(domain.storage_router_layout['regular'])
                    possible_storagerouters = [StorageRouter(guid) for guid in possible_storagerouter_guids]
            else:  # New DTL targets defined --> Retrieve all Storage Routers linked to these domains
                vdisk.has_manual_dtl = True
                vdisk.save()

                possible_storagerouters = set()
                for domain_guid in new_dtl_targets:
                    domain = Domain(domain_guid)
                    possible_storagerouters.update(StorageRouterList.get_primary_storagerouters_for_domain(domain))
                possible_storagerouters = list(possible_storagerouters)

            if storagerouter in possible_storagerouters:
                possible_storagerouters.remove(storagerouter)

            if len(possible_storagerouters) == 0:
                raise ValueError('Cannot reconfigure DTL to StorageRouter {0} because the vDisk is hosted on this StorageRouter'.format(storagerouter.name))

            try:
                current_dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id, req_timeout_secs=10)
            except Exception:
                VDiskController._logger.exception('Failed to retrieve current DTL configuration for vDisk {0}'.format(vdisk.name))
                raise Exception('Retrieving current DTL configuration failed for vDisk {0}'.format(vdisk.name))

            dtl_failed = False
            if old_dtl_mode != new_dtl_mode or current_dtl_config.host not in [sd.storage_ip for sr in possible_storagerouters for sd in sr.storagedrivers if sd.vpool_guid == vdisk.vpool_guid]:
                if os.environ.get('RUNNING_UNITTESTS') == 'True':
                    possible_storagerouters.sort(key=lambda i: i.guid)
                else:
                    random.shuffle(possible_storagerouters)
                dtl_config = None
                for storagerouter in possible_storagerouters:
                    for sd in sorted(storagerouter.storagedrivers, key=lambda i: i.guid):  # DTL can reside on any node in the cluster running a volumedriver and having a DTL process running
                        if sd.vpool_guid != vdisk.vpool_guid:
                            continue
                        dtl_config = DTLConfig(str(sd.storage_ip), sd.ports['dtl'], StorageDriverClient.VDISK_DTL_MODE_MAP[new_dtl_mode])
                        vdisk.storagedriver_client.set_manual_dtl_config(volume_id, dtl_config, req_timeout_secs=10)
                        vdisk.invalidate_dynamics(['dtl_status'])
                        break
                    if dtl_config is not None:
                        break

                if dtl_config is None:
                    VDiskController._logger.error('No suitable StorageRouters found in chosen Domains which have a DTL process for this vPool')
                    errors = True
                    dtl_failed = True

            if dtl_failed is False:
                # Reset relations
                for junction in vdisk.domains_dtl:
                    junction.delete()
                for domain_guid in new_dtl_targets:
                    vdisk_domain = VDiskDomain()
                    vdisk_domain.vdisk = vdisk
                    vdisk_domain.domain = Domain(domain_guid)
                    vdisk_domain.save()

        # Update all the rest
        for key in new_config_params:
            try:
                if key in ['sco_size', 'dtl_mode', 'dtl_target']:
                    continue

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
                    else:
                        raise KeyError('Unsupported property provided: "{0}"'.format(key))
                    VDiskController._logger.info('Updated property {0}'.format(key))
            except Exception as ex:
                VDiskController._logger.error('Error updating "{0}": {1}'.format(key, ex))
                errors = True

        if errors is True:
            raise Exception('Failed to update the values for vDisk {0}'.format(vdisk.name))

    @staticmethod
    @celery.task(name='ovs.vdisk.dtl_checkup', schedule=Schedule(minute='15', hour='0,4,8,12,16,20'))
    @ensure_single(task_name='ovs.vdisk.dtl_checkup', mode='DEDUPED')
    def dtl_checkup(vpool_guid=None, vdisk_guid=None, storagerouters_to_exclude=None):
        """
        Check DTL for all volumes, for all volumes of a vPool or for 1 specific volume
        :param vpool_guid: vPool to check the DTL configuration of all its vDisks
        :type vpool_guid: str
        :param vdisk_guid: vDisk to check its DTL configuration
        :type vdisk_guid: str
        :param storagerouters_to_exclude: Storage Router Guids to exclude from possible targets
        :type storagerouters_to_exclude: list
        :return: None
        """
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
                    VDiskController._logger.info('    Verifying vDisk {0} with guid {1}'.format(vdisk.name, vdisk.guid))
                    vdisk.invalidate_dynamics(['storagedriver_client', 'storagerouter_guid'])
                    if vdisk.storagedriver_client is None:
                        vdisks.remove(vdisk)
                        VDiskController._logger.warning('    VDisk {0} with guid {1} does not have a storagedriver client'.format(vdisk.name, vdisk.guid))
                        continue

                    vpool = vdisk.vpool
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

                    # Verify whether a currently configured DTL target is no longer part of any regular domain --> overrules manual config
                    this_storage_router = StorageRouter(vdisk.storagerouter_guid)
                    if vdisk.has_manual_dtl is True:
                        VDiskController._logger.info('    VDisk {0} with guid {1} has a manual DTL configuration'.format(vdisk.name, vdisk.guid))
                        if current_dtl_config is None:
                            VDiskController._logger.info('    VDisk {0} with guid {1} has a manually disabled DTL'.format(vdisk.name, vdisk.guid))
                            vdisks.remove(vdisk)
                            continue

                        dtl_target = [sd for sd in vpool.storagedrivers if sd.storage_ip == current_dtl_config.host][0].storagerouter
                        sr_domains = set([junction.domain for junction in dtl_target.domains])
                        vd_domains = set([junction.domain for junction in vdisk.domains_dtl])
                        if vd_domains.intersection(sr_domains):
                            VDiskController._logger.info('    VDisk {0} with guid {1} manual DTL configuration is valid'.format(vdisk.name, vdisk.guid))
                            vdisks.remove(vdisk)
                            continue

                        VDiskController._logger.info('    vDisk {0} with guid {1} DTL target will be updated'.format(vdisk.name, vdisk.guid))
                        # Current DTL target for vDisk is not correct anymore --> Check if other vDiskDomains can be used
                        new_location_found = False
                        for junction in vdisk.domains_dtl:
                            primary_srs = StorageRouterList.get_primary_storagerouters_for_domain(junction.domain)
                            if this_storage_router in primary_srs:
                                primary_srs.remove(this_storage_router)
                            if len(primary_srs) > 0:
                                new_location_found = True

                        if new_location_found is False:
                            VDiskController._logger.info('    VDisk {0} with guid {1} has a manually DTL, but overruling because some Domains no longer have any Storage Routers linked to it'.format(vdisk.name, vdisk.guid))
                            vdisk.has_manual_dtl = False
                            vdisk.save()

                    lock_key = 'dtl_checkup_{0}'.format(vdisk.guid)
                    if dtl_vpool_enabled is False and current_dtl_config is None:
                        VDiskController._logger.info('    DTL is globally disabled for vPool {0} with guid {1}'.format(vpool.name, vpool.guid))
                        try:
                            with volatile_mutex(lock_key, wait=time_to_wait_for_lock):
                                vdisk.storagedriver_client.set_manual_dtl_config(volume_id, None, req_timeout_secs=10)
                                vdisk.invalidate_dynamics(['dtl_status'])
                        except NoLockAvailableException:
                            VDiskController._logger.info('    Could not acquire lock, continuing with next vDisk')
                            continue
                        vdisks.remove(vdisk)
                        continue
                    elif current_dtl_config_mode == DTLConfigMode.MANUAL and current_dtl_config is None and vdisk.has_manual_dtl is True:
                        VDiskController._logger.info('    DTL is disabled for vDisk {0} with guid {1}'.format(vdisk.name, vdisk.guid))
                        vdisks.remove(vdisk)
                        continue

                    # Create a pool of StorageRouters being a part of the primary and secondary domains of this Storage Router
                    if len(vdisk.domains_dtl) > 0:
                        primary_domains = [junction.domain for junction in vdisk.domains_dtl]
                        secondary_domains = []
                    else:
                        primary_domains = [junction.domain for junction in this_storage_router.domains if junction.backup is False]
                        secondary_domains = [junction.domain for junction in this_storage_router.domains if junction.backup is True]

                    possible_primary_srs = []
                    available_primary_srs = set()
                    possible_secondary_srs = []
                    available_secondary_srs = set()
                    for domain in primary_domains:
                        available_primary_srs.update(StorageRouterList.get_primary_storagerouters_for_domain(domain))
                    for domain in secondary_domains:
                        available_secondary_srs.update(StorageRouterList.get_primary_storagerouters_for_domain(domain))

                    # In case no domains have been configured
                    if len(available_primary_srs) == 0 and len(available_secondary_srs) == 0 and len(primary_domains) == 0 and len(secondary_domains) == 0:
                        available_primary_srs = set(StorageRouterList.get_storagerouters())

                    if this_storage_router in available_primary_srs:
                        available_primary_srs.remove(this_storage_router)
                    if this_storage_router in available_secondary_srs:
                        available_secondary_srs.remove(this_storage_router)

                    # Remove all storagerouters from secondary which are present in primary
                    current_sr = None
                    available_primary_srs = available_primary_srs.difference(available_secondary_srs)
                    if current_dtl_config is not None:
                        sds = [sd for sd in vpool.storagedrivers if sd.storage_ip == current_dtl_config.host]
                        if len(sds) > 0:
                            current_sr = sds[0].storagerouter

                    for importance, possible_srs in {'secondary': possible_secondary_srs,
                                                     'primary': possible_primary_srs}.iteritems():
                        available_srs = list(available_primary_srs) if importance == 'primary' else list(available_secondary_srs)
                        random.shuffle(available_srs)

                        # Make sure currently configured DTL host is at index 0 of available SRs, so its included in possibilities
                        if current_sr in available_srs:
                            available_srs.remove(current_sr)
                            available_srs.insert(0, current_sr)

                        for storagerouter in available_srs:
                            if len(possible_srs) == 3:
                                break
                            if vpool.guid not in storagerouter.vpools_guids or storagerouter == this_storage_router:
                                continue
                            if storagerouter.guid in storagerouters_to_exclude:
                                continue
                            if storagerouter not in root_client_map:
                                root_client_map[storagerouter] = None
                                try:
                                    root_client = SSHClient(storagerouter, username='root')
                                    service_name = 'dtl_{0}'.format(vpool.name)
                                    if ServiceManager.has_service(service_name, client=root_client) is True and ServiceManager.get_service_status(service_name, client=root_client)[0] is True:
                                        root_client_map[storagerouter] = root_client
                                        possible_srs.append(storagerouter)
                                    else:
                                        VDiskController._logger.warning('    DTL service on Storage Router with IP {0} is not reachable'.format(storagerouter.ip))
                                except UnableToConnectException:
                                    VDiskController._logger.warning('    Storage Router with IP {0} of vDisk {1} is not reachable'.format(storagerouter.ip, vdisk.name))
                            elif root_client_map[storagerouter] is not None:
                                possible_srs.append(storagerouter)

                    if (len(possible_primary_srs) == 0 and len(possible_secondary_srs) == 0) or (len(possible_primary_srs) == 0 and len(vdisk.domains_dtl) > 0):
                        VDiskController._logger.info('    No Storage Routers could be found as valid DTL target, setting DTL for vDisk to STANDALONE')
                        try:
                            with volatile_mutex(lock_key, wait=time_to_wait_for_lock):
                                vdisk.storagedriver_client.set_manual_dtl_config(volume_id, None, req_timeout_secs=10)
                                vdisk.invalidate_dynamics(['dtl_status'])
                        except NoLockAvailableException:
                            VDiskController._logger.info('    Could not acquire lock, continuing with next vDisk')
                            continue
                        vdisks.remove(vdisk)
                        continue

                    # Check whether reconfiguration is required
                    reconfigure_required = False
                    if current_dtl_config is None:
                        VDiskController._logger.info('        No DTL configuration found, but there are Storage Routers available')
                        reconfigure_required = True
                    elif current_dtl_config_mode == DTLConfigMode.AUTOMATIC:
                        VDiskController._logger.info('        DTL configuration set to AUTOMATIC, switching to manual')
                        reconfigure_required = True
                    elif dtl_vpool_config_mode == DTLConfigMode.MANUAL and dtl_vpool_enabled is True:
                        VDiskController._logger.info('        DTL configuration set to MANUAL, but static host provided ... overruling')
                        reconfigure_required = True
                    elif current_sr is None:
                        VDiskController._logger.info('        DTL configuration set to MANUAL, but no StorageRouter found ... correcting')
                        reconfigure_required = True
                    else:
                        dtl_host = current_dtl_config.host
                        dtl_mode = current_dtl_config.mode
                        dtl_port = current_dtl_config.port
                        storage_drivers = [sd for sd in vpool.storagedrivers if sd.storage_ip == dtl_host]

                        VDiskController._logger.info('        DTL host: {0}'.format(dtl_host))
                        VDiskController._logger.info('        DTL port: {0}'.format(dtl_port))
                        VDiskController._logger.info('        DTL mode: {0}'.format(dtl_mode))
                        if len(vdisk.domains_dtl) > 0:
                            if dtl_host not in [sd.storage_ip for sr in possible_primary_srs for sd in sr.storagedrivers if sd.vpool_guid == vpool.guid]:
                                VDiskController._logger.info('        Host not in available Storage Routers, manual DTL will be overruled')
                                reconfigure_required = True
                        elif len(possible_secondary_srs) > 0:
                            if dtl_host not in [sd.storage_ip for sr in possible_secondary_srs for sd in sr.storagedrivers if sd.vpool_guid == vpool.guid]:
                                VDiskController._logger.info('        Host not in available secondary Storage Routers')
                                reconfigure_required = True
                        elif len(possible_primary_srs) > 0:
                            if dtl_host not in [sd.storage_ip for sr in possible_primary_srs for sd in sr.storagedrivers if sd.vpool_guid == vpool.guid]:
                                VDiskController._logger.info('        Host not in available primary Storage Routers')
                                reconfigure_required = True
                        if dtl_port != storage_drivers[0].ports['dtl']:
                            VDiskController._logger.info('        Configured port does not match expected port ({0} vs {1})'.format(dtl_port, storage_drivers[0].ports['dtl']))
                            reconfigure_required = True

                    # Perform the reconfiguration
                    if reconfigure_required is True:
                        possible_srs = possible_primary_srs if len(vdisk.domains_dtl) > 0 else possible_secondary_srs if len(possible_secondary_srs) > 0 else possible_primary_srs
                        VDiskController._logger.info('        Reconfigure required, randomly choosing')
                        dtl_target = random.choice(possible_srs)
                        storage_drivers = [sd for sd in vpool.storagedrivers if sd.storagerouter == dtl_target]
                        if len(storage_drivers) == 0:
                            VDiskController._logger.error('Could not retrieve related storagedriver')
                            errors_found = True
                            vdisks.remove(vdisk)
                            continue

                        port = storage_drivers[0].ports['dtl']
                        ip = storage_drivers[0].storage_ip
                        if vdisk.has_manual_dtl is True:
                            dtl_mode = StorageDriverClient.REVERSE_DTL_MODE_MAP[current_dtl_config.mode]
                        else:
                            dtl_mode = vpool_config['dtl_mode']
                        VDiskController._logger.info('        DTL config that will be set -->  Host: {0}, Port: {1}, Mode: {2}'.format(ip, port, dtl_mode))
                        dtl_config = DTLConfig(str(ip), port, StorageDriverClient.VDISK_DTL_MODE_MAP[dtl_mode])
                        try:
                            with volatile_mutex(lock_key, wait=time_to_wait_for_lock):
                                vdisk.storagedriver_client.set_manual_dtl_config(volume_id, dtl_config, req_timeout_secs=10)
                                vdisk.invalidate_dynamics(['dtl_status'])
                        except NoLockAvailableException:
                            VDiskController._logger.info('    Could not acquire lock, continuing with next vDisk')
                            continue
                except Exception:
                    errors_found = True
                    VDiskController._logger.exception('Something went wrong configuring the DTL for vDisk {0} with guid {1}'.format(vdisk.name, vdisk.guid))
                vdisks.remove(vdisk)

        if errors_found is True:
            VDiskController._logger.error('DTL checkup ended with errors')
            raise Exception('DTL checkup failed with errors. Please check logging for more information')
        VDiskController._logger.info('DTL checkup ended')

    @staticmethod
    @celery.task(name='ovs.vdisk.dtl_state_transition')
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
                                            storagerouters_to_exclude=[storagedriver.storagerouter.guid],
                                            ensure_single_timeout=600)

    @staticmethod
    @celery.task(name='ovs.vdisk.schedule_backend_sync')
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
    @celery.task(name='ovs.vdisk.is_volume_synced_up_to_tlog')
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
    @celery.task(name='ovs.vdisk.is_volume_synced_up_to_snapshot')
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
    @celery.task(name='ovs.vdisk.scrub_single_vdisk')
    def scrub_single_vdisk(vdisk_guid, storagerouter_guid):
        """
        Scrubs a given vDisk on a given StorageRouter
        :param vdisk_guid: The guid of the vDisk to scrub
        :type vdisk_guid: str
        :param storagerouter_guid: The guid of the StorageRouter to scrub on
        :type storagerouter_guid: str
        :return: None
        """
        from ovs.lib.generic import GenericController

        vdisk = VDisk(vdisk_guid)
        storagerouter = StorageRouter(storagerouter_guid)
        scrub_partitions = storagerouter.partition_config.get(DiskPartition.ROLES.SCRUB, [])
        if len(scrub_partitions) == 0:
            raise RuntimeError('No scrub locations found on StorageRouter {0}'.format(storagerouter.name))
        partition = DiskPartition(scrub_partitions[0])
        queue = Queue()
        queue.put(vdisk_guid)
        scrub_info = {'scrub_path': str(partition.folder),
                      'storage_router': storagerouter}
        error_messages = []
        GenericController.execute_scrub_work(queue, vdisk.vpool, scrub_info, error_messages)
        if len(error_messages) > 0:
            raise RuntimeError('Error when scrubbing vDisk {0}:\n- {1}'.format(vdisk.guid, '\n- '.join(error_messages)))

    @staticmethod
    @celery.task(name='ovs.vdisk.restart')
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
    @celery.task(name='ovs.vdisk.sync_with_reality')
    def sync_with_reality(vpool_guid=None):
        """
        Syncs vDisks in the model with reality
        :param vpool_guid: Optional vPool guid. All vPools if omitted
        :type vpool_guid: str or None
        :return: None
        :rtype: NoneType
        """
        if vpool_guid is None:
            vpools = VPoolList.get_vpools()
        else:
            vpools = [VPool(vpool_guid)]
        for vpool in vpools:
            vdisks = dict((str(vdisk.volume_id), vdisk) for vdisk in vpool.vdisks)
            for entry in vpool.objectregistry_client.get_all_registrations():
                volume_id = entry.object_id()
                if volume_id not in vdisks:
                    with volatile_mutex('voldrv_event_disk_{0}'.format(volume_id), wait=30):
                        new_vdisk = VDiskList.get_vdisk_by_volume_id(volume_id)
                        if new_vdisk is None:
                            VDiskController._logger.info('Adding missing vDisk in the model for {0}'.format(volume_id))
                            new_vdisk = VDisk()
                            new_vdisk.volume_id = volume_id
                            new_vdisk.vpool = vpool
                            try:
                                fsmetadata_client = new_vdisk.fsmetadata_client
                                devicename = fsmetadata_client.lookup(volume_id)
                                name = VDiskController.extract_volumename(devicename)
                            except FeatureNotAvailableException:
                                VDiskController._logger.exception('Could not load devicename from StorageDriver')
                                devicename = '/{0}.raw'.format(volume_id)
                                name = volume_id
                            new_vdisk.name = name
                            new_vdisk.description = name
                            new_vdisk.devicename = devicename
                            new_vdisk.size = new_vdisk.info['volume_size']
                            new_vdisk.metadata = {'lba_size': new_vdisk.info['lba_size'],
                                                  'cluster_multiplier': new_vdisk.info['cluster_multiplier']}
                            new_vdisk.pagecache_ratio = 1.0
                            new_vdisk.save()
                            VDiskController.vdisk_checkup(new_vdisk)
                else:
                    del vdisks[volume_id]
            for volume_id, vdisk in vdisks.iteritems():
                with volatile_mutex('voldrv_event_disk_{0}'.format(volume_id), wait=30):
                    if vpool.objectregistry_client.find(str(volume_id)) is None:
                        VDiskController._logger.info('Removing obsolete vDisk {0} from model'.format(vdisk.guid))
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
        A volume has a cluster_size of 4k (default) and a metadata_page_capacity of 64. A single page addresses 4k * 64 = 256k of a volume
        So if a volume's size is 256M, the cache should have a capacity (cache_capacity) of 1024 to be completely in memory

        Example 2:
        A volume has a size of 256M, and a cluster_size of 4k, and a metadata_page_capacity of 64
        If we want 10% of that volume to be cached, we need 256 / (4k * 64 = 256k) = 1024 => a cache_capacity of 102

        :param vdisk: Object vDisk
        :type vdisk: VDisk
        :return: None
        """
        storagedriver_id = vdisk.storagedriver_id
        if storagedriver_id is None:
            raise SRCObjectNotFoundException()
        ratio = vdisk.pagecache_ratio
        storagedriver_config = StorageDriverConfiguration('storagedriver', vdisk.vpool_guid, storagedriver_id)
        storagedriver_config.load()
        cluster_size = storagedriver_config.configuration.get('volume_manager', {}).get('default_cluster_size', 4096)

        metadata_page_size = float(StorageDriverClient.METADATA_PAGE_CAPACITY * cluster_size)
        cache_capacity = int(vdisk.size / metadata_page_size * ratio)

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
        :return: A cleaned devicename
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
        Extracts a reasonable volume name out of a given devicename
        :param devicename: A raw devicename of a volume (e.g. /foo/bar.raw)
        :type devicename: str
        :return: A cleaned up volumename (e.g. bar)
        """
        return devicename.rsplit('/', 1)[-1].rsplit('.', 1)[0]
