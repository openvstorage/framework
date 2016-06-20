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
from celery.schedules import crontab
from ovs.celery_run import celery
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.hybrids.domain import Domain
from ovs.dal.hybrids.j_vdiskdomain import VDiskDomain
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.domainlist import DomainList
from ovs.dal.lists.mgmtcenterlist import MgmtCenterList
from ovs.dal.lists.pmachinelist import PMachineList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs.extensions.generic.volatilemutex import NoLockAvailableException, volatile_mutex
from ovs.extensions.hypervisor.factory import Factory
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.storageserver.storagedriver import StorageDriverClient, StorageDriverConfiguration
from ovs.lib.helpers.decorators import ensure_single, log
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.mdsservice import MDSServiceController
from ovs.log.log_handler import LogHandler
from volumedriver.storagerouter import storagerouterclient, VolumeDriverEvents_pb2
from volumedriver.storagerouter.storagerouterclient import DTLConfig, DTLConfigMode, MDSMetaDataBackendConfig, MDSNodeConfig


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

        :return: Volumes known by the vPool
        :rtype: list
        """
        if vpool_guid is not None:
            vpool = VPool(vpool_guid)
            storagedriver_client = StorageDriverClient.load(vpool)
            response = storagedriver_client.list_volumes()
        else:
            response = []
            for vpool in VPoolList.get_vpools():
                storagedriver_client = StorageDriverClient.load(vpool)
                response.extend(storagedriver_client.list_volumes())
        return response

    @staticmethod
    @celery.task(name='ovs.vdisk.delete_from_voldrv')
    @log('VOLUMEDRIVER_TASK')
    def delete_from_voldrv(volumename, storagedriver_id):
        """
        Delete a disk
        Triggered by volumedriver messages on the queue
        :param volumename: volume ID of the disk
        :type volumename: str

        :param storagedriver_id: ID of the storagedriver serving the volume to delete
        :type storagedriver_id: str

        :return: None
        """
        disk = VDiskList.get_vdisk_by_volume_id(volumename)
        if disk is not None:
            mutex = volatile_mutex('{0}_{1}'.format(volumename, disk.devicename))
            try:
                mutex.acquire(wait=20)
                pmachine = None
                try:
                    pmachine = PMachineList.get_by_storagedriver_id(disk.storagedriver_id)
                except RuntimeError as ex:
                    if 'could not be found' not in str(ex):
                        raise
                    # else: pmachine can't be loaded, because the volumedriver doesn't know about it anymore
                if pmachine is not None:
                    limit = 5
                    storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
                    hypervisor = Factory.get(pmachine)
                    exists = hypervisor.file_exists(storagedriver, disk.devicename)
                    while limit > 0 and exists is True:
                        time.sleep(1)
                        exists = hypervisor.file_exists(storagedriver, disk.devicename)
                        limit -= 1
                    if exists is True:
                        VDiskController._logger.info('Disk {0} still exists, ignoring delete'.format(disk.devicename))
                        return
                VDiskController._logger.info('Delete disk {0}'.format(disk.name))
                for mds_service in disk.mds_services:
                    mds_service.delete()
                for domain_junction in disk.domains_dtl:
                    domain_junction.delete()
                disk.delete()
            finally:
                mutex.release()

    @staticmethod
    @celery.task(name='ovs.vdisk.delete')
    def delete(diskguid):
        """
        Delete a vdisk through API
        :param diskguid: Guid of the vdisk to delete
        :type diskguid: str

        :return: None
        """
        vdisk = VDisk(diskguid)
        storagedriver = StorageDriverList.get_by_storagedriver_id(vdisk.storagedriver_id)
        hypervisor = Factory.get(storagedriver.storagerouter.pmachine)
        VDiskController._logger.info('Deleting disk {0}'.format(vdisk.name))
        hypervisor.delete_volume(storagedriver.mountpoint, storagedriver.storage_ip, vdisk.name)
        VDiskController._logger.info('Deleted disk {0}'.format(vdisk.name))

    @staticmethod
    @celery.task(name='ovs.vdisk.extend')
    def extend(diskguid, size):
        """
        Extend a vdisk through API
        :param diskguid: Guid of the vdisk to extend
        :type diskguid: str

        :param size: New size (GB)
        :type size: int

        :return: None
        """
        vdisk = VDisk(diskguid)
        storagedriver = StorageDriverList.get_by_storagedriver_id(vdisk.storagedriver_id)
        hypervisor = Factory.get(storagedriver.storagerouter.pmachine)
        VDiskController._logger.info('Extending disk {0}'.format(vdisk.name))
        hypervisor.extend_volume(storagedriver.mountpoint, storagedriver.storage_ip, vdisk.name, size)
        VDiskController._logger.info('Extended disk {0}'.format(vdisk.name))

    @staticmethod
    @celery.task(name='ovs.vdisk.resize_from_voldrv')
    @log('VOLUMEDRIVER_TASK')
    def resize_from_voldrv(volumename, volumesize, volumepath, storagedriver_id):
        """
        Resize a disk
        Triggered by volumedriver messages on the queue

        :param volumename: volume ID of the disk
        :type volumename: str

        :param volumesize: size of the volume
        :type volumesize: int

        :param volumepath: path on hypervisor to the volume
        :type volumepath: str

        :param storagedriver_id: ID of the storagedriver serving the volume to resize
        :type storagedriver_id: str

        :return: None
        """
        pmachine = PMachineList.get_by_storagedriver_id(storagedriver_id)
        storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
        hypervisor = Factory.get(pmachine)
        volumepath = hypervisor.clean_backing_disk_filename(volumepath)
        mutex = volatile_mutex('{0}_{1}'.format(volumename, volumepath))
        try:
            mutex.acquire(wait=30)
            disk = VDiskList.get_vdisk_by_volume_id(volumename)
            if disk is None:
                disk = VDiskList.get_by_devicename_and_vpool(volumepath, storagedriver.vpool)
                if disk is None:
                    disk = VDisk()
            disk.devicename = volumepath
            disk.volume_id = volumename
            disk.size = volumesize
            disk.vpool = storagedriver.vpool
            disk.metadata = {'lba_size': disk.info['lba_size'],
                             'cluster_multiplier': disk.info['cluster_multiplier']}
            disk.save()
            VDiskController._set_vdisk_metadata_pagecache_size(disk)
        finally:
            mutex.release()

        VDiskController.sync_with_mgmtcenter(disk, pmachine, storagedriver)
        MDSServiceController.ensure_safety(disk)
        VDiskController.dtl_checkup.delay(vdisk_guid=disk.guid)

    @staticmethod
    @celery.task(name='ovs.vdisk.rename_from_voldrv')
    @log('VOLUMEDRIVER_TASK')
    def rename_from_voldrv(volumename, volume_old_path, volume_new_path, storagedriver_id):
        """
        Rename a disk
        Triggered by volumedriver messages

        :param volumename: volume ID of the disk
        :type volumename: str

        :param volume_old_path: old path on hypervisor to the volume
        :type volume_old_path: str

        :param volume_new_path: new path on hypervisor to the volume
        :type volume_new_path: str

        :param storagedriver_id: ID of the storagedriver serving the volume to rename
        :type storagedriver_id: str

        :return: None
        """
        pmachine = PMachineList.get_by_storagedriver_id(storagedriver_id)
        hypervisor = Factory.get(pmachine)
        volume_old_path = hypervisor.clean_backing_disk_filename(volume_old_path)
        volume_new_path = hypervisor.clean_backing_disk_filename(volume_new_path)
        disk = VDiskList.get_vdisk_by_volume_id(volumename)
        if disk:
            VDiskController._logger.info('Move disk {0} from {1} to {2}'.format(disk.name,
                                                                                volume_old_path,
                                                                                volume_new_path))
            disk.devicename = volume_new_path
            disk.save()

    @staticmethod
    @celery.task(name='ovs.vdisk.migrate_from_voldrv')
    @log('VOLUMEDRIVER_TASK')
    def migrate_from_voldrv(volume_id, new_owner_id):
        """
        Triggered when volume has changed owner (Clean migration or stolen due to other reason)
        Triggered by volumedriver messages

        :param volume_id: Volume ID of the disk
        :type volume_id: unicode

        :param new_owner_id: ID of the storage driver the volume migrated to
        :type new_owner_id: unicode

        :return: None
        """
        sd = StorageDriverList.get_by_storagedriver_id(storagedriver_id=new_owner_id)
        vdisk = VDiskList.get_vdisk_by_volume_id(volume_id=volume_id)
        if vdisk is not None:
            VDiskController._logger.info('Migration - Guid {0} - ID {1} - Detected migration for virtual disk {2}'.format(vdisk.guid, vdisk.volume_id, vdisk.name))
            if sd is not None:
                VDiskController._logger.info('Migration - Guid {0} - ID {1} - Storage Router {2} is the new owner of virtual disk {3}'.format(vdisk.guid, vdisk.volume_id, sd.storagerouter.name, vdisk.name))
            MDSServiceController.mds_checkup()
            VDiskController.dtl_checkup(vdisk_guid=vdisk.guid)

    @staticmethod
    @celery.task(name='ovs.vdisk.clone')
    def clone(diskguid, snapshotid, devicename, pmachineguid, machinename=None, machineguid=None, detached=False):
        """
        Clone a disk
        :param diskguid: Guid of the disk to clone
        :type diskguid: str

        :param snapshotid: ID of the snapshot to clone from
        :type snapshotid: str

        :param devicename: Name of the device to use in clone's description
        :type devicename: str

        :param pmachineguid: Guid of the physical machine
        :type pmachineguid: str

        :param machinename: Name of the machine the disk is attached to
        :type machinename: str

        :param machineguid: Guid of the machine
        :type machineguid: str

        :param detached: Boolean indicating the disk is attached to a machine or not
        :type detached: bool

        ;return: Information about the cloned volume
        :rtype: dict
        """
        # 1. Validations
        name_regex = "^[0-9a-zA-Z][-_a-zA-Z0-9]{1,48}[a-zA-Z0-9]$"
        if not re.match(name_regex, devicename):
            raise RuntimeError("Invalid name for virtual disk clone")

        if VDiskList.get_vdisk_by_name(vdiskname=devicename) is not None:
            raise RuntimeError("A virtual disk with this name already exists")

        vdisk = VDisk(diskguid)
        storagedriver = StorageDriverList.get_by_storagedriver_id(vdisk.storagedriver_id)
        if storagedriver is None:
            raise RuntimeError('Could not find StorageDriver with ID {0}'.format(vdisk.storagedriver_id))

        if machineguid is not None and detached is True:
            raise ValueError('A vMachine GUID was specified while detached is True')

        # 2. Create new snapshot if required
        if snapshotid is None:
            timestamp = str(int(time.time()))
            metadata = {'label': '',
                        'is_consistent': False,
                        'timestamp': timestamp,
                        'machineguid': machineguid,
                        'is_automatic': True}
            sd_snapshot_id = VDiskController.create_snapshot(diskguid, metadata)
            tries = 25  # 5 minutes
            while snapshotid is None and tries > 0:
                time.sleep(25 - tries)
                tries -= 1
                vdisk.invalidate_dynamics(['snapshots'])
                for snapshot in vdisk.snapshots:
                    if snapshot['guid'] != sd_snapshot_id:
                        continue
                    if snapshot['in_backend'] is True:
                        snapshotid = snapshot['guid']
            if snapshotid is None:
                try:
                    VDiskController.delete_snapshot(diskguid=diskguid,
                                                    snapshotid=sd_snapshot_id)
                except:
                    pass
                raise RuntimeError('Could not find created snapshot in time')

        # 3. Model new cloned virtual disk
        hypervisor = Factory.get(PMachine(pmachineguid))
        location = hypervisor.get_disk_path(machinename, devicename)

        new_vdisk = VDisk()
        new_vdisk.copy(vdisk, include=['description', 'size', 'type', 'retentionpolicyguid', 'snapshotpolicyguid', 'autobackup'])
        new_vdisk.parent_vdisk = vdisk
        new_vdisk.name = devicename
        new_vdisk.description = devicename if machinename is None else '{0} {1}'.format(machinename, devicename)
        new_vdisk.devicename = hypervisor.clean_backing_disk_filename(location)
        new_vdisk.parentsnapshot = snapshotid
        if detached is False:
            new_vdisk.vmachine = VMachine(machineguid) if machineguid else vdisk.vmachine
        new_vdisk.vpool = vdisk.vpool
        new_vdisk.save()

        # 4. Configure Storage Driver
        try:
            mds_service = MDSServiceController.get_preferred_mds(storagedriver.storagerouter, vdisk.vpool)[0]
            if mds_service is None:
                raise RuntimeError('Could not find a MDS service')

            VDiskController._logger.info('Clone snapshot {0} of disk {1} to location {2}'.format(snapshotid, vdisk.name, location))
            # noinspection PyArgumentList
            backend_config = MDSMetaDataBackendConfig([MDSNodeConfig(address=str(mds_service.service.storagerouter.ip),
                                                                     port=mds_service.service.ports[0])])
            volume_id = vdisk.storagedriver_client.create_clone(target_path=location,
                                                                metadata_backend_config=backend_config,
                                                                parent_volume_id=str(vdisk.volume_id),
                                                                parent_snapshot_id=str(snapshotid),
                                                                node_id=str(vdisk.storagedriver_id))
        except Exception as ex:
            VDiskController._logger.error('Caught exception during clone, trying to delete the volume. {0}'.format(ex))
            try:
                VDiskController.clean_bad_disk(new_vdisk.guid)
            except Exception as ex2:
                VDiskController._logger.exception('Exception during exception handling of "create_clone_from_template" : {0}'.format(str(ex2)))
            raise

        new_vdisk.volume_id = volume_id
        new_vdisk.save()
        VDiskController._set_vdisk_metadata_pagecache_size(new_vdisk)

        # 5. Check MDS & DTL for new clone
        try:
            MDSServiceController.ensure_safety(new_vdisk)
        except Exception as ex:
            VDiskController._logger.error('Caught exception during "ensure_safety" {0}'.format(ex))
        VDiskController.dtl_checkup.delay(vdisk_guid=new_vdisk.guid)

        return {'diskguid': new_vdisk.guid,
                'name': new_vdisk.name,
                'backingdevice': location}

    @staticmethod
    @celery.task(name='ovs.vdisk.create_snapshot')
    def create_snapshot(diskguid, metadata, snapshotid=None):
        """
        Create a disk snapshot

        :param diskguid: Guid of the disk
        :type diskguid: str

        :param metadata: Dictionary of metadata
        :type metadata: dict

        :param snapshotid: ID of the snapshot
        :type snapshotid: str

        :return: ID of the newly created snapshot
        :rtype: str
        """
        if not isinstance(metadata, dict):
            raise ValueError('Expected metadata as dict, got {0} instead'.format(type(metadata)))
        disk = VDisk(diskguid)
        VDiskController._logger.info('Create snapshot for disk {0}'.format(disk.name))
        if snapshotid is None:
            snapshotid = str(uuid.uuid4())
        metadata = pickle.dumps(metadata)
        disk.storagedriver_client.create_snapshot(str(disk.volume_id),
                                                  snapshot_id=str(snapshotid),
                                                  metadata=metadata)
        disk.invalidate_dynamics(['snapshots'])
        return snapshotid

    @staticmethod
    @celery.task(name='ovs.vdisk.delete_snapshot')
    def delete_snapshot(diskguid, snapshotid):
        """
        Delete a disk snapshot

        :param diskguid: Guid of the disk
        :type diskguid: str

        :param snapshotid: ID of the snapshot
        :type snapshotid: str

        :return: None

        @todo: Check if new volumedriver storagedriver upon deletion
        of a snapshot has built-in protection to block it from being deleted
        if a clone was created from it.
        """
        disk = VDisk(diskguid)
        if snapshotid not in [snap['guid'] for snap in disk.snapshots]:
            raise RuntimeError('Snapshot {0} does not belong to disk {1}'.format(snapshotid, disk.name))
        clones_of_snapshot = VDiskList.get_by_parentsnapshot(snapshotid)
        if len(clones_of_snapshot) > 0:
            raise RuntimeError('Snapshot {0} has {1} volumes cloned from it, cannot remove'.format(snapshotid, len(clones_of_snapshot)))
        VDiskController._logger.info('Deleting snapshot {0} from disk {1}'.format(snapshotid, disk.name))
        disk.storagedriver_client.delete_snapshot(str(disk.volume_id), str(snapshotid))
        disk.invalidate_dynamics(['snapshots'])

    @staticmethod
    @celery.task(name='ovs.vdisk.set_as_template')
    def set_as_template(diskguid):
        """
        Set a vDisk as template

        :param diskguid: Guid of the vDisk
        :type diskguid: str

        :return: None
        """
        vdisk = VDisk(diskguid)
        if vdisk.is_vtemplate is True:
            VDiskController._logger.info('vDisk {0} has already been set as vTemplate'.format(vdisk.name))
            return
        VDiskController._logger.info('Setting vDisk {0} as template'.format(vdisk.name))
        vdisk.storagedriver_client.set_volume_as_template(str(vdisk.volume_id))
        vdisk.invalidate_dynamics(['is_vtemplate'])

    @staticmethod
    @celery.task(name='ovs.vdisk.rollback')
    def rollback(diskguid, timestamp):
        """
        Rolls back a disk based on a given disk snapshot timestamp
        :param diskguid: Guid of the disk to rollback
        :type diskguid: str

        :param timestamp: Timestamp of the snapshot to rollback from
        :type timestamp: str

        :return: True
        :rtype: bool
        """
        disk = VDisk(diskguid)
        snapshots = [snap for snap in disk.snapshots if snap['timestamp'] == timestamp]
        if not snapshots:
            raise ValueError('No snapshot found for timestamp {0}'.format(timestamp))
        snapshotguid = snapshots[0]['guid']
        disk.storagedriver_client.rollback_volume(str(disk.volume_id), snapshotguid)
        disk.invalidate_dynamics(['snapshots'])
        return True

    @staticmethod
    @celery.task(name='ovs.vdisk.create_from_template')
    def create_from_template(diskguid, devicename, pmachineguid, machinename='', machineguid=None):
        """
        Create a disk from a template

        :param diskguid: Guid of the disk
        :type diskguid: str

        :param machinename: Name of the machine
        :type machinename: str

        :param devicename: Device file name for the disk (eg: my_disk-flat.vmdk)
        :type devicename: str

        :param pmachineguid: Guid of pmachine to create new vdisk on
        :type pmachineguid: str

        :param machineguid: Guid of the machine to assign disk to
        :type machineguid: str

        :return diskguid: Information about the new volume
        :rtype: dict
        """
        pmachine = PMachine(pmachineguid)
        hypervisor = Factory.get(pmachine)
        new_vdisk_vmachine = None
        if machineguid is not None:
            new_vdisk_vmachine = VMachine(machineguid)
            machinename = new_vdisk_vmachine.name
        disk_path = hypervisor.get_disk_path(machinename, devicename)

        description = '{0} {1}'.format(machinename, devicename)
        properties_to_clone = ['description', 'size', 'type', 'retentionpolicyid',
                               'snapshotpolicyid', 'vmachine', 'vpool']

        vdisk = VDisk(diskguid)
        if vdisk.vmachine and not vdisk.vmachine.is_vtemplate:
            # Disk might not be attached to a vmachine, but still be a template
            raise RuntimeError('The given vdisk does not belong to a template')
        if not vdisk.is_vtemplate:
            raise RuntimeError('The given vdisk is not a template')

        storagedriver = None
        for sd in vdisk.vpool.storagedrivers:
            if sd.storagerouter_guid in pmachine.storagerouters_guids:
                storagedriver = sd
                break

        if storagedriver is None:
            raise RuntimeError('Could not find Storage Driver')

        new_vdisk = VDisk()
        new_vdisk.copy(vdisk, include=properties_to_clone)
        new_vdisk.vpool = vdisk.vpool
        new_vdisk.devicename = hypervisor.clean_backing_disk_filename(disk_path)
        new_vdisk.parent_vdisk = vdisk
        new_vdisk.name = '{0}-clone'.format(vdisk.name)
        new_vdisk.description = description
        new_vdisk.vmachine = new_vdisk_vmachine if machineguid else vdisk.vmachine
        new_vdisk.save()

        mds_service = MDSServiceController.get_preferred_mds(storagedriver.storagerouter, new_vdisk.vpool)[0]
        if mds_service is None:
            raise RuntimeError('Could not find a MDS service')

        VDiskController._logger.info('Create disk from template {0} to new disk {1} to location {2}'.format(vdisk.name, new_vdisk.name, disk_path))

        try:
            # noinspection PyArgumentList
            backend_config = MDSNodeConfig(address=str(mds_service.service.storagerouter.ip),
                                           port=mds_service.service.ports[0])
            volume_id = vdisk.storagedriver_client.create_clone_from_template(target_path=disk_path,
                                                                              metadata_backend_config=MDSMetaDataBackendConfig([backend_config]),
                                                                              parent_volume_id=str(vdisk.volume_id),
                                                                              node_id=str(storagedriver.storagedriver_id))
            new_vdisk.volume_id = volume_id
            new_vdisk.save()
            MDSServiceController.ensure_safety(new_vdisk)
            VDiskController.dtl_checkup.delay(vdisk_guid=new_vdisk.guid)
            VDiskController._set_vdisk_metadata_pagecache_size(new_vdisk)
        except Exception as ex:
            VDiskController._logger.error('Clone disk on volumedriver level failed with exception: {0}'.format(str(ex)))
            try:
                VDiskController.clean_bad_disk(new_vdisk.guid)
            except Exception as ex2:
                VDiskController._logger.exception('Exception during exception handling of "create_clone_from_template" : {0}'.format(str(ex2)))
            raise ex

        return {'diskguid': new_vdisk.guid,
                'name': new_vdisk.name,
                'backingdevice': disk_path}

    @staticmethod
    @celery.task(name='ovs.vdisk.create_new')
    def create_new(diskname, size, storagedriver_guid):
        """
        Create a new vdisk/volume using hypervisor calls
        :param diskname: Name of the disk
        :type diskname: str

        :param size: Size of the disk (GB)
        :type size: int

        :param storagedriver_guid: Guid of the Storagedriver
        :type storagedriver_guid: str

        :return: Guid of the new disk
        :rtype: str
        """
        VDiskController._logger.info('Creating new empty disk {0} of {1} GB'.format(diskname, size))
        storagedriver = StorageDriver(storagedriver_guid)
        hypervisor = Factory.get(storagedriver.storagerouter.pmachine)
        disk_path = hypervisor.create_volume(storagedriver.mountpoint, storagedriver.storage_ip, diskname, size)
        VDiskController._logger.info('Created volume. Location {0}'.format(disk_path))

        backoff = 1
        timeout = 30  # seconds
        start = time.time()
        while time.time() < start + timeout:
            vdisk = VDiskList.get_by_devicename_and_vpool(disk_path, storagedriver.vpool)
            if vdisk is None:
                VDiskController._logger.debug('Waiting for disk to be picked up by voldrv')
                time.sleep(backoff)
                backoff += 1
            else:
                return vdisk.guid
        raise RuntimeError('Disk {0} was not created in {1} seconds.'.format(diskname, timeout))

    @staticmethod
    @celery.task(name='ovs.vdisk.create_volume')
    def create_volume(location, size, storagerouter_guid=None):
        """
        Create a volume
        !! This method is for compatibility with the cinder driver
        !! Other callers should use VDiskController.create_new

        :param location: Location, filename
        :type location: str

        :param size: Size of volume (GB)
        :type size: int

        :param storagerouter_guid: Use SSH client to create file on remote storagerouter
        :type storagerouter_guid: str

        :return: None
        """
        VDiskController._logger.info('Creating volume {0} of {1} GB'.format(location, size))

        if storagerouter_guid is not None:
            storagerouter = StorageRouter(storagerouter_guid)
        else:
            storagerouter = System.get_my_storagerouter()
        for storagedriver in storagerouter.storagedrivers:
            if location.startswith('{0}/'.format(storagedriver.mountpoint)):
                diskname = location.split('/')[-1].split('.')[0]
                return VDiskController.create_new(diskname, size, storagedriver.guid)

        raise RuntimeError('Cannot create volume {0}. No storagedriver found for this location.'.format(location))

    @staticmethod
    @celery.task(name='ovs.vdisk.delete_volume')
    def delete_volume(location):
        """
        Delete a volume

        !! This method is for compatibility with the cinder driver
        !! Other callers should use VDiskController.delete

        :param location: Location, filename
        :type location: str

        :return: None
        """
        storagerouter = System.get_my_storagerouter()
        for storagedriver in storagerouter.storagedrivers:
            if location.startswith('{0}/'.format(storagedriver.mountpoint)):
                devicename = location.split('/')[-1]
                disk = VDiskList.get_by_devicename_and_vpool(devicename, storagedriver.vpool)
                if disk is None:
                    VDiskController._logger.info('Disk {0} already deleted'.format(location))
                    return
                return VDiskController.delete(disk.guid)

        raise RuntimeError('Cannot delete volume {0}. No storagedriver found for this location.'.format(location))

    @staticmethod
    @celery.task(name='ovs.vdisk.extend_volume')
    def extend_volume(location, size):
        """
        Extend a volume

        !! This method is for compatibility with the cinder driver
        !! Other callers should use VDiskController.extend

        :param location: Location, filename
        :type location: str

        :param size: Size of volume (GB)
        :type size: int

        :return: None
        """
        storagerouter = System.get_my_storagerouter()
        for storagedriver in storagerouter.storagedrivers:
            if location.startswith('{0}/'.format(storagedriver.mountpoint)):
                devicename = location.split('/')[-1]
                disk = VDiskList.get_by_devicename_and_vpool(devicename, storagedriver.vpool)
                if disk is None:
                    raise RuntimeError('Disk {0} does not exist'.format(location))
                return VDiskController.extend(disk.guid, size)

        raise RuntimeError('Cannot extend volume {0}. No storagedriver found for this location.'.format(location))

    @staticmethod
    @celery.task(name='ovs.vdisk.update_vdisk_name')
    def update_vdisk_name(volume_id, old_name, new_name):
        """
        Update a vDisk name using Management Center: set new name
        :param volume_id: ID of the volume to update its name
        :type volume_id: str

        :param old_name: Old name of the volume
        :type old_name: str

        :param new_name: New name of the volume
        :type new_name: str

        :return: None
        """
        vdisk = None
        for mgmt_center in MgmtCenterList.get_mgmtcenters():
            mgmt = Factory.get_mgmtcenter(mgmt_center = mgmt_center)
            try:
                disk_info = mgmt.get_vdisk_device_info(volume_id)
                device_path = disk_info['device_path']
                vpool_name = disk_info['vpool_name']
                vp = VPoolList.get_vpool_by_name(vpool_name)
                file_name = os.path.basename(device_path)
                vdisk = VDiskList.get_by_devicename_and_vpool(file_name, vp)
                if vdisk:
                    break
            except Exception as ex:
                VDiskController._logger.info('Trying to get mgmt center failed for disk {0} with volume_id {1}. {2}'.format(old_name, volume_id, ex))
        if not vdisk:
            VDiskController._logger.error('No vdisk found for name {0}'.format(old_name))
            return

        vpool = vdisk.vpool
        mutex = volatile_mutex('{0}_{1}'.format(old_name, vpool.guid if vpool is not None else 'none'))
        try:
            mutex.acquire(wait=5)
            vdisk.name = new_name
            vdisk.save()
        finally:
            mutex.release()

    @staticmethod
    @celery.task(name='ovs.vdisk.get_config_params')
    def get_config_params(vdisk_guid):
        """
        Retrieve the configuration parameters for the given disk from the storagedriver.
        :param vdisk_guid: Guid of the virtual disk to retrieve the configuration for
        :type vdisk_guid: str

        :return: Storage driver configuration information for the vDisk
        :rtype: dict
        """
        vdisk = VDisk(vdisk_guid)
        vpool = VPool(vdisk.vpool_guid)

        storagedriver_config = StorageDriverConfiguration('storagedriver', vpool.guid, vpool.storagedrivers[0].storagedriver_id)
        storagedriver_config.load()
        volume_manager = storagedriver_config.configuration.get('volume_manager', {})

        volume_id = str(vdisk.volume_id)
        sco_size = vdisk.storagedriver_client.get_sco_multiplier(volume_id) / 1024 * 4
        dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id)
        dedupe_mode = vdisk.storagedriver_client.get_readcache_mode(volume_id)
        cache_strategy = vdisk.storagedriver_client.get_readcache_behaviour(volume_id)
        tlog_multiplier = vdisk.storagedriver_client.get_tlog_multiplier(volume_id)
        readcache_limit = vdisk.storagedriver_client.get_readcache_limit(volume_id)
        non_disposable_sco_factor = vdisk.storagedriver_client.get_sco_cache_max_non_disposable_factor(volume_id)
        metadata_cache_size = vdisk.storagedriver_client.get_metadata_cache_capacity(volume_id)
        if not metadata_cache_size:
            metadata_cache_size = StorageDriverClient.DEFAULT_METADATA_CACHE_SIZE
        else:
            metadata_cache_size *= StorageDriverClient.METADATA_CACHE_PAGE_SIZE

        dtl_target = []
        if dtl_config is None:
            dtl_mode = 'no_sync'
        else:
            if dtl_config.host == 'null':
                dtl_mode = 'no_sync'
            else:
                dtl_mode = StorageDriverClient.REVERSE_DTL_MODE_MAP[dtl_config.mode]
                dtl_target = [junction.domain_guid for junction in vdisk.domains_dtl]

        if dedupe_mode is None:
            dedupe_mode = volume_manager.get('read_cache_default_mode', StorageDriverClient.VOLDRV_CONTENT_BASED)
        if cache_strategy is None:
            cache_strategy = volume_manager.get('read_cache_default_behaviour', StorageDriverClient.VOLDRV_CACHE_ON_READ)
        if tlog_multiplier is None:
            tlog_multiplier = volume_manager.get('number_of_scos_in_tlog', 20)
        if readcache_limit is not None:
            vol_info = vdisk.storagedriver_client.info_volume(volume_id)
            block_size = vol_info.lba_size * vol_info.cluster_multiplier or 4096
            readcache_limit = readcache_limit * block_size / 1024 / 1024 / 1024
        if non_disposable_sco_factor is None:
            non_disposable_sco_factor = volume_manager.get('non_disposable_scos_factor', 12)

        return {'sco_size': sco_size,
                'dtl_mode': dtl_mode,
                'dedupe_mode': StorageDriverClient.REVERSE_DEDUPE_MAP[dedupe_mode],
                'write_buffer': int(tlog_multiplier * sco_size * non_disposable_sco_factor),
                'dtl_target': dtl_target,
                'cache_strategy': StorageDriverClient.REVERSE_CACHE_MAP[cache_strategy],
                'readcache_limit': readcache_limit,
                'metadata_cache_size': metadata_cache_size}

    @staticmethod
    @celery.task(name='ovs.vdisk.set_config_params')
    def set_config_params(vdisk_guid, new_config_params):
        """
        Sets configuration parameters for a given vdisk.
        :param vdisk_guid: Guid of the virtual disk to set the configuration parameters for
        :type vdisk_guid: str

        :param new_config_params: New configuration parameters
        :type new_config_params: dict

        :return: None
        """
        required_params = {'dtl_mode': (str, StorageDriverClient.VDISK_DTL_MODE_MAP.keys()),
                           'sco_size': (int, StorageDriverClient.TLOG_MULTIPLIER_MAP.keys()),
                           'dedupe_mode': (str, StorageDriverClient.VDISK_DEDUPE_MAP.keys()),
                           'write_buffer': (int, {'min': 128, 'max': 10 * 1024}),
                           'cache_strategy': (str, StorageDriverClient.VDISK_CACHE_MAP.keys()),
                           'readcache_limit': (int, {'min': 1, 'max': 10 * 1024}, False)}

        if new_config_params.get('dtl_target') is not None:
            required_params.update({'dtl_target': (list, Toolbox.regex_guid)})

        if new_config_params.get('metadata_cache_size') is not None:
            required_params.update({'metadata_cache_size': (int, {'min': StorageDriverClient.METADATA_CACHE_PAGE_SIZE})})

        Toolbox.verify_required_params(required_params, new_config_params)

        if new_config_params['dtl_mode'] != 'no_sync' and new_config_params.get('dtl_target') is None:
            raise Exception('If DTL mode is Asynchronous or Synchronous, a Domain guid should always be specified')

        errors = False
        vdisk = VDisk(vdisk_guid)
        volume_id = str(vdisk.volume_id)
        old_config_params = VDiskController.get_config_params(vdisk.guid)

        # 1st update SCO size, because this impacts TLOG multiplier which on its turn impacts write buffer
        new_sco_size = new_config_params['sco_size']
        old_sco_size = old_config_params['sco_size']
        if new_sco_size != old_sco_size:
            write_buffer = float(new_config_params['write_buffer'])
            tlog_multiplier = StorageDriverClient.TLOG_MULTIPLIER_MAP[new_sco_size]
            sco_factor = write_buffer / tlog_multiplier / new_sco_size
            try:
                VDiskController._logger.info('Updating property sco_size on vDisk {0} to {1}'.format(vdisk.name, new_sco_size))
                vdisk.storagedriver_client.set_sco_multiplier(volume_id, new_sco_size / 4 * 1024)
                vdisk.storagedriver_client.set_tlog_multiplier(volume_id, tlog_multiplier)
                vdisk.storagedriver_client.set_sco_cache_max_non_disposable_factor(volume_id, sco_factor)
                VDiskController._logger.info('Updated property sco_size')
            except Exception as ex:
                VDiskController._logger.error('Error updating "sco_size": {0}'.format(ex))
                errors = True

        # 2nd Check for DTL changes
        new_dtl_mode = new_config_params['dtl_mode']
        old_dtl_mode = old_config_params['dtl_mode']
        new_dtl_targets = set(new_config_params.get('dtl_target', []))  # Domain guids
        old_dtl_targets = set(old_config_params['dtl_target'])

        if new_dtl_mode == 'no_sync':
            vdisk.has_manual_dtl = True
            vdisk.save()
            if old_dtl_mode != new_dtl_mode:
                VDiskController._logger.info('Disabling DTL for vDisk {0}'.format(vdisk.name))
                vdisk.storagedriver_client.set_manual_dtl_config(volume_id, None)
                for junction in vdisk.domains_dtl:
                    junction.delete()
        elif new_dtl_mode != old_dtl_mode or new_dtl_targets != old_dtl_targets:  # Mode is sync or async and targets changed or DTL mode changed
            # Delete all original relations
            for junction in vdisk.domains_dtl:
                junction.delete()

            # Create all new relations
            for domain_guid in new_dtl_targets:
                vdisk_domain = VDiskDomain()
                vdisk_domain.vdisk = vdisk
                vdisk_domain.domain = Domain(domain_guid)
                vdisk_domain.save()

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

            current_dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id)
            if old_dtl_mode != new_dtl_mode or current_dtl_config.host not in [sr.ip for sr in possible_storagerouters]:
                random.shuffle(possible_storagerouters)
                dtl_config = None
                for storagerouter in possible_storagerouters:
                    for sd in storagerouter.storagedrivers:  # DTL can reside on any node in the cluster running a volumedriver and having a DTL process running
                        dtl_config = DTLConfig(str(storagerouter.ip), sd.ports['dtl'], StorageDriverClient.VDISK_DTL_MODE_MAP[new_dtl_mode])
                        vdisk.storagedriver_client.set_manual_dtl_config(volume_id, dtl_config)
                        break
                    if dtl_config is not None:
                        break

                if dtl_config is None:
                    VDiskController._logger.error('No Storage Routers found in chosen Domains which have a DTL process running')
                    errors = True

        # 2nd update rest
        for key in required_params:
            try:
                if key in ['sco_size', 'dtl_mode', 'dtl_target']:
                    continue

                new_value = new_config_params[key]
                old_value = old_config_params[key]
                if new_value != old_value:
                    VDiskController._logger.info('Updating property {0} on vDisk {1} from to {2}'.format(key, vdisk.name, new_value))
                    if key == 'dedupe_mode':
                        vdisk.storagedriver_client.set_readcache_mode(volume_id, StorageDriverClient.VDISK_DEDUPE_MAP[new_value])
                    elif key == 'write_buffer':
                        tlog_multiplier = vdisk.storagedriver_client.get_tlog_multiplier(volume_id) or StorageDriverClient.TLOG_MULTIPLIER_MAP[new_sco_size]
                        sco_factor = float(new_value) / tlog_multiplier / new_sco_size
                        vdisk.storagedriver_client.set_sco_cache_max_non_disposable_factor(volume_id, sco_factor)
                    elif key == 'cache_strategy':
                        vdisk.storagedriver_client.set_readcache_behaviour(volume_id, StorageDriverClient.VDISK_CACHE_MAP[new_value])
                    elif key == 'readcache_limit':
                        vol_info = vdisk.storagedriver_client.info_volume(volume_id)
                        block_size = vol_info.lba_size * vol_info.cluster_multiplier or 4096
                        limit = new_value * 1024 * 1024 * 1024 / block_size if new_value else None
                        vdisk.storagedriver_client.set_readcache_limit(volume_id, limit)
                    elif key == 'metadata_cache_size':
                        vdisk.storagedriver_client.set_metadata_cache_capacity(volume_id, new_value / StorageDriverClient.METADATA_CACHE_PAGE_SIZE)
                    else:
                        raise KeyError('Unsupported property provided: "{0}"'.format(key))
                    VDiskController._logger.info('Updated property {0}'.format(key))
            except Exception as ex:
                VDiskController._logger.error('Error updating "{0}": {1}'.format(key, ex))
                errors = True
        if errors is True:
            raise Exception('Failed to update the values for vDisk {0}'.format(vdisk.name))

    @staticmethod
    def sync_with_mgmtcenter(disk, pmachine, storagedriver):
        """
        Update disk info using management center (if available)
        If no management center, try with hypervisor
        If no info retrieved, use devicename
        :param disk: vDisk hybrid (vdisk to be updated)
        :type disk: VDisk

        :param pmachine: Pmachine hybrid (pmachine running the storagedriver)
        :type pmachine: PMachine

        :param storagedriver: Storagedriver hybrid (storagedriver serving the vdisk)
        :type storagedriver: StorageDriver

        :return: None
        """
        disk_name = None
        if pmachine.mgmtcenter is not None:
            VDiskController._logger.debug('Sync vdisk {0} with management center {1} on storagedriver {2}'.format(disk.name, pmachine.mgmtcenter.name, storagedriver.name))
            mgmt = Factory.get_mgmtcenter(mgmt_center = pmachine.mgmtcenter)
            volumepath = disk.devicename
            mountpoint = storagedriver.mountpoint
            devicepath = '{0}/{1}'.format(mountpoint, volumepath)
            try:
                disk_mgmt_center_info = mgmt.get_vdisk_model_by_devicepath(devicepath)
                if disk_mgmt_center_info is not None:
                    disk_name = disk_mgmt_center_info.get('name')
            except Exception as ex:
                VDiskController._logger.error('Failed to sync vdisk {0} with mgmt center {1}. {2}'.format(disk.name, pmachine.mgmtcenter.name, str(ex)))

        if disk_name is None and disk.vmachine is not None:
            VDiskController._logger.info('Sync vdisk with hypervisor on {0}'.format(pmachine.name))
            try:
                hv = Factory.get(pmachine)
                info = hv.get_vm_agnostic_object(disk.vmachine.hypervisor_id)
                for _disk in info.get('disks', {}):
                    if _disk.get('filename', '') == disk.devicename:
                        disk_name = _disk.get('name', None)
                        break
            except Exception as ex:
                VDiskController._logger.error('Failed to get vdisk info from hypervisor. %s' % ex)

        if disk_name is None:
            VDiskController._logger.info('No info retrieved from hypervisor, using devicename')
            disk_name = os.path.splitext(disk.devicename)[0]

        if disk_name is not None:
            disk.name = disk_name
            disk.save()

    @staticmethod
    @celery.task(name='ovs.vdisk.dtl_checkup', schedule=crontab(minute='15', hour='0,4,8,12,16,20'))
    @ensure_single(task_name='ovs.vdisk.dtl_checkup', mode='DEDUPED')
    def dtl_checkup(vpool_guid=None, vdisk_guid=None, storagerouters_to_exclude=None):
        """
        Check DTL for all volumes, for all volumes of a vPool or for 1 specific volume
        :param vpool_guid: vPool to check the DTL configuration of all its vDisks
        :type vpool_guid: str

        :param vdisk_guid: Virtual Disk to check its DTL configuration
        :type vdisk_guid: str

        :param storagerouters_to_exclude: Storage Router Guids to exclude from possible targets
        :type storagerouters_to_exclude: list

        :return: None
        """
        if vpool_guid is not None and vdisk_guid is not None:
            raise ValueError('vPool and vDisk are mutually exclusive')
        if storagerouters_to_exclude is None:
            storagerouters_to_exclude = []

        from ovs.lib.vpool import VPoolController

        VDiskController._logger.info('DTL checkup started')
        required_params = {'dtl_mode': (str, StorageDriverClient.VPOOL_DTL_MODE_MAP.keys()),
                           'dtl_enabled': (bool, None)}

        vdisk = None
        vpool = None
        if vdisk_guid is not None:
            try:
                vdisk = VDisk(vdisk_guid)
            except ObjectNotFoundException:
                VDiskController._logger.warning('    Virtual disk with guid {0} no longer available in model, skipping this iteration'.format(vdisk_guid))
                return
        if vpool_guid is not None:
            try:
                vpool = VPool(vpool_guid)
            except ObjectNotFoundException:
                VDiskController._logger.warning('    vPool with guid {0} no longer available in model, skipping this iteration'.format(vpool_guid))
                return

        errors_found = False
        root_client_map = {}
        vpool_dtl_config_cache = {}
        vdisks = VDiskList.get_vdisks() if vdisk is None and vpool is None else vpool.vdisks if vpool is not None else [vdisk]
        iteration = 0
        while len(vdisks) > 0:
            time_to_wait_for_lock = iteration * 10 + 1
            iteration += 1
            if time_to_wait_for_lock > 40:
                VDiskController._logger.error('Virtual disks with guids {0} could not be checked'.format(', '.join([vdisk.guid for vdisk in vdisks])))
                errors_found = True
                break
            vdisks_copy = list(vdisks)
            for vdisk in vdisks_copy:
                VDiskController._logger.info('    Verifying vDisk {0} with guid {1}'.format(vdisk.name, vdisk.guid))
                vdisk.invalidate_dynamics(['storagedriver_client', 'storagerouter_guid'])
                if vdisk.storagedriver_client is None:
                    vdisks.remove(vdisk)
                    VDiskController._logger.warning('    VDisk {0} with guid {1} does not have a storagedriver client'.format(vdisk.name, vdisk.guid))
                    continue

                vpool = vdisk.vpool
                if vpool.guid not in vpool_dtl_config_cache:
                    vpool_config = VPoolController.get_configuration(vpool.guid)  # Config on vPool is permanent for DTL settings
                    vpool_dtl_config_cache[vpool.guid] = vpool_config
                    Toolbox.verify_required_params(required_params, vpool_config)

                volume_id = str(vdisk.volume_id)
                vpool_config = vpool_dtl_config_cache[vpool.guid]
                dtl_vpool_enabled = vpool_config['dtl_enabled']
                try:
                    current_dtl_config = vdisk.storagedriver_client.get_dtl_config(volume_id)
                    current_dtl_config_mode = vdisk.storagedriver_client.get_dtl_config_mode(volume_id)
                except RuntimeError as rte:
                    # Can occur when a volume has not been stolen yet from a dead node
                    VDiskController._logger.exception('Retrieving DTL configuration from storage driver failed with error: {0}'.format(rte))
                    errors_found = True
                    vdisks.remove(vdisk)
                    continue

                # Verify whether a currently configured DTL target is no longer part of any regular domain --> overrules manual config
                this_storage_router = StorageRouter(vdisk.storagerouter_guid)
                if vdisk.has_manual_dtl is True:
                    VDiskController._logger.info('    VDisk {0} with guid {1} has a manual DTL configuration'.format(vdisk.name, vdisk.guid))
                    if current_dtl_config is None or current_dtl_config.host == 'null':
                        VDiskController._logger.info('    VDisk {0} with guid {1} has a manually disabled DTL'.format(vdisk.name, vdisk.guid))
                        vdisks.remove(vdisk)
                        continue

                    dtl_target = StorageRouterList.get_by_ip(current_dtl_config.host)
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
                if dtl_vpool_enabled is False and (current_dtl_config is None or current_dtl_config.host == 'null'):
                    VDiskController._logger.info('    DTL is globally disabled for vPool {0} with guid {1}'.format(vpool.name, vpool.guid))
                    try:
                        with volatile_mutex(lock_key, wait=time_to_wait_for_lock):
                            vdisk.storagedriver_client.set_manual_dtl_config(volume_id, None)
                    except NoLockAvailableException:
                        VDiskController._logger.info('    Could not acquire lock, continuing with next Virtual Disk')
                        continue
                    vdisks.remove(vdisk)
                    continue
                elif current_dtl_config_mode == DTLConfigMode.MANUAL and (current_dtl_config is None or current_dtl_config.host == 'null'):
                    VDiskController._logger.info('    DTL is disabled for virtual disk {0} with guid {1}'.format(vdisk.name, vdisk.guid))
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
                if len(available_primary_srs) == 0 and len(available_secondary_srs) == 0:
                    available_primary_srs = set(StorageRouterList.get_storagerouters())

                if this_storage_router in available_primary_srs:
                    available_primary_srs.remove(this_storage_router)
                if this_storage_router in available_secondary_srs:
                    available_secondary_srs.remove(this_storage_router)

                # Remove all storagerouters from secondary which are present in primary
                current_sr = None
                available_primary_srs = available_primary_srs.difference(available_secondary_srs)
                if current_dtl_config is not None and current_dtl_config.host != 'null':
                    current_sr = StorageRouterList.get_by_ip(current_dtl_config.host)

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
                                if ServiceManager.has_service(service_name, client=root_client) is True and ServiceManager.get_service_status(service_name, client=root_client) is True:
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
                            vdisk.storagedriver_client.set_manual_dtl_config(volume_id, None)
                    except NoLockAvailableException:
                        VDiskController._logger.info('    Could not acquire lock, continuing with next Virtual Disk')
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
                else:
                    dtl_host = current_dtl_config.host
                    dtl_port = current_dtl_config.port
                    storage_drivers = [sd for sd in vpool.storagedrivers if sd.storagerouter.ip == dtl_host]

                    VDiskController._logger.info('        DTL host: {0}'.format(dtl_host or '-'))
                    VDiskController._logger.info('        DTL port: {0}'.format(dtl_port or '-'))
                    if len(vdisk.domains_dtl) > 0:
                        if dtl_host not in [sr.ip for sr in possible_primary_srs]:
                            VDiskController._logger.info('        Host not in available Storage Routers, manual DTL will be overruled')
                            reconfigure_required = True
                    elif len(possible_secondary_srs) > 0:
                        if dtl_host not in [sr.ip for sr in possible_secondary_srs]:
                            VDiskController._logger.info('        Host not in available secondary Storage Routers')
                            reconfigure_required = True
                    elif len(possible_primary_srs) > 0:
                        if dtl_host not in [sr.ip for sr in possible_primary_srs]:
                            VDiskController._logger.info('        Host not in available primary Storage Routers')
                            reconfigure_required = True
                    if dtl_port != storage_drivers[0].ports['dtl']:
                        VDiskController._logger.info('        Configured port does not match expected port ({0} vs {1})'.format(dtl_port, storage_drivers[0].ports['dtl']))
                        reconfigure_required = True

                # Perform the reconfiguration
                if reconfigure_required is True:
                    possible_srs = possible_primary_srs if len(vdisk.domains_dtl) > 0 else possible_secondary_srs if len(possible_secondary_srs) > 0 else possible_primary_srs
                    VDiskController._logger.info('        Reconfigure required, randomly choosing from {0}'.format(', '.join([sr.ip for sr in possible_srs])))
                    index = random.randint(0, len(possible_srs) - 1)
                    dtl_target = possible_srs[index]
                    storage_drivers = [sd for sd in vpool.storagedrivers if sd.storagerouter == dtl_target]
                    if len(storage_drivers) == 0:
                        VDiskController._logger.error('Could not retrieve related storagedriver')
                        errors_found = True
                        vdisks.remove(vdisk)
                        continue

                    port = storage_drivers[0].ports['dtl']
                    if vdisk.has_manual_dtl is True:
                        dtl_mode = StorageDriverClient.REVERSE_DTL_MODE_MAP[current_dtl_config.mode]
                    else:
                        dtl_mode = vpool_config.get('dtl_mode', StorageDriverClient.FRAMEWORK_DTL_ASYNC)
                    VDiskController._logger.info('        DTL config that will be set -->  Host: {0}, Port: {1}, Mode: {2}'.format(dtl_target.ip, port, dtl_mode))
                    dtl_config = DTLConfig(str(dtl_target.ip), port, StorageDriverClient.VDISK_DTL_MODE_MAP[dtl_mode])
                    try:
                        with volatile_mutex(lock_key, wait=time_to_wait_for_lock):
                            vdisk.storagedriver_client.set_manual_dtl_config(volume_id, dtl_config)
                    except NoLockAvailableException:
                        VDiskController._logger.info('    Could not acquire lock, continuing with next Virtual Disk')
                        continue
                vdisks.remove(vdisk)
        if errors_found is True:
            VDiskController._logger.error('DTL checkup ended with errors')
            raise Exception('DTL checkup failed with errors. Please check /var/log/ovs/lib.log for more information')
        VDiskController._logger.info('DTL checkup ended')

    @staticmethod
    @celery.task(name='ovs.vdisk.dtl_state_transition')
    @log('VOLUMEDRIVER_TASK')
    def dtl_state_transition(volume_name, old_state, new_state, storagedriver_id):
        """
        Triggered by volumedriver when DTL state changes
        :param volume_name: ID of the volume
        :type volume_name: str

        :param old_state: Previous DTL status
        :type old_state: int

        :param new_state: New DTL status
        :type new_state: int

        :param storagedriver_id: ID of the storagedriver hosting the volume
        :type storagedriver_id: str

        :return: None
        """
        if new_state == VolumeDriverEvents_pb2.Degraded and old_state != VolumeDriverEvents_pb2.Standalone:
            vdisk = VDiskList.get_vdisk_by_volume_id(volume_name)
            if vdisk:
                VDiskController._logger.info('Degraded DTL detected for volume {0} with guid {1}'.format(vdisk.name, vdisk.guid))
                storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
                VDiskController.dtl_checkup(vdisk_guid=vdisk.guid,
                                            storagerouters_to_exclude=[storagedriver.storagerouter.guid],
                                            ensure_single_timeout=600)

    @staticmethod
    @celery.task(name='ovs.vdisk.clean_bad_disk')
    def clean_bad_disk(vdiskguid):
        """
        Cleanup bad vdisk:
        - in case create_from_template failed
        - remove mds_services so the vdisk can be properly cleaned up
        :param vdiskguid: Guid of vdisk
        :type vdiskguid: str

        :return: None
        """
        vdisk = VDisk(vdiskguid)
        VDiskController._logger.info('Cleanup vdisk {0}'.format(vdisk.name))
        for mdss in vdisk.mds_services:
            mdss.delete()
        for domain_junction in vdisk.domains_dtl:
            domain_junction.delete()
        storagedriver = StorageDriverList.get_by_storagedriver_id(vdisk.storagedriver_id)
        if storagedriver is not None and vdisk.devicename is not None:
            VDiskController._logger.debug('Removing volume from hypervisor')
            VDiskController.delete(vdisk.guid)

        VDiskController._logger.debug('Deleting vdisk {0} from model'.format(vdisk.name))
        vdisk.delete()

    @staticmethod
    @celery.task(name='ovs.vdisk.schedule_backend_sync')
    def schedule_backend_sync(vdisk_guid):
        """
        Schedule a backend sync on a vdisk
        :param vdisk_guid: Guid of vdisk to schedule a backend sync to
        :type vdisk_guid: str

        :return: TLogName associated with the data sent off to the backend
        :rtype: str
        """
        vdisk = VDisk(vdisk_guid)
        VDiskController._logger.info('Schedule backend sync for vdisk {0}'.format(vdisk.name))
        storagedriver_client = StorageDriverClient.load(vdisk.vpool)

        return storagedriver_client.schedule_backend_sync(str(vdisk.volume_id))

    @staticmethod
    @celery.task(name='ovs.vdisk.is_volume_synced_up_to_tlog')
    def is_volume_synced_up_to_tlog(vdisk_guid, tlog_name):
        """
        Verify if a volume is synced up to a specific tlog
        :param vdisk_guid: Guid of vdisk to verify
        :type vdisk_guid: str

        :param tlog_name: Tlog_name to verify
        :type tlog_name: str

        :return: True or False
        :rtype: bool
        """
        vdisk = VDisk(vdisk_guid)
        storagedriver_client = StorageDriverClient.load(vdisk.vpool)

        return storagedriver_client.is_volume_synced_up_to_tlog(str(vdisk.volume_id), str(tlog_name))

    @staticmethod
    @celery.task(name='ovs.vdisk.is_volume_synced_up_to_snapshot')
    def is_volume_synced_up_to_snapshot(vdisk_guid, snapshot_id):
        """
        Verify if a volume is synced up to a specific snapshot
        :param vdisk_guid: Guid of vdisk to verify
        :type vdisk_guid: str

        :param snapshot_id: Snapshot_id to verify
        :type snapshot_id: str

        :return: True or False
        :rtype: bool
        """
        vdisk = VDisk(vdisk_guid)
        storagedriver_client = StorageDriverClient.load(vdisk.vpool)

        return storagedriver_client.is_volume_synced_up_to_snapshot(str(vdisk.volume_id), str(snapshot_id))

    @staticmethod
    def _set_vdisk_metadata_pagecache_size(vdisk):
        """
        Set metadata page cache size to ratio 1:500 of vdisk.size
        :param vdisk: Object VDisk
        """
        storagedriver_config = StorageDriverConfiguration('storagedriver', vdisk.vpool_guid, vdisk.storagedriver_id)
        storagedriver_config.load()
        metadata_page_capacity = 256
        cluster_size = storagedriver_config.configuration.get('volume_manager', {}).get('default_cluster_size', 4096)
        num_pages = int(vdisk.size / (metadata_page_capacity * cluster_size))
        VDiskController._logger.info('Setting metadata pagecache size for vdisk {0} to {1}'.format(vdisk.name, num_pages))
        vdisk.storagedriver_client.set_metadata_cache_capacity(str(vdisk.volume_id), num_pages)
