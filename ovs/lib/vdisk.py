# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module for VDiskController
"""
import os
import re
import pickle
import random
import time
import uuid
from celery.schedules import crontab
from ovs.celery_run import celery
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.hybrids.failuredomain import FailureDomain
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.mgmtcenterlist import MgmtCenterList
from ovs.dal.lists.pmachinelist import PMachineList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.sshclient import UnableToConnectException
from ovs.extensions.generic.volatilemutex import NoLockAvailableException
from ovs.extensions.generic.volatilemutex import VolatileMutex
from ovs.extensions.hypervisor.factory import Factory
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.storageserver.storagedriver import StorageDriverClient
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
from ovs.lib.helpers.decorators import ensure_single
from ovs.lib.helpers.decorators import log
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.mdsservice import MDSServiceController
from ovs.log.logHandler import LogHandler
from subprocess import check_output
from volumedriver.storagerouter import storagerouterclient
from volumedriver.storagerouter import VolumeDriverEvents_pb2
from volumedriver.storagerouter.storagerouterclient import DTLConfig
from volumedriver.storagerouter.storagerouterclient import DTLConfigMode
from volumedriver.storagerouter.storagerouterclient import MDSMetaDataBackendConfig
from volumedriver.storagerouter.storagerouterclient import MDSNodeConfig

logger = LogHandler.get('lib', name='vdisk')
storagerouterclient.Logger.setupLogging(LogHandler.load_path('storagerouterclient'))
storagerouterclient.Logger.enableLogging()


class VDiskController(object):
    """
    Contains all BLL regarding VDisks
    """

    @staticmethod
    @celery.task(name='ovs.vdisk.list_volumes')
    def list_volumes(vpool_guid=None):
        """
        List all known volumes on a specific vpool or on all
        :param vpool_guid: Guid of the vPool to list the volumes for
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
        :param storagedriver_id: ID of the storagedriver serving the volume to delete
        """
        disk = VDiskList.get_vdisk_by_volume_id(volumename)
        if disk is not None:
            mutex = VolatileMutex('{0}_{1}'.format(volumename, disk.devicename))
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
                        logger.info('Disk {0} still exists, ignoring delete'.format(disk.devicename))
                        return
                logger.info('Delete disk {0}'.format(disk.name))
                for mds_service in disk.mds_services:
                    mds_service.delete()
                disk.delete()
            finally:
                mutex.release()

    @staticmethod
    @celery.task(name='ovs.vdisk.delete')
    def delete(diskguid):
        """
        Delete a vdisk through API
        @param diskguid: GUID of the vdisk to delete
        """
        vdisk = VDisk(diskguid)
        storagedriver = StorageDriverList.get_by_storagedriver_id(vdisk.storagedriver_id)
        location = os.path.join(storagedriver.mountpoint, vdisk.devicename)
        logger.info('Deleting disk {0} on location {1}'.format(vdisk.name, location))
        VDiskController.delete_volume(location=location)
        logger.info('Deleted disk {0}'.format(location))

    @staticmethod
    @celery.task(name='ovs.vdisk.resize_from_voldrv')
    @log('VOLUMEDRIVER_TASK')
    def resize_from_voldrv(volumename, volumesize, volumepath, storagedriver_id):
        """
        Resize a disk
        Triggered by volumedriver messages on the queue

        :param volumename: volume ID of the disk
        :param volumesize: size of the volume
        :param volumepath: path on hypervisor to the volume
        :param storagedriver_id: ID of the storagedriver serving the volume to resize
        """
        pmachine = PMachineList.get_by_storagedriver_id(storagedriver_id)
        storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
        hypervisor = Factory.get(pmachine)
        volumepath = hypervisor.clean_backing_disk_filename(volumepath)
        mutex = VolatileMutex('{0}_{1}'.format(volumename, volumepath))
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
            disk.save()
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
        :param volume_old_path: old path on hypervisor to the volume
        :param volume_new_path: new path on hypervisor to the volume
        :param storagedriver_id: ID of the storagedriver serving the volume to rename
        """
        pmachine = PMachineList.get_by_storagedriver_id(storagedriver_id)
        hypervisor = Factory.get(pmachine)
        volume_old_path = hypervisor.clean_backing_disk_filename(volume_old_path)
        volume_new_path = hypervisor.clean_backing_disk_filename(volume_new_path)
        disk = VDiskList.get_vdisk_by_volume_id(volumename)
        if disk:
            logger.info('Move disk {0} from {1} to {2}'.format(disk.name,
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

        :param volume_id:    Volume ID of the disk
        :type volume_id:     unicode

        :param new_owner_id: ID of the storage driver the volume migrated to
        :type new_owner_id:  unicode

        :returns:            None
        """
        sd = StorageDriverList.get_by_storagedriver_id(storagedriver_id=new_owner_id)
        vdisk = VDiskList.get_vdisk_by_volume_id(volume_id=volume_id)
        if vdisk is not None:
            logger.info('Migration - Guid {0} - ID {1} - Detected migration for virtual disk {2}'.format(vdisk.guid, vdisk.volume_id, vdisk.name))
            if sd is not None:
                logger.info('Migration - Guid {0} - ID {1} - Storage Router {2} is the new owner of virtual disk {3}'.format(vdisk.guid, vdisk.volume_id, sd.storagerouter.name, vdisk.name))
            MDSServiceController.mds_checkup()
            VDiskController.dtl_checkup(vdisk_guid=vdisk.guid)

    @staticmethod
    @celery.task(name='ovs.vdisk.clone')
    def clone(diskguid, snapshotid, devicename, pmachineguid, machinename=None, machineguid=None, detached=False):
        """
        Clone a disk
        :param diskguid: Guid of the disk to clone
        :param snapshotid: ID of the snapshot to clone from
        :param devicename: Name of the device to use in clone's description
        :param pmachineguid: Guid of the physical machine
        :param machinename: Name of the machine the disk is attached to
        :param machineguid: Guid of the machine
        :param detached: Boolean indicating the disk is attached to a machine or not
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
            mds_service = MDSServiceController.get_preferred_mds(storagedriver.storagerouter, vdisk.vpool)
            if mds_service is None:
                raise RuntimeError('Could not find a MDS service')

            logger.info('Clone snapshot {0} of disk {1} to location {2}'.format(snapshotid, vdisk.name, location))
            backend_config = MDSMetaDataBackendConfig([MDSNodeConfig(address=str(mds_service.service.storagerouter.ip),
                                                                     port=mds_service.service.ports[0])])
            volume_id = vdisk.storagedriver_client.create_clone(target_path=location,
                                                                metadata_backend_config=backend_config,
                                                                parent_volume_id=str(vdisk.volume_id),
                                                                parent_snapshot_id=str(snapshotid),
                                                                node_id=str(vdisk.storagedriver_id))
        except Exception as ex:
            logger.error('Caught exception during clone, trying to delete the volume. {0}'.format(ex))
            try:
                VDiskController.clean_bad_disk(new_vdisk.guid)
            except Exception as ex2:
                logger.exception('Exception during exception handling of "create_clone_from_template" : {0}'.format(str(ex2)))
            raise

        new_vdisk.volume_id = volume_id
        new_vdisk.save()

        # 5. Check MDS & DTL for new clone
        try:
            MDSServiceController.ensure_safety(new_vdisk)
        except Exception as ex:
            logger.error('Caught exception during "ensure_safety" {0}'.format(ex))
        VDiskController.dtl_checkup.delay(vdisk_guid=new_vdisk.guid)

        return {'diskguid': new_vdisk.guid,
                'name': new_vdisk.name,
                'backingdevice': location}

    @staticmethod
    @celery.task(name='ovs.vdisk.create_snapshot')
    def create_snapshot(diskguid, metadata, snapshotid=None):
        """
        Create a disk snapshot

        :param diskguid: guid of the disk
        :param metadata: dict of metadata
        :param snapshotid: ID of the snapshot
        """
        if not isinstance(metadata, dict):
            raise ValueError('Expected metadata as dict, got {0} instead'.format(type(metadata)))
        disk = VDisk(diskguid)
        logger.info('Create snapshot for disk {0}'.format(disk.name))
        if snapshotid is None:
            snapshotid = str(uuid.uuid4())
        metadata = pickle.dumps(metadata)
        disk.storagedriver_client.create_snapshot(str(disk.volume_id),
                                                  snapshot_id=snapshotid,
                                                  metadata=metadata)
        disk.invalidate_dynamics(['snapshots'])
        return snapshotid

    @staticmethod
    @celery.task(name='ovs.vdisk.delete_snapshot')
    def delete_snapshot(diskguid, snapshotid):
        """
        Delete a disk snapshot

        @param diskguid: guid of the disk
        @param snapshotid: ID of the snapshot

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
        logger.info('Deleting snapshot {0} from disk {1}'.format(snapshotid, disk.name))
        disk.storagedriver_client.delete_snapshot(str(disk.volume_id), str(snapshotid))
        disk.invalidate_dynamics(['snapshots'])

    @staticmethod
    @celery.task(name='ovs.vdisk.set_as_template')
    def set_as_template(diskguid):
        """
        Set a disk as template

        @param diskguid: guid of the disk
        """
        disk = VDisk(diskguid)
        if disk.is_vtemplate is True:
            raise RuntimeError('Disk {0} is already set as template'.format(disk.name))
        logger.info('Setting disk {0} as template'.format(disk.name))
        disk.storagedriver_client.set_volume_as_template(str(disk.volume_id))

    @staticmethod
    @celery.task(name='ovs.vdisk.rollback')
    def rollback(diskguid, timestamp):
        """
        Rolls back a disk based on a given disk snapshot timestamp
        :param diskguid: Guid of the disk to rollback
        :param timestamp: Timestamp of the snapshot to rollback from
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
        :param machinename: Name of the machine
        :param devicename: Device file name for the disk (eg: my_disk-flat.vmdk)
        :param pmachineguid: Guid of pmachine to create new vdisk on
        :param machineguid: Guid of the machine to assign disk to
        :return diskguid: Guid of new disk
        """
        pmachine = PMachine(pmachineguid)
        hypervisor = Factory.get(pmachine)
        new_vdisk_vmachine = None
        if machineguid is not None:
            new_vdisk_vmachine = VMachine(machineguid)
            machinename = new_vdisk_vmachine.name
        disk_path = hypervisor.get_disk_path(machinename, devicename)

        description = '{0} {1}'.format(machinename, devicename)
        properties_to_clone = [
            'description', 'size', 'type', 'retentionpolicyid',
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

        mds_service = MDSServiceController.get_preferred_mds(storagedriver.storagerouter, new_vdisk.vpool)
        if mds_service is None:
            raise RuntimeError('Could not find a MDS service')

        logger.info('Create disk from template {0} to new disk {1} to location {2}'.format(vdisk.name, new_vdisk.name, disk_path))

        try:
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
        except Exception as ex:
            logger.error('Clone disk on volumedriver level failed with exception: {0}'.format(str(ex)))
            try:
                VDiskController.clean_bad_disk(new_vdisk.guid)
            except Exception as ex2:
                logger.exception('Exception during exception handling of "create_clone_from_template" : {0}'.format(str(ex2)))
            raise ex

        return {'diskguid': new_vdisk.guid, 'name': new_vdisk.name,
                'backingdevice': disk_path}

    @staticmethod
    @celery.task(name='ovs.vdisk.create_new')
    def create_new(diskname, size, storagedriver_guid):
        """
        Create a new vdisk/volume using filesystem calls
        :param diskname: name of the disk
        :param size: size of the disk (GB)
        :param storagedriver_guid: guid of the Storagedriver
        :return: guid of the new disk
        """
        logger.info('Creating new empty disk {0} of {1} GB'.format(diskname, size))
        storagedriver = StorageDriver(storagedriver_guid)
        vp_mountpoint = storagedriver.mountpoint
        hypervisor = Factory.get(storagedriver.storagerouter.pmachine)
        disk_path = hypervisor.clean_backing_disk_filename(hypervisor.get_disk_path(None, diskname))
        location = os.path.join(vp_mountpoint, disk_path)
        VDiskController.create_volume(location, size)

        backoff = 1
        timeout = 30  # seconds
        start = time.time()
        while time.time() < start + timeout:
            vdisk = VDiskList.get_by_devicename_and_vpool(disk_path, storagedriver.vpool)
            if vdisk is None:
                logger.debug('Waiting for disk to be picked up by voldrv')
                time.sleep(backoff)
                backoff += 1
            else:
                return vdisk.guid
        raise RuntimeError('Disk {0} was not created in {1} seconds.'.format(diskname, timeout))

    @staticmethod
    @celery.task(name='ovs.vdisk.create_volume')
    def create_volume(location, size):
        """
        Create a volume using filesystem calls
        Calls "truncate" to create sparse raw file
        TODO: use volumedriver API
        TODO: model VDisk() and return guid

        @param location: location, filename
        @param size: size of volume, GB
        @return None
        """
        logger.info('Creating volume {0} of {1} GB'.format(location, size))
        if os.path.exists(location):
            raise RuntimeError('File already exists at %s' % location)

        output = check_output('truncate -s {0}G "{1}"'.format(size, location), shell=True).strip()
        output = output.replace('\xe2\x80\x98', '"').replace('\xe2\x80\x99', '"')

        if not os.path.exists(location):
            raise RuntimeError('Cannot create file %s. Output: %s' % (location, output))

    @staticmethod
    @celery.task(name='ovs.vdisk.delete_volume')
    def delete_volume(location):
        """
        Create a volume using filesystem calls
        Calls "rm" to delete raw file

        @param location: location, filename
        @return None
        """
        if not os.path.exists(location):
            logger.error('File already deleted at %s' % location)
            return
        output = check_output('rm "{0}"'.format(location), shell=True).strip()
        output = output.replace('\xe2\x80\x98', '"').replace('\xe2\x80\x99', '"')
        logger.info(output)
        if os.path.exists(location):
            raise RuntimeError('Could not delete file %s, check logs. Output: %s' % (location, output))
        if output == '':
            return True
        raise RuntimeError(output)

    @staticmethod
    @celery.task(name='ovs.vdisk.extend_volume')
    def extend_volume(location, size):
        """
        Extend a volume using filesystem calls
        Calls "truncate" to create sparse raw file
        TODO: use volumedriver API
        TODO: model VDisk() and return guid

        @param location: location, filename
        @param size: size of volume, GB
        @return None
        """
        if not os.path.exists(location):
            raise RuntimeError('Volume not found at %s, use create_volume first.' % location)
        output = check_output('truncate -s {0}G "{1}"'.format(size, location), shell=True).strip()
        output = output.replace('\xe2\x80\x98', '"').replace('\xe2\x80\x99', '"')
        logger.info(output)

    @staticmethod
    @celery.task(name='ovs.vdisk.update_vdisk_name')
    def update_vdisk_name(volume_id, old_name, new_name):
        """
        Update a vDisk name using Management Center: set new name
        :param volume_id: ID of the volume to update its name
        :param old_name: Old name of the volume
        :param new_name: New name of the volume
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
                logger.info('Trying to get mgmt center failed for disk {0} with volume_id {1}. {2}'.format(old_name, volume_id, ex))
        if not vdisk:
            logger.error('No vdisk found for name {0}'.format(old_name))
            return

        vpool = vdisk.vpool
        mutex = VolatileMutex('{0}_{1}'.format(old_name, vpool.guid if vpool is not None else 'none'))
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

        dtl_target = None
        if dtl_config is None:
            dtl_mode = 'no_sync'
        else:
            if dtl_config.host == 'null':
                dtl_mode = 'no_sync'
            else:
                dtl_mode = StorageDriverClient.REVERSE_DTL_MODE_MAP[dtl_config.mode]
                dtl_target = StorageRouterList.get_by_ip(dtl_config.host).primary_failure_domain.guid

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
        :param new_config_params: New configuration parameters
        """
        required_params = {'dtl_mode': (str, StorageDriverClient.VDISK_DTL_MODE_MAP.keys()),
                           'sco_size': (int, StorageDriverClient.TLOG_MULTIPLIER_MAP.keys()),
                           'dedupe_mode': (str, StorageDriverClient.VDISK_DEDUPE_MAP.keys()),
                           'write_buffer': (int, {'min': 128, 'max': 10 * 1024}),
                           'cache_strategy': (str, StorageDriverClient.VDISK_CACHE_MAP.keys()),
                           'readcache_limit': (int, {'min': 1, 'max': 10 * 1024}, False)}

        if new_config_params.get('dtl_target') is not None:
            required_params.update({'dtl_target': (str, Toolbox.regex_guid)})

        if new_config_params.get('metadata_cache_size') is not None:
            required_params.update({'metadata_cache_size': (int, {'min': StorageDriverClient.METADATA_CACHE_PAGE_SIZE})})

        Toolbox.verify_required_params(required_params, new_config_params)

        if new_config_params['dtl_mode'] != 'no_sync' and new_config_params.get('dtl_target') is None:
            raise Exception('If DTL mode is Asynchronous or Synchronous, a target Failure Domain guid should always be specified')

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
                logger.info('Updating property sco_size on vDisk {0} to {1}'.format(vdisk_guid, new_sco_size))
                vdisk.storagedriver_client.set_sco_multiplier(volume_id, new_sco_size / 4 * 1024)
                vdisk.storagedriver_client.set_tlog_multiplier(volume_id, tlog_multiplier)
                vdisk.storagedriver_client.set_sco_cache_max_non_disposable_factor(volume_id, sco_factor)
                logger.info('Updated property sco_size')
            except Exception as ex:
                logger.error('Error updating "sco_size": {0}'.format(ex))
                errors = True

        # 2nd Check for DTL changes
        new_dtl_mode = new_config_params['dtl_mode']
        old_dtl_mode = old_config_params['dtl_mode']
        new_dtl_target = new_config_params.get('dtl_target')
        old_dtl_target = old_config_params['dtl_target']
        if old_dtl_mode != new_dtl_mode or new_dtl_target != old_dtl_target:
            if old_dtl_mode != new_dtl_mode and new_dtl_mode == 'no_sync':
                logger.info('Disabling DTL for vDisk {0}'.format(vdisk_guid))
                vdisk.storagedriver_client.set_manual_dtl_config(volume_id, None)
            elif (new_dtl_target is not None and new_dtl_target != old_dtl_target or old_dtl_mode != new_dtl_mode) and new_dtl_mode != 'no_sync':
                logger.info('Changing DTL to use global values for vDisk {0}'.format(vdisk_guid))
                possible_srs = FailureDomain(new_dtl_target).primary_storagerouters
                if len(possible_srs) is None:
                    logger.error('Failed to retrieve primary Storage Routers for Failure Domain with guid {0}'.format(new_dtl_target))
                    errors = True

                possible_srs.shuffle()
                dtl_config = None
                for storagerouter in possible_srs:
                    if storagerouter.guid == vdisk.storagerouter_guid:
                        continue
                    if dtl_config is not None:
                        break
                    for sd in storagerouter.storagedrivers:
                        if sd.vpool == vdisk.vpool:
                            dtl_config = DTLConfig(str(storagerouter.ip), sd.ports[2], StorageDriverClient.VDISK_DTL_MODE_MAP[new_dtl_mode])
                            vdisk.storagedriver_client.set_manual_dtl_config(volume_id, dtl_config)
                            vdisk.has_manual_dtl = True
                            vdisk.save()
                            break
                if dtl_config is None:
                    logger.error('Failure Domain with guid {0} has no Storage Routers serving vPool {1}'.format(new_dtl_target, vdisk.vpool.name))
                    errors = True

        # 2nd update rest
        for key in required_params:
            try:
                if key in ['sco_size', 'dtl_mode', 'dtl_target']:
                    continue

                new_value = new_config_params[key]
                old_value = old_config_params[key]
                if new_value != old_value:
                    logger.info('Updating property {0} on vDisk {1} from to {2}'.format(key, vdisk_guid, new_value))
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
                    elif key =='metadata_cache_size':
                        vdisk.storagedriver_client.set_metadata_cache_capacity(volume_id, new_value / StorageDriverClient.METADATA_CACHE_PAGE_SIZE)
                    else:
                        raise KeyError('Unsupported property provided: "{0}"'.format(key))
                    logger.info('Updated property {0}'.format(key))
            except Exception as ex:
                logger.error('Error updating "{0}": {1}'.format(key, ex))
                errors = True
        if errors is True:
            raise Exception('Failed to update the values for vDisk {0}'.format(vdisk.name))

    @staticmethod
    def sync_with_mgmtcenter(disk, pmachine, storagedriver):
        """
        Update disk info using management center (if available)
        If no management center, try with hypervisor
        If no info retrieved, use devicename
        @param disk: vDisk hybrid (vdisk to be updated)
        @param pmachine: pmachine hybrid (pmachine running the storagedriver)
        @param storagedriver: storagedriver hybrid (storagedriver serving the vdisk)
        """
        disk_name = None
        if pmachine.mgmtcenter is not None:
            logger.debug('Sync vdisk {0} with management center {1} on storagedriver {2}'.format(disk.name, pmachine.mgmtcenter.name, storagedriver.name))
            mgmt = Factory.get_mgmtcenter(mgmt_center = pmachine.mgmtcenter)
            volumepath = disk.devicename
            mountpoint = storagedriver.mountpoint
            devicepath = '{0}/{1}'.format(mountpoint, volumepath)
            try:
                disk_mgmt_center_info = mgmt.get_vdisk_model_by_devicepath(devicepath)
                if disk_mgmt_center_info is not None:
                    disk_name = disk_mgmt_center_info.get('name')
            except Exception as ex:
                logger.error('Failed to sync vdisk {0} with mgmt center {1}. {2}'.format(disk.name, pmachine.mgmtcenter.name, str(ex)))

        if disk_name is None and disk.vmachine is not None:
            logger.info('Sync vdisk with hypervisor on {0}'.format(pmachine.name))
            try:
                hv = Factory.get(pmachine)
                info = hv.get_vm_agnostic_object(disk.vmachine.hypervisor_id)
                for _disk in info.get('disks', {}):
                    if _disk.get('filename', '') == disk.devicename:
                        disk_name = _disk.get('name', None)
                        break
            except Exception as ex:
                logger.error('Failed to get vdisk info from hypervisor. %s' % ex)

        if disk_name is None:
            logger.info('No info retrieved from hypervisor, using devicename')
            disk_name = os.path.splitext(disk.devicename)[0]

        if disk_name is not None:
            disk.name = disk_name
            disk.save()

    @staticmethod
    @celery.task(name='ovs.vdisk.dtl_checkup', schedule=crontab(minute='15', hour='0,4,8,12,16,20'))
    @ensure_single(task_name='ovs.vdisk.dtl_checkup', mode='DEDUPED')
    def dtl_checkup(vpool_guid=None, vdisk_guid=None, storagerouters_to_exclude=None):
        """
        Check DTL for all volumes
        :param vpool_guid:                vPool to check the DTL configuration of all its disks
        :type vpool_guid:                 String

        :param vdisk_guid:                Virtual Disk to check its DTL configuration
        :type vdisk_guid:                 String

        :param storagerouters_to_exclude: Storage Router Guids to exclude from possible targets
        :type storagerouters_to_exclude:  List

        :return:                          None
        """
        if vpool_guid is not None and vdisk_guid is not None:
            raise ValueError('vpool and vdisk are mutually exclusive')
        if storagerouters_to_exclude is None:
            storagerouters_to_exclude = []

        from ovs.lib.vpool import VPoolController

        logger.info('DTL checkup started')
        required_params = {'dtl_mode': (str, StorageDriverClient.VPOOL_DTL_MODE_MAP.keys()),
                           'dtl_enabled': (bool, None)}

        vdisk = None
        vpool = None
        if vdisk_guid is not None:
            try:
                vdisk = VDisk(vdisk_guid)
            except ObjectNotFoundException:
                logger.warning('    Virtual disk with guid {0} no longer available in model, skipping this iteration'.format(vdisk_guid))
                return
        if vpool_guid is not None:
            try:
                vpool = VPool(vpool_guid)
            except ObjectNotFoundException:
                logger.warning('    vPool with guid {0} no longer available in model, skipping this iteration'.format(vpool_guid))
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
                logger.error('Virtual disks with guids {0} could not be checked'.format(', '.join([vdisk.guid for vdisk in vdisks])))
                errors_found = True
                break
            vdisks_copy = list(vdisks)
            for vdisk in vdisks_copy:
                logger.info('    Verifying vDisk {0} with guid {1}'.format(vdisk.name, vdisk.guid))
                vdisk.invalidate_dynamics(['storagedriver_client', 'storagerouter_guid'])
                if vdisk.storagedriver_client is None:
                    vdisks.remove(vdisk)
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
                    logger.error('Retrieving DTL configuration from storage driver failed with error: {0}'.format(rte))
                    errors_found = True
                    vdisks.remove(vdisk)
                    continue

                lock_key = 'dtl_checkup_{0}'.format(vdisk.guid)
                if dtl_vpool_enabled is False and (current_dtl_config is None or current_dtl_config.host == 'null'):
                    logger.info('    DTL is globally disabled for vPool {0} with guid {1}'.format(vpool.name, vpool.guid))
                    try:
                        with VolatileMutex(lock_key, wait=time_to_wait_for_lock):
                            vdisk.storagedriver_client.set_manual_dtl_config(volume_id, None)
                    except NoLockAvailableException:
                        logger.info('    Could not acquire lock, continuing with next Virtual Disk')
                        continue
                    vdisks.remove(vdisk)
                    continue
                elif current_dtl_config_mode == DTLConfigMode.MANUAL and (current_dtl_config is None or current_dtl_config.host == 'null'):
                    logger.info('    DTL is disabled for virtual disk {0} with guid {1}'.format(vdisk.name, vdisk.guid))
                    vdisks.remove(vdisk)
                    continue

                this_storage_router = StorageRouter(vdisk.storagerouter_guid)
                available_primary_storagerouters = []
                available_secondary_storagerouters = []
                # 1. Check available storage routers in the backup failure domain
                for failure_domain in [this_storage_router.secondary_failure_domain, this_storage_router.primary_failure_domain]:
                    if failure_domain is None:
                        continue
                    available_storagerouters = available_primary_storagerouters if failure_domain == this_storage_router.primary_failure_domain else available_secondary_storagerouters

                    primary_storage_routers = failure_domain.primary_storagerouters
                    primary_storage_routers.shuffle()
                    for storagerouter in primary_storage_routers:
                        if len(available_storagerouters) == 3:
                            break
                        if vpool.guid not in storagerouter.vpools_guids or storagerouter == this_storage_router:
                            continue
                        if storagerouter.guid in storagerouters_to_exclude:
                            continue
                        if storagerouter not in root_client_map:
                            try:
                                root_client = SSHClient(storagerouter, username='root')
                                service_name = 'dtl_{0}'.format(vpool.name)
                                if ServiceManager.has_service(service_name, client=root_client) is True and ServiceManager.get_service_status(service_name, client=root_client) is True:
                                    root_client_map[storagerouter] = root_client
                                    available_storagerouters.append(storagerouter)
                                else:
                                    logger.warning('    DTL service on Storage Router with IP {0} is not reachable'.format(storagerouter.ip))
                                    root_client_map[storagerouter] = None
                            except UnableToConnectException:
                                logger.warning('    Storage Router with IP {0} of vDisk {1} is not reachable'.format(storagerouter.ip, vdisk.name))
                                root_client_map[storagerouter] = None
                        elif root_client_map[storagerouter] is not None:
                            available_storagerouters.append(storagerouter)

                if len(available_primary_storagerouters) == 0 and len(available_secondary_storagerouters) == 0:
                    logger.info('    No Storage Routers could be found as valid DTL target, setting DTL for vDisk to STANDALONE')
                    try:
                        with VolatileMutex(lock_key, wait=time_to_wait_for_lock):
                            vdisk.storagedriver_client.set_manual_dtl_config(volume_id, None)
                    except NoLockAvailableException:
                        logger.info('    Could not acquire lock, continuing with next Virtual Disk')
                        continue
                    vdisks.remove(vdisk)
                    continue

                # Check whether reconfiguration is required
                reconfigure_required = False
                if current_dtl_config is None:
                    logger.info('        No DTL configuration found, but there are Storage Routers available')
                    reconfigure_required = True
                elif current_dtl_config_mode == DTLConfigMode.AUTOMATIC:
                    logger.info('        DTL configuration set to AUTOMATIC, switching to manual')
                    reconfigure_required = True
                else:
                    dtl_host = current_dtl_config.host
                    dtl_port = current_dtl_config.port
                    storage_drivers = [sd for sd in vpool.storagedrivers if sd.storagerouter.ip == dtl_host]

                    logger.info('        DTL host: {0}'.format(dtl_host or '-'))
                    logger.info('        DTL port: {0}'.format(dtl_port or '-'))
                    if vdisk.has_manual_dtl is True:
                        if dtl_host not in [sr.ip for sr in available_secondary_storagerouters + available_primary_storagerouters]:
                            logger.info('        Host not in available Storage Routers, manual DTL will be overruled')
                            reconfigure_required = True
                    elif len(available_secondary_storagerouters) > 0:
                        if dtl_host not in [sr.ip for sr in available_secondary_storagerouters]:
                            logger.info('        Host not in available secondary Storage Routers')
                            reconfigure_required = True
                    elif len(available_primary_storagerouters) > 0:
                        if dtl_host not in [sr.ip for sr in available_primary_storagerouters]:
                            logger.info('        Host not in available primary Storage Routers')
                            reconfigure_required = True
                    if dtl_port != storage_drivers[0].ports[2]:
                        logger.info('        Configured port does not match expected port ({0} vs {1})'.format(dtl_port, storage_drivers[0].ports[2]))
                        reconfigure_required = True

                # Perform the reconfiguration
                if reconfigure_required is True:
                    available_storagerouters = available_secondary_storagerouters if len(available_secondary_storagerouters) > 0 else available_primary_storagerouters
                    logger.info('        Reconfigure required, randomly choosing from {0}'.format(', '.join([sr.ip for sr in available_storagerouters])))
                    index = random.randint(0, len(available_storagerouters) - 1)
                    dtl_target = available_storagerouters[index]
                    storage_drivers = [sd for sd in vpool.storagedrivers if sd.storagerouter == dtl_target]
                    if len(storage_drivers) == 0:
                        logger.error('Could not retrieve related storagedriver')
                        errors_found = True
                        vdisks.remove(vdisk)
                        continue

                    port = storage_drivers[0].ports[2]
                    vpool_dtl_mode = vpool_config.get('dtl_mode', StorageDriverClient.FRAMEWORK_DTL_ASYNC)
                    logger.info('        DTL config that will be set -->  Host: {0}, Port: {1}, Mode: {2}'.format(dtl_target.ip, port, vpool_dtl_mode))
                    dtl_config = DTLConfig(str(dtl_target.ip), port, StorageDriverClient.VDISK_DTL_MODE_MAP[vpool_dtl_mode])
                    try:
                        with VolatileMutex(lock_key, wait=time_to_wait_for_lock):
                            vdisk.storagedriver_client.set_manual_dtl_config(volume_id, dtl_config)
                            if vdisk.has_manual_dtl is True:
                                vdisk.has_manual_dtl = False
                                vdisk.save()
                    except NoLockAvailableException:
                        logger.info('    Could not acquire lock, continuing with next Virtual Disk')
                        continue
                vdisks.remove(vdisk)
        if errors_found is True:
            logger.error('DTL checkup ended with errors')
            raise Exception('DTL checkup failed with errors. Please check /var/log/ovs/lib.log for more information')
        logger.info('DTL checkup ended')

    @staticmethod
    @celery.task(name='ovs.vdisk.dtl_state_transition')
    @log('VOLUMEDRIVER_TASK')
    def dtl_state_transition(volume_name, old_state, new_state, storagedriver_id):
        """
        Triggered by volumedriver when DTL state changes
        :param volume_name: ID of the volume
        :param old_state: Previous DTL status
        :param new_state: New DTL status
        :param storagedriver_id: ID of the storagedriver hosting the volume
        :return: None
        """
        if new_state == VolumeDriverEvents_pb2.Degraded and old_state != VolumeDriverEvents_pb2.Standalone:
            vdisk = VDiskList.get_vdisk_by_volume_id(volume_name)
            if vdisk:
                logger.info('Degraded DTL detected for volume {0} with guid {1}'.format(vdisk.name, vdisk.guid))
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
        :param vdiskguid: guid of vdisk
        :return: None
        """
        vdisk = VDisk(vdiskguid)
        logger.info('Cleanup vdisk {0}'.format(vdisk.name))
        for mdss in vdisk.mds_services:
            mdss.delete()
        storagedriver = StorageDriverList.get_by_storagedriver_id(vdisk.storagedriver_id)
        if storagedriver is not None and vdisk.devicename is not None:
            logger.debug('Removing volume from filesystem')
            volumepath = vdisk.devicename
            mountpoint = storagedriver.mountpoint
            devicepath = '{0}/{1}'.format(mountpoint, volumepath)
            VDiskController.delete_volume(devicepath)

        logger.debug('Deleting vdisk {0} from model'.format(vdisk.name))
        vdisk.delete()

    @staticmethod
    @celery.task(name='ovs.vdisk.schedule_backend_sync')
    def schedule_backend_sync(vdisk_guid):
        """
        Schedule a backend sync on a vdisk
        :param vdisk_guid: Guid of vdisk to schedule a backend sync to
        :return: TLogName associated with the data sent off to the backend
        """
        vdisk = VDisk(vdisk_guid)
        logger.info('Schedule backend sync for vdisk {0}'.format(vdisk.name))
        storagedriver_client = StorageDriverClient.load(vdisk.vpool)

        return storagedriver_client.schedule_backend_sync(str(vdisk.volume_id))

    @staticmethod
    @celery.task(name='ovs.vdisk.is_volume_synced_up_to_tlog')
    def is_volume_synced_up_to_tlog(vdisk_guid, tlog_name):
        """
        Verify if a volume is synced up to a specific tlog
        :param vdisk_guid: Guid of vdisk to verify
        :param tlog_name: tlog_name to verify
        """
        vdisk = VDisk(vdisk_guid)
        storagedriver_client = StorageDriverClient.load(vdisk.vpool)

        return storagedriver_client.is_volume_synced_up_to_tlog(str(vdisk.volume_id), str(tlog_name))

    @staticmethod
    @celery.task(name='ovs.vdisk.is_volume_synced_up_to_snapshot')
    def is_volume_synced_up_to_snapshot(vdisk_guid, snapshot_id):
        """
        Verify i a volume is synced up to a specific snapshot
        :param vdisk_guid: Guid of vdisk to verify
        :return: snapshot_id: snapshot_id to verify
        """
        vdisk = VDisk(vdisk_guid)
        storagedriver_client = StorageDriverClient.load(vdisk.vpool)

        return storagedriver_client.is_volume_synced_up_to_snapshot(str(vdisk.volume_id), str(snapshot_id))
