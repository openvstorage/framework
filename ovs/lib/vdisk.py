# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module for VDiskController
"""
import pickle
import uuid
import os

from ovs.celery import celery
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.dal.lists.pmachinelist import PMachineList
from ovs.dal.hybrids.vpool import VPool
from ovs.extensions.hypervisor.factory import Factory
from ovs.extensions.storageserver.storagedriver import StorageDriverClient
from ovs.log.logHandler import LogHandler
from ovs.extensions.generic.sshclient import SSHClient

logger = LogHandler('lib', name='vdisk')


class VDiskController(object):
    """
    Contains all BLL regarding VDisks
    """

    @staticmethod
    @celery.task(name='ovs.disk.list_volumes')
    def list_volumes(vpool_guid=None):
        """
        List all known volumes on a specific vpool or on all
        """
        if vpool_guid is not None:
            vpool = VPool(vpool_guid)
            storagedriver_client = StorageDriverClient().load(vpool)
            response = storagedriver_client.list_volumes()
        else:
            response = []
            for vpool in VPoolList.get_vpools():
                storagedriver_client = StorageDriverClient().load(vpool)
                response.extend(storagedriver_client.list_volumes())
        return response

    @staticmethod
    @celery.task(name='ovs.disk.delete_from_voldrv')
    def delete_from_voldrv(volumename):
        """
        Delete a disk
        Triggered by volumedriver messages on the queue
        @param volumename: volume id of the disk
        """
        disk = VDiskList.get_vdisk_by_volume_id(volumename)
        if disk is not None:
            logger.info('Delete disk {}'.format(disk.name))
            disk.delete()

    @staticmethod
    @celery.task(name='ovs.disk.resize_from_voldrv')
    def resize_from_voldrv(volumename, volumesize, volumepath, storagedriver_id):
        """
        Resize a disk
        Triggered by volumedriver messages on the queue

        @param volumepath: path on hypervisor to the volume
        @param volumename: volume id of the disk
        @param volumesize: size of the volume
        """
        pmachine = PMachineList.get_by_storagedriver_id(storagedriver_id)
        storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
        hypervisor = Factory.get(pmachine)
        volumepath = hypervisor.clean_backing_disk_filename(volumepath)
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

    @staticmethod
    @celery.task(name='ovs.disk.rename_from_voldrv')
    def rename_from_voldrv(volumename, volume_old_path, volume_new_path, storagedriver_id):
        """
        Rename a disk
        Triggered by volumedriver messages

        @param volumename: volume id of the disk
        @param volume_old_path: old path on hypervisor to the volume
        @param volume_new_path: new path on hypervisor to the volume
        """
        pmachine = PMachineList.get_by_storagedriver_id(storagedriver_id)
        hypervisor = Factory.get(pmachine)
        volume_old_path = hypervisor.clean_backing_disk_filename(volume_old_path)
        volume_new_path = hypervisor.clean_backing_disk_filename(volume_new_path)
        disk = VDiskList.get_vdisk_by_volume_id(volumename)
        if disk:
            logger.info('Move disk {} from {} to {}'.format(disk.name,
                                                            volume_old_path,
                                                            volume_new_path))
            disk.devicename = volume_new_path
            disk.save()

    @staticmethod
    @celery.task(name='ovs.disk.clone')
    def clone(diskguid, snapshotid, devicename, pmachineguid, machinename, machineguid=None, **kwargs):
        """
        Clone a disk

        @param location: location where virtual device should be created (eg: myVM)
        @param devicename: device file name for the disk (eg: mydisk-flat.vmdk)
        @param parentdiskguid: guid of the disk
        @param snapshotid: guid of the snapshot
        @param machineguid: guid of the machine to assign disk to
        """
        _ = kwargs
        pmachine = PMachine(pmachineguid)
        hypervisor = Factory.get(pmachine)
        description = '{} {}'.format(machinename, devicename)
        properties_to_clone = ['description', 'size', 'type', 'retentionpolicyguid',
                               'snapshotpolicyguid', 'autobackup']

        new_disk = VDisk()
        disk = VDisk(diskguid)
        _log = 'Clone snapshot {} of disk {} to location {}'
        _location = hypervisor.get_backing_disk_path(machinename, devicename)
        _id = '{}'.format(disk.volume_id)
        _snap = '{}'.format(snapshotid)
        logger.info(_log.format(_snap, disk.name, _location))
        volume_id = disk.storagedriver_client.create_clone(_location, _id, _snap)
        new_disk.copy(disk, include=properties_to_clone)
        new_disk.parent_vdisk = disk
        new_disk.name = '{}-clone'.format(disk.name)
        new_disk.description = description
        new_disk.volume_id = volume_id
        new_disk.devicename = hypervisor.clean_backing_disk_filename(_location)
        new_disk.parentsnapshot = snapshotid
        new_disk.vmachine = VMachine(machineguid) if machineguid else disk.vmachine
        new_disk.vpool = disk.vpool
        new_disk.save()
        return {'diskguid': new_disk.guid,
                'name': new_disk.name,
                'backingdevice': _location}

    @staticmethod
    @celery.task(name='ovs.disk.create_snapshot')
    def create_snapshot(diskguid, metadata, snapshotid=None):
        """
        Create a disk snapshot

        @param diskguid: guid of the disk
        @param metadata: dict of metadata
        """
        disk = VDisk(diskguid)
        logger.info('Create snapshot for disk {}'.format(disk.name))
        if snapshotid is None:
            snapshotid = str(uuid.uuid4())
        metadata = pickle.dumps(metadata)
        disk.storagedriver_client.create_snapshot(
            str(disk.volume_id),
            snapshot_id=snapshotid,
            metadata=metadata
        )
        disk.invalidate_dynamics(['snapshots'])
        return snapshotid

    @staticmethod
    @celery.task(name='ovs.disk.delete_snapshot')
    def delete_snapshot(diskguid, snapshotid):
        """
        Delete a disk snapshot

        @param diskguid: guid of the disk
        @param snapshotguid: guid of the snapshot

        @todo: Check if new volumedriver storagedriver upon deletion
        of a snapshot has built-in protection to block it from being deleted
        if a clone was created from it.
        """
        disk = VDisk(diskguid)
        logger.info('Deleting snapshot {} from disk {}'.format(snapshotid, disk.name))
        disk.storagedriver_client.delete_snapshot(str(disk.volume_id), str(snapshotid))
        disk.invalidate_dynamics(['snapshots'])

    @staticmethod
    @celery.task(name='ovs.disk.set_as_template')
    def set_as_template(diskguid):
        """
        Set a disk as template

        @param diskguid: guid of the disk
        """
        disk = VDisk(diskguid)
        disk.storagedriver_client.set_volume_as_template(str(disk.volume_id))

    @staticmethod
    @celery.task(name='ovs.disk.rollback')
    def rollback(diskguid, timestamp):
        """
        Rolls back a disk based on a given disk snapshot timestamp
        """
        disk = VDisk(diskguid)
        snapshots = [snap for snap in disk.snapshots if snap['timestamp'] == timestamp]
        if not snapshots:
            raise ValueError('No snapshot found for timestamp {}'.format(timestamp))
        snapshotguid = snapshots[0]['guid']
        disk.storagedriver_client.rollback_volume(str(disk.volume_id), snapshotguid)
        disk.invalidate_dynamics(['snapshots'])
        return True

    @staticmethod
    @celery.task(name='ovs.disk.create_from_template')
    def create_from_template(diskguid, machinename, devicename, pmachineguid, machineguid=None, storagedriver_guid=None):
        """
        Create a disk from a template

        @param parentdiskguid: guid of the disk
        @param location: location where virtual device should be created (eg: myVM)
        @param devicename: device file name for the disk (eg: mydisk-flat.vmdk)
        @param machineguid: guid of the machine to assign disk to
        @return diskguid: guid of new disk
        """

        pmachine = PMachine(pmachineguid)
        hypervisor = Factory.get(pmachine)
        disk_path = hypervisor.get_disk_path(machinename, devicename)

        description = '{} {}'.format(machinename, devicename)
        properties_to_clone = [
            'description', 'size', 'type', 'retentionpolicyid',
            'snapshotpolicyid', 'vmachine', 'vpool']

        disk = VDisk(diskguid)
        if disk.vmachine and not disk.vmachine.is_vtemplate:
            # Disk might not be attached to a vmachine, but still be a template
            raise RuntimeError('The given disk does not belong to a template')

        if storagedriver_guid is not None:
            storagedriver_id = StorageDriver(storagedriver_guid).storagedriver_id
        else:
            storagedriver_id = disk.storagedriver_id

        new_disk = VDisk()
        new_disk.copy(disk, include=properties_to_clone)
        new_disk.vpool = disk.vpool
        new_disk.devicename = hypervisor.clean_backing_disk_filename(disk_path)
        new_disk.parent_vdisk = disk
        new_disk.name = '{}-clone'.format(disk.name)
        new_disk.description = description
        new_disk.vmachine = VMachine(machineguid) if machineguid else disk.vmachine
        new_disk.save()

        logger.info('Create disk from template {} to new disk {} to location {}'.format(
            disk.name, new_disk.name, disk_path
        ))
        try:
            volume_id = disk.storagedriver_client.create_clone_from_template(disk_path, str(disk.volume_id), node_id=str(storagedriver_id))
            new_disk.volume_id = volume_id
            new_disk.save()
        except Exception as ex:
            logger.error('Clone disk on volumedriver level failed with exception: {0}'.format(str(ex)))
            new_disk.delete()
            raise

        return {'diskguid': new_disk.guid, 'name': new_disk.name,
                'backingdevice': disk_path}

    @staticmethod
    @celery.task(name='ovs.disk.create_volume')
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
        if os.path.exists(location):
            raise RuntimeError('File already exists at %s' % location)
        client = SSHClient.load('127.0.0.1')
        client.run_local('truncate -s %sG %s' % (size, location))

    @staticmethod
    @celery.task(name='ovs.disk.delete_volume')
    def delete_volume(location):
        """
        Create a volume using filesystem calls
        Calls "rm" to delete raw file
        TODO: use volumedriver API
        TODO: delete VDisk from model

        @param location: location, filename
        @return None
        """
        if not os.path.exists(location):
            logger.error('File already deleted at %s' % location)
            return
        client = SSHClient.load('127.0.0.1')
        output = client.run_local('rm -f %s' % (location))
        output = output.replace('\xe2\x80\x98', '"').replace('\xe2\x80\x99', '"')
        if os.path.exists(location):
            raise RuntimeError('Could not delete file %s, check logs. Output: %s' % (location, output))
        if output == '':
            return True
        raise RuntimeError(output)

    @staticmethod
    @celery.task(name='ovs.disk.extend_volume')
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
        client = SSHClient.load('127.0.0.1')
        client.run_local('truncate -s %sG %s' % (size, location))
