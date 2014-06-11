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

from ovs.celery import celery
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.hybrids.volumestoragerouter import VolumeStorageRouter
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.volumestoragerouterlist import VolumeStorageRouterList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.dal.lists.pmachinelist import PMachineList
from ovs.dal.hybrids.vpool import VPool
from ovs.extensions.hypervisor.factory import Factory
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient
from ovs.log.logHandler import LogHandler

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
            vsr_client = VolumeStorageRouterClient().load(vpool)
            response = vsr_client.list_volumes()
        else:
            response = []
            for vpool in VPoolList.get_vpools():
                vsr_client = VolumeStorageRouterClient().load(vpool)
                response.extend(vsr_client.list_volumes())
        return response

    @staticmethod
    @celery.task(name='ovs.disk.delete_from_voldrv')
    def delete_from_voldrv(volumename):
        """
        Delete a disk
        Triggered by volumedriver messages on the queue
        @param volumename: volume id of the disk
        """
        disk = VDiskList.get_vdisk_by_volumeid(volumename)
        if disk is not None:
            logger.info('Delete disk {}'.format(disk.name))
            disk.delete()

    @staticmethod
    @celery.task(name='ovs.disk.resize_from_voldrv')
    def resize_from_voldrv(volumename, volumesize, volumepath, vsrid):
        """
        Resize a disk
        Triggered by volumedriver messages on the queue

        @param volumepath: path on hypervisor to the volume
        @param volumename: volume id of the disk
        @param volumesize: size of the volume
        """
        pmachine = PMachineList.get_by_vsrid(vsrid)
        vsr = VolumeStorageRouterList.get_by_vsrid(vsrid)
        hypervisor = Factory.get(pmachine)
        volumepath = hypervisor.clean_backing_disk_filename(volumepath)
        disk = VDiskList.get_vdisk_by_volumeid(volumename)
        if disk is None:
            disk = VDiskList.get_by_devicename_and_vpool(volumepath, vsr.vpool)
            if disk is None:
                disk = VDisk()
        disk.devicename = volumepath
        disk.volumeid = volumename
        disk.size = volumesize
        disk.vpool = vsr.vpool
        disk.save()

    @staticmethod
    @celery.task(name='ovs.disk.rename_from_voldrv')
    def rename_from_voldrv(volumename, volume_old_path, volume_new_path, vsrid):
        """
        Rename a disk
        Triggered by volumedriver messages

        @param volumename: volume id of the disk
        @param volume_old_path: old path on hypervisor to the volume
        @param volume_new_path: new path on hypervisor to the volume
        """
        pmachine = PMachineList.get_by_vsrid(vsrid)
        hypervisor = Factory.get(pmachine)
        volume_old_path = hypervisor.clean_backing_disk_filename(volume_old_path)
        volume_new_path = hypervisor.clean_backing_disk_filename(volume_new_path)
        disk = VDiskList.get_vdisk_by_volumeid(volumename)
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
        _id = '{}'.format(disk.volumeid)
        _snap = '{}'.format(snapshotid)
        logger.info(_log.format(_snap, disk.name, _location))
        volumeid = disk.vsr_client.create_clone(_location, _id, _snap)
        new_disk.copy_blueprint(disk, include=properties_to_clone)
        new_disk.parent_vdisk = disk
        new_disk.name = '{}-clone'.format(disk.name)
        new_disk.description = description
        new_disk.volumeid = volumeid
        new_disk.devicename = hypervisor.clean_backing_disk_filename(_location)
        new_disk.parentsnapshot = snapshotid
        new_disk.machine = VMachine(machineguid) if machineguid else disk.machine
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
        disk.vsr_client.create_snapshot(
            str(disk.volumeid),
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

        @todo: Check if new volumedriver storagerouter upon deletion
        of a snapshot has built-in protection to block it from being deleted
        if a clone was created from it.
        """
        disk = VDisk(diskguid)
        logger.info('Deleting snapshot {} from disk {}'.format(snapshotid, disk.name))
        disk.vsr_client.delete_snapshot(str(disk.volumeid), str(snapshotid))
        disk.invalidate_dynamics(['snapshots'])

    @staticmethod
    @celery.task(name='ovs.disk.set_as_template')
    def set_as_template(diskguid):
        """
        Set a disk as template

        @param diskguid: guid of the disk
        """
        disk = VDisk(diskguid)
        disk.vsr_client.set_volume_as_template(str(disk.volumeid))

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
        disk.vsr_client.rollback_volume(str(disk.volumeid), snapshotguid)
        disk.invalidate_dynamics(['snapshots'])
        return True

    @staticmethod
    @celery.task(name='ovs.disk.create_from_template')
    def create_from_template(diskguid, machinename, devicename, pmachineguid, machineguid=None, vsrguid=None):
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
            'snapshotpolicyid', 'has_autobackup', 'vmachine', 'vpool']

        disk = VDisk(diskguid)
        if not disk.vmachine.is_vtemplate:
            raise RuntimeError('The given disk does not belong to a template')

        if vsrguid is not None:
            vsrid = VolumeStorageRouter(vsrguid).vsrid
        else:
            vsrid = disk.vsrid

        new_disk = VDisk()
        new_disk.copy_blueprint(disk, include=properties_to_clone)
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
            volumeid = disk.vsr_client.create_clone_from_template(disk_path, str(disk.volumeid), node_id=str(vsrid))
            new_disk.volumeid = volumeid
            new_disk.save()
        except Exception as ex:
            logger.error('Clone disk on volumedriver level failed with exception: {0}'.format(str(ex)))
            new_disk.delete()
            raise

        return {'diskguid': new_disk.guid, 'name': new_disk.name,
                'backingdevice': disk_path}
