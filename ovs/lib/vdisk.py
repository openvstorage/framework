# license see http://www.openvstorage.com/licenses/opensource/
"""
Module for VDiskController
"""
import logging
import pickle
import uuid
import time

from ovs.celery import celery
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.volumestoragerouterlist import VolumeStorageRouterList
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient

vsr_client = VolumeStorageRouterClient().load()


class VDiskController(object):

    """
    Contains all BLL regarding VDisks
    """

    @staticmethod
    @celery.task(name='ovs.disk.list_volumes')
    def list_volumes():
        """
        List all known volumes
        """
        response = vsr_client.list_volumes()
        return response

    @staticmethod
    @celery.task(name='ovs.disk.create_from_voldrv')
    def create_from_voldrv(volumepath, volumename, volumesize, vsrid, **kwargs):
        """
        Adds an existing volume to the disk model
        Triggered by volumedriver messages on the queue

        @param volumepath: path on hypervisor to the volume
        @param volumename: volume id of the disk
        @param volumesize: size of the volume
        """
        vsr = VolumeStorageRouterList.get_by_vsrid(vsrid)
        if vsr is None:
            raise RuntimeError('VolumeStorageRouter could not be found')
        disk = VDiskList.get_vdisk_by_volumeid(volumename)
        if disk is None:
            disk = VDisk()
        disk.devicename = volumepath.replace('-flat.vmdk', '.vmdk').strip('/')
        disk.volumeid = volumename
        disk.size = volumesize
        disk.vpool = vsr.vpool
        disk.save()
        return kwargs

    @staticmethod
    @celery.task(name='ovs.disk.delete_from_voldrv')
    def delete_from_voldrv(volumename, **kwargs):
        """
        Delete a disk
        Triggered by volumedriver messages on the queue
        @param volumename: volume id of the disk
        """
        disk = VDiskList.get_vdisk_by_volumeid(volumename)
        if disk is not None:
            logging.info('Delete disk {}'.format(disk.name))
            disk.delete()
        return kwargs

    @staticmethod
    @celery.task(name='ovs.disk.resize_from_voldrv')
    def resize_from_voldrv(volumename, volumesize, **kwargs):
        """
        Resize a disk
        Triggered by volumedriver messages on the queue

        @param volumepath: path on hypervisor to the volume
        @param volumename: volume id of the disk
        @param volumesize: size of the volume
        """

        disk = VDiskList.get_vdisk_by_volumeid(volumename)
        limit = 10
        while disk is None and limit > 0:
            time.sleep(1)
            limit -= 1
            disk = VDiskList.get_vdisk_by_volumeid(volumename)
        if disk is None:
            raise RuntimeError('Disk with devicename {} could not be found'.format(volumename))
        logging.info('Resize disk {} from {} to {}'.format(disk.name if disk.name else volumename,
                                                           disk.size,
                                                           volumesize))
        disk.size = volumesize
        disk.save()
        return kwargs

    @staticmethod
    @celery.task(name='ovs.disk.rename_from_voldrv')
    def rename_from_voldrv(volumename, volume_old_path, volume_new_path, **kwargs):
        """
        Rename a disk
        Triggered by volumedriver messages

        @param volumename: volume id of the disk
        @param volume_old_path: old path on hypervisor to the volume
        @param volume_new_path: new path on hypervisor to the volume
        """
        disk = VDiskList.get_vdisk_by_volumeid(volumename)
        if disk:
            logging.info('Move disk {} from {} to {}'.format(disk.name,
                                                             volume_old_path,
                                                             volume_new_path))
            disk.devicename = volume_new_path
            disk.save()
        return kwargs

    @staticmethod
    @celery.task(name='ovs.disk.clone')
    def clone(diskguid, snapshotid, location, devicename, machineguid=None, **kwargs):
        """
        Clone a disk

        @param location: location where virtual device should be created (eg: myVM)
        @param devicename: device file name for the disk (eg: mydisk-flat.vmdk)
        @param parentdiskguid: guid of the disk
        @param snapshotid: guid of the snapshot
        @param machineguid: guid of the machine to assign disk to
        """
        _ = kwargs
        description = '{} {}'.format(location, devicename)
        properties_to_clone = ['description', 'size', 'type', 'retentionpolicyguid',
                               'snapshotpolicyguid', 'autobackup']

        new_disk = VDisk()
        disk = VDisk(diskguid)
        _log = 'Clone snapshot {} of disk {} to location {}'
        _location = '{}/{}-flat.vmdk'.format(location, devicename)
        _id = '{}'.format(disk.volumeid)
        _snap = '{}'.format(snapshotid)
        logging.info(_log.format(_snap, disk.name, _location))
        volumeid = vsr_client.create_clone(_location, _id, _snap)
        new_disk.copy_blueprint(disk, include=properties_to_clone)
        new_disk.parent_vdisk = disk
        new_disk.name = '{}-clone'.format(disk.name)
        new_disk.description = description
        new_disk.volumeid = volumeid
        new_disk.devicename = '{}.vmdk'.format(devicename)
        new_disk.parentsnapshot = snapshotid
        new_disk.machine = VMachine(machineguid) if machineguid else disk.machine
        new_disk.save()
        return {'diskguid': new_disk.guid, 'name': new_disk.name,
                'backingdevice': '{}/{}.vmdk'.format(location, devicename)}

    @staticmethod
    @celery.task(name='ovs.disk.create_snapshot')
    def create_snapshot(diskguid, metadata, snapshotid=None):
        """
        Create a disk snapshot

        @param diskguid: guid of the disk
        @param metadata: dict of metadata
        """
        disk = VDisk(diskguid)
        logging.info('Create snapshot for disk {}'.format(disk.name))
        if snapshotid is None:
            snapshotid = str(uuid.uuid4())
        metadata = pickle.dumps(metadata)
        vsr_client.create_snapshot(
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
        logging.info('Deleting snapshot {} from disk {}'.format(snapshotid, disk.name))
        vsr_client.delete_snapshot(str(disk.volumeid), str(snapshotid))
        disk.invalidate_dynamics(['snapshots'])

    @staticmethod
    @celery.task(name='ovs.disk.set_as_template')
    def set_as_template(diskguid):
        """
        Set a disk as template

        @param diskguid: guid of the disk
        """

        disk = VDisk(diskguid)
        vsr_client.set_volume_as_template(str(disk.volumeid))

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
        vsr_client.rollback_volume(str(disk.volumeid), snapshotguid)
        disk.invalidate_dynamics(['snapshots'])
        return True

    @staticmethod
    @celery.task(name='ovs.disk.create_from_template')
    def create_from_template(diskguid, location, devicename, machineguid=None):
        """
        Create a disk from a template

        @param parentdiskguid: guid of the disk
        @param location: location where virtual device should be created (eg: myVM)
        @param devicename: device file name for the disk (eg: mydisk-flat.vmdk)
        @param machineguid: guid of the machine to assign disk to
        @return diskguid: guid of new disk
        """

        description = '{} {}'.format(location, devicename)
        properties_to_clone = [
            'description', 'size', 'type', 'retentionpolicyid',
            'snapshotpolicyid', 'has_autobackup', 'vmachine', 'vpool']

        disk = VDisk(diskguid)
        if not disk.vmachine.is_vtemplate:
            raise RuntimeError('The given disk does not belong to a template')

        device_location = '{}/{}.vmdk'.format(location, devicename)

        new_disk = VDisk()
        new_disk.copy_blueprint(disk, include=properties_to_clone)
        new_disk.vpool = disk.vpool
        new_disk.devicename = device_location
        new_disk.parent_vdisk = disk
        new_disk.name = '{}-clone'.format(disk.name)
        new_disk.description = description
        new_disk.vmachine = VMachine(machineguid) if machineguid else disk.vmachine
        new_disk.save()

        logging.info('Create disk from template {} to new disk {} to location {}'.format(
            disk.name, new_disk.name, device_location
        ))
        try:
            volumeid = vsr_client.create_clone_from_template('/' + device_location, str(disk.volumeid))
            new_disk.volumeid = volumeid
            new_disk.save()
        except Exception as ex:
            logging.error('Clone disk on volumedriver level failed with exception: {0}'.format(str(ex)))
            new_disk.delete()
            raise ex

        return {'diskguid': new_disk.guid, 'name': new_disk.name,
                'backingdevice': device_location.strip('/')}
