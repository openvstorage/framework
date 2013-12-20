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
    @celery.task(name='ovs.disk.new_volume')
    def new_volume(location, devicename, size, name=None, machineguid=None, **kwargs):
        """
        Create a new disk

        @param vpool: name of the vpool
                      vpool is served by one or more storagerouter APPLICATION,
                      this application contains the information where
                      the storagerouterclient should connect to
        @param name: name of the disk
        @param location: location where virtual device should be created (eg: myVM)
        @param devicename: device file name for the disk (eg: mydisk-flat.vmdk)
        @param size: Size of the disk in MiB
        @param maxSize: Maximum size of allowed non disposable SCO's
        @param machineguid: guid of the machine to assign the disk to
        """
        name = name if name else devicename
        description = '{} {}'.format(location, name)
        volumeid = vsr_client.create_volume(
            targetPath='{}/{}'.format(location, devicename),
            volumeSize='{}MiB'.format(size),
            scoMultiplier=1024)
        disk = VDisk()
        disk.name = name
        disk.description = description
        disk.devicename = devicename
        disk.volumeid = volumeid
        disk.machine = VMachine(machineguid) if machineguid else None
        disk.save()
        return kwargs

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
        properties_to_clone = [
            'description', 'size', 'type', 'retentionpolicyguid',
            'snapshotpolicyguid', 'autobackup', 'machine']

        new_disk = VDisk()
        disk = VDisk(diskguid)
        _log = 'Clone snapshot {} of disk {} to location {}'
        _location = '{}/{}-flat.vmdk'.format(location, devicename)
        _id = '{}'.format(disk.volumeid)
        _snap = '{}'.format(snapshotid)
        logging.info(_log.format(_snap, disk.name, _location))
        volumeid = vsr_client.create_clone(_location, _id, _snap)
        for item in properties_to_clone:
            setattr(new_disk, item, getattr(disk, item))
        new_disk.parent_vdisk = disk
        new_disk.name = '{}-clone'.format(disk.name)
        new_disk.description = description
        new_disk.volumeid = volumeid
        new_disk.devicename = '{}.vmdk'.format(devicename)
        new_disk.parentsnapshot = snapshotid
        new_disk.machine = VMachine(
            machineguid) if machineguid else disk.machine
        new_disk.save()
        return {'diskguid': new_disk.guid, 'name': new_disk.name,
                'backingdevice': '{}/{}.vmdk'.format(location, devicename)}

    @staticmethod
    @celery.task(name='ovs.disk.create_snapshot')
    def create_snapshot(diskguid, metadata, **kwargs):
        """
        Create a disk snapshot

        @param diskguid: guid of the disk
        @param metadata: dict of metadata
        """
        disk = VDisk(diskguid)
        logging.info('Create snapshot for disk {}'.format(disk.name))
        # if not srClient.canTakeSnapshot(diskguid):
        #    raise ValueError('Volume {} not found'.format(diskguid))
        metadata = pickle.dumps(metadata)
        snapshotguid = vsr_client.create_snapshot(
            str(disk.volumeid),
            snapshot_id=str(uuid.uuid4()),
            metadata=metadata
        )
        kwargs['result'] = snapshotguid
        return kwargs

    @staticmethod
    @celery.task(name='ovs.disk.delete_snapshot')
    def delete_snapshot(diskguid, snapshotid, **kwargs):
        """
        Delete a disk snapshot

        @param diskguid: guid of the disk
        @param snapshotguid: guid of the snapshot

        @todo: Check if new volumedriver storagerouter upon deletion
        of a snapshot has built-in protection to block it from being deleted
        if a clone was created from it.
        """
        disk = VDisk(diskguid)
        _snap = '{}'.format(snapshotid)
        logging.info(
            'Deleting snapshot {} from disk {}'.format(_snap, diskguid))
        vsr_client.delete_snapshot(disk.volumeid, _snap)
        return kwargs

    @staticmethod
    @celery.task(name='ovs.disk.set_as_template')
    def set_as_template(diskguid, **kwargs):
        """
        Set a disk as template

        @param diskguid: guid of the disk
        """

        # @todo: enable when method is exposed on vsr client
        disk = VDisk(diskguid)
        vsr_client.set_volume_as_template(str(disk.volumeid))

        return kwargs

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
        return True

    @staticmethod
    @celery.task(name='ovs.disk.patchvmdk')
    def patchvmdk(source_base_name, target_base_name):
        """
        THIS IS A TEMPORARY SOLUTION
        Clones/patches a vmdk from a source to a target.
        @param source_base_name: The source base name of the vmdk, including folder
        @param target_base_name: The target base name of the vmdk, including folder

        Example:
          source_base_name = 'test01/test01'
          target_base_name = 'test03/test03'
        What will be executed:
          1. /mnt/dfs/<source_base_name>.vmdk will be opened into string
          2. The in memory string will be updated:
             <source_base_name>-flat.vmdk will be updated to <target_base_name>-flat.vmdk
          3. String will be written to /mnt/dfs/target_base_name>.vmdk

        How to use:
          1. Make sure the clone is executed towards a -flat.vmdk file
          2. Pass in the source and target (target is the above string, without the -flat.vmdk)
          3. Since (1) will result in a vdisk being created automatically with the correct vmdk ref,
             nothing has to be updated anymore.
          4. Attach disk to the vm (or create the vm), which will trigger a sync of the vm,
             which will result in the vdisk being updated and linked to the vm
        """

        if source_base_name.endswith('.vmdk'):
            source_base_name = source_base_name[:-5]

        if target_base_name.endswith('.vmdk'):
            target_base_name = target_base_name[:-5]

        with open('/mnt/dfs/{}.vmdk'.format(source_base_name), 'r') as sourcefile:
            contents = sourcefile.read()

        if contents is not None:
            contents.replace('{}-flat.vmdk'.format(source_base_name),
                             '{}-flat.vmdk'.format(target_base_name))

        with open('/mnt/dfs/{}.vmdk'.format(target_base_name), 'w') as targetfile:
            targetfile.write(contents)

    @staticmethod
    @celery.task(name='ovs.disk.create_from_template')
    def create_from_template(diskguid, location, devicename, machineguid=None, **kwargs):
        """
        Create a disk from a template

        @param parentdiskguid: guid of the disk
        @param location: location where virtual device should be created (eg: myVM)
        @param devicename: device file name for the disk (eg: mydisk-flat.vmdk)
        @param machineguid: guid of the machine to assign disk to
        @return diskguid: guid of new disk
        """

        # @todo verify diskguid specified is actually a template

        _ = kwargs
        description = '{} {}'.format(location, devicename)
        properties_to_clone = [
            'description', 'size', 'type', 'retentionpolicyid',
            'snapshotpolicyid', 'has_autobackup', 'vmachine', 'vpool']

        new_disk = VDisk()
        disk = VDisk(diskguid)
        _log = 'Create disk from template {} to new disk {} to location {}'
        # @todo volume driver does not support space in filenames
        _location = '{}/{}-flat.vmdk'.format(location, devicename).replace(' ', '')
        _id = '{}'.format(disk.volumeid)
        logging.info(_log.format(disk.name, new_disk.name, '/' + _location))
        volumeid = vsr_client.create_clone_from_template('/' + _location, _id)
        for item in properties_to_clone:
            setattr(new_disk, item, getattr(disk, item))
        new_devicename = '{}.vmdk'.format(devicename).replace(' ', '')
        new_disk.parent_vdisk = disk
        new_disk.name = '{}-clone'.format(disk.name)
        new_disk.description = description
        new_disk.volumeid = volumeid
        new_disk.devicename = new_devicename
        new_disk.vmachine = VMachine(
            machineguid) if machineguid else disk.vmachine
        new_disk.save()
        VDiskController.patchvmdk(disk.devicename,
                                  '{}/{}'.format(new_disk.vmachine.name,
                                                 new_disk.devicename))
        return {'diskguid': new_disk.guid, 'name': new_disk.name,
                'backingdevice': _location.replace('-flat', '')}

