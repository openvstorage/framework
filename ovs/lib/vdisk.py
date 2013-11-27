# license see http://www.openvstorage.com/licenses/opensource/
"""
Module for VDiskController
"""
import logging

from ovs.celery import celery
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vmachine import VMachine
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
    @celery.task(name='ovs.disk.create')
    def create(location, devicename, size, name=None, machineguid=None, **kwargs):
        """
        Create a new disk

        @param vpool: name of the vpool
                      vpool is served by one or more storagerouter APPLICATION,
                      this application contains the information where the storagerouterclient should connect to
        @param name: name of the disk
        @param location: location where virtual device should be created (eg: myVM)
        @param devicename: device file name for the disk (eg: mydisk-flat.vmdk)
        @param size: Size of the disk in MiB
        @param maxSize: Maximum size of allowed non disposable SCO's
        @param machineguid: guid of the machine to assign the disk to
        """
        name = name if name else devicename
        description = '{} {}'.format(location, name)
        volumeid = vsr_client.create_volume(targetPath='{}/{}'.format(location, devicename),
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
    @celery.task(name='ovs.disk.delete')
    def delete(diskguid, **kwargs):
        """
        Delete a disk

        @param diskguid: guid of the disk
        """
        disk = VDisk(diskguid)
        logging.info('Delete disk {}'.format(disk.name))
        disk.delete()
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
        disk.children.append(new_disk.guid)
        disk.save()
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
    def create_snapshot(diskguid, **kwargs):
        """
        Create a disk snapshot

        @param diskguid: guid of the disk
        """
        disk = VDisk(diskguid)
        _id = '{}'.format(disk.volumeid)
        logging.info('Create snapshot for disk {}'.format(disk.name))
        #if not srClient.canTakeSnapshot(diskguid):
        #    raise ValueError('Volume {} not found'.format(diskguid))
        snapshotguid = vsr_client.create_snapshot(_id)
        kwargs['result'] = snapshotguid
        return kwargs

    @staticmethod
    @celery.task(name='ovs.disk.delete_snapshot')
    def delete_snapshot(diskguid, snapshotid, **kwargs):
        """
        Delete a disk snapshot

        @param diskguid: guid of the disk
        @param snapshotguid: guid of the snapshot

        @todo: Check if new volumedriver storagerouter upon deletion of a snapshot has built-in protection
        to block it from being deleted if a clone was created from it.
        """
        disk = VDisk(diskguid)
        _snap = '{}'.format(snapshotid)
        logging.info('Deleting snapshot {} from disk {}'.format(_snap, diskguid))
        vsr_client.destroy_snapshot(disk.volumeid, _snap)
        return kwargs
