import uuid
import logging

from ovs.celery import celery
from ovs.dal.hybrids.vdisk import vDisk
from ovs.dal.lists.vdisklist import vDiskList
from volumedriver.daemon import VolumeDriver, Volume


class VDiskController(object):
    #celery = Celery('tasks')
    #celery.config_from_object('celeryconfig')

    @celery.task(name='ovs.disk.listVolumes')
    def listVolumes():
        """
        List all known volumes
        """
        response = VolumeDriver.listVolumes()
        return response

    @celery.task(name='ovs.disk.getInfo')
    def getInfo(*args, **kwargs):
        """
        Get info from a specific disk

        @param diskguid: Guid of the disk
        """
        diskguid = kwargs['diskguid']
        response = Volume.info(diskguid)
        return response

    @celery.task(name='ovs.disk.create')
    def create(*args, **kwargs):
        """
        Create a new disk

        @param diskguid: Guid of the new disk to create
        @param size: Size of the disk in MiB
        @param maxSize: Maximum size of allowed non disposable SCO's
        """
        diskguid = kwargs['diskguid']
        size = kwargs['size']
        maxSize = kwargs.get('maxSize', '338')
        Volume.create(uniqueVolumeIdentifier = diskguid,
                      volumeSize             = '%sMiB'%size,
                      scoMultiplier          = 1024)
        Volume.attach(name)
        Volume.setSCOCacheLimits(uniqueVolumeIdentifier = name,
                                 minSize                = '0B',
                                 maxNonDisposableSize   = '%sMiB'%maxSize)
        kwargs['result'] = Volume.getDevice(name)
        vDisk(guid).save()
        return kwargs

    @celery.task(name='ovs.disk.delete')
    def delete(*args, **kwargs):
        """
        Delete a disk

        @param diskguid: guid of the disk
        """
        diskguid = kwargs['diskguid']
        logging.info('Delete disk %s'%guid )
        if name in VolumeDriver.listVolumes():
            if Volume.info()['attached']:
                Volume.detach(uniqueVolumeIdentifier = guid)
            Volume.destroy(uniqueVolumeIdentifier = guid,
                           force = True,
                           migrateCacheToParent = False)
        vDisk(guid).delete()
        return kwargs

    @celery.task(name='ovs.disk.clone')
    def clone(*args, **kwargs):
        """
        Clone a disk

        @param parentdiskguid: guid of the disk
        @param snapshotguid: guid of the snapshot
        @param devicepath: path to the disk
        """
        diskguid = kwargs['parentdiskguid']
        snapshotguid = kwargs['snapshotguid']
        devicepath = kwargs['devicepath']
        machineguid = kwargs.get('machineguid', None)
        propertiesToClone = ['description', 'size', 'vpoolguid', 'type', 'retentionpolicyguid', 'snapshotpolicyguid', 'autobackup', 'machine']

        logging.info('Clone snapshot %s of disk %s'%(snapshot, name))
        newDisk = vDisk()
        disk = vDisk(diskguid)
        Volume.clone(diskguid, snapshotguid, newDisk.guid, devicepath)
        for property in propertiesToClone:
            setattr(newDisk, property, getattr(disk, property))
        disk.children.append(newDisk.guid)
        disk.save()
        newDisk.machine = vMachine(machineguid) if machineguid else disk.machine
        newDisk.save()
        kwargs['result'] = newDisk.guid
        return kwargs

    @celery.task(name='ovs.disk.createSnapshot')
    def createSnapshot(*args, **kwargs):
        """
        Create a disk snapshot

        @param diskguid: guid of the disk
        """
        diskguid = kwargs['diskguid']
        snapshotguid = uuid.uuid4()
        logging.info('Create snapshot %s on disk %s'%(snapshotguid, diskguid))
        if not Volume.canTakeSnapshot(diskguid):
            raise ValueError('Volume %s not found'%diskguid)
        Volume.snapShotCreate(diskguid, snapshotguid)
        kwargs['result'] = snapshotguid
        return kwargs

    @celery.task(name='ovs.disk.deleteSnapshot')
    def deleteSnapshot(*args, **kwargs):
        """
        Delete a disk snapshot

        @param diskguid: guid of the disk
        @param snapshotguid: guid of the snapshot

        @todo: Check if new volumedriver storagerouter upon deletion of a snapshot has built-in protection
        to block it from being deleted if a clone was created from it.
        """
        diskguid = kwargs['diskguid']
        snapshotguid = kwargs['snapshotguid']
        logging.info('Delete snapshot %s from disk %s'%(snapshotguid, diskguid))
        Volume.snapShotDestroy(diskguid, snapshotguid)
        return kwargs

    @celery.task(name='ovs.disk.listSnapshots')
    def listSnapshot(*args, **kwargs):
        """
        List snapshots of a disk
        """
        diskguid = kwargs['diskguid']
        logging.info('List snapshots from disk %s'%diskguid)
        Volume.listSnapshots(diskguid)
        return kwargs

    @celery.task(name='ovs.disk.createDiskChain')
    def exampleCreateChain(*args, **kwargs):
        a = addNameSpace.s(*args, **kwargs)
        a.link_error(deleteNameSpace.s(**kwargs))
        b = createVolume.s(**kwargs)
        b.link_error(removeVolumeChain(**kwargs))
        c = echo.s('Succesful created %s'%kwargs)
        return chain(a,b)

    @celery.task(name='ovs.disk.deleteDiskChain')
    def exampleDeleteChain(*args, **kwargs):
        a = deleteVolume.s(*args, **kwargs)
        b = deleteNameSpace.s(**kwargs)
        c = echo.s('Succesful deleted %s'%kwargs)
        return chain(a,b)
