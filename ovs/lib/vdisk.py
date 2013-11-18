import logging

from ovs.celery import celery
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.lists.vdisklist import VDiskList
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient
vsrClient = VolumeStorageRouterClient().load()


class VDiskController(object):
    #celery = Celery('tasks')
    #celery.config_from_object('celeryconfig')

    @celery.task(name='ovs.disk.listVolumes')
    def listVolumes():
        """
        List all known volumes
        """
        response = vsrClient.listVolumes()
        return response

    @celery.task(name='ovs.disk.getInfo')
    def getInfo(*args, **kwargs):
        """
        Get info from a specific disk

        @param diskguid: Guid of the disk
        """
        diskguid = kwargs['diskguid']
        response = vsrClient.info(diskguid)
        return response

    @celery.task(name='ovs.disk.create')
    def create(*args, **kwargs):
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
        location = kwargs['location']
        devicename = kwargs['devicename']
        size = kwargs['size']
        name = kwargs.get('name', devicename)
        description = '{0} {1}'.format(location, name)
        machineguid = kwargs.get('machineguid', None)
        volumeid = vsrClient.create(targetPath = '{0}/{1}'.format(location, devicename),
                                    volumeSize = '%sMiB'%size,
                                    scoMultiplier = 1024)
#        Volume.attach(name)
#         Volume.setSCOCacheLimits(uniqueVolumeIdentifier = name,
#                                  minSize                = '0B',
#                                  maxNonDisposableSize   = '%sMiB'%maxSize)
#        kwargs['result'] = Volume.getDevice(name)
        disk = VDisk()
        disk.name = name
        disk.description = description
        disk.devicename = devicename
        disk.volumeid = volumeid
        disk.machine = VMachine(machineguid) if machineguid else None
        disk.save()
        return kwargs

    @celery.task(name='ovs.disk.delete')
    def delete(*args, **kwargs):
        """
        Delete a disk

        @param diskguid: guid of the disk
        """
        diskguid = kwargs['diskguid']
        disk = VDisk(diskguid)
        logging.info('Delete disk %s'%disk.name)
        #if disk.volumeid in vsrClient.listVolumes():
            #if Volume.info()['attached']:
            #    Volume.detach(uniqueVolumeIdentifier = guid)
            #vsrClient.destroy(uniqueVolumeIdentifier = disk.volumeid)
        disk.delete()
        return kwargs

    @celery.task(name='ovs.disk.clone')
    def clone(*args, **kwargs):
        """
        Clone a disk

        @param location: location where virtual device should be created (eg: myVM)
        @param devicename: device file name for the disk (eg: mydisk-flat.vmdk)
        @param parentdiskguid: guid of the disk
        @param snapshotid: guid of the snapshot
        @param machineguid: guid of the machine to assign disk to
        """
        diskguid = kwargs['parentdiskguid']
        snapshotid = kwargs['snapshotid']
        location = kwargs['location']
        deviceNamePrefix = kwargs['devicename']
        machineguid = kwargs.get('machineguid', None)
        description = '{0} {1}'.format(location, deviceNamePrefix)
        propertiesToClone = ['description', 'size', 'type', 'retentionpolicyguid', 'snapshotpolicyguid', 'autobackup', 'machine']

        newDisk = VDisk()
        disk = VDisk(diskguid)
        logging.info('Clone snapshot %s of disk %s'%(snapshotid, disk.name))
        volumeid = vsrClient.clone('{0}/{1}'.format(location, '%s-flat.vmdk'%deviceNamePrefix), disk.volumeid, snapshotid)
        for property in propertiesToClone:
            setattr(newDisk, property, getattr(disk, property))
        disk.children.append(newDisk.guid)
        disk.save()
        newDisk.name = '%s-clone'%disk.name
        newDisk.description = description
        newDisk.volumeid = volumeid
        newDisk.devicename = '%s.vmdk'%deviceNamePrefix
        newDisk.parentsnapshot = snapshotid
        newDisk.machine = VMachine(machineguid) if machineguid else disk.machine
        newDisk.save()
        return {'diskguid': newDisk.guid,'name': newDisk.name, 'backingdevice': '{0}/{1}.vmdk'.format(location, deviceNamePrefix)}

    @celery.task(name='ovs.disk.createSnapshot')
    def createSnapshot(*args, **kwargs):
        """
        Create a disk snapshot

        @param diskguid: guid of the disk
        """
        diskguid = kwargs['diskguid']
        disk = VDisk(diskguid)
        logging.info('Create snapshot for disk %s'%(disk.name))
        #if not srClient.canTakeSnapshot(diskguid):
        #    raise ValueError('Volume %s not found'%diskguid)
        snapshotguid = vsrClient.snapShotCreate(disk.volumeid)
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
        snapshotid = kwargs['snapshotid']
        logging.info('Delete snapshot %s from disk %s'%(snapshotguid, diskguid))
        vsrClient.snapShotDestroy(disk.volumeid, snapshotid)
        return kwargs
