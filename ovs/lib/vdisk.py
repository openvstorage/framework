import time
import uuid
import logging

from ovs.celery import celery

from volumedriver.daemon import VolumeDriver, Volume

class vdisk(object):
    #celery = Celery('tasks')
    #celery.config_from_object('celeryconfig')

    @celery.task(name='ovs.disk.listVolumes')
    def listVolumes():
        response = VolumeDriver.listVolumes()
        return response
    
    @celery.task(name='ovs.disk.getInfo')
    def getInfo(*args, **kwargs):
        name = kwargs['name']
        response = Volume.info(name)
        return response
    
    @celery.task(name='ovs.disk.create')
    def create(*args, **kwargs):
        name = kwargs['name']
        size = kwargs['size']
        Volume.create(uniqueVolumeIdentifier = name,
                      namespace              = name,
                      volumeSize             = '%sMiB'%size,
                      scoMultiplier          = 1024)
        Volume.attach(name)
        Volume.setSCOCacheLimits(uniqueVolumeIdentifier = name,
                                 minSize                = '0B',
                                 maxNonDisposableSize   = '338MiB')
        kwargs['result'] = Volume.getDevice(name)
        return kwargs
    
    @celery.task(name='ovs.disk.delete')
    def delete(*args, **kwargs):
        name = kwargs['name']
        logging.info('Delete disk %s'%name )
        if name in VolumeDriver.listVolumes():
            if Volume.info()['attached']:
                Volume.detach(uniqueVolumeIdentifier = name)
            Volume.destroy(uniqueVolumeIdentifier = name,
                           force = True,
                           migrateCacheToParent = False)
        return kwargs
    
    @celery.task(name='ovs.disk.clone')
    def clone(*args, **kwargs):
        name = kwargs['name']
        snapshot = kwargs['snapshot']
        logging.info('Delete snapshot %s from disk %s'%(snapshot, name))
        Volume.snapShotDestroy(name, snapshot)
        return kwargs
    
    @celery.task(name='ovs.disk.createSnapshot')
    def createSnapshot(*args, **kwargs):
        name = kwargs['name']
        snapshot = kwargs['snapshot']
        logging.info('Create snapshot %s on disk %s'%(snapshot, name))
        if not Volume.canTakeSnapshot(name):
            raise ValueError('Volume %s not found'%name)
        Volume.snapShotCreate(name, snapshot)
        return kwargs
    
    @celery.task(name='ovs.disk.deleteSnapshot')
    def deleteSnapshot(*args, **kwargs):
        name = kwargs['name']
        snapshot = kwargs['snapshot']
        logging.info('Delete snapshot %s from disk %s'%(snapshot, name))
        Volume.snapShotDestroy(name, snapshot)
        return kwargs
    
    @celery.task(name='ovs.disk.listSnapshots')
    def listSnapshot(*args, **kwargs):
        name = kwargs['name']
        logging.info('List snapshots from disk %s'%name)
        Volume.listSnapshots(name)
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
