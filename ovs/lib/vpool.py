# license see http://www.openvstorage.com/licenses/opensource/
"""
VPool module
"""

from celery import group
from ovs.celery import celery
from ovs.extensions.fs.exportfs import Nfsexports


class VPoolController(object):
    """
    Contains all BLL related to VPools
    """

    @staticmethod
    @celery.task(name='ovs.vpool.mountpoint_available_from_voldrv')
    def mountpoint_available_from_voldrv(mountpoint, vsrid):
        nfs = Nfsexports()
        nfs.unexport(mountpoint)
        nfs.export(mountpoint)