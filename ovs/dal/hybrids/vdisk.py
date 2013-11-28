# license see http://www.openvstorage.com/licenses/opensource/
"""
VDisk module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient

_vsr_client = VolumeStorageRouterClient().load()


class VDisk(DataObject):
    """
    The VDisk class represents a vDisk that can be used by vMachines. It has
    a one-to-one link with the volumedriver which is responsible for that particular volume
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':              (None,   str,  'Name of the virtual disk'),
                  'description':       (None,   str,  'Description of the virtual disk'),
                  'size':              (0,      int,  'Size of the virtual disk'),
                  'devicename':        (None,   str,  'The name of the container file backing the vDisk'),
                  'order':             (None,   int,  'Order of the virtual disk in which they are attached'),
                  'volumeid':          (None,   str,  'Volume ID representing the virtual disk'),
                  'parentsnapshot':    (None,   str,  'Points to a parent voldrvsnapshotid'),
                  'children':          (list(), list, 'List of child vDisks'),  # @TODO: discuss purpose of field, there might be a better solution
                  'retentionpolicyid': (None,   str,  'Retention policy used by the virtual disk'),
                  'snapshotpolicyid':  (None,   str,  'Snapshot polity used by the virtual disk'),
                  'tags':              (list(), list, 'Tags of the virtual disk'),
                  'has_autobackup':    (False,  bool, 'Indicates whether this disk has autobackup'),
                  'type':              ('DSSVOL', ['DSSVOL'], 'Type of the virtual disk')}
    _relations = {'vmachine': (VMachine, 'vdisks'),
                  'vpool':    (VPool,    'vdisks')}
    _expiry = {'snapshots':  (60, list),
               'info':       (60, dict),
               'statistics':  (5, dict),
               'vsrid':      (60, str)}
    # pylint: enable=line-too-long

    def _snapshots(self):
        """
        Fetches a list of snapshots for this virtual disk
        """
        if not self.volumeid:
            return []
        return _vsr_client.list_snapShots(str(self.volumeid))

    def _info(self):
        """
        Fetches the info for this volume and converts it to a dict
        """
        if self.volumeid:
            vdiskinfo = _vsr_client.info_volume(str(self.volumeid))
            vdiskinfodict = dict()

            for infoattribute in dir(vdiskinfo):
                if not infoattribute.startswith('_'):
                    vdiskinfodict[infoattribute] = getattr(vdiskinfo, infoattribute)

            return vdiskinfodict
        else:
            return dict()

    def _statistics(self):
        """
        Fetches the statistics for this volume and converts it to a dict
        """
        if self.volumeid:
            vdiskstats = _vsr_client.statistics_volume(str(self.volumeid))
            vdiskstatsdict = dict()

            for statsattribute in dir(vdiskstats):
                if not statsattribute.startswith('_'):
                    vdiskstatsdict[statsattribute] = getattr(vdiskstats, statsattribute)

            return vdiskstatsdict
        else:
            return dict()

    def _vsrid(self):
        """
        Returns the VSR on which the virtual disk is stored
        """
        _ = self
        return None
