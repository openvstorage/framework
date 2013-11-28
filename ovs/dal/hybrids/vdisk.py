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
    The VDisk class represents a vDisk. A vDisk is a Virtual Disk served by Open vStorage. vDisks can be part of a vMachine or stand-alone.
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':              (None,   str,  'Name of the vDisk.'),
                  'description':       (None,   str,  'Description of the vDisk.'),
                  'size':              (0,      int,  'Size of the vDisk in Bytes.'),
                  'devicename':        (None,   str,  'The name of the container file (e.g. the VMDK-file) describing the vDisk.'),
                  'order':             (None,   int,  'Order with which vDisk is attached to a vMachine. None if not attached to a vMachine.'),
                  'volumeid':          (None,   str,  'ID of the vDisk in the Open vStorage Volume Driver.'),
                  'parentsnapshot':    (None,   str,  'Points to a parent voldrvsnapshotid. None if there is no parent Snapshot'),
                  'children':          (list(), list, 'List of child vDisks.'),  # @TODO: discuss purpose of field, there might be a better solution
                  'retentionpolicyid': (None,   str,  'Retention policy used by the vDisk.'),
                  'snapshotpolicyid':  (None,   str,  'Snapshot policy used by the vDisk.'),
                  'tags':              (list(), list, 'Tags of the vDisk.'),
                  'has_autobackup':    (False,  bool, 'Indicates whether this vDisk has autobackup enabled.'),
                  'type':              ('DSSVOL', ['DSSVOL'], 'Type of the vDisk.')}
    _relations = {'vmachine': (VMachine, 'vdisks'),
                  'vpool':    (VPool,    'vdisks')}
    _expiry = {'snapshots':  (60, list),
               'info':       (60, dict),
               'statistics':  (5, dict),
               'vsrid':      (60, str)}
    # pylint: enable=line-too-long

    def _snapshots(self):
        """
        Fetches a list of Snapshots for the vDisk
        """
        if not self.volumeid:
            return []
        return _vsr_client.list_snapShots(str(self.volumeid))

    def _info(self):
        """
        Fetches the info (see Volume Driver API) for the vDisk.
		@return: dict
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
        Fetches the Statistics for the vDisk.
		@return: dict
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
        Returns the Volume Storage Router ID to which the vDisk is connected.
		@return: @TODO
        """
        _ = self
        return None
