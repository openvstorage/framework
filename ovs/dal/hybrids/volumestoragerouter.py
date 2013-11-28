# license see http://www.openvstorage.com/licenses/opensource/
"""
VolumeStorageRouter module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.vmachine import VMachine


class VolumeStorageRouter(DataObject):
    """
    The VolumeStorageRouter class represents a volume storage router
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':        (None, str, 'Name of the VSR'),
                  'description': (None, str, 'Description of the VSR'),
                  'port':        (None, int, 'Port on which the VSR is listening'),
                  'ip':          (None, str, 'IP address on which the VSR is listening'),
                  'vsrid':       (None, str, 'Internal volumedriver reference ID')}
    _relations = {'vpool':            (VPool,    'vsrs'),
                  'serving_vmachine': (VMachine, 'served_vsrs')}
    _expiry = {'status':      (30, str),
               'statistics':   (5, dict),
               'stored_data': (60, int)}
    # pylint: enable=line-too-long

    def _status(self):
        """
        Fetches the status of the volume
        """
        _ = self
        return None

    def _statistics(self):
        """
        Agregates the statistics for this vsr
        """
        data = dict()
        if self.vpool is not None:
            for disk in self.vpool.vdisks:
                if disk.vsrid == self.vsrid:
                    statistics = disk.statistics
                    for key, value in statistics.iteritems():
                        data[key] = data.get(key, 0) + value
        return data

    def _stored_data(self):
        """
        Agregates the stored data for this vsr
        """
        if self.vpool is not None:
            return sum([disk.info['stored'] for disk in self.vpool.vdisks])
        return 0

