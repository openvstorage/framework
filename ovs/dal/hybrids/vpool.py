# license see http://www.openvstorage.com/licenses/opensource/
"""
VPool module
"""
from ovs.dal.dataobject import DataObject


class VPool(DataObject):
    """
    The VPool class represents a vPool. A vPool is a Virtual Storage Pool, a Filesystem, used to 
    deploy vMachines. a vPool can span multiple VSRs and connetcs to a single Storage Backend.
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':               (None, str, 'Name of the vPool.'),
                  'description':        (None, str, 'Description of the vPool.'),
                  'size':               (None, int, 'Size of the vPool expressed in Bytes. Set to zero if not applicable.'),
                  'backend_login':      (None, str, 'Login/Username for the Storage Backend.'),
                  'backend_password':   (None, str, 'Password for the Storage Backend.'),
                  'backend_connection': (None, str, 'Connection (IP, URL, Domainname, Zone, ...) for the Storage Backend.'),
                  'backend_type':       (None, ['S3', 'FILESYSTEM'], 'Type of the Storage Backend.')}
    _relations = {}
    _expiry = {'status':      (10, str),
               'statistics':   (5, dict),
               'stored_data': (60, int)}
    # pylint: enable=line-too-long

    def _status(self):
        """
        Fetches the Status of the vPool.
        @return: dict
        """
        _ = self
        return None

    def _statistics(self):
        """
        Aggregates the Statistics (IOPS, Bandwidth, ...) of each vDisk served by the vPool.
        @return: dict
        """
        data = dict()
        for disk in self.vdisks:
            statistics = disk.statistics
            for key, value in statistics.iteritems():
                data[key] = data.get(key, 0) + value
        return data

    def _stored_data(self):
        """
        Aggregates the Stored Data of each vDisk served by the vPool.
        @return: int
        """
        return sum([disk.info['stored'] for disk in self.vdisks])
