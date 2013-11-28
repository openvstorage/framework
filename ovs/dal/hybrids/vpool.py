# license see http://www.openvstorage.com/licenses/opensource/
"""
VPool module
"""
from ovs.dal.dataobject import DataObject


class VPool(DataObject):
    """
    The VPool class represents a vPool. A vPool covers a given backend
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':               (None, str, 'Name of the virtual pool'),
                  'description':        (None, str, 'Description of the virtual pool'),
                  'size':               (None, int, 'Size of the virtual pool'),
                  'backend_login':      (None, str, 'Login for the backend'),
                  'backend_password':   (None, str, 'Password for the backend'),
                  'backend_connection': (None, str, 'Connection for the backend'),
                  'backend_type':       (None, ['S3', 'FILESYSTEM'], 'Type of the backend')}
    _relations = {}
    _expiry = {'status':      (10, str),
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
        Agregates the statistics for this vpool
        """
        data = dict()
        for disk in self.vdisks:
            statistics = disk.statistics
            for key, value in statistics.iteritems():
                data[key] = data.get(key, 0) + value
        return data

    def _stored_data(self):
        """
        Agregates the stored data for this vpool
        """
        return sum([disk.info['stored'] for disk in self.vdisks])
