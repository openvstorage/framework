# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
VPool module
"""
from ovs.dal.dataobject import DataObject
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient
import time

_vsr_client = VolumeStorageRouterClient().load()


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
                  'backend_type':       (None, ['S3', 'LOCAL', 'REST'], 'Type of the Storage Backend.')}
    _relations = {}
    _expiry = {'status':        (10, str),
               'statistics':     (4, dict),
               'stored_data':   (60, int)}
    # pylint: enable=line-too-long

    def _status(self):
        """
        Fetches the Status of the vPool.
        """
        _ = self
        return None

    def _statistics(self):
        """
        Aggregates the Statistics (IOPS, Bandwidth, ...) of each vDisk served by the vPool.
        """
        vdiskstats = _vsr_client.empty_statistics()
        vdiskstatsdict = {}
        for key, value in vdiskstats.__class__.__dict__.items():
            if type(value) is property:
                vdiskstatsdict[key] = getattr(vdiskstats, key)
        for disk in self.vdisks:
            statistics = disk._statistics()  # Prevent double caching
            for key in vdiskstatsdict.iterkeys():
                vdiskstatsdict[key] += statistics[key]
        vdiskstatsdict['timestamp'] = time.time()
        return vdiskstatsdict

    def _stored_data(self):
        """
        Aggregates the Stored Data of each vDisk served by the vPool.
        """
        return sum([disk.info['stored'] for disk in self.vdisks])
