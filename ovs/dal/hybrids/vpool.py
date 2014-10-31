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
from ovs.dal.structures import Dynamic, Property, Relation
from ovs.extensions.storageserver.storagedriver import StorageDriverClient
from ovs.dal.hybrids.backendtype import BackendType
import time


class VPool(DataObject):
    """
    The VPool class represents a vPool. A vPool is a Virtual Storage Pool, a Filesystem, used to
    deploy vMachines. a vPool can span multiple Storage Drivers and connects to a single Storage BackendType.
    """
    __properties = [Property('name', str, doc='Name of the vPool'),
                    Property('description', str, mandatory=False, doc='Description of the vPool'),
                    Property('size', int, mandatory=False, doc='Size of the vPool expressed in Bytes. Set to zero if not applicable.'),
                    Property('login', str, mandatory=False, doc='Login/Username for the Storage BackendType.'),
                    Property('password', str, mandatory=False, doc='Password for the Storage BackendType.'),
                    Property('connection', str, mandatory=False, doc='Connection (IP, URL, Domainname, Zone, ...) for the Storage BackendType.'),
                    Property('metadata', dict, mandatory=False, doc='Metadata for the backend, as used by the Storage Drivers.'),
                    Property('configuration', dict, default=dict(), doc='Hypervisor/volumedriver specifc fallback configurations')]
    __relations = [Relation('backend_type', BackendType, 'vpools', doc='Type of storage backend.')]
    __dynamics = [Dynamic('status',      str, 10),
                  Dynamic('statistics',  dict, 0),
                  Dynamic('stored_data', int, 60)]

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
        client = StorageDriverClient()
        vdiskstatsdict = {}
        for key in client.stat_keys:
            vdiskstatsdict[key] = 0
            vdiskstatsdict['{0}_ps'.format(key)] = 0
        for vdisk in self.vdisks:
            for key, value in vdisk.statistics.iteritems():
                if key != 'timestamp':
                    vdiskstatsdict[key] += value
        vdiskstatsdict['timestamp'] = time.time()
        return vdiskstatsdict

    def _stored_data(self):
        """
        Aggregates the Stored Data of each vDisk served by the vPool.
        """
        return sum([disk.info['stored'] for disk in self.vdisks])
