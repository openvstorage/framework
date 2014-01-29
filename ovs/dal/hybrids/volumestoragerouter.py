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
VolumeStorageRouter module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.vmachine import VMachine
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient
import time


class VolumeStorageRouter(DataObject):
    """
    The VolumeStorageRouter class represents a Volume Storage Router (VSR). A VSR is an application
    on a VSA to which the vDisks connect. The VSR is the gateway to the Storage Backend.
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':        (None, str, 'Name of the VSR.'),
                  'description': (None, str, 'Description of the VSR.'),
                  'port':        (None, int, 'Port on which the VSR is listening.'),
                  'cluster_ip':  (None, str, 'IP address on which the VSR is listening.'),
                  'storage_ip':  (None, str, 'IP address on which the vpool is shared to hypervisor'),
                  'vsrid':       (None, str, 'ID of the VSR in the Open vStorage Volume Driver.'),
                  'mountpoint':  (None, str, 'Mountpoint from which the VSR serves data')}
    _relations = {'vpool':            (VPool, 'vsrs'),
                  'serving_vmachine': (VMachine, 'served_vsrs')}
    _expiry = {'status':        (30, str),
               'statistics':     (4, dict),
               'stored_data':   (60, int)}
    # pylint: enable=line-too-long

    def __init__(self, *args, **kwargs):
        """
        Initializes a vDisk, setting up it's additional helpers
        """
        DataObject.__init__(self, *args, **kwargs)
        if self.vpool:
            self._frozen = False
            self.vsr_client = VolumeStorageRouterClient().load(vsr=self)
            self._frozen = True

    def _status(self):
        """
        Fetches the Status of the VSR.
        """
        _ = self
        return None

    def _statistics(self):
        """
        Aggregates the Statistics (IOPS, Bandwidth, ...) of the vDisks connected to the VSR.
        """
        vdiskstats = VolumeStorageRouterClient().empty_statistics()
        vdiskstatsdict = {}
        for key, value in vdiskstats.__class__.__dict__.items():
            if type(value) is property:
                vdiskstatsdict[key] = getattr(vdiskstats, key)
        if self.vpool is not None:
            for disk in self.vpool.vdisks:
                if disk.vsrid == self.vsrid:
                    statistics = disk._statistics()  # Prevent double caching
                    for key in vdiskstatsdict.iterkeys():
                        vdiskstatsdict[key] += statistics[key]
        vdiskstatsdict['timestamp'] = time.time()
        return vdiskstatsdict

    def _stored_data(self):
        """
        Aggregates the Stored Data in Bytes of the vDisks connected to the VSR.
        """
        if self.vpool is not None:
            return sum([disk.info['stored'] for disk in self.vpool.vdisks])
        return 0
