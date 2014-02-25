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

from ovs.celery import celery
from ovs.dal.hybrids.vmachine import VMachine
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient


class VolumeStorageRouterController(object):
    """
    Contains all BLL related to VolumeStorageRouters
    """

    @staticmethod
    @celery.task(name='ovs.vsr.move_away')
    def move_away(vsa_guid):
        """
        Moves away all vDisks from all VSRs this VSA is serving
        """
        served_vsrs = VMachine(vsa_guid).served_vsrs
        if len(served_vsrs) > 0:
            vsr_client = VolumeStorageRouterClient().load(served_vsrs[0].vpool)
            for vsr in served_vsrs:
                vsr_client.mark_node_offline(str(vsr.vsrid))
