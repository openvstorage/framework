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
MDSServiceVDisk module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.j_mdsservice import MDSService


class MDSServiceVDisk(DataObject):
    """
    The MDSServiceVDisk class represents the junction table between the MetadataServerService and VDisk.
    Examples:
    * my_vdisk.mds_services[0].mds_service
    * my_mds_service.vdisks[0].vdisk
    """
    __properties = [Property('is_master', bool, default=False, doc='Is this the master MDSService for this VDisk.')]
    __relations = [Relation('vdisk', VDisk, 'mds_services'),
                   Relation('mds_service', MDSService, 'vdisks')]
    __dynamics = []
