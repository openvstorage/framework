# Copyright 2015 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
FailureDomain module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property


class FailureDomain(DataObject):
    """
    The FailureDomain class represents a domain where DTL and/or volumedriver MDS services can be configured to
     Eg:
     || Storage Router || Failure Domain || Backup Failure Domain ||
     |        sr1       |       fd1       |          fd2           |
     |        sr2       |       fd1       |          fd2           |
     |        sr3       |       fd2       |          fd1           |
     |        sr4       |       fd2       |          fd1           |
      - Storage router 1 is part of failure domain 1 and backup failure domain 2
      - Storage router 2 is part of failure domain 1 and backup failure domain 2
      - Storage router 3 is part of failure domain 2 and backup failure domain 1
      - Storage router 4 is part of failure domain 2 and backup failure domain 1

      Storage router 1 will have its DTL configured within its own failure domain (by default), which means on storage router 2

      Each storage router CAN also have a backup failure domain
      For storage router 1 this will be backup failure domain 2 and for storage router 4 this will be backup failure domain 1
      If storage router 2 would go down, the backup failure domain will be used, which means storage router 3 and storage router 4 can be used for DTL of storage router 1
    """
    __properties = [Property('name', str, doc='The name for the (backup) failure domain'),
                    Property('address', str, mandatory=False, doc='Address of the domain'),
                    Property('city', str, mandatory=False, doc='City where domain is located'),
                    Property('country', str, mandatory=False, doc='Country where domain is located')]
    __relations = []
    __dynamics = []
