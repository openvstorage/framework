# Copyright 2015 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Disk module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.storagerouter import StorageRouter


class Disk(DataObject):
    """
    The Disk class represents physical disks that are available to a storagerouter (technically, they can be
    a virtual disk, but from the OS (and framework) point of view, they're considered physical)
    """
    __properties = [Property('path', str, doc='The device path'),
                    Property('vendor', str, mandatory=False, doc='The disks vendor'),
                    Property('model', str, mandatory=False, doc='The disks model'),
                    Property('state', ['OK', 'ERROR', 'MISSING'], doc='The state of the disk'),
                    Property('name', str, doc='Name of the disk (e.g. sda)'),
                    Property('size', int, doc='Size of the disk, in bytes'),
                    Property('is_ssd', bool, doc='The type of the disk')]
    __relations = [Relation('storagerouter', StorageRouter, 'disks')]
    __dynamics = []
