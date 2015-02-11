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
Log module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.user import User
from ovs.dal.hybrids.storagedriver import StorageDriver


class Log(DataObject):
    """
    The Log class.
    """
    __properties = {Property('source', ['API', 'VOLUMEDRIVER_EVENT', 'VOLUMEDRIVER_TASK'], doc='Source of the call'),
                    Property('module', str, doc='Module containing the method.'),
                    Property('method', str, doc='Method name that has been called.'),
                    Property('method_args', list, mandatory=False, doc='Method args.'),
                    Property('method_kwargs', dict, mandatory=False, doc='Method kwargs.'),
                    Property('time', float, doc='Timestamp of the event'),
                    Property('metadata', dict, mandatory=False, doc='Extra metadata about the entry')}
    __relations = [Relation('user', User, 'logs', mandatory=False),
                   Relation('storagedriver', StorageDriver, 'logs', mandatory=False)]
