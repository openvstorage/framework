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
License module
"""
import json
import zlib
import base64
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Dynamic
from ovs.lib.helpers.toolbox import Toolbox


class License(DataObject):
    """
    The License class.
    """
    __properties = [Property('component', str, doc='License component (e.g. support)'),
                    Property('name', str, doc='Userfriendly name of the license (e.g. Something Edition)'),
                    Property('token', str, doc='License token, used for license differentation)'),
                    Property('data', dict, doc='License data'),
                    Property('valid_until', float, mandatory=False, doc='License is valid until'),
                    Property('signature', str, mandatory=False, doc='License signature')]
    __relations = []
    __dynamics = [Dynamic('can_remove', bool, 3600),
                  Dynamic('hash', str, 3600)]

    def _can_remove(self):
        """
        Can be removed
        """
        return len(Toolbox.fetch_hooks('license', '{0}.remove'.format(self.component))) == 1

    def _hash(self):
        """
        Generates a hash for this particular license
        """
        return base64.b64encode(zlib.compress(json.dumps(self.export())))
