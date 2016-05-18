# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

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
