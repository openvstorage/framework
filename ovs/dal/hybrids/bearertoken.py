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
Bearer Token module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.client import Client


class BearerToken(DataObject):
    """
    The Bearer Token class represents the Bearer tokens used by the API by means of OAuth 2.0
    """
    __properties = [Property('access_token', str, mandatory=False, doc='Access token'),
                    Property('refresh_token', str, mandatory=False, doc='Refresh token'),
                    Property('expiration', int, doc='Expiration timestamp')]
    __relations = [Relation('client', Client, 'tokens')]
    __dynamics = []
