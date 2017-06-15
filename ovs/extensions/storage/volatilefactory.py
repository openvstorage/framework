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
Override volatile factory.
"""
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.storage.volatilefactory import VolatileFactory as _VolatileFactory


class VolatileFactory(_VolatileFactory):
    """
    The VolatileFactory will generate certain default clients.
    """

    @classmethod
    def _get_store_info(cls):
        return {'nodes': Configuration.get('/ovs/framework/memcache|endpoints')}

    @classmethod
    def _get_client_type(cls):
        return Configuration.get('/ovs/framework/stores|volatile')
