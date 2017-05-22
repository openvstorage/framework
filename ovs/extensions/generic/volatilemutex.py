# Copyright (C) 2017 iNuron NV
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
Volatile mutex module
"""
from ovs_extensions.generic.volatilemutex import volatile_mutex as _volatile_mutex, NoLockAvailableException
from ovs.extensions.storage.volatilefactory import VolatileFactory


class volatile_mutex(_volatile_mutex):
    """
    This is a volatile, distributed mutex to provide cross thread, cross process and cross node
    locking. However, this mutex is volatile and thus can fail. You want to make sure you don't
    lock for longer than a few hundred milliseconds to prevent this.
    """

    @classmethod
    def _get_volatile_client(cls):
        return VolatileFactory.get_client()
