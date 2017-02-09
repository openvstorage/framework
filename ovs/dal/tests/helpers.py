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
Helper module
"""
from ovs.dal.datalist import DataList
from ovs.extensions.generic import fakesleep
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory


class Helper(object):
    """
    This class contains functionality used by all UnitTests related to the DAL
    """
    @staticmethod
    def setup(**kwargs):
        """
        Execute several actions before starting a new UnitTest
        :param kwargs: Additional key word arguments
        :type kwargs: dict
        """
        volatile = VolatileFactory.get_client()
        persistent = PersistentFactory.get_client()

        volatile.clean()
        persistent.clean()
        DataList.test_hooks = {}

        if kwargs.get('fake_sleep', False) is True:
            fakesleep.monkey_patch()
        return volatile, persistent

    @staticmethod
    def teardown(**kwargs):
        """
        Execute several actions when ending a UnitTest
        :param kwargs: Additional key word arguments
        :type kwargs: dict
        """
        volatile = VolatileFactory.get_client()
        persistent = PersistentFactory.get_client()

        volatile.clean()
        persistent.clean()
        if kwargs.get('fake_sleep', False) is True:
            fakesleep.monkey_restore()
