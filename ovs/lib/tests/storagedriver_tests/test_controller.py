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
Test module for some generic logic in the StorageDriver controller
"""
import unittest
from ovs.lib.storagedriver import StorageDriverController


class SDControllerTest(unittest.TestCase):
    """
    This test class will validate generic StorageDriverController logic
    """

    def test_backoff_gap(self):
        """
        Validates different node distances generated (to be passed into the StorageDriver)
        """
        scenarios = {1 * 1024 ** 3: {'backoff': int(1 * 1024 ** 3 * 0.1),
                                     'trigger': int(1 * 1024 ** 3 * 0.08)},
                     2 * 1024 ** 4: {'backoff': int(500 * 1024 ** 3 * 0.1),  # Upper limits based on 500GiB volume
                                     'trigger': int(500 * 1024 ** 3 * 0.08)},
                     5: {'backoff': 2,
                         'trigger': 1},
                     None: {'backoff': 2 * 1024 ** 3,  # Invalid size, return default
                            'trigger': 1 * 1024 ** 3}}
        for size, gap_config in scenarios.iteritems():
            self.assertDictEqual(StorageDriverController.calculate_trigger_and_backoff_gap(size), gap_config)
