# Copyright (C) 2019 iNuron NV
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
import logging
from ovs_extensions.testing.testcase import LogTestCase as _LogTestCase


class LogTestCase(_LogTestCase):

    def setUp(self):
        """
        Setup and add nullHandler by default
        """
        super(LogTestCase, self).setUp()
        root_logger = logging.getLogger()
        root_logger.addHandler(logging.NullHandler())

    def tearDown(self):
        """
        Remove the nullHandler
        """
        root_logger = logging.getLogger()
        root_logger.removeHandler(root_logger.handlers[0])
