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
Disk module
"""
from ovs_extensions.generic.disk import DiskTools as _DiskTools
from ovs.extensions.generic.logger import Logger
from ovs.extensions.os.osfactory import OSFactory


class DiskTools(_DiskTools):
    """
    This class contains various helper methods wrt Disk maintenance
    """
    logger = Logger('extensions-generic')

    def __init__(self):
        super(DiskTools, self).__init__()

    @classmethod
    def _get_os_manager(cls):
        return OSFactory.get_manager()
