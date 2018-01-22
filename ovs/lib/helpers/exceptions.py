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
Custom exception module
"""


class EnsureSingleTimeoutReached(Exception):
    """
    Exception thrown when a celery.task with the ensure_single decorator could not be started in due time
    """
    pass

class RoleDuplicationException(Exception):
    """
    Raised when the DB or DTL role is tried to be assigned, while this role is already present in one of the storagerouter's disks
    """
    pass