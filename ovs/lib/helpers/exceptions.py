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


class EnsureSingleException(Exception):
    """
    Exception that occurred during EnsureSingle logic
    """


class EnsureSingleTimeoutReached(EnsureSingleException):
    """
    Exception thrown when a celery.task with the ensure_single decorator could not be started in due time
    """


class EnsureSingleDoCallBack(EnsureSingleException):
    """
    Raised when the lock could not be acquired and invoking the callback is required
    """


class EnsureSingleTaskDiscarded(EnsureSingleException):
    """
    Raised when the lock could not be acquired and running the task was discarded
    """


class EnsureSingleNoRunTimeInfo(EnsureSingleException):
    """
    Raised when the ensure single would perform an operation without knowing anything about the runtime
    """


class EnsureSingleSimilarJobsCompleted(EnsureSingleException):
    """
    Raised when polling and all relevant jobs have been completed or removed
    """
