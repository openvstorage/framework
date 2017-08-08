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
OS Factory module
"""
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.system import System
from ovs_extensions.os.osfactory import OSFactory as _OSFactory


class OSFactory(_OSFactory):
    """
    Factory class returning specialized classes
    """

    def __init__(self):
        raise RuntimeError('Cannot be instantiated, please use OSFactory.get_manager() instead')

    @classmethod
    def _get_configuration(cls):
        return Configuration

    @classmethod
    def _get_system(cls):
        return System
