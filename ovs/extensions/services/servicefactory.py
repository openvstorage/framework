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
Service Factory for the OVS Framework
"""
import logging
from ovs_extensions.services.servicefactory import ServiceFactory as _ServiceFactory

logger = logging.getLogger(__name__)


class ServiceFactory(_ServiceFactory):
    """
    Service Factory for the OVS Framework
    """
    RUN_FILE_DIR = '/opt/OpenvStorage/run'
    CONFIG_TEMPLATE_DIR = '/opt/OpenvStorage/config/templates/{0}'

    def __init__(self):
        """Init method"""
        raise Exception('This class cannot be instantiated')
