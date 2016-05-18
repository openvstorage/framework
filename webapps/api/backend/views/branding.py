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
Contains the BrandingViewSet
"""

from rest_framework import viewsets
from ovs.dal.lists.brandinglist import BrandingList
from ovs.dal.hybrids.branding import Branding
from backend.decorators import return_object, return_list, load, limit, log


class BrandingViewSet(viewsets.ViewSet):
    """
    Information about branding
    """
    prefix = r'branding'
    base_name = 'branding'

    @log()
    @limit(amount=60, per=60, timeout=60)
    @return_list(Branding)
    @load()
    def list(self):
        """
        Overview of all brandings
        """
        return BrandingList.get_brandings()

    @log()
    @limit(amount=60, per=60, timeout=60)
    @return_object(Branding)
    @load(Branding)
    def retrieve(self, branding):
        """
        Load information about a given branding
        """
        return branding
