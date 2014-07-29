# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Contains the BrandingViewSet
"""

from rest_framework import viewsets
from ovs.dal.lists.brandinglist import BrandingList
from ovs.dal.hybrids.branding import Branding
from backend.decorators import expose, return_object, return_list, discover


class BrandingViewSet(viewsets.ViewSet):
    """
    Information about branding
    """
    prefix = r'branding'
    base_name = 'branding'

    @expose(internal=True)
    @return_list(Branding)
    @discover()
    def list(self):
        """
        Overview of all brandings
        """
        return BrandingList.get_brandings()

    @expose(internal=True)
    @return_object(Branding)
    @discover(Branding)
    def retrieve(self, branding):
        """
        Load information about a given branding
        """
        return branding
