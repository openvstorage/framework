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
Contains the BackendViewSet
"""

from rest_framework import viewsets
from ovs.dal.lists.backendlist import BackendList
from ovs.dal.hybrids.backend import Backend
from backend.decorators import return_object, return_list, load


class BackendViewSet(viewsets.ViewSet):
    """
    Information about backends
    """
    prefix = r'backends'
    base_name = 'backends'

    @return_list(Backend)
    @load()
    def list(self):
        """
        Overview of all backends
        """
        return BackendList.get_backends()

    @return_object(Backend)
    @load(Backend)
    def retrieve(self, backend):
        """
        Load information about a given backend
        """
        return backend
