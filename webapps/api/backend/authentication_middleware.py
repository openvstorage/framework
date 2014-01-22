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
Authentication middleware module
"""
from django.contrib.auth import login, logout, authenticate
from toolbox import Toolbox


class AuthenticationMiddleware(object):
    """
    Provides authentication middleware
    """
    def process_view(self, request, view_func, view_args, view_kwargs):
        """
        Processes a view to handle authentication with URL hash
        """
        _ = view_func, view_args, view_kwargs
        user_guid = request.GET.get('user_guid')
        if user_guid is not None:
            if Toolbox.is_uuid(user_guid):
                user = authenticate(user_guid=user_guid)
                if user:
                    login(request, user)
                else:
                    logout(request)
