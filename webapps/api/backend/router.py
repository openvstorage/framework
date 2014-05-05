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
This module contains a custom REST router
"""
from collections import namedtuple
from django.core.urlresolvers import NoReverseMatch
from rest_framework.routers import DefaultRouter
from rest_framework import views
from rest_framework.response import Response
from rest_framework.reverse import reverse
from ovs.log.logHandler import LogHandler

logger = LogHandler('api', name='router')
Route = namedtuple('Route', ['url', 'mapping', 'name', 'initkwargs'])


class Meta(type):
    pass


class OVSRouter(DefaultRouter):
    """
    This Router has functionality to filter only views that are allowed by its mode.
    """

    def __init__(self, mode, docstring):
        """
        Initializes the OVSRouter
        """
        super(OVSRouter, self).__init__()
        self.api_mode = mode
        self.docstring = docstring
        self.root_view_name = '%s-api-root' % self.api_mode
        self.routes = [
            # List route.
            Route(
                url=r'^{prefix}{trailing_slash}$',
                mapping={
                    'get': 'list',
                    'post': 'create'
                },
                name='%s-{basename}-list' % self.api_mode,
                initkwargs={'suffix': 'List'}
            ),
            # Filter route.
            Route(
                url=r'^{prefix}/filter{trailing_slash}$',
                mapping={
                    'post': 'filter'
                },
                name='%s-{basename}-filter' % self.api_mode,
                initkwargs={'suffix': 'Filter'}
            ),
            # Detail route.
            Route(
                url=r'^{prefix}/{lookup}{trailing_slash}$',
                mapping={
                    'get': 'retrieve',
                    'put': 'update',
                    'patch': 'partial_update',
                    'delete': 'destroy'
                },
                name='%s-{basename}-detail' % self.api_mode,
                initkwargs={'suffix': 'Instance'}
            ),
            # Dynamically generated routes.
            # Generated using @action or @link decorators on methods of the viewset.
            Route(
                url=r'^{prefix}/{lookup}/{methodname}{trailing_slash}$',
                mapping={
                    '{httpmethod}': '{methodname}',
                },
                name='%s-{basename}-{methodnamehyphen}' % self.api_mode,
                initkwargs={}
            ),
        ]

    def get_method_map(self, viewset, method_map):
        """
        Overridden method filtering out certain methods based on customer/internal API

        Given a viewset, and a mapping of http methods to actions,
        return a new mapping which only includes any mappings that
        are actually implemented by the viewset.
        """
        bound_methods = {}
        for method, action in method_map.items():
            if hasattr(viewset, action):
                attr = getattr(viewset, action)
                api_mode = getattr(attr, 'api_mode', None)
                if self.api_mode in api_mode:
                    bound_methods[method] = action
                    # To debug the loaded/exposed methods:
                    # logger.debug('[%s] %s.%s' % (self.api_mode, viewset.__name__, action))
        return bound_methods

    def get_api_root_view(self):
        """
        Overridden method to be able to add a docstring to the APIRoot

        Return a view to use as the API root.
        """
        api_root_dict = {}
        list_name = self.routes[0].name
        for prefix, viewset, basename in self.registry:
            api_root_dict[prefix] = list_name.format(basename=basename)

        class APIRoot(views.APIView):
            """
            APIRoot class
            """
            __metaclass__ = Meta
            _ignore_model_permissions = True

            def get(self, request, format=None):
                """
                Default GET view providing an API index overview
                """
                APIRoot.__doc__ = APIRoot.__doc__ \
                    % {'customerapi': reverse('customer-api-root', request=request, format=format)}
                ret = {}
                for key, url_name in api_root_dict.items():
                    try:
                        ret[key] = reverse(url_name, request=request, format=format)
                    except NoReverseMatch:
                        pass
                return Response(ret)

        APIRoot.__doc__ = self.docstring
        APIRoot.__name__ = '%sAPI' % self.api_mode.capitalize()

        return APIRoot.as_view()
