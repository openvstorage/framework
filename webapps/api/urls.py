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
Django URL module for main API
"""
import os
import imp
import inspect
from django.conf.urls import patterns, include, url
from django.views.generic import RedirectView
from views import ObtainAuthToken
from backend.router import OVSRouter


def build_router_urls(api_mode, docs):
    """
    Creates a router instance to generate API urls for Customer and Internal API
    """
    routes = []
    path = os.path.join(os.path.dirname(__file__), 'backend', 'views')
    for filename in os.listdir(path):
        if os.path.isfile(os.path.join(path, filename)) and filename.endswith('.py'):
            name = filename.replace('.py', '')
            module = imp.load_source(name, os.path.join(path, filename))
            for member in inspect.getmembers(module):
                if inspect.isclass(member[1]) \
                        and member[1].__module__ == name \
                        and 'ViewSet' in [base.__name__ for base in member[1].__bases__]:
                    routes.append({'prefix': member[1].prefix,
                                   'viewset': member[1],
                                   'base_name': member[1].base_name})

    router = OVSRouter(api_mode, docs)
    for route in routes:
        router.register(**route)
    return router.urls

customer_docs = """
The Customer API can be used for integration or automatisation with 3rd party applications.
"""
internal_docs = """
The Internal API is for **internal use only** (used by the Open vStorage framework) and is subject
to continuous changes without warning. It should not be used by 3rd party applications.
*Unauthorized usage of this API can lead to unexpected results, issues or even data loss*. See
the [Customer API](%(customerapi)s).
"""

urlpatterns = patterns('',
    url(r'^auth/',      ObtainAuthToken.as_view()),
    url(r'^api-auth/',  include('rest_framework.urls', namespace='rest_framework')),
    url(r'^customer/',  include(build_router_urls('customer', customer_docs))),
    url(r'^internal/',  include(build_router_urls('internal', internal_docs))),
    url(r'^$',          RedirectView.as_view(url='customer/')),
)
