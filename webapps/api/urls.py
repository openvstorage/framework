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
from oauth2.tokenview import OAuth2TokenView
from view import MetadataView
from rest_framework.routers import SimpleRouter


def build_router_urls():
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
    router = SimpleRouter()
    for route in routes:
        router.register(**route)
    return router.urls

urlpatterns = patterns('',
    url(r'^oauth2/token/', OAuth2TokenView.as_view()),
    url(r'^$',             MetadataView.as_view()),
    url(r'',               include(build_router_urls()))
)
