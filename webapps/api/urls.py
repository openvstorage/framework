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
Django URL module for main API
"""
import os
import imp
import inspect
from django.conf.urls import patterns, include, url
from rest_framework.routers import SimpleRouter
from api.oauth2.tokenview import OAuth2TokenView
from api.oauth2.redirectview import OAuth2RedirectView
from api.openapi import OpenAPIView
from api.view import MetadataView, relay


def build_router_urls():
    """
    Creates a router instance to generate API urls for Customer and Internal API
    """
    routes = []
    path = '/'.join([os.path.dirname(__file__), 'backend', 'views'])
    for filename in os.listdir(path):
        if os.path.isfile('/'.join([path, filename])) and filename.endswith('.py'):
            name = filename.replace('.py', '')
            mod = imp.load_source(name, '/'.join([path, filename]))
            for member in inspect.getmembers(mod):
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
                       url(r'^oauth2/redirect/', OAuth2RedirectView.as_view()),
                       url(r'^relay/', relay),
                       url(r'^swagger.json', OpenAPIView.as_view()),
                       url(r'^$', MetadataView.as_view()),
                       url(r'', include(build_router_urls())))
