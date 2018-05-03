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
from inspect import getmembers
from collections import namedtuple
from django.conf.urls import patterns, include, url
from django.core.exceptions import ImproperlyConfigured
from rest_framework.routers import SimpleRouter, flatten
from api.oauth2.tokenview import OAuth2TokenView
from api.oauth2.redirectview import OAuth2RedirectView
from api.openapi import OpenAPIView
from api.view import MetadataView, relay
from api.backend.views.storagerouters import StorageRouterViewSet
from ovs.extensions.generic.logger import Logger


logger = Logger('url')


Route = namedtuple('Route', ['url', 'mapping', 'name', 'detail', 'initkwargs'])
DynamicRoute = namedtuple('DynamicRoute', ['url', 'name', 'detail', 'initkwargs'])


def escape_curly_brackets(url_path):
    """
    Double brackets in regex of url_path for escape string formatting
    """
    if ('{' and '}') in url_path:
        url_path = url_path.replace('{', '{{').replace('}', '}}')
    return url_path


class OVSRouter(SimpleRouter):
    """
    Extended SimpleRouter
    Brings in feature from RestFramework 3.8.2 - to enable generic routes
    Adjusted a tiny bit to enable it in RestFramework 2.3.12

    This might break certain URL resolving using the Django Framework like reversing of URLs (not used in OVS)
    """
    routes = [
        # List route.
        Route(
            url=r'^{prefix}{trailing_slash}$',
            mapping={
                'get': 'list',
                'post': 'create'
            },
            name='{basename}-list',
            detail=False,
            initkwargs={'suffix': 'List'}
        ),
        # Dynamically generated list routes. Generated using
        # @action(detail=False) decorator on methods of the viewset.
        DynamicRoute(
            url=r'^{prefix}/{url_path}{trailing_slash}$',
            name='{basename}-{url_name}',
            detail=False,
            initkwargs={}
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
            name='{basename}-detail',
            detail=True,
            initkwargs={'suffix': 'Instance'}
        ),
        # Dynamically generated detail routes. Generated using
        # @action(detail=True) decorator on methods of the viewset.
        DynamicRoute(
            url=r'^{prefix}/{lookup}/{url_path}{trailing_slash}$',
            name='{basename}-{url_name}',
            detail=True,
            initkwargs={}
        ),
    ]

    def __init__(self, trailing_slash=True):
        self.trailing_slash = '/' if trailing_slash else ''
        super(SimpleRouter, self).__init__()

    def get_default_base_name(self, viewset):
        """
        If `base_name` is not specified, attempt to automatically determine
        it from the viewset.
        """
        queryset = getattr(viewset, 'queryset', None)

        assert queryset is not None, '`base_name` argument not specified, and could ' \
            'not automatically determine the name from the viewset, as ' \
            'it does not have a `.queryset` attribute.'

        return queryset.model._meta.object_name.lower()

    def get_routes(self, viewset):
        """
        Augment `self.routes` with any dynamically generated routes.

        Returns a list of the Route namedtuple.
        """

        def get_extra_actions(cls):
            """
            Get the methods that are marked as an extra ViewSet `@action`.
            """
            return [method for _, method in getmembers(cls, lambda attr: hasattr(attr, 'bind_to_methods'))]

        def get_detail(action):
            """
            Return the possible detail attr of an action
            Used for backwards compatibility between decorator of 3.8.2 and 2.8.12
            """
            if hasattr(action, 'detail'):
                return action.detail
            return None

        # converting to list as iterables are good for one pass, known host needs to be checked again and again for
        # different functions.
        known_actions = list(flatten([route.mapping.values() for route in self.routes if isinstance(route, Route)]))
        extra_actions = get_extra_actions(viewset)
        # checking action names against the known actions list
        not_allowed = [
            action.__name__ for action in extra_actions
            if action.__name__ in known_actions
        ]
        if not_allowed:
            msg = ('Cannot use the @action decorator on the following '
                   'methods, as they are existing routes: %s')
            raise ImproperlyConfigured(msg % ', '.join(not_allowed))

        # partition detail and list actions
        detail_actions = []
        list_actions = []
        for action in extra_actions:
            if get_detail(action):
                detail_actions.append(action)
            else:
                list_actions.append(action)

        routes = []
        for route in self.routes:
            if isinstance(route, DynamicRoute) and route.detail:
                routes += [self._get_dynamic_route(route, action) for action in detail_actions]
            elif isinstance(route, DynamicRoute) and not route.detail:
                routes += [self._get_dynamic_route(route, action) for action in list_actions]
            else:
                routes.append(route)

        return routes

    def _get_dynamic_route(self, route, action):
        def get_url():
            """
            Retrieve the name for the route
            Used for backwards compatibility between decorator of 3.8.2 and 2.8.12
            """
            replace = escape_curly_brackets(action.__name__)
            if hasattr(action, 'url_path'):
                replace = escape_curly_brackets(action.url_path)
            return route.url.replace('{url_path}', replace)

        def get_route_name():
            """
            Retrieve the name for the route
            Used for backwards compatibility between decorator of 3.8.2 and 2.8.12
            """
            replace = escape_curly_brackets(action.__name__)
            if hasattr(action, 'url_name'):
                replace = action.url_name
            return route.name.replace('{url_name}', replace)

        initkwargs = route.initkwargs.copy()
        initkwargs.update(action.kwargs)

        return Route(
            url=get_url(),
            mapping={http_method: action.__name__
                     for http_method in action.bind_to_methods},
            name=get_route_name(),
            detail=route.detail,
            initkwargs=initkwargs,
        )

    def get_method_map(self, viewset, method_map):
        """
        Given a viewset, and a mapping of http methods to actions,
        return a new mapping which only includes any mappings that
        are actually implemented by the viewset.
        """
        bound_methods = {}
        for method, action in method_map.items():
            if hasattr(viewset, action):
                bound_methods[method] = action
        return bound_methods

    def get_lookup_regex(self, viewset, lookup_prefix=''):
        """
        Given a viewset, return the portion of URL regex that is used
        to match against a single instance.

        Note that lookup_prefix is not used directly inside REST rest_framework
        itself, but is required in order to nicely support nested router
        implementations, such as drf-nested-routers.

        https://github.com/alanjds/drf-nested-routers
        """
        base_regex = '(?P<{lookup_prefix}{lookup_url_kwarg}>{lookup_value})'
        # Use `pk` as default field, unset set.  Default regex should not
        # consume `.json` style suffixes and should break at '/' boundaries.
        lookup_field = getattr(viewset, 'lookup_field', 'pk')
        lookup_url_kwarg = getattr(viewset, 'lookup_url_kwarg', None) or lookup_field
        lookup_value = getattr(viewset, 'lookup_value_regex', '[^/.]+')
        return base_regex.format(
            lookup_prefix=lookup_prefix,
            lookup_url_kwarg=lookup_url_kwarg,
            lookup_value=lookup_value
        )

    def get_urls(self):
        """
        Use the registered viewsets to generate a list of URL patterns.
        """
        ret = []

        for prefix, viewset, basename in self.registry:
            lookup = self.get_lookup_regex(viewset)
            routes = self.get_routes(viewset)

            for route in routes:

                # Only actions which actually exist on the viewset will be bound
                mapping = self.get_method_map(viewset, route.mapping)
                if not mapping:
                    continue

                # Build the url pattern
                regex = route.url.format(prefix=prefix, lookup=lookup, trailing_slash=self.trailing_slash)

                # If there is no prefix, the first part of the url is probably
                #   controlled by project's urls.py and the router is in an app,
                #   so a slash in the beginning will (A) cause Django to give
                #   warnings and (B) generate URLS that will require using '//'.
                if not prefix and regex[:2] == '^/':
                    regex = '^' + regex[2:]

                initkwargs = route.initkwargs.copy()
                initkwargs.update({
                    'basename': basename,
                    'detail': route.detail,
                })

                # Monkey patch required, used for backwards compatibility between decorator of 3.8.2 and 2.8.12
                # The detail initkwarg is reserved for introspecting the viewset type.
                setattr(viewset, 'detail', None)
                # Setting a basename allows a view to reverse its action urls. This
                # value is provided by the router through the initkwargs.
                setattr(viewset, 'basename', None)
                # End monkey patch
                view = viewset.as_view(mapping, **initkwargs)
                name = route.name.format(basename=basename)
                ret.append(url(regex, view, name=name))

        return ret


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
            for member_name, member in inspect.getmembers(mod, predicate=inspect.isclass):
                if member.__module__ == name and 'ViewSet' in [base.__name__ for base in member.__bases__]:
                    routes.append({'prefix': member.prefix,
                                   'viewset': member,
                                   'base_name': member.base_name})
    router = OVSRouter()
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
