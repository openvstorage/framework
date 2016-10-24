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
Metadata views
"""

import os
import re
import imp
import inspect
from django.views.generic import View
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
from backend.decorators import load
from oauth2.decorators import auto_response
from ovs.dal.lists.backendtypelist import BackendTypeList
from ovs.dal.helpers import HybridRunner, Descriptor
from ovs.dal.relations import RelationMapper
from ovs.extensions.generic.configuration import Configuration
from ovs.log.log_handler import LogHandler


class OpenAPIView(View):
    """
    Implements retrieval of generic metadata about the services
    """
    _logger = LogHandler.get('api', name='openapi')

    @auto_response(beautify=True)
    @load()
    def get(self, request):
        """
        returns OpenAPI specs
        """
        path = request.path
        data = {}
        if re.match('^.*/swagger\.json$', path):
            version = settings.VERSION[-1]
            data = {'swagger': '2.0',
                    'info': {'title': 'Open vStorage',
                             'description': 'The Open vStorage API',
                             'version': str(version)},
                    'basePath': '/api',
                    'schemes': ['https'],
                    'consumes': ['application/json'],
                    'produces': ['application/json; version={0}'.format(version)],
                    'paths': {'/': {'get': {'summary': 'Retrieve API metadata',
                                            'operationId': 'api',
                                            'responses': {'200': {'descirption': 'API metadata',
                                                                  'schema': {'type': 'object',
                                                                             'title': 'APIMetadata',
                                                                             'properties': {'authenticated': {'type': 'boolean',
                                                                                                              'description': 'Indicates whether the client is authenticated'},
                                                                                            'authentication_state': {'type': 'string',
                                                                                                                     'description': 'Povides more information on the "authenticated" state of a client',
                                                                                                                     'enum': ['unauthenticated',
                                                                                                                              'invalid_authorization_type',
                                                                                                                              'invalid_token',
                                                                                                                              'token_expired',
                                                                                                                              'inactive_user',
                                                                                                                              'authenticated',
                                                                                                                              'unexpected_exception']},
                                                                                            'authentication_metadata': {'type': 'object',
                                                                                                                        'title': 'AuthenticationMetadata',
                                                                                                                        'description': 'Contains information on the usage of an optional 3rd party OAuth2.0 authentication service',
                                                                                                                        'properties': {'ip': {'type': 'string',
                                                                                                                                              'description': 'The IP address of the current node'},
                                                                                                                                       'mode': {'type': 'string',
                                                                                                                                                'description': 'Indicates wheter the "local" or a "remote" authentication endpoint should be used',
                                                                                                                                                'enum': ['local',
                                                                                                                                                         'remote']},
                                                                                                                                       'authorize_uri': {'type': 'string',
                                                                                                                                                         'description': 'The URI to which the user has to be redirect to authenticate'},
                                                                                                                                       'client_id': {'type': 'string',
                                                                                                                                                     'description': 'The client identifier to be used when authenticating'},
                                                                                                                                       'scope': {'type': 'string',
                                                                                                                                                 'description': 'The scope that has to be requested to the authentication endpoint'}},
                                                                                                                        'required': []},
                                                                                            'username': {'type': 'string',
                                                                                                         'description': 'The username of the client or null if not available'},
                                                                                            'userguid': {'type': 'string',
                                                                                                         'description': 'The GUID (primary key) of the client\'s user or null if not available'},
                                                                                            'roles': {'type': 'array',
                                                                                                      'description': 'An array of the scopes that were granted to the client',
                                                                                                      'items': {'type': 'string'}},
                                                                                            'identification': {'type': 'object',
                                                                                                               'title': 'APIIdentification',
                                                                                                               'description': 'Contains identification information about the API/environment',
                                                                                                               'properties': {'cluster_id': {'type': 'string',
                                                                                                                                             'description': 'Environment identification string'}},
                                                                                                               'required': ['cluster_id']},
                                                                                            'storagerouter_ips': {'type': 'array',
                                                                                                                  'description': 'An array containing the IP addresses of all StorageRouters in the environment',
                                                                                                                  'items': {'type': 'string'}},
                                                                                            'versions': {'type': 'array',
                                                                                                         'description': 'An array of all versions that this instance of the API supports',
                                                                                                         'items': {'type': 'integer'}},
                                                                                            'plugins': {}},
                                                                             'required': ['authenticated',
                                                                                          'authentication_state',
                                                                                          'authentication_metadata',
                                                                                          'username',
                                                                                          'userguid',
                                                                                          'roles',
                                                                                          'identification',
                                                                                          'storagerouter_ips',
                                                                                          'versions',
                                                                                          'plugins']}}}}}},
                    'definitions': {},
                    'securityDefinitions': {'oauth2': {'type': 'oauth2',
                                                       'flow': 'password',
                                                       'tokenUrl': 'oauth2/token',
                                                       'scopes': {'read': 'Read access',
                                                                  'write': 'Write access',
                                                                  'manage': 'Management access'}}},
                    'security': [{'oauth2': ['read', 'write', 'manage']}]}

            # Plugin information
            plugins = {}
            for backend_type in BackendTypeList.get_backend_types():
                if backend_type.has_plugin is True:
                    if backend_type.code not in plugins:
                        plugins[backend_type.code] = []
                    plugins[backend_type.code] += ['backend', 'gui']
            generic_plugins = Configuration.get('/ovs/framework/plugins/installed|generic')
            for plugin_name in generic_plugins:
                if plugin_name not in plugins:
                    plugins[plugin_name] = []
                plugins[plugin_name] += ['gui']

            data['paths']['/']['get']['responses']['200']['schema']['properties']['plugins'] = {
                'type': 'object',
                'title': 'PluginMetadata',
                'description': 'Contains information about plugins active in the system. Each property represents a plugin and the area where they provide functionality',
                'properties': {plugin: {'type': 'array',
                                        'description': 'An array of all areas the plugin provides functionality',
                                        'items': {'type': 'string'}} for (plugin, info) in plugins.iteritems()},
                'required': []
            }

            # API paths
            def load_parameters(_fun):
                mandatory_args = _fun.ovs_metadata['load']['mandatory']
                optional_args = _fun.ovs_metadata['load']['optional']
                object_type = _fun.ovs_metadata['load']['object_type']
                entries = ['version', 'request', 'local_storagerouter']
                if object_type is not None:
                    entries.append(object_type.__name__.lower())
                for entry in entries:
                    if entry in mandatory_args:
                        mandatory_args.remove(entry)
                    if entry in optional_args:
                        optional_args.remove(entry)
                docs = _fun.__doc__
                doc_info = {}
                if docs is not None:
                    for match in re.finditer(':(param|type) (.*?): (.*)', docs, re.MULTILINE):
                        entries = match.groups()
                        if entries[1] not in doc_info:
                            doc_info[entries[1]] = {}
                        doc_info[entries[1]][entries[0]] = entries[2]
                parameter_info = []
                for argument in mandatory_args + optional_args:
                    info = {'name': argument,
                            'in': 'query',
                            'required': argument in mandatory_args,
                            'type': 'string'}
                    if argument in doc_info:
                        description = doc_info[argument].get('param')
                        if description:
                            info['description'] = description
                        type_info = doc_info[argument].get('type')
                        if type_info:
                            if type_info in ['int', 'long']:
                                info['type'] = 'integer'
                            elif type_info in ['float']:
                                info['type'] = 'number'
                            elif type_info in ['str', 'basestring', 'unicode']:
                                info['type'] = 'string'
                    parameter_info.append(info)
                return parameter_info

            paths = data['paths']
            path = '/'.join([os.path.dirname(__file__), 'backend', 'views'])
            for filename in os.listdir(path):
                if os.path.isfile('/'.join([path, filename])) and filename.endswith('.py'):
                    name = filename.replace('.py', '')
                    module = imp.load_source(name, '/'.join([path, filename]))
                    for member in inspect.getmembers(module):
                        if inspect.isclass(member[1]) \
                                and member[1].__module__ == name \
                                and 'ViewSet' in [base.__name__ for base in member[1].__bases__]:
                            cls = member[1]
                            if hasattr(cls, 'list'):
                                fun = cls.list
                                docstring = fun.__doc__.strip().split('\n')[0]
                                parameters = load_parameters(fun)
                                route = {'get': {'summary': docstring,
                                                 'operationId': '{0}.list'.format(member[1].prefix),
                                                 'responses': {'200': {'description': docstring,
                                                                       'schema': {}}}},
                                         'parameters': parameters}
                                paths['/{0}/'.format(member[1].prefix)] = route
                            if hasattr(cls, 'retrieve'):
                                fun = cls.retrieve
                                docstring = fun.__doc__.strip().split('\n')[0]
                                parameters = load_parameters(fun)
                                route = {'get': {'summary': docstring,
                                                 'operationId': '{0}.retrieve'.format(member[1].prefix),
                                                 'responses': {'200': {'description': docstring,
                                                                       'schema': {}}}},
                                         'parameters': [{'name': 'guid',
                                                         'in': 'path',
                                                         'description': 'Identifier of the requested object',
                                                         'required': True,
                                                         'type': 'string'}] + parameters}
                                paths['/{0}/{{guid}}/'.format(member[1].prefix)] = route

            # DataObject / hybrids
            def build_property(prop):
                _docstring = prop.docstring or prop.name
                _docstring = _docstring.replace('None', 'null').replace('True', 'true').replace('False', 'false')
                info = {'description': _docstring}
                if prop.default is not None:
                    info['default'] = prop.default
                if prop.property_type == int:
                    info['type'] = 'integer'
                elif prop.property_type == float:
                    info['type'] = 'number'
                elif prop.property_type == long:
                    info['type'] = 'integer'
                elif prop.property_type == str:
                    info['type'] = 'string'
                elif prop.property_type == bool:
                    info['type'] = 'boolean'
                elif prop.property_type == list:
                    info['type'] = 'array'
                elif prop.property_type == dict:
                    info['type'] = 'object'
                elif prop.property_type == set:
                    info['type'] = 'array'
                elif isinstance(prop.property_type, list):  # enumerator
                    info['type'] = 'string'
                    info['enum'] = prop.property_type
                return info

            def build_relation(_cls, relation):
                itemtype = relation.foreign_type.__name__ if relation.foreign_type is not None else _cls.__name__
                _docstring = '{1} instance identifier{3}. One-to-{0} relation with {1}.{2}'.format(
                    'one' if relation.onetoone is True else 'many',
                    itemtype,
                    ('{0}_guid' if relation.onetoone is True else '{0}_guids').format(relation.foreign_key),
                    '' if relation.mandatory is True else ', null if relation is not set'
                )
                info = {'description': _docstring,
                        'type': 'string'}
                return '{0}_guid'.format(relation.name), info

            def build_dynamic(_cls, dynamic):
                _docstring = dynamic.name
                if hasattr(_cls, '_{0}'.format(dynamic.name)):
                    docs = getattr(_cls, '_{0}'.format(dynamic.name)).__doc__
                    if docs is not None:
                        _docstring = docs.strip().split('\n')[0]
                        _docstring = _docstring.replace('None', 'null').replace('True', 'true').replace('False', 'false')
                _docstring = '{0} (dynamic property, cache timeout: {1}s)'.format(_docstring, dynamic.timeout)
                info = {'description': _docstring,
                        'readOnly': True}
                if dynamic.return_type == int:
                    info['type'] = 'integer'
                elif dynamic.return_type == float:
                    info['type'] = 'number'
                elif dynamic.return_type == long:
                    info['type'] = 'integer'
                elif dynamic.return_type == str:
                    info['type'] = 'string'
                elif dynamic.return_type == bool:
                    info['type'] = 'boolean'
                elif dynamic.return_type == list:
                    info['type'] = 'array'
                elif dynamic.return_type == dict:
                    info['type'] = 'object'
                elif dynamic.return_type == set:
                    info['type'] = 'array'
                elif isinstance(dynamic.return_type, list):  # enumerator
                    info['type'] = 'string'
                    info['enum'] = dynamic.return_type
                return info

            def build_remote_relation(relation):
                key, relation_info = relation
                remote_cls = Descriptor().load(relation_info['class']).get_object()
                _docstring = '{1} instance identifier{3}. One-to-{0} relation with {1}.{2}'.format(
                    'one' if relation_info['list'] is False else 'many',
                    remote_cls.__name__,
                    '{0}_guid'.format(relation_info['key']),
                    '' if relation_info['list'] is False else 's'
                )
                info = {'description': _docstring,
                        'readOnly': True}
                if relation_info['list'] is True:
                    info['type'] = 'array'
                    info['items'] = {'type': 'string'}
                    _name = '{0}_guids'.format(key)
                else:
                    info['type'] = 'string'
                    _name = '{0}_guid'.format(key)
                return _name, info

            def get_properties(_cls):
                properties = {}
                properties.update({prop.name: build_property(prop) for prop in _cls._properties})
                properties.update(dict(build_relation(_cls, relation) for relation in _cls._relations))
                properties.update({dynamic.name: build_dynamic(_cls, dynamic) for dynamic in _cls._dynamics})
                relation_info = RelationMapper.load_foreign_relations(_cls)
                if relation_info is not None:
                    properties.update(dict(build_remote_relation(relation) for relation in relation_info.iteritems()))
                return properties

            def get_required_properties(_cls):
                required = []
                for prop in _cls._properties:
                    if prop.mandatory is True:
                        required.append(prop.name)
                for relation in _cls._relations:
                    if relation.mandatory is True:
                        required.append('{0}_guid'.format(relation.name))
                return required

            definitions = data['definitions']
            definitions['DataObject'] = {'type': 'object',
                                         'title': 'DataObject',
                                         'description': 'Root object inherited by all hybrid objects. Shall not be used directly',
                                         'properties': {'guid': {'type': 'string',
                                                                 'description': 'Identifier of the object'}},
                                         'required': ['guid']}
            hybrid_structure = HybridRunner.get_hybrids()
            for class_descriptor in hybrid_structure.values():
                cls = Descriptor().load(class_descriptor).get_object()
                definitions[cls.__name__] = {'description': cls.__doc__.strip().split('\n')[0],
                                             'allOf': [{'$ref': '#/definitions/DataObject'},
                                                       {'type': 'object',
                                                        'properties': get_properties(cls),
                                                        'required': get_required_properties(cls)}]}
        return data

    @csrf_exempt
    def dispatch(self, request, *args, **kwargs):
        """
        Pass through method to add the CSRF exempt
        """
        return super(OpenAPIView, self).dispatch(request, *args, **kwargs)
