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
from api.backend.decorators import load
from api.oauth2.decorators import auto_response
from ovs.dal.lists.backendtypelist import BackendTypeList
from ovs.dal.helpers import HybridRunner, Descriptor
from ovs.dal.relations import RelationMapper
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.system import System


# noinspection PyProtectedMember
class OpenAPIView(View):
    """
    Implements retrieval of generic metadata about the services
    """
    _logger = Logger('api')

    @auto_response(beautify=True)
    @load()
    def get(self):
        """
        returns OpenAPI specs
        """
        version = settings.VERSION[-1]
        data = {'swagger': '2.0',
                'info': {'title': 'Open vStorage',
                         'description': 'The Open vStorage API.',
                         'version': str(version)},
                'basePath': '/api',
                'schemes': ['https'],
                'consumes': ['application/json'],
                'produces': ['application/json; version={0}'.format(version)],
                'paths': {'/': {'get': {'summary': 'Retrieve API metadata',
                                        'operationId': 'api',
                                        'responses': {'200': {'description': 'API metadata',
                                                              'schema': {'type': 'object',
                                                                         'title': 'APIMetadata',
                                                                         'properties': {'authenticated': {'type': 'boolean',
                                                                                                          'description': 'Indicates whether the client is authenticated.'},
                                                                                        'authentication_state': {'type': 'string',
                                                                                                                 'description': 'Provides more information on the "authenticated" state of a client.',
                                                                                                                 'enum': ['unauthenticated',
                                                                                                                          'invalid_authorization_type',
                                                                                                                          'invalid_token',
                                                                                                                          'token_expired',
                                                                                                                          'inactive_user',
                                                                                                                          'authenticated',
                                                                                                                          'unexpected_exception']},
                                                                                        'authentication_metadata': {'type': 'object',
                                                                                                                    'title': 'AuthenticationMetadata',
                                                                                                                    'description': 'Contains information on the usage of an optional 3rd party OAuth2.0 authentication service.',
                                                                                                                    'properties': {'ip': {'type': 'string',
                                                                                                                                          'description': 'The IP address of the current node.'},
                                                                                                                                   'mode': {'type': 'string',
                                                                                                                                            'description': 'Indicates whether the "local" or a "remote" authentication endpoint should be used.',
                                                                                                                                            'enum': ['local',
                                                                                                                                                     'remote']},
                                                                                                                                   'authorize_uri': {'type': 'string',
                                                                                                                                                     'description': 'The URI to which the user has to be redirect to authenticate.'},
                                                                                                                                   'client_id': {'type': 'string',
                                                                                                                                                 'description': 'The client identifier to be used when authenticating.'},
                                                                                                                                   'scope': {'type': 'string',
                                                                                                                                             'description': 'The scope that has to be requested to the authentication endpoint.'}},
                                                                                                                    'required': []},
                                                                                        'username': {'type': 'string',
                                                                                                     'description': 'The username of the client or null if not available.'},
                                                                                        'userguid': {'type': 'string',
                                                                                                     'description': 'The GUID (primary key) of the client\'s user or null if not available.'},
                                                                                        'roles': {'type': 'array',
                                                                                                  'description': 'An array of the scopes that were granted to the client.',
                                                                                                  'items': {'type': 'string'}},
                                                                                        'identification': {'type': 'object',
                                                                                                           'title': 'APIIdentification',
                                                                                                           'description': 'Contains identification information about the API/environment.',
                                                                                                           'properties': {'cluster_id': {'type': 'string',
                                                                                                                                         'description': 'Environment identification string.'}},
                                                                                                           'required': ['cluster_id']},
                                                                                        'storagerouter_ips': {'type': 'array',
                                                                                                              'description': 'An array containing the IP addresses of all StorageRouters in the environment.',
                                                                                                              'items': {'type': 'string'}},
                                                                                        'versions': {'type': 'array',
                                                                                                     'description': 'An array of all versions that this instance of the API supports.',
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
                'definitions': {'APIError': {'type': 'object',
                                             'properties': {'error': {'type': 'string',
                                                                      'description': 'An error code'},
                                                            'error_description': {'type': 'string',
                                                                                  'description': 'Descriptive error message'}},
                                             'required': ['error', 'error_description']}},
                'securityDefinitions': {'oauth2': {'type': 'oauth2',
                                                   'flow': 'password',
                                                   'tokenUrl': 'https://{0}/api/oauth2/token/'.format(System.get_my_storagerouter().ip),
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
            'description': 'Contains information about plugins active in the system. Each property represents a plugin and the area where they provide functionality.',
            'properties': {plugin: {'type': 'array',
                                    'description': 'An array of all areas the plugin provides functionality.',
                                    'items': {'type': 'string'}} for (plugin, info) in plugins.iteritems()},
            'required': []
        }

        # API paths
        def load_parameters(_fun):
            # Parameters by @load decorators
            parameter_info = []
            mandatory_args = _fun.ovs_metadata['load']['mandatory']
            optional_args = _fun.ovs_metadata['load']['optional']
            object_type = _fun.ovs_metadata['load']['object_type']
            entries = ['version', 'request', 'local_storagerouter', 'pk', 'contents']
            if object_type is not None:
                object_arg = object_type.__name__.lower()
                if object_arg in mandatory_args or object_arg in optional_args:
                    parameter_info.append({'name': 'guid',
                                           'in': 'path',
                                           'description': 'Identifier of the object on which to call is applied.',
                                           'required': True,
                                           'type': 'string'})
                entries.append(object_arg)
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
                        elif type_info in ['bool']:
                            info['type'] = 'boolean'
                        elif type_info in ['str', 'basestring', 'unicode']:
                            info['type'] = 'string'
                        elif type_info in ['dict']:
                            info['type'] = 'object'
                parameter_info.append(info)
            # Parameters by @returns_* decorators
            return_info = _fun.ovs_metadata.get('returns', None)
            if return_info is not None:
                # Extra parameters
                params = return_info['parameters']
                fields = []
                if 'contents' in params or 'sorting' in params:
                    _cls = return_info['object_type']
                    fields = [prop.name for prop in _cls._properties] + \
                             ['{0}_guid'.format(rel.name) for rel in _cls._relations] + \
                             [dynamic.name for dynamic in _cls._dynamics]
                    relation_info = RelationMapper.load_foreign_relations(_cls)
                    if relation_info is not None:
                        fields += [('{0}_guid' if rel_info['list'] is False else '{0}_guids').format(key)
                                   for key, rel_info in relation_info.iteritems()]
                    fields = fields + ['-{0}'.format(field) for field in fields]
                for parameter in params:
                    if parameter == 'contents':
                        parameter_info.append({'name': 'contents',
                                               'in': 'query',
                                               'description': 'Specify the returned contents.',
                                               'required': True,
                                               'collectionFormat': 'csv',
                                               'type': 'array',
                                               'enum': ['_dynamics', '_relations', 'guid'] + fields,
                                               'items': {'type': 'string'}})
                    elif parameter == 'paging':
                        parameter_info.append({'name': 'page',
                                               'in': 'query',
                                               'description': 'Specifies the page to be returned.',
                                               'required': False,
                                               'type': 'integer'})
                        parameter_info.append({'name': 'page_size',
                                               'in': 'query',
                                               'description': 'Specifies the size of a page. Supported values: 10, 25, 50 and 100. Requires "page" to be set.',
                                               'required': False,
                                               'type': 'integer'})
                    elif parameter == 'sorting':
                        parameter_info.append({'name': 'sort',
                                               'in': 'query',
                                               'description': 'Specifies the sorting of the list.',
                                               'required': False,
                                               'default': params[parameter],
                                               'enum': ['guid', '-guid'] + fields,
                                               'type': 'array',
                                               'items': {'type': 'string'}})
            return parameter_info

        def load_response(_fun):
            response_code = '200'
            response_schema = None
            return_info = _fun.ovs_metadata.get('returns', None)
            if return_info is not None:
                return_type, _return_code = return_info['returns']
                if _return_code is not None:
                    response_code = _return_code
                if return_type == 'object':
                    _cls = return_info['object_type']
                    response_schema = {'$ref': '#/definitions/{0}'.format(_cls.__name__)}
                elif return_type == 'list':
                    _cls = return_info['object_type']
                    class_schema = {'$ref': '#/definitions/{0}'.format(_cls.__name__)}
                    fields = [prop.name for prop in _cls._properties] + \
                             ['{0}_guid'.format(rel.name) for rel in _cls._relations] + \
                             [dynamic.name for dynamic in _cls._dynamics]
                    relation_info = RelationMapper.load_foreign_relations(_cls)
                    if relation_info is not None:
                        fields += [('{0}_guid' if rel_info['list'] is False else '{0}_guids').format(key)
                                   for key, rel_info in relation_info.iteritems()]
                    fields = fields + ['-{0}'.format(field) for field in fields]
                    response_schema = {'type': 'object',
                                       'title': 'DataList',
                                       'properties': {'_contents': {'type': 'array',
                                                                    'description': 'Requested contents.',
                                                                    'items': {'type': 'string'},
                                                                    'required': True,
                                                                    'collectionFormat': 'csv',
                                                                    'enum': ['_dynamics', '_relations', 'guid'] + fields},
                                                      '_paging': {'type': 'object',
                                                                  'title': 'PagingMetadata',
                                                                  'properties': {'total_items': {'type': 'integer',
                                                                                                 'description': 'Total items available.'},
                                                                                 'max_page': {'type': 'integer',
                                                                                              'description': 'Last page available.'},
                                                                                 'end_number': {'type': 'integer',
                                                                                                'description': '1-based index of the last item in the current page.'},
                                                                                 'current_page': {'type': 'integer',
                                                                                                  'description': 'Current page number.'},
                                                                                 'page_size': {'type': 'integer',
                                                                                               'description': 'Number of items in the current page.'},
                                                                                 'start_number': {'type': 'integer',
                                                                                                  'description': '1-based index of the first item in the current page'}},
                                                                  'required': ['total_items', 'max_page', 'end_number', 'current_page', 'page_size', 'start_number']},
                                                      '_sorting': {'type': 'array',
                                                                   'description': 'Applied sorting',
                                                                   'items': {'type': 'string'},
                                                                   'required': True,
                                                                   'collectionFormat': 'csv',
                                                                   'enum': ['-guid', 'guid'] + fields},
                                                      'data': {'type': 'array',
                                                               'description': 'List of serialized {0}s.'.format(_cls.__name__),
                                                               'required': True,
                                                               'items': class_schema}},
                                       'required': ['_contents', '_paging', '_sorting', 'data']}
                else:
                    docs = _fun.__doc__
                    doc_info = {}
                    if docs is not None:
                        for match in re.finditer(':(return|rtype): (.*)', docs, re.MULTILINE):
                            entries = match.groups()
                            doc_info[entries[0]] = entries[1]
                    if return_type == 'task':
                        task_return = ''
                        if 'return' in doc_info:
                            task_return = ' The task returns: {0}'.format(doc_info['return'])
                        response_schema = {'type': 'string',
                                           'description': 'A task identifier.{0}'.format(task_return)}
                    elif return_type is None:
                        response_schema = {'type': 'string'}
                        if 'return' in doc_info:
                            response_schema['description'] = doc_info['return']
                        if 'rtype' in doc_info:
                            type_info = doc_info['rtype']
                            if type_info in ['int', 'long']:
                                response_schema['type'] = 'integer'
                            elif type_info in ['float']:
                                response_schema['type'] = 'number'
                            elif type_info in ['bool']:
                                response_schema['type'] = 'boolean'
                            elif type_info in ['str', 'basestring', 'unicode']:
                                response_schema['type'] = 'string'
                            elif type_info in ['dict']:
                                response_schema['type'] = 'object'
                            elif type_info in ['None']:
                                response_schema = None
                                response_code = '204'
            return response_code, response_schema

        paths = data['paths']
        path = '/'.join([os.path.dirname(__file__), 'backend', 'views'])
        for filename in os.listdir(path):
            if os.path.isfile('/'.join([path, filename])) and filename.endswith('.py'):
                name = filename.replace('.py', '')
                mod = imp.load_source(name, '/'.join([path, filename]))
                for member in inspect.getmembers(mod, predicate=inspect.isclass):
                    if member[1].__module__ == name and 'ViewSet' in [base.__name__ for base in member[1].__bases__]:
                        cls = member[1]
                        if hasattr(cls, 'skip_spec') and cls.skip_spec is True:
                            continue
                        base_calls = {'list': ['get', '/{0}/'],
                                      'retrieve': ['get', '/{0}/{{guid}}/'],
                                      'create': ['post', '/{0}/'],
                                      'destroy': ['delete', '/{0}/{{guid}}/'],
                                      'partial_update': ['patch', '/{0}/{{guid}}/']}
                        for call, route_data in base_calls.iteritems():
                            if hasattr(cls, call):
                                fun = getattr(cls, call)
                                docstring = fun.__doc__.strip().split('\n')[0]
                                parameters = load_parameters(fun)
                                return_code, schema = load_response(fun)
                                route = {route_data[0]: {'summary': docstring,
                                                         'operationId': '{0}.{1}'.format(member[1].prefix, call),
                                                         'responses': {return_code: {'description': docstring},
                                                                       'default': {'description': 'Error payload',
                                                                                   'schema': {'$ref': '#/definitions/APIError'}}},
                                                         'parameters': parameters}}
                                if schema is not None:
                                    route[route_data[0]]['responses'][return_code]['schema'] = schema
                                current_path = route_data[1].format(member[1].prefix)
                                if current_path not in paths:
                                    paths[current_path] = {}
                                paths[current_path].update(route)
                        funs = [fun[1] for fun in inspect.getmembers(cls, predicate=inspect.ismethod) if fun[0] not in base_calls.keys()]
                        for fun in funs:
                            if hasattr(fun, 'bind_to_methods'):
                                routes = {}
                                docstring = fun.__doc__.strip().split('\n')[0]
                                parameters = load_parameters(fun)
                                return_code, schema = load_response(fun)
                                name = fun.__name__
                                for verb in fun.bind_to_methods:
                                    routes[verb] = {'summary': docstring,
                                                    'operationId': '{0}.{1}_{2}'.format(member[1].prefix, verb, name),
                                                    'responses': {return_code: {'description': docstring},
                                                                  'default': {'description': 'Error payload',
                                                                              'schema': {'$ref': '#/definitions/APIError'}}},
                                                    'parameters': parameters}
                                    if schema is not None:
                                        routes[verb]['responses'][return_code]['schema'] = schema
                                paths['/{0}/{{guid}}/{1}/'.format(member[1].prefix, name)] = routes

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
            _docstring = '{1} instance identifier{3}. One-to-{0} relation with {1}.{2}.'.format(
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
            _docstring = '{1} instance identifier{3}. One-to-{0} relation with {1}.{2}.'.format(
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
                                     'description': 'Root object inherited by all hybrid objects. Shall not be used directly.',
                                     'properties': {'guid': {'type': 'string',
                                                             'description': 'Identifier of the object.'}},
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
