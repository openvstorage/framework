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
VPool module
"""

import copy
import json
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs_extensions.api.client import OVSClient
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.lib.helpers.decorators import log
from ovs.lib.helpers.decorators import ovs_task
from ovs.lib.helpers.toolbox import Toolbox


class VPoolController(object):
    """
    Contains all BLL related to VPools
    """
    _logger = Logger('lib')

    @staticmethod
    @ovs_task(name='ovs.vpool.up_and_running')
    @log('VOLUMEDRIVER_TASK')
    def up_and_running(storagedriver_id):
        """
        Volumedriver informs us that the service is completely started. Post-start events can be executed
        :param storagedriver_id: ID of the storagedriver
        """
        storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
        if storagedriver is None:
            raise RuntimeError('A Storage Driver with id {0} could not be found.'.format(storagedriver_id))
        storagedriver.startup_counter += 1
        storagedriver.save()

    # noinspection PyTypeChecker
    @staticmethod
    @ovs_task(name='ovs.storagerouter.create_hprm_config_files')
    def create_hprm_config_files(vpool_guid, local_storagerouter_guid, parameters):
        """
        Create the required configuration files to be able to make use of HPRM (aka PRACC)
        This configuration will be zipped and made available for download
        :param vpool_guid: The guid of the VPool for which a HPRM manager needs to be deployed
        :type vpool_guid: str
        :param local_storagerouter_guid: The guid of the StorageRouter the API was requested on
        :type local_storagerouter_guid: str
        :param parameters: Additional information required for the HPRM configuration files
        :type parameters: dict
        :return: Name of the zipfile containing the configuration files
        :rtype: str
        """
        from ovs.lib.storagerouter import StorageRouterController  # Avoid circular import

        # Validations
        required_params = {'port': (int, {'min': 1, 'max': 65535}),
                           'identifier': (str, Toolbox.regex_vpool)}
        Toolbox.verify_required_params(actual_params=parameters,
                                       required_params=required_params)
        vpool = VPool(vpool_guid)
        identifier = parameters['identifier']
        config_path = None
        local_storagerouter = StorageRouter(local_storagerouter_guid)
        for sd in vpool.storagedrivers:
            if len(sd.alba_proxies) == 0:
                raise ValueError('No ALBA proxies configured for vPool {0} on StorageRouter {1}'.format(vpool.name,
                                                                                                        sd.storagerouter.name))
            config_path = '/ovs/vpools/{0}/proxies/{1}/config/{{0}}'.format(vpool.guid, sd.alba_proxies[0].guid)

        if config_path is None:
            raise ValueError('vPool {0} has not been extended any StorageRouter'.format(vpool.name))
        proxy_cfg = Configuration.get(key=config_path.format('main'))

        cache_info = {}
        arakoons = {}
        cache_types = VPool.CACHES.values()
        if not any(ctype in parameters for ctype in cache_types):
            raise ValueError('At least one cache type should be passed: {0}'.format(', '.join(cache_types)))
        for ctype in cache_types:
            if ctype not in parameters:
                continue
            required_dict = {'read': (bool, None),
                             'write': (bool, None)}
            required_params.update({ctype: (dict, required_dict)})
            Toolbox.verify_required_params(actual_params=parameters,
                                           required_params=required_params)
            read = parameters[ctype]['read']
            write = parameters[ctype]['write']
            if read is False and write is False:
                cache_info[ctype] = ['none']
                continue
            path = parameters[ctype].get('path')
            if path is not None:
                path = path.strip()
                if not path or path.endswith('/.') or '..' in path or '/./' in path:
                    raise ValueError('Invalid path specified')
                required_dict.update({'path': (str, None),
                                      'size': (int, {'min': 1, 'max': 10 * 1024})})
                Toolbox.verify_required_params(actual_params=parameters,
                                               required_params=required_params)
                while '//' in path:
                    path = path.replace('//', '/')
                cache_info[ctype] = ['local', {'path': path,
                                               'max_size': parameters[ctype]['size'] * 1024 ** 3,
                                               'cache_on_read': read,
                                               'cache_on_write': write}]
            else:
                required_dict.update({'backend_info': (dict, {'preset': (str, Toolbox.regex_preset),
                                                              'alba_backend_guid': (str, Toolbox.regex_guid),
                                                              'alba_backend_name': (str, Toolbox.regex_backend)}),
                                      'connection_info': (dict, {'host': (str, Toolbox.regex_ip, False),
                                                                 'port': (int, {'min': 1, 'max': 65535}, False),
                                                                 'client_id': (str, Toolbox.regex_guid, False),
                                                                 'client_secret': (str, None, False)})})
                Toolbox.verify_required_params(actual_params=parameters,
                                               required_params=required_params)
                connection_info = parameters[ctype]['connection_info']
                if connection_info['host']:  # Remote Backend for accelerated Backend
                    alba_backend_guid = parameters[ctype]['backend_info']['alba_backend_guid']
                    ovs_client = OVSClient(ip=connection_info['host'],
                                           port=connection_info['port'],
                                           credentials=(
                                           connection_info['client_id'], connection_info['client_secret']),
                                           version=6)
                    arakoon_config = StorageRouterController._retrieve_alba_arakoon_config(alba_backend_guid=alba_backend_guid,
                                                                                           ovs_client=ovs_client)
                    arakoons[ctype] = ArakoonClusterConfig.convert_config_to(arakoon_config, return_type='INI')
                else:  # Local Backend for accelerated Backend
                    alba_backend_name = parameters[ctype]['backend_info']['alba_backend_name']
                    if Configuration.exists(key='/ovs/arakoon/{0}-abm/config'.format(alba_backend_name),
                                            raw=True) is False:
                        raise ValueError('Arakoon cluster for ALBA Backend {0} could not be retrieved'.format(alba_backend_name))
                    arakoons[ctype] = Configuration.get(key='/ovs/arakoon/{0}-abm/config'.format(alba_backend_name),
                                                        raw=True)
                cache_info[ctype] = ['alba', {'albamgr_cfg_url': '/etc/hprm/{0}/{1}_cache_arakoon.ini'.format(identifier, ctype),
                                              'bucket_strategy': ['1-to-1', {'prefix': vpool.guid,
                                                                             'preset': parameters[ctype]['backend_info']['preset']}],
                                              'manifest_cache_size': proxy_cfg['manifest_cache_size'],
                                              'cache_on_read': read,
                                              'cache_on_write': write}]

        tgz_name = 'hprm_config_files_{0}_{1}.tgz'.format(identifier, vpool.name)
        config = {'ips': ['127.0.0.1'],
                  'port': parameters['port'],
                  'pracc': {'uds_path': '/var/run/hprm/{0}/uds_path'.format(identifier),
                            'max_clients': 1000,
                            'max_read_buf_size': 64 * 1024,  # Buffer size for incoming requests (in bytes)
                            'thread_pool_size': 64},  # Amount of threads
                  'transport': 'tcp',
                  'log_level': 'info',
                  'read_preference': proxy_cfg['read_preference'],
                  'albamgr_cfg_url': '/etc/hprm/{0}/arakoon.ini'.format(identifier),
                  'manifest_cache_size': proxy_cfg['manifest_cache_size']}
        file_contents_map = {}
        for ctype in cache_types:
            if ctype in cache_info:
                config['{0}_cache'.format(ctype)] = cache_info[ctype]
            if ctype in arakoons:
                file_contents_map['/opt/OpenvStorage/config/{0}/{1}_cache_arakoon.ini'.format(identifier, ctype)] = arakoons[ctype]
        file_contents_map.update({'/opt/OpenvStorage/config/{0}/config.json'.format(identifier): json.dumps(config, indent=4),
                                  '/opt/OpenvStorage/config/{0}/arakoon.ini'.format(identifier): Configuration.get(key=config_path.format('abm'), raw=True)})

        local_client = SSHClient(endpoint=local_storagerouter)
        local_client.dir_create(directories='/opt/OpenvStorage/config/{0}'.format(identifier))
        local_client.dir_create(directories='/opt/OpenvStorage/webapps/frontend/downloads')
        for file_name, contents in file_contents_map.iteritems():
            local_client.file_write(contents=contents, filename=file_name)
        local_client.run(command=['tar', '--transform', 's#^config/{0}#{0}#'.format(identifier),
                                  '-czf', '/opt/OpenvStorage/webapps/frontend/downloads/{0}'.format(tgz_name),
                                  'config/{0}'.format(identifier)])
        local_client.dir_delete(directories='/opt/OpenvStorage/config/{0}'.format(identifier))
        return tgz_name

    @staticmethod
    def get_renewed_backend_metadata(vpool_guid, sco_size, metadata=None, new_vpool=False):
        """
        Renews and fills in the metadata from a given vpool
        :param vpool_guid: Guid of the vPool to renew the metadata for
        :param metadata: vPool metadata that could have been modified (Optional, will use vpools metadata when not provided)
        :param new_vpool: Indicates whether the supplied vpool is new
        :param sco_size: Sco size in bytes
        :return: dict with renewed metadata
        """
        from ovs.lib.storagerouter import StorageRouterController  # Avoid circular import

        def _renew_backend_metadata(_vpool_guid, _backend_info, _sco_size, _new_vpool):
            vpool = VPool(_vpool_guid)
            new_backend_info = copy.deepcopy(_backend_info)
            ovs_client = OVSClient(ip=_backend_info['connection_info']['host'],
                                   port=_backend_info['connection_info']['port'],
                                   credentials=(_backend_info['connection_info']['client_id'],
                                                _backend_info['connection_info']['client_secret']),
                                   version=6,
                                   cache_store=VolatileFactory.get_client())
            preset_name = _backend_info['preset']
            alba_backend_guid = _backend_info['alba_backend_guid']
            arakoon_config = StorageRouterController._retrieve_alba_arakoon_config(alba_backend_guid=alba_backend_guid,
                                                                                   ovs_client=ovs_client)
            backend_dict = ovs_client.get('/alba/backends/{0}/'.format(alba_backend_guid),
                                          params={'contents': 'name,usages,presets,backend,remote_stack'})
            preset_info = dict((preset['name'], preset) for preset in backend_dict['presets'])
            print preset_info
            if preset_name not in preset_info:
                raise RuntimeError(
                    'Given preset {0} is not available in backend {1}'.format(preset_name, backend_dict['name']))

            policies = []
            for policy_info in preset_info[preset_name]['policies']:
                policy = json.loads('[{0}]'.format(policy_info.strip('()')))
                policies.append(policy)
            new_backend_info.update({'name': backend_dict['name'],
                                     'preset': preset_name,
                                     'policies': policies,
                                     'sco_size': _sco_size * 1024.0 ** 2 if _new_vpool is True else vpool.configuration['sco_size'] * 1024.0 ** 2,
                                     'frag_size': float(preset_info[preset_name]['fragment_size']),
                                     'total_size': float(backend_dict['usages']['size']),
                                     'backend_guid': backend_dict['backend_guid'],
                                     'alba_backend_guid': alba_backend_guid,
                                     'connection_info': new_backend_info['connection_info'],
                                     'arakoon_config': arakoon_config})
            return new_backend_info

        vpool = VPool(vpool_guid)
        if metadata is None:
            metadata = vpool.metadata
        new_metadata = copy.deepcopy(metadata)
        new_metadata['backend']['backend_info'] = _renew_backend_metadata(vpool_guid, new_metadata['backend']['backend_info'], sco_size, new_vpool)
        # Check for fragment and block cache
        for storagerouter_guid, caching_data in new_metadata['caching_info'].iteritems():
            for cache_type, cache_type_data in caching_data.iteritems():
                if cache_type_data['is_backend'] is True:
                    cache_type_data['backend_info'] = _renew_backend_metadata(vpool_guid, cache_type_data['backend_info'], sco_size, new_vpool)
        return new_metadata

    @staticmethod
    def calculate_read_preferences(vpool_guid, storagerouter_guid, metadata=None):
        """
        Calculates the read preferences for a vpools metadata
        :param vpool_guid: Guid of the vPool to calculate the read preference off
        :param storagerouter_guid: Guid of the Storagerouter to calculate the read preference for
        :param metadata: vPool metadata that could have been modified (Optional, will use vpools metadata when not provided)
        :return: list with all read preferences
        :rtype: list
        """
        def _calculate_read_preference(backend_info, regular_domains):
            read_preferences = []
            alba_backend_guid = backend_info['alba_backend_guid']
            ovs_client = OVSClient(ip=backend_info['connection_info']['host'],
                                   port=backend_info['connection_info']['port'],
                                   credentials=(backend_info['connection_info']['client_id'],
                                                backend_info['connection_info']['client_secret']),
                                   version=6,
                                   cache_store=VolatileFactory.get_client())
            backend_dict = ovs_client.get('/alba/backends/{0}/'.format(alba_backend_guid), params={'contents': 'name,usages,presets,backend,remote_stack'})
            if backend_dict['scaling'] == 'GLOBAL' and backend_info['connection_info']['local'] is True:
                for node_id, value in backend_dict['remote_stack'].iteritems():
                    if value.get('domain') is not None and value['domain']['guid'] in regular_domains:
                        read_preferences.append(node_id)
            return read_preferences

        vpool = VPool(vpool_guid)
        storagerouter = StorageRouter(storagerouter_guid)
        all_read_preferences = []
        if metadata is None:
            metadata = vpool.metadata
        vpool_backend_info = metadata['backend']['backend_info']
        all_read_preferences.extend(_calculate_read_preference(vpool_backend_info, storagerouter.regular_domains))
        # Check for fragment and block cache
        for storagerouter_guid, caching_data in metadata['caching_info'].iteritems():
            for cache_type, cache_type_data in caching_data.iteritems():
                if cache_type_data['is_backend'] is True:
                    all_read_preferences.extend(_calculate_read_preference( cache_type_data['backend_info'], storagerouter.regular_domains))
        return all_read_preferences
