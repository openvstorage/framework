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
Module for the Support Agent
"""

import json
import time
import base64
import requests
import collections
from subprocess import check_output
from ConfigParser import RawConfigParser
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System
from ovs.extensions.packages.packagefactory import PackageFactory
from ovs.extensions.services.servicefactory import ServiceFactory


class ConfigurationNotFoundError(RuntimeError):
    """
    Raised if the config key could not be found
    """
    pass


class SupportAgentCache(object):
    """
    This function tries to refresh the cashed output of a function.
    Refreshing occurs when the cache its timestamp is older then the cache refresh time
    The cache object should look like this:
        CACHE = {'object_name': {'time': 15136942,
                                 'content': 'xyz'}}
    """
    # @TODO Think about clearing the cache when an update has been issued

    logger = Logger('extensions-support')

    CACHE_REFRESH_TIME = 60 * 5

    def __init__(self, support_agent):
        self.cache = {}
        self.support_agent = support_agent

    def try_refresh_object(self, object_name, refresh_time=CACHE_REFRESH_TIME):
        """
        :param object_name: name of the function in SupportAgent to call
        :type object_name: str
        :param refresh_time: time after which the content should be refreshed
        :type refresh_time: int
        :return: cache object[object_name]['content']
        :raises AttributeError function 'object_name' is no function of the SupportAgent
        """
        if object_name not in self.cache:
            self.cache[object_name] = {'time': 0,
                                       'content': None}
        if time.time() - self.cache[object_name]['time'] >= refresh_time:
            self.cache[object_name]['content'] = getattr(self.support_agent, object_name)()
            self.cache[object_name]['time'] = time.time()
            self.logger.debug('Refreshing caching for function {0}. Timestamp {1}'.format(object_name, self.cache[object_name]['time']))
        else:
            self.logger.debug('Returned cached results for function {0}. Timestamp {1}'.format(object_name, self.cache[object_name]['time']))
        return self.cache[object_name]['content']


class SupportAgent(object):
    """
    Represents the Support client
    """
    logger = Logger('extensions-support')

    _MISSING = object()  # Used to check missing properties in function

    DEFAULT_INTERVAL = 60
    DEFAULT_ERROR_DELAY = 10  # Seconds to wait before rerunning loop on errors
    DEFAULT_SUPPORT_ENABLED = False
    DEFAULT_REMOTE_ACCESS_ENABLED = False

    KEY_SUPPORT_AGENT = 'support_agent'
    KEY_REMOTE_ACCESS = 'remote_access'
    KEY_INTERVAL = 'interval'
    FALLBACK_CONFIG = '/opt/OpenvStorage/config/support_agent.json'
    LOCATION_CLUSTER_ID = '/ovs/framework/cluster_id'
    LOCATION_INTERVAL = '/ovs/framework/support|{0}'.format(KEY_INTERVAL)
    LOCATION_SUPPORT_AGENT = '/ovs/framework/support|{0}'.format(KEY_SUPPORT_AGENT)
    LOCATION_REMOTE_ACCESS = '/ovs/framework/support|{0}'.format(KEY_REMOTE_ACCESS)

    def __init__(self):
        """
        Initializes the client
        """
        # Safe calls
        self._node_id = System.get_my_machine_id().replace(r"'", r"'\''")
        self._package_manager = PackageFactory.get_manager()
        self._service_manager = ServiceFactory.get_manager()

        self._service_type = ServiceFactory.get_service_type()
        if self._service_type != 'systemd':
            raise NotImplementedError('Only Systemd is supported')

        # Potential failing calls
        self._cluster_id = self.get_config_key(self.LOCATION_CLUSTER_ID, fallback=[Configuration.CONFIG_STORE_LOCATION, 'cluster_id'])
        self.interval = self.get_config_key(self.LOCATION_INTERVAL, fallback=[self.FALLBACK_CONFIG, self.KEY_INTERVAL], default=self.DEFAULT_INTERVAL)
        self._openvpn_service_name = 'openvpn@ovs_{0}-{1}'.format(self._cluster_id, self._node_id)

        # Calls to look out for. These could still be None when using them
        self._storagerouter = None
        self._client = None
        self._set_storagerouter()
        self._set_client()

        # Safe call, start caching
        self.caching = SupportAgentCache(self)

    @classmethod
    def get_config_key(cls, key, fallback=_MISSING, default=_MISSING):
        """
        Get a certain key from config management
        When fetching from config management fails, it can fallback to a file on the local filesystem
        When the fallback also fails (due to the file being missing or invalid or the fallback key is not present) an optional default can be returned
        :param key: Key to retrieve from the config management
        :param default: Default value to return
        :param fallback: List with the fallback location and optionally a different fallback key
        The fallback location has to be a json file.
        Example: [/opt/OpenvStorage/config/framework.json] will look for the specified key in the framework.json
        Example 2: [/opt/OpenvStorage/config/framework.json, cluster_id_fallback] will look for 'cluster_id_fall' in the file
        Defaults to /opt/OpenvStorage/config/framework.json
        Pass anything other than a list to disable fallback
        :raises ConfigurationNotFoundError: When neither options could retrieve a value
        :return: Value of the requested config
        :rtype: any
        """
        if fallback == cls._MISSING:
            fallback = [cls.FALLBACK_CONFIG]
        default_specified = default != cls._MISSING
        try:
            return Configuration.get(key)
        except Exception:
            cls.logger.error('Unable to retrieve "{0}" within the configuration Arakoon'.format(key))
            if isinstance(fallback, list) and len(fallback) > 0:
                fallback_file = fallback[0]
                fallback_key = fallback[1] if len(fallback) > 1 else None
                cls.logger.warning('Relying on the fallback: file: {0}, key: {1}'.format(fallback_file, fallback_key))
                try:
                    with open(fallback_file, 'r') as the_file:
                        try:
                            config = json.load(the_file)
                            if fallback_key is not None and fallback_key in config:
                                return config[fallback_key]
                            cls.logger.warning('Fallback file "{0}" has no setting for "{1}"'.format(fallback_file, fallback_key))
                        except ValueError:
                            cls.logger.exception('Fallback file "{0}" is not a valid JSON file'.format(fallback_file))
                except IOError:
                    cls.logger.exception('Fallback file "{0}" could not be opened'.format(fallback_file))
            if default_specified is True:
                cls.logger.warning('Relying on the default value ({0}) for "{1}"'.format(default, key))
                return default
            raise ConfigurationNotFoundError('Could not determine any value for "{0}". Exhausted all options'.format(key))

    def _set_storagerouter(self):
        """
        Set the clients storagerouter if the storagerouter is None.
        :return: Value for StorageRouter (either None or the StorageRouter object)
        :rtype: NoneType or ovs.dal.hybrids.storagerouter.StorageRouter
        """
        if self._storagerouter is None:
            try:
                # Will fail when Arakoon is down
                self._storagerouter = System.get_my_storagerouter()
            except Exception:
                self.logger.exception('Unable to set the storagerouter. Heartbeat will be affected.')
        return self._storagerouter

    def _set_client(self):
        """
        Sets the clients SSHClient
        :return: Value for client (either None or the SSHClient object)
        :rtype: NoneType or ovs.extensions.generic.sshclient.SSHClient
        """
        if self._client is None:
            try:
                self._client = SSHClient(endpoint='127.0.0.1')
            except Exception:
                self.logger.exception('Could not instantiate a local client')
        return self._client

    def _get_package_information(self):
        versions_dict = collections.OrderedDict()
        for pkg_name, version in self._package_manager.get_installed_versions(client=self._client).iteritems():
            versions_dict[pkg_name] = str(version)
        return versions_dict

    def _get_version_information(self):
        services = collections.OrderedDict()
        for service_info in sorted(self._service_manager.list_services(client=self._client, add_status_info=True)):
            if not service_info.startswith('ovs-'):
                continue
            service_name = service_info.split()[0].strip()
            services[service_name] = ' '.join(service_info.split()[1:])
        return services

    def get_heartbeat_data(self):
        """
        Returns heartbeat data
        """
        errors = []
        version_info = collections.OrderedDict()
        service_info = collections.OrderedDict()

        # Check for the existence of the client
        if self._client is None and self._set_client() is None:
            errors.append('Unable to create a local client')
        else:
            # Versions
            try:
                version_info = self.caching.try_refresh_object('_get_package_information')
            except Exception as ex:
                errors.append(str(ex))
            # Services
            try:
                service_info = self.caching.try_refresh_object('_get_version_information')
            except Exception as ex:
                errors.append(str(ex))

        data = {'cid': self._cluster_id,
                'nid': self._node_id,
                'metadata': {'versions': version_info,
                             'services': service_info}}
        if len(errors) > 0:
            data['errors'] = errors
        return data

    @staticmethod
    def _update_config(key, value):
        """
        Updates the support configuration
        """
        filename = '/opt/OpenvStorage/config/ovs.cfg'
        config = RawConfigParser()
        config.read(filename)
        config.set('support', key, value)
        with open(filename, 'w') as config_file:
            config.write(config_file)

    def _process_task(self, task_code, metadata):
        """
        Processes a task
        """
        try:
            self.logger.debug('Processing: {0}'.format(task_code))
            if task_code == 'OPEN_TUNNEL':
                self.open_tunnel(metadata)
            elif task_code == 'CLOSE_TUNNEL':
                self.close_tunnel(metadata)
            elif task_code == 'UPLOAD_LOGFILES':
                self.upload_files(metadata)
            else:
                raise RuntimeError('Unknown task')
        except Exception:
            self.logger.exception('Unexpected error while processing task {0} (data: {1})'.format(task_code, json.dumps(metadata)))
            raise
        finally:
            self.logger.debug('Completed')

    def open_tunnel(self, metadata):
        """
        Opens up the openvpn tunnel
        :param metadata: Metadata about the files to use for tunnel
        :type metadata: dict
        Example: {'files': {...}}.
        The files dict will contain file names (keys) and their contents (value) Example: {my_file: my_file_contents}
        These files will always be /etc/openvpn/ovs_ + 'ca.crt', 'ta.key', '{0}.crt'.format(identifier), '{0}.key'.format(identifier), '{0}.conf'.format(identifier)
        with identifier = cluster id + node id
        :return: None
        :rtype: NoneType
        """
        # Close tunnel and update configs
        self.close_tunnel(metadata)
        for filename, contents in metadata['files'].iteritems():
            with open(filename, 'w') as the_file:
                the_file.write(base64.b64decode(contents))
        check_output("systemctl start '{0}'".format(self._openvpn_service_name), shell=True)

    @staticmethod
    def upload_files(metadata):
        """
        Upload the collected log files to a destination
        :param metadata: Metadata about the destination.
        Example: {'filename': name to upload the tarball as, 'endpoint': ip of the ftp, 'user': an ftp user, 'password': ftp users password}
        :type metadata: dict
        :return: None
        :rtype: NoneType
        """
        logfile = check_output('ovs collect logs', shell=True).strip()
        check_output("mv '{0}' '/tmp/{1}'; curl -T '/tmp/{1}' 'ftp://{2}' --user '{3}:{4}'; rm -f '{0}' '/tmp/{1}'".format(
            logfile.replace(r"'", r"'\''"),
            metadata['filename'].replace(r"'", r"'\''"),
            metadata['endpoint'].replace(r"'", r"'\''"),
            metadata['user'].replace(r"'", r"'\''"),
            metadata['password'].replace(r"'", r"'\''")), shell=True)

    def close_tunnel(self, metadata):
        """
        Closes the openvpn tunnel
        :param metadata: Metadata for the task (empty dict for close tunnel)
        :type metadata: dict
        :return: None
        :rtype: NoneType
        """
        _ = metadata
        check_output("systemctl stop '{0}'".format(self._openvpn_service_name), shell=True)
        check_output('rm -f /etc/openvpn/ovs_*', shell=True)

    def _send_heartbeat(self):
        """
        Send heart beat to the monitoring server
        :raises RuntimeError when invalid status code is returned by the api
        :return: Returns the response from the server
        Example return: {u'tasks': [{'code': 'OPEN_TUNNEL', 'metadata': {'files': {'/etc/openvpn/ovs_ca.crt': 'CERTIFACTE_CONTENTS'}}}]}
        The tasks returned from the server are classified by a task code which is one of the following: OPEN_TUNNEL, CLOSE_TUNNEL, UPLOAD_LOGFILES
        - OPEN TUNNEL tasks receive metadata which has a files entry. Example: {'metadata': {'files': {...}}}.
          The files dict will contain file names (keys) and their contents (value) Example: {my_file: my_file_contents}
          These files will always be /etc/openvpn/ovs_ + 'ca.crt', 'ta.key', '{0}.crt'.format(identifier), '{0}.key'.format(identifier), '{0}.conf'.format(identifier)
          with identifier = cluster id + node id
        - CLOSE TUNNEL tasks have no metadata
        - UPLOAD_LOGFILES tasks receive metadata about where to upload tar balled logs
          Example: {'metadata': {'filename': name to upload the tarball as, 'endpoint': ip of the ftp, 'user': an ftp user, 'password': ftp users password}}
        :rtype: dict
        """
        response = requests.post(url='https://monitoring.openvstorage.com/api/support/heartbeat/',
                                 data={'data': json.dumps(self.get_heartbeat_data())},
                                 headers={'Accept': 'application/json; version=1'})
        if response.status_code != 200:
            raise RuntimeError('Received invalid status code: {0} - {1}'.format(response.status_code, response.text))
        return response.json()

    def run(self):
        """
        Executes a call
        """
        self.logger.debug('Processing heartbeat')
        try:
            return_data = self._send_heartbeat()
            self.logger.debug('Requested return data: {0}'.format(return_data))
        except Exception:
            self.logger.exception('Unexpected error during support call')
            raise

        try:
            # Try to save the timestamp at which we last successfully send the heartbeat data
            if self._storagerouter is None:
                self._set_storagerouter()  # Try to set the storagerouter.
            self._storagerouter.last_heartbeat = time.time()
            self._storagerouter.save()
        except Exception:
            self.logger.exception('Could not save last heartbeat timestamp')
            # Ignore this error, it's not mandatory for the support agent

        remote_access_enabled = self.get_config_key(self.LOCATION_REMOTE_ACCESS,
                                                    fallback=[self.FALLBACK_CONFIG, self.KEY_REMOTE_ACCESS],
                                                    default=self.DEFAULT_REMOTE_ACCESS_ENABLED)
        if remote_access_enabled is True:
            try:
                for task in return_data['tasks']:
                    self._process_task(task['code'], task['metadata'])
            except Exception:
                self.logger.exception('Unexpected error processing tasks')
                raise

        # Currently not returned by the monitoring server
        if 'interval' in return_data:
            interval = return_data['interval']
            if interval != self.interval:
                self.interval = interval
                self._update_config('interval', str(interval))

    def main(self):
        """
        Runs the Support Agent main loop
        :return: None
        :rtype: NoneType
        """
        while True:
            try:
                # Reconfiguring the settings using the GUI will restart the service but checking the values within to loop to support non-GUI edits
                support_agent_enabled = SupportAgent.get_config_key(SupportAgent.LOCATION_SUPPORT_AGENT,
                                                                    fallback=[SupportAgent.FALLBACK_CONFIG, 'support_agent'],
                                                                    default=SupportAgent.DEFAULT_SUPPORT_ENABLED)
                if support_agent_enabled is False:
                    SupportAgent.logger.info('Support not enabled. Checking again in {0} seconds'.format(client.interval))
                    time.sleep(self.interval)
                    continue
                self.run()
                time.sleep(self.interval)
            except KeyboardInterrupt:
                self.logger.info('Stopping (Keyboard interrupt received)')
                break
            except Exception:
                SupportAgent.logger.exception('Unexpected error during run. Retrying in {0} seconds.'.format(self.DEFAULT_ERROR_DELAY))
                time.sleep(self.DEFAULT_ERROR_DELAY)


if __name__ == '__main__':
    SupportAgent.logger.info('Starting up')
    client = SupportAgent()
    client.main()
