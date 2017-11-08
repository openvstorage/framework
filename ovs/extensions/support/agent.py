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

import sys
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


class SupportAgent(object):
    """
    Represents the Support client
    """
    _logger = Logger('extensions-support')

    DEFAULT_INTERVAL = 5
    LOCATION_CLUSTER_ID = '/ovs/framework/cluster_id'
    LOCATION_INTERVAL = '/ovs/framework/support|interval'
    LOCATION_SUPPORT_AGENT = '/ovs/framework/support|support_agent'
    LOCATION_REMOTE_ACCESS = '/ovs/framework/support|remote_access'
    CONFIGURATION_MESSAGE = 'Unable to retrieve "{0}" within the configuration Arakoon. Running support agent with the default.'

    def __init__(self):
        """
        Initializes the client
        """
        # Safe calls
        self._node_id = System.get_my_machine_id().replace(r"'", r"'\''")
        self._package_manager = PackageFactory.get_manager()
        self._service_manager = ServiceFactory.get_manager()
        self._service_type = ServiceFactory.get_service_type()

        # Potential failing calls
        try:
            # Might fail when Arakoon would be down
            self._cluster_id = Configuration.get(self.LOCATION_CLUSTER_ID).replace(r"'", r"'\''")
        except:
            self._logger.exception(self.CONFIGURATION_MESSAGE.format(self.LOCATION_CLUSTER_ID))
            # Fall back to the config file
            with open(Configuration.CONFIG_STORE_LOCATION, 'r') as config_file:
                config = json.load(config_file)
                if 'cluster_id' in config:
                    self._cluster_id = config['cluster_id']
                else:
                    raise
        try:
            self.interval = Configuration.get(self.LOCATION_INTERVAL, default=self.DEFAULT_INTERVAL)
        except:
            self._logger.exception(self.CONFIGURATION_MESSAGE.format(self.LOCATION_INTERVAL))
            self.interval = self.DEFAULT_INTERVAL

        self._openvpn_service_name = 'openvpn@ovs_{0}-{1}'.format(self._cluster_id, self._node_id)

        # Calls to look out for. These could still be None when using them
        self._storagerouter = None
        self._client = None
        self._set_storagerouter()
        self._set_client()

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
            except:
                self._logger.exception('Unable to set the storagerouter. Heartbeat will be affected.')
        return self._storagerouter

    def _set_client(self):
        """
        Sets the clients SSHClient if the storagerouter is not None.
        :return: Value for client (either None or the SSHClient object)
        :rtype: NoneType or ovs.extensions.generic.sshclient.SSHClient
        """
        if self._storagerouter is None:
            logger.exception('Unable to build a local client, no storagerouter was found to use.')
            return
        else:
            if self._client is None:
                self._client = SSHClient(endpoint=self._storagerouter)
        return self._client

    def get_heartbeat_data(self):
        """
        Returns heartbeat data
        """
        errors = []
        versions = collections.OrderedDict()
        services = collections.OrderedDict()

        # Check for the existence of the client
        if self._client is None and self._set_client() is None:
            errors.append('Unable to create a local client')
        else:
            # Versions
            try:
                for pkg_name, version in self._package_manager.get_installed_versions(client=self._client).iteritems():
                    versions[pkg_name] = str(version)
            except Exception as ex:
                errors.append(str(ex))

            # Services
            try:
                for service_info in sorted(self._service_manager.list_services(client=self._client, add_status_info=True)):
                    if not service_info.startswith('ovs-'):
                        continue
                    service_name = service_info.split()[0].strip()
                    services[service_name] = ' '.join(service_info.split()[1:])
            except Exception as ex:
                errors.append(str(ex))

        data = {'cid': self._cluster_id,
                'nid': self._node_id,
                'metadata': {'versions': versions,
                             'services': services}}
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
            SupportAgent._logger.debug('Processing: {0}'.format(task_code))
            if task_code == 'OPEN_TUNNEL':
                self.open_tunnel(metadata)
            elif task_code == 'CLOSE_TUNNEL':
                self.close_tunnel(metadata)
            elif task_code == 'UPLOAD_LOGFILES':
                self.upload_files(metadata)
            else:
                raise RuntimeError('Unknown task')
        except Exception:
            SupportAgent._logger.exception('Unexpected error while processing task {0} (data: {1})'.format(task_code, json.dumps(metadata)))
            raise
        finally:
            SupportAgent._logger.debug('Completed')

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
        if self._service_type == 'upstart':
            check_output('service openvpn start', shell=True)
        else:
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
        @TODO: only close the ovs remote tunnel on 14.04 (all tunnels are currently closed)
        :param metadata: Metadata for the task (empty dict for close tunnel)
        :type metadata: dict
        :return: None
        :rtype: NoneType
        """
        _ = metadata
        if self._service_type == 'upstart':
            check_output('service openvpn stop', shell=True)
        else:
            check_output("systemctl stop '{0}'".format(self._openvpn_service_name), shell=True)
        check_output('rm -f /etc/openvpn/ovs_*', shell=True)

    def _send_heartbeat(self):
        """
        Send heart beat to the monitoring server
        :raises RuntimeError when in valid status code is returned by the api
        :return: Returns the response from the server
        Example return: {u'tasks': [{'code': 'OPEN_TUNNEL', 'metadata': {'files': {'/etc/openvpn/ovs_ca.crt': 'CERTIFACTECONTENTS'}}}]}
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
        self._logger.debug('Processing heartbeat')
        try:
            return_data = self._send_heartbeat()
            self._logger.debug('Requested return data: {0}'.format(return_data))
        except Exception:
            SupportAgent._logger.exception('Unexpected error during support call')
            raise

        try:
            # Try to save the timestamp at which we last successfully send the heartbeat data
            if self._storagerouter is None:
                self._set_storagerouter()  # Try to set the storagerouter.
            self._storagerouter.last_heartbeat = time.time()
            self._storagerouter.save()
        except Exception:
            self._logger.exception('Could not save last heartbeat timestamp')
            # Ignore this error, it's not mandatory for the support agent

        try:
            remote_access_enabled = Configuration.get(self.LOCATION_REMOTE_ACCESS)
        except Exception:
            remote_access_enabled = True
            self._logger.exception(self.CONFIGURATION_MESSAGE.format(self.LOCATION_REMOTE_ACCESS))
        if remote_access_enabled is True:
            try:
                for task in return_data['tasks']:
                    self._process_task(task['code'], task['metadata'])
            except Exception:
                self._logger.exception('Unexpected error processing tasks')
                raise

        # Currently not returned by the monitoring server
        if 'interval' in return_data:
            interval = return_data['interval']
            if interval != self.interval:
                self.interval = interval
                self._update_config('interval', str(interval))


if __name__ == '__main__':
    logger = Logger('extensions-support')
    try:
        support_agent_enabled = Configuration.get(SupportAgent.LOCATION_SUPPORT_AGENT)
    except Exception:
        support_agent_enabled = True
        logger.exception(SupportAgent.CONFIGURATION_MESSAGE.format(SupportAgent.LOCATION_SUPPORT_AGENT))

    if support_agent_enabled is False:
        logger.info('Support not enabled')
        sys.exit(0)

    logger.info('Starting up')
    client = SupportAgent()
    while True:
        try:
            client.run()
            time.sleep(client.interval)
        except KeyboardInterrupt:
            logger.info('Stopping (keyboard interrupt)')
            break
        except Exception:
            logger.exception('Unexpected error during run')
            time.sleep(10)
