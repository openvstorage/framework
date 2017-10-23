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

    def __init__(self):
        """
        Initializes the client
        """
        self._cluster_id = Configuration.get('/ovs/framework/cluster_id').replace(r"'", r"'\''")
        self._service_type = ServiceFactory.get_service_type()
        self._storagerouter = System.get_my_storagerouter()
        self._package_manager = PackageFactory.get_manager()
        self._service_manager = ServiceFactory.get_manager()
        self._client = SSHClient(endpoint=self._storagerouter)

        self.interval = Configuration.get('/ovs/framework/support|interval', default=60)

    def get_heartbeat_data(self):
        """
        Returns heartbeat data
        """
        errors = []
        versions = collections.OrderedDict()
        services = collections.OrderedDict()

        # Versions
        try:
            for pkg_name, version in self._package_manager.get_installed_versions().iteritems():
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
                'nid': self._storagerouter.machine_id,
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
            node_id = self._storagerouter.machine_id.replace(r"'", r"'\''")
            service_name = 'openvpn@ovs_{0}-{1}'.format(self._cluster_id, node_id)

            if task_code == 'OPEN_TUNNEL':
                if self._service_type == 'upstart':
                    check_output('service openvpn stop', shell=True)
                else:
                    check_output("systemctl stop '{0}' || true".format(service_name), shell=True)
                check_output('rm -f /etc/openvpn/ovs_*', shell=True)
                for filename, contents in metadata['files'].iteritems():
                    with open(filename, 'w') as the_file:
                        the_file.write(base64.b64decode(contents))
                if self._service_type == 'upstart':
                    check_output('service openvpn start', shell=True)
                else:
                    check_output("systemctl start '{0}'".format(service_name), shell=True)
            elif task_code == 'CLOSE_TUNNEL':
                if self._service_type == 'upstart':
                    check_output('service openvpn stop', shell=True)
                else:
                    check_output("systemctl stop '{0}'".format(service_name), shell=True)
                check_output('rm -f /etc/openvpn/ovs_*', shell=True)
            elif task_code == 'UPLOAD_LOGFILES':
                logfile = check_output('ovs collect logs', shell=True).strip()
                check_output("mv '{0}' '/tmp/{1}'; curl -T '/tmp/{1}' 'ftp://{2}' --user '{3}:{4}'; rm -f '{0}' '/tmp/{1}'".format(
                    logfile.replace(r"'", r"'\''"),
                    metadata['filename'].replace(r"'", r"'\''"),
                    metadata['endpoint'].replace(r"'", r"'\''"),
                    metadata['user'].replace(r"'", r"'\''"),
                    metadata['password'].replace(r"'", r"'\''")
                ), shell=True)
            else:
                raise RuntimeError('Unknown task')
        except Exception:
            SupportAgent._logger.exception('Unexpected error while processing task {0} (data: {1})'.format(task_code, json.dumps(metadata)))
            raise
        finally:
            SupportAgent._logger.debug('Completed')

    def run(self):
        """
        Executes a call
        """
        SupportAgent._logger.debug('Processing heartbeat')
        try:
            response = requests.post(url='https://monitoring.openvstorage.com/api/support/heartbeat/',
                                     data={'data': json.dumps(self.get_heartbeat_data())},
                                     headers={'Accept': 'application/json; version=1'})
            if response.status_code != 200:
                raise RuntimeError('Received invalid status code: {0} - {1}'.format(response.status_code, response.text))
            return_data = response.json()
        except Exception:
            SupportAgent._logger.exception('Unexpected error during support call')
            raise

        try:
            # Try to save the timestamp at which we last successfully send the heartbeat data
            self._storagerouter.last_heartbeat = time.time()
            self._storagerouter.save()
        except Exception:
            SupportAgent._logger.exception('Could not save last heartbeat timestamp')
            # Ignore this error, it's not mandatory for the support agent

        if Configuration.get('/ovs/framework/support|remote_access') is True:
            try:
                for task in return_data['tasks']:
                    self._process_task(task['code'], task['metadata'])
            except Exception:
                SupportAgent._logger.exception('Unexpected error processing tasks')
                raise

        if 'interval' in return_data:
            interval = return_data['interval']
            if interval != self.interval:
                self.interval = interval
                self._update_config('interval', str(interval))


if __name__ == '__main__':
    logger = Logger('extensions-support')
    if Configuration.get('/ovs/framework/support|support_agent') is False:
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
