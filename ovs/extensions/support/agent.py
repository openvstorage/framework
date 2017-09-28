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
from subprocess import check_output
from ConfigParser import RawConfigParser
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.system import System
from ovs.extensions.packages.packagefactory import PackageFactory


class SupportAgent(object):
    """
    Represents the Support client
    """
    _logger = Logger('extensions-support')

    def __init__(self):
        """
        Initializes the client
        """
        self._enable_support = Configuration.get('/ovs/framework/support|enablesupport')
        self.interval = Configuration.get('/ovs/framework/support|interval')
        self._url = 'https://monitoring.openvstorage.com/api/support/heartbeat/'
        init_info = check_output('cat /proc/1/comm', shell=True)
        # All service classes used in below code should share the exact same interface!
        if 'init' in init_info:
            version_info = check_output('init --version', shell=True)
            if 'upstart' in version_info:
                self.servicemanager = 'upstart'
            else:
                RuntimeError('There was no known service manager detected in /proc/1/comm')
        elif 'systemd' in init_info:
            self.servicemanager = 'systemd'
        else:
            raise RuntimeError('There was no known service manager detected in /proc/1/comm')

    def get_heartbeat_data(self):
        """
        Returns heartbeat data
        """
        data = {'cid': Configuration.get('/ovs/framework/cluster_id'),
                'nid': System.get_my_machine_id(),
                'metadata': {},
                'errors': []}

        try:
            # Versions
            manager = PackageFactory.get_manager()
            data['metadata']['versions'] = dict((pkg_name, str(version)) for pkg_name, version in manager.get_installed_versions().iteritems())  # Fallback to check_output
        except Exception, ex:
            data['errors'].append(str(ex))
        try:
            if self.servicemanager == 'upstart':
                services = check_output('initctl list | grep ovs-', shell=True).strip().splitlines()
            else:
                services = check_output('systemctl -l | grep ovs- | tr -s " "', shell=True).strip().splitlines()
            # Service status
            service_data = {}
            for service in services:
                split = service.strip().split(' ')
                split = [part.strip() for part in split if part.strip()]
                while split and not split[0].strip().startswith('ovs-'):
                    split.pop(0)
                service_data[split[0]] = ' '.join(split[1:])
            data['metadata']['services'] = service_data
        except Exception, ex:
            data['errors'].append(str(ex))
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

    @staticmethod
    def _process_task(task, metadata, servicemanager):
        """
        Processes a task
        """
        try:
            SupportAgent._logger.debug('Processing: {0}'.format(task))
            cid = Configuration.get('/ovs/framework/cluster_id').replace(r"'", r"'\''")
            nid = System.get_my_machine_id().replace(r"'", r"'\''")

            if task == 'OPEN_TUNNEL':
                if servicemanager == 'upstart':
                    check_output('service openvpn stop', shell=True)
                else:
                    check_output("systemctl stop 'openvpn@ovs_{0}-{1}' || true".format(cid, nid), shell=True)
                check_output('rm -f /etc/openvpn/ovs_*', shell=True)
                for filename, contents in metadata['files'].iteritems():
                    with open(filename, 'w') as the_file:
                        the_file.write(base64.b64decode(contents))
                if servicemanager == 'upstart':
                    check_output('service openvpn start', shell=True)
                else:
                    check_output("systemctl start 'openvpn@ovs_{0}-{1}'".format(cid, nid), shell=True)
            elif task == 'CLOSE_TUNNEL':
                if servicemanager == 'upstart':
                    check_output('service openvpn stop', shell=True)
                else:
                    check_output("systemctl stop 'openvpn@ovs_{0}-{1}'".format(cid, nid), shell=True)
                check_output('rm -f /etc/openvpn/ovs_*', shell=True)
            elif task == 'UPLOAD_LOGFILES':
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
        except Exception, ex:
            SupportAgent._logger.exception('Unexpected error while processing task {0} (data: {1}): {2}'.format(task, json.dumps(metadata), ex))
            raise
        finally:
            SupportAgent._logger.debug('Completed')

    def run(self):
        """
        Executes a call
        """
        SupportAgent._logger.debug('Processing heartbeat')

        try:
            response = requests.post(self._url,
                                     data={'data': json.dumps(self.get_heartbeat_data())},
                                     headers={'Accept': 'application/json; version=1'})
            if response.status_code != 200:
                raise RuntimeError('Received invalid status code: {0} - {1}'.format(response.status_code, response.text))
            return_data = response.json()
        except Exception, ex:
            SupportAgent._logger.exception('Unexpected error during support call: {0}'.format(ex))
            raise

        try:
            # Try to save the timestamp at which we last successfully send the heartbeat data
            from ovs.extensions.generic.system import System
            storagerouter = System.get_my_storagerouter()
            storagerouter.last_heartbeat = time.time()
            storagerouter.save()
        except Exception:
            SupportAgent._logger.error('Could not save last heartbeat timestamp')
            # Ignore this error, it's not mandatory for the support agent

        if self._enable_support:
            try:
                for task in return_data['tasks']:
                    self._process_task(task['code'], task['metadata'], self.servicemanager)
            except Exception, ex:
                SupportAgent._logger.exception('Unexpected error processing tasks: {0}'.format(ex))
                raise
        if 'interval' in return_data:
            interval = return_data['interval']
            if interval != self.interval:
                self.interval = interval
                self._update_config('interval', str(interval))
            self.interval = return_data['interval']


if __name__ == '__main__':
    logger = Logger('extensions-support')
    try:
        if Configuration.get('/ovs/framework/support|enabled') is False:
            print 'Support not enabled'
            sys.exit(0)
        logger.info('Starting up')
        client = SupportAgent()
        while True:
            try:
                client.run()
                time.sleep(client.interval)
            except KeyboardInterrupt:
                raise
            except Exception, exception:
                logger.exception('Unexpected error during run: {0}'.format(exception))
                time.sleep(10)
    except KeyboardInterrupt:
        print 'Aborting...'
        logger.info('Stopping (keyboard interrupt)')
