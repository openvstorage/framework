# Copyright 2015 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module for the Support Agent
"""

import os
import sys
import json
import time
import base64
import requests
from subprocess import check_output
from ConfigParser import RawConfigParser
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.packages.package import PackageManager
from ovs.log.logHandler import LogHandler


logger = LogHandler.get('support', name='agent')


class SupportAgent(object):
    """
    Represents the Support client
    """

    def __init__(self):
        """
        Initializes the client
        """
        self._enable_support = Configuration.get('ovs.support.enablesupport')
        self.interval = int(Configuration.get('ovs.support.interval'))
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
        data = {'cid': Configuration.get('ovs.support.cid'),
                'nid': Configuration.get('ovs.support.nid'),
                'metadata': {},
                'errors': []}

        try:
            # Versions
            data['metadata']['versions'] = PackageManager.get_versions()
        except Exception, ex:
            data['errors'].append(str(ex))
        try:
            if self.servicemanager == 'upstart':
                services = check_output("initctl list | grep ovs", shell=True).strip().split('\n')
            else:
                services = check_output("systemctl -l | grep ovs", shell=True).strip().split('\n')
            # Service status
            servicedata = dict((service.split(' ')[0].strip(), service.split(' ', 1)[1].strip()) for service in services)
            data['metadata']['services'] = servicedata
        except Exception, ex:
            data['errors'].append(str(ex))
        try:
            # Licensing
            data['metadata']['licenses'] = []
            if os.path.exists('/opt/OpenvStorage/config/licenses'):
                for lic in check_output('cat /opt/OpenvStorage/config/licenses', shell=True).split('\n'):
                    if lic.strip() != '':
                        data['metadata']['licenses'].append(lic.strip())
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
            logger.debug('Processing: {0}'.format(task))
            cid = Configuration.get('ovs.support.cid')
            nid = Configuration.get('ovs.support.nid')

            if task == 'OPEN_TUNNEL':
                if servicemanager == 'upstart':
                    check_output('service openvpn stop', shell=True)
                else:
                    check_output('systemctl stop openvpn@ovs_{0}-{1} || true'.format(cid, nid), shell=True)
                check_output('rm -f /etc/openvpn/ovs_*', shell=True)
                for filename, contents in metadata['files'].iteritems():
                    with open(filename, 'w') as the_file:
                        the_file.write(base64.b64decode(contents))
                if servicemanager == 'upstart':
                    check_output('service openvpn start', shell=True)
                else:
                    check_output('systemctl start openvpn@ovs_{0}-{1}'.format(cid, nid), shell=True)
            elif task == 'CLOSE_TUNNEL':
                if servicemanager == 'upstart':
                    check_output('service openvpn stop', shell=True)
                else:
                    check_output('systemctl stop openvpn@ovs_{0}-{1}'.format(cid, nid), shell=True)
                check_output('rm -f /etc/openvpn/ovs_*', shell=True)
            elif task == 'UPLOAD_LOGFILES':
                logfile = check_output('ovs collect logs', shell=True).strip()
                check_output('mv {0} /tmp/{1}; curl -T /tmp/{1} ftp://{2} --user {3}:{4}; rm -f {0} /tmp/{1}'.format(
                    logfile, metadata['filename'], metadata['endpoint'], metadata['user'], metadata['password']
                ), shell=True)
            else:
                raise RuntimeError('Unknown task')
        except Exception, ex:
            logger.exception('Unexpected error while processing task {0} (data: {1}): {2}'.format(task, json.dumps(metadata), ex))
            raise
        finally:
            logger.debug('Completed')

    def run(self):
        """
        Executes a call
        """
        logger.debug('Processing heartbeat')

        try:
            response = requests.post(self._url,
                                     data={'data': json.dumps(self.get_heartbeat_data())},
                                     headers={'Accept': 'application/json; version=1'})
            if response.status_code != 200:
                raise RuntimeError('Received invalid status code: {0} - {1}'.format(response.status_code, response.text))
            return_data = response.json()
        except Exception, ex:
            logger.exception('Unexpected error during support call: {0}'.format(ex))
            raise

        try:
            # Try to save the timestamp at which we last succefully send the heartbeat data
            from ovs.extensions.generic.system import System
            storagerouter = System.get_my_storagerouter()
            storagerouter.last_heartheat = time.time()
            storagerouter.save()
        except Exception:
            logger.error('Could not save last heartbeat timestamp')
            # Ignore this error, it's not mandatory for the support agent

        if self._enable_support:
            try:
                for task in return_data['tasks']:
                    self._process_task(task['code'], task['metadata'], self.servicemanager)
            except Exception, ex:
                logger.exception('Unexpected error processing tasks: {0}'.format(ex))
                raise
        if 'interval' in return_data:
            interval = return_data['interval']
            if interval != self.interval:
                self.interval = interval
                self._update_config('interval', str(interval))
            self.interval = return_data['interval']


if __name__ == '__main__':
    try:
        if Configuration.get('ovs.support.enabled') is False:
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
