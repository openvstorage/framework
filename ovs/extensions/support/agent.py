# Copyright 2015 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module for the Support Agent
"""

import json
import time
import requests
from subprocess import check_output
from ConfigParser import RawConfigParser
from ovs.plugin.provider.configuration import Configuration
from ovs.plugin.provider.package import Package
from ovs.log.logHandler import LogHandler


logger = LogHandler('support', name='agent')


class SupportAgent(object):
    """
    Represents the Support client
    """

    def __init__(self):
        """
        Initializes the client
        """
        self._endpoint = Configuration.get('ovs.support.endpoint')
        self._api = Configuration.get('ovs.support.api')
        self._enable_support = int(Configuration.get('ovs.support.enablesupport')) > 0
        self.interval = int(Configuration.get('ovs.support.interval'))
        self._url = 'http://{0}/{1}'.format(self._endpoint, self._api)  # @TODO: Use HTTPS

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
    def _process_task(task, metadata):
        """
        Processes a task
        """
        try:
            logger.debug('Processing: {0}'.format(task))
            if task == 'OPEN_TUNNEL':
                return check_output('autossh {0}@{1} -fNR {2}:127.0.0.1:22'.format(
                    metadata['user'], metadata['endpoint'], metadata['port']
                ), shell=True)
            if task == 'CLOSE_TUNNEL':
                ssh_pid = check_output("ps aux | grep '{0}@{1} -NR  {2}:127.0.0.1:22' | grep -v grep | sed 's/\s\s*/ /g' | cut -d ' ' -f 2 || true".format(
                    metadata['user'], metadata['endpoint'], metadata['port']
                ), shell=True).strip()
                if ssh_pid != '':
                    return check_output('kill {0}'.format(ssh_pid), shell=True)
                else:
                    raise RuntimeError('Could not find ssh process')
            if task == 'UPLOAD_LOGFILES':
                logfile = check_output('ovs collect logs')
                return check_output('scp {0} {1}@{1}:/mnt/logs/{3}.tar.gz -p {4} -o UserKnownHostsFile=/dev/null'.format(
                    logfile, metadata['user'], metadata['endpoint'], metadata['filename'], metadata['port']
                ), shell=True)
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

        data = {'cid': Configuration.get('ovs.support.cid'),
                'nid': Configuration.get('ovs.support.nid'),
                'metadata': {},
                'errors': []}
        try:
            # Versions
            data['metadata']['versions'] = Package.get_versions()
        except Exception, ex:
            data['errors'].append(str(ex))
        try:
            # Service status
            services = check_output("initctl list | grep ovs", shell=True).strip().split('\n')
            servicedata = dict((service.split(' ')[0].strip(), service.split(' ', 1)[1].strip()) for service in services)
            data['metadata']['services'] = servicedata
        except Exception, ex:
            data['errors'].append(str(ex))

        try:
            request = requests.post(self._url, data={'data': json.dumps(data)})
            return_data = request.json()
        except Exception, ex:
            logger.exception('Unexpected error during support call: {0}'.format(ex))
            raise

        if self._enable_support:
            try:
                for task in return_data['tasks']:
                    self._process_task(task['task'], task['metadata'])
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
