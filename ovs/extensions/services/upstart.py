# Copyright 2015 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Upstart module
"""

import re
from subprocess import CalledProcessError
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('extensions', name='servicemanager')


class Upstart(object):
    """
    Contains all logic related to Upstart services
    """

    @staticmethod
    def _service_exists(name, client, path):
        if path is None:
            path = '/etc/init/'
        file_to_check = '{0}{1}.conf'.format(path, name)
        return client.file_exists(file_to_check)

    @staticmethod
    def _get_name(name, client, path=None):
        """
        Make sure that for e.g. 'ovs-workers' the given service name can be either 'ovs-workers' as just 'workers'
        """
        if Upstart._service_exists(name, client, path):
            return name
        if client.file_exists('/etc/init.d/{0}'.format(name)):
            return name
        name = 'ovs-{0}'.format(name)
        if Upstart._service_exists(name, client, path):
            return name
        logger.info('Service {0} could not be found.'.format(name))
        raise ValueError('Service {0} could not be found.'.format(name))

    @staticmethod
    def prepare_template(base_name, target_name, client):
        template_name = '/opt/OpenvStorage/config/templates/upstart/{0}.conf'
        if client.file_exists(template_name.format(base_name)):
            client.run('cp -f {0} {1}'.format(
                template_name.format(base_name),
                template_name.format(target_name)
            ))

    @staticmethod
    def add_service(name, client, params=None, target_name=None, additional_dependencies=None):
        if params is None:
            params = {}

        name = Upstart._get_name(name, client, '/opt/OpenvStorage/config/templates/upstart/')
        template_conf = '/opt/OpenvStorage/config/templates/upstart/{0}.conf'

        if not client.file_exists(template_conf.format(name)):
            # Given template doesn't exist so we are probably using system
            # init scripts
            return

        template_file = client.file_read(template_conf.format(name))

        for key, value in params.iteritems():
            template_file = template_file.replace('<{0}>'.format(key), value)
        if '<SERVICE_NAME>' in template_file:
            template_file = template_file.replace('<SERVICE_NAME>', name.lstrip('ovs-'))

        dependencies = ''
        if additional_dependencies:
            for service in additional_dependencies:
                dependencies += '{0} '.format(service)
        template_file = template_file.replace('<ADDITIONAL_DEPENDENCIES>', dependencies)

        if target_name is None:
            client.file_write('/etc/init/{0}.conf'.format(name), template_file)
        else:
            client.file_write('/etc/init/{0}.conf'.format(target_name), template_file)

    @staticmethod
    def get_service_status(name, client):
        try:
            name = Upstart._get_name(name, client)
            output = client.run('service {0} status || true'.format(name))
            # Special cases (especially old SysV ones)
            if 'rabbitmq' in name:
                return re.search('\{pid,\d+?\}', output) is not None
            # Normal cases - or if the above code didn't yield an outcome
            if 'start' in output or 'is running' in output:
                return True
            if 'stop' in output or 'not running' in output:
                return False
            return False
        except CalledProcessError, ex:
            logger.error('Get {0}.service status failed: {1}'.format(name, ex))
            raise Exception('Retrieving status for service "{0}" failed'.format(name))

    @staticmethod
    def remove_service(name, client):
        # remove upstart.conf file
        name = Upstart._get_name(name, client)
        client.file_delete('/etc/init/{0}.conf'.format(name))
        client.file_delete('/etc/init/{0}.override'.format(name))

    @staticmethod
    def disable_service(name, client):
        name = Upstart._get_name(name, client)
        client.run('echo "manual" > /etc/init/{0}.override'.format(name))

    @staticmethod
    def enable_service(name, client):
        name = Upstart._get_name(name, client)
        client.file_delete('/etc/init/{0}.override'.format(name))

    @staticmethod
    def start_service(name, client):
        try:
            name = Upstart._get_name(name, client)
            output = client.run('service {0} start'.format(name))
        except CalledProcessError as cpe:
            output = cpe.output
            logger.error('Start {0} failed, {1}'.format(name, output))
        return output

    @staticmethod
    def stop_service(name, client):
        try:
            name = Upstart._get_name(name, client)
            output = client.run('service {0} stop'.format(name))
        except CalledProcessError as cpe:
            output = cpe.output
            logger.error('Stop {0} failed, {1}'.format(name, output))
        return output

    @staticmethod
    def restart_service(name, client):
        try:
            name = Upstart._get_name(name, client)
            output = client.run('service {0} restart'.format(name))
        except CalledProcessError as cpe:
            output = cpe.output
            logger.error('Restart {0} failed, {1}'.format(name, output))
        return output

    @staticmethod
    def has_service(name, client):
        try:
            Upstart._get_name(name, client)
            return True
        except ValueError:
            return False

    @staticmethod
    def is_enabled(name, client):
        name = Upstart._get_name(name, client)
        if client.file_exists('/etc/init/{0}.override'.format(name)):
            return False
        return True

    @staticmethod
    def get_service_pid(name, client):
        name = Upstart._get_name(name, client)
        if Upstart.get_service_status(name, client):
            output = client.run('service {0} status'.format(name))
            if output:
                # Special cases (especially old SysV ones)
                if 'rabbitmq' in name:
                    match = re.search('\{pid,(?P<pid>\d+?)\}', output)
                else:
                    # Normal cases - or if the above code didn't yield an outcome
                    match = re.search('start/running, process (?P<pid>\d+)', output)
                if match is not None:
                    match_groups = match.groupdict()
                    if 'pid' in match_groups:
                        return match_groups['pid']
        return -1
