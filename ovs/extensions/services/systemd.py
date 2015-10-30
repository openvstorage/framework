# Copyright 2015 CloudFounders NV
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
Systemd module
"""

from subprocess import CalledProcessError
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('extensions', name='servicemanager')


class Systemd(object):
    """
    Contains all logic related to Systemd services
    """

    @staticmethod
    def _service_exists(name, client, path):
        if path is None:
            path = '/lib/systemd/system/'
        file_to_check = '{0}{1}.service'.format(path, name)
        return client.file_exists(file_to_check)

    @staticmethod
    def _get_name(name, client, path=None):
        """
        Make sure that for e.g. 'ovs-workers' the given service name can be either 'ovs-workers' as just 'workers'
        """
        if Systemd._service_exists(name, client, path):
            return name
        if Systemd._service_exists(name, client, '/lib/systemd/system/'):
            return name
        name = 'ovs-{0}'.format(name)
        if Systemd._service_exists(name, client, path):
            return name
        logger.info('Service {0} could not be found.'.format(name))
        raise ValueError('Service {0} could not be found.'.format(name))

    @staticmethod
    def prepare_template(base_name, target_name, client):
        template_name = '/opt/OpenvStorage/config/templates/systemd/{0}.service'
        if client.file_exists(template_name.format(base_name)):
            client.run('cp -f {0} {1}'.format(
                template_name.format(base_name),
                template_name.format(target_name)
            ))

    @staticmethod
    def add_service(name, client, params=None, target_name=None, additional_dependencies=None):
        if params is None:
            params = {}

        name = Systemd._get_name(name, client, '/opt/OpenvStorage/config/templates/systemd/')
        template_service = '/opt/OpenvStorage/config/templates/systemd/{0}.service'

        if not client.file_exists(template_service.format(name)):
            # Given template doesn't exist so we are probably using system
            # init scripts
            return

        template_file = client.file_read(template_service.format(name))

        for key, value in params.iteritems():
            template_file = template_file.replace('<{0}>'.format(key), value)
        if '<SERVICE_NAME>' in template_file:
            template_file = template_file.replace('<SERVICE_NAME>', name.lstrip('ovs-'))

        dependencies = ''
        if additional_dependencies:
            for service in additional_dependencies:
                dependencies += '{0}.service '.format(service)
        template_file = template_file.replace('<ADDITIONAL_DEPENDENCIES>', dependencies)

        if target_name is None:
            client.file_write('/lib/systemd/system/{0}.service'.format(name), template_file)
        else:
            client.file_write('/lib/systemd/system/{0}.service'.format(target_name), template_file)
            name = target_name

        try:
            client.run('systemctl daemon-reload')
            client.run('systemctl enable {0}.service'.format(name))
        except CalledProcessError as cpe:
            output = cpe.output
            logger.error('Add {0}.service failed, {1}'.format(name, output))
            raise Exception('Add {0}.service failed, {1}'.format(name, output))

    @staticmethod
    def get_service_status(name, client):
        name = Systemd._get_name(name, client)
        output = client.run('systemctl is-active {0} || true'.format(name))
        if 'active' == output:
            return True
        if 'inactive' == output:
            return False
        return False

    @staticmethod
    def remove_service(name, client):
        # remove systemd.service file
        name = Systemd._get_name(name, client)
        client.file_delete('/lib/systemd/system/{0}.service'.format(name))
        client.run('systemctl daemon-reload')

    @staticmethod
    def disable_service(name, client):
        name = Systemd._get_name(name, client)
        try:
            client.run('systemctl disable {0}.service'.format(name))
        except CalledProcessError as cpe:
            output = cpe.output
            logger.error('Disable {0} failed, {1}'.format(name, output))
            raise Exception('Disable {0} failed, {1}'.format(name, output))

    @staticmethod
    def enable_service(name, client):
        name = Systemd._get_name(name, client)
        try:
            client.run('systemctl enable {0}.service'.format(name))
        except CalledProcessError as cpe:
            output = cpe.output
            logger.error('Enable {0} failed, {1}'.format(name, output))
            raise Exception('Enable {0} failed, {1}'.format(name, output))

    @staticmethod
    def start_service(name, client):
        try:
            name = Systemd._get_name(name, client)
            output = client.run('systemctl start {0}.service'.format(name))
        except CalledProcessError as cpe:
            output = cpe.output
            logger.error('Start {0} failed, {1}'.format(name, output))
        return output

    @staticmethod
    def stop_service(name, client):
        try:
            name = Systemd._get_name(name, client)
            output = client.run('systemctl stop {0}.service'.format(name))
        except CalledProcessError as cpe:
            output = cpe.output
            logger.error('Stop {0} failed, {1}'.format(name, output))
        return output

    @staticmethod
    def restart_service(name, client):
        try:
            name = Systemd._get_name(name, client)
            output = client.run('systemctl restart {0}.service'.format(name))
        except CalledProcessError as cpe:
            output = cpe.output
            logger.error('Restart {0} failed, {1}'.format(name, output))
        return output

    @staticmethod
    def has_service(name, client):
        try:
            Systemd._get_name(name, client)
            return True
        except ValueError:
            return False

    @staticmethod
    def is_enabled(name, client):
        name = Systemd._get_name(name, client)
        output = client.run('systemctl is-enabled {0} || true'.format(name))
        if 'enabled' in output:
            return True
        if 'disabled' in output:
            return False
        return False

    @staticmethod
    def get_service_pid(name, client):
        pid = 0
        name = Systemd._get_name(name, client)
        if Systemd.get_service_status(name, client):
            output = client.run('systemctl status {0} || true'.format(name))
            if output:
                output = output.splitlines()
                for line in output:
                    if 'Main PID' in line:
                        pid = line.split(' ')[3]
                        if not pid.isdigit():
                            pid = 0
                        break
        return pid
