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
Upstart module
"""

from subprocess import CalledProcessError


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
        name = 'ovs-{0}'.format(name)
        if Upstart._service_exists(name, client, path):
            return name
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
    def add_service(name, client, params=None):
        if params is None:
            params = {}

        name = Upstart._get_name(name, client, '/opt/OpenvStorage/config/templates/upstart/')
        template_dir = '/opt/OpenvStorage/config/templates/upstart/{0}'
        upstart_dir = '/etc/init/{0}'
        upstart_conf = '{0}.conf'.format(name)
        template_conf = client.file_read(template_dir.format(upstart_conf))

        for key, value in params.iteritems():
            template_conf = template_conf.replace('<{0}>'.format(key), value)
        if '<SERVICE_NAME>' in template_conf:
            template_conf = template_conf.replace('<SERVICE_NAME>', name.lstrip('ovs-'))

        client.file_write(upstart_dir.format(upstart_conf), template_conf)

    @staticmethod
    def get_service_status(name, client):
        try:
            name = Upstart._get_name(name, client)
            output = client.run('status {0}'.format(name))
            if 'start' in output:
                return True
            if 'stop' in output:
                return False
            return False
        except CalledProcessError:
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
            output = client.run('start {0}'.format(name))
        except CalledProcessError as cpe:
            output = cpe.output
        return output

    @staticmethod
    def stop_service(name, client):
        try:
            name = Upstart._get_name(name, client)
            output = client.run('stop {0}'.format(name))
        except CalledProcessError as cpe:
            output = cpe.output
        return output

    @staticmethod
    def restart_service(name, client):
        try:
            name = Upstart._get_name(name, client)
            output = client.run('restart {0}'.format(name))
        except CalledProcessError as cpe:
            output = cpe.output
        return output

    @staticmethod
    def has_service(name, client):
        try:
            Upstart._get_name(name, client)
            return True
        except ValueError:
            return False
