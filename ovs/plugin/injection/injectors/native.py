# Copyright 2014 CloudFounders NV
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
Native Injector module
"""
import os
from ConfigParser import RawConfigParser
from subprocess import check_output, CalledProcessError


class Injector(object):
    """
    Injector class, provides all logic to inject. However, the unittest injector
    only provides functionality required in the unittests
    """

    def __init__(self):
        """
        This class should be fully static
        """
        raise RuntimeError('This class should not be instantiated.')

    @staticmethod
    def inject_configuration(provider):
        """ Injects the Config module """
        def _get(key):
            filename, section, item = key.split('.', 2)
            config = RawConfigParser()
            config.read('/opt/OpenvStorage/config/{0}.cfg'.format(filename))
            return config.get(section, item)

        def _set(key, value):
            filename, section, item = key.split('.', 2)
            config = ConfigParser.RawConfigParser()
            config.read('/opt/OpenvStorage/config/{0}.cfg'.format(filename))
            config.set(section, item, value)
            with open('/opt/OpenvStorage/config/{0}.cfg'.format(filename), 'w') as config_file:
                config.write(config_file)

        def _get_int(key):
            return int(_get(key))

        provider.get = staticmethod(_get)
        provider.getInt = staticmethod(_get_int)
        provider.set = staticmethod(_set)
        return provider

    @staticmethod
    def inject_remote(provider):
        """ Injects the remote module """
        class Cuisine:
            import cuisine
            import fabric

            api = cuisine
            fabric = fabric.api

        provider.fabric = Cuisine().fabric
        provider.cuisine = Cuisine()

        return provider

    @staticmethod
    def inject_service(provider):
        """ Injects the Service module """

        def _service_exists(name, path=None):
            if path is None:
                path = '/etc/init/'
            return os.path.exists('{0}{1}.conf'.format(path, name))

        def _get_name(name, path=None):
            if _service_exists(name, path):
                return name
            name = 'ovs-{0}'.format(name)
            if _service_exists(name, path):
                return name
            raise ValueError('Service {0} could not be found.'.format(name))

        def add_service(package, name, command, stop_command, params=None):
            _ = package, command, stop_command
            if params is None:
                params = {}

            name = _get_name(name, '/opt/OpenvStorage/config/templates/upstart/')
            template_dir = '/opt/OpenvStorage/config/templates/upstart/{0}'
            upstart_dir = '/etc/init/{0}'
            upstart_conf = '{0}.conf'.format(name)
            with open(template_dir.format(upstart_conf), 'r') as template_file:
                template_conf = template_file.read()

            for key, value in params.iteritems():
                print 'replacing {0} by {1}'.format(key, value)
                template_conf = template_conf.replace(key, value)

            print '\n\n\n service {0} configfile \n {1}'.format(name, template_conf)
            with open(upstart_dir.format(upstart_conf), 'wb') as upstart_file:
                upstart_file.write(template_conf)

        def get_service_status(name):
            try:
                name = _get_name(name)
                output = check_output('status {0}'.format(name), shell=True)
                if 'start' in output:
                    return True
                if 'stop' in output:
                    return False
            except CalledProcessError:
                pass
            return None

        def remove_service(domain, name):
            _ = domain
            # remove upstart.conf file
            name = _get_name(name)
            check_output('rm -rf /etc/init/{0}.conf'.format(name), shell=True)
            check_output('rm -rf /etc/init/{0}.override'.format(name), shell=True)

        def disable_service(name):
            name = _get_name(name)
            check_output('echo "manual" > /etc/init/{0}.override'.format(name), shell=True)

        def enable_service(name):
            name = _get_name(name)
            check_output('rm -f /etc/init/{0}.override'.format(name), shell=True)

        def start_service(name):
            try:
                name = _get_name(name)
                output = check_output('start {0}'.format(name), shell=True)
            except CalledProcessError as cpe:
                output = cpe.output
            return output

        def stop_service(name):
            try:
                name = _get_name(name)
                output = check_output('stop {0}'.format(name), shell=True)
            except CalledProcessError as cpe:
                output = cpe.output
            return output

        def restart_service(name):
            try:
                name = _get_name(name)
                output = check_output('restart {0}'.format(name), shell=True)
            except CalledProcessError as cpe:
                output = cpe.output
            return output

        def has_service(name):
            try:
                _get_name(name)
                return True
            except ValueError:
                return False

        provider.add_service = staticmethod(add_service)
        provider.remove_service = staticmethod(remove_service)
        provider.get_service_status = staticmethod(get_service_status)
        provider.disable_service = staticmethod(disable_service)
        provider.enable_service = staticmethod(enable_service)
        provider.start_service = staticmethod(start_service)
        provider.stop_service = staticmethod(stop_service)
        provider.restart_service = staticmethod(restart_service)
        provider.has_service = staticmethod(has_service)
        return provider

    @staticmethod
    def inject_process(provider):
        """ Injects the Process module """

        def check_process(name):
            output = check_output('ps aux | grep -v grep | grep {0} || true'.format(name), shell = True)
            # It returns 1 if the process is not running, else it returns 0. Don't ask questions...
            return 1 if name not in output else 0

        provider.checkProcess = staticmethod(check_process)
        return provider

    @staticmethod
    def inject_package(provider):
        """ Injects the Package module """

        def _get_version(package):
            return check_output("aptitude show {0} | grep Version | cut -d ' ' -f 2".format(package), shell=True).strip()

        def get_versions():
            return {'openvstorage': _get_version('openvstorage'),
                    'openvstorage-webapps': _get_version('openvstorage-webapps'),
                    'openvstorage-core': _get_version('openvstorage-core'),
                    'volumedriver': _get_version('volumedriver-server')}

        provider.get_versions = staticmethod(get_versions)
        return provider
