# Copyright 2014 iNuron NV
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

from subprocess import check_output, CalledProcessError, PIPE, Popen
from ConfigParser import RawConfigParser
from ovs.log.logHandler import LogHandler
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.helpers import Descriptor

import os
import re
import grp
import pwd
import glob
import json
import time
import types
import logging
import tempfile
import paramiko
import socket

logger = LogHandler.get('extensions', name='sshclient')


def connected():
    """
    Makes sure a call is executed against a connected client if required
    """

    def wrap(f):
        """
        Wrapper function
        """

        def new_function(self, *args, **kwargs):
            """
            Wrapped function
            """
            try:
                if self.client is not None and not self.client.is_connected():
                    self._connect()
                return f(self, *args, **kwargs)
            except AttributeError as ex:
                if "'NoneType' object has no attribute 'open_session'" in str(ex):
                    self._connect()  # Reconnect
                    return f(self, *args, **kwargs)
                raise

        new_function.__name__ = f.__name__
        new_function.__module__ = f.__module__
        return new_function

    return wrap


def is_connected(self):
    """
    Monkey-patch method to check whether the Paramiko client is connected
    """
    return self._transport is not None


class UnableToConnectException(Exception):
    pass


class SSHClient(object):
    """
    Remote/local client
    """

    client_cache = {}
    IP_REGEX = re.compile('^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$')

    def __init__(self, endpoint, username='ovs', password=None):
        """
        Initializes an SSHClient
        """
        storagerouter = None
        if isinstance(endpoint, basestring):
            ip = endpoint
            if not re.findall(SSHClient.IP_REGEX, ip):
                raise ValueError('Incorrect IP {0} specified'.format(ip))
        elif Descriptor.isinstance(endpoint, StorageRouter):
            # Refresh the object before checking its attributes
            storagerouter = StorageRouter(endpoint.guid)
            ip = storagerouter.ip
        else:
            raise ValueError('The endpoint parameter should be either an ip address or a StorageRouter')

        self.ip = ip
        local_ips = check_output("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1", shell=True).strip().splitlines()
        self.local_ips = [ip.strip() for ip in local_ips]
        self.is_local = self.ip in self.local_ips

        if self.is_local is False and storagerouter is not None:
            process_heartbeat = storagerouter.heartbeats.get('process')
            if process_heartbeat is not None:
                if time.time() - process_heartbeat > 300:
                    message = 'StorageRouter {0} process heartbeat > 300s'.format(ip)
                    logger.error(message)
                    raise UnableToConnectException(message)

        current_user = check_output('whoami', shell=True).strip()
        if username is None:
            self.username = current_user
        else:
            self.username = username
            if username != current_user:
                self.is_local = False  # If specified user differs from current executing user, we always use the paramiko SSHClient
        self.password = password

        self.client = None
        if not self.is_local:
            logging.getLogger('paramiko').setLevel(logging.WARNING)
            key = '{0}@{1}'.format(self.ip, self.username)
            if key not in SSHClient.client_cache:
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.is_connected = types.MethodType(is_connected, client)
                SSHClient.client_cache[key] = client
            self.client = SSHClient.client_cache[key]

    def __del__(self):
        """
        Class destructor
        """
        try:
            if not self.is_local:
                self._disconnect()
        except Exception:
            pass  # Absorb destructor exceptions

    def _connect(self):
        """
        Connects to the remote end
        """
        if self.is_local is True:
            return

        try:
            try:
                self.client.connect(self.ip, username=self.username, password=self.password)
            except:
                try:
                    self.client.close()
                except:
                    pass
                raise
        except socket.error as ex:
            if 'No route to host' in str(ex):
                message = 'SocketException: No route to host {0}'.format(self.ip)
                logger.error(message)
                raise UnableToConnectException(message)
            raise

    def _disconnect(self):
        """
        Disconnects from the remote end
        """
        if self.is_local is True:
            return

        self.client.close()

    @staticmethod
    def _shell_safe(path_to_check):
        """Makes sure that the given path/string is escaped and safe for shell"""
        return "".join([("\\" + _) if _ in " '\";`|" else _ for _ in path_to_check])

    @connected()
    def run(self, command, debug=False):
        """
        Executes a shell command
        """
        if self.is_local is True:
            try:
                try:
                    process = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
                except OSError as ose:
                    logger.error('Command: "{0}" failed with output: "{1}"'.format(command, str(ose)))
                    raise CalledProcessError(1, command, str(ose))
                out, err = process.communicate()
                if debug:
                    logger.debug('stdout: {0}'.format(out))
                    logger.debug('stderr: {0}'.format(err))
                    return out.strip(), err
                else:
                    return out.strip()

            except CalledProcessError as cpe:
                logger.error('Command: "{0}" failed with output: "{1}"'.format(command, cpe.output))
                raise cpe
        else:
            _, stdout, stderr = self.client.exec_command(command)  # stdin, stdout, stderr
            exit_code = stdout.channel.recv_exit_status()
            if exit_code != 0:  # Raise same error as check_output
                stderr = ''.join(stderr.readlines())
                stdout = ''.join(stdout.readlines())
                logger.error('Command: "{0}" failed with output "{1}" and error "{2}"'.format(command, stdout, stderr))
                raise CalledProcessError(exit_code, command, stderr)
            if debug:
                return '\n'.join(line.rstrip() for line in stdout).strip(), stderr
            else:
                return '\n'.join(line.rstrip() for line in stdout).strip()

    def dir_create(self, directories):
        """
        Ensures a directory exists on the remote end
        """
        if isinstance(directories, basestring):
            directories = [directories]
        for directory in directories:
            directory = self._shell_safe(directory)
            if self.is_local is True:
                if not os.path.exists(directory):
                    os.makedirs(directory)
            else:
                self.run('mkdir -p "{0}"; echo true'.format(directory))

    def dir_delete(self, directories):
        """
        Remove a directory (or multiple directories) from the remote filesystem recursively
        """
        if isinstance(directories, basestring):
            directories = [directories]
        for directory in directories:
            directory = self._shell_safe(directory)
            if self.is_local is True:
                if os.path.exists(directory):
                    for dirpath, dirnames, filenames in os.walk(directory, topdown=False):
                        for filename in filenames:
                            os.remove(os.path.join(dirpath, filename))
                        for sub_directory in dirnames:
                            os.rmdir(os.path.join(dirpath, sub_directory))
                    os.rmdir(directory)
            else:
                if self.dir_exists(directory):
                    self.run('rm -rf "{0}"'.format(directory))

    def dir_exists(self, directory):
        """
        Checks if a directory exists on a remote host
        """
        if self.is_local is True:
            return os.path.isdir(self._shell_safe(directory))
        else:
            command = """import os, json
print json.dumps(os.path.isdir('{0}'))""".format(self._shell_safe(directory))
            return json.loads(self.run('python -c """{0}"""'.format(command)))

    def dir_chmod(self, directories, mode, recursive=False):
        if not isinstance(mode, int):
            raise ValueError('Mode should be an integer')

        if isinstance(directories, basestring):
            directories = [directories]
        for directory in directories:
            directory = self._shell_safe(directory)
            if self.is_local is True:
                os.chmod(directory, mode)
                if recursive is True:
                    for root, dirs, _ in os.walk(directory):
                        for sub_dir in dirs:
                            os.chmod(os.path.join(root, sub_dir), mode)
            else:
                recursive_str = '-R' if recursive is True else ''
                self.run('chmod {0} {1} {2}'.format(recursive_str, oct(mode), directory))

    def dir_chown(self, directories, user, group, recursive=False):
        all_users = [user_info[0] for user_info in pwd.getpwall()]
        all_groups = [group_info[0] for group_info in grp.getgrall()]

        if user not in all_users:
            raise ValueError('User "{0}" is unknown on the system'.format(user))
        if group not in all_groups:
            raise ValueError('Group "{0}" is unknown on the system'.format(group))

        uid = pwd.getpwnam(user)[2]
        gid = grp.getgrnam(group)[2]
        if isinstance(directories, basestring):
            directories = [directories]
        for directory in directories:
            directory = self._shell_safe(directory)
            if self.is_local is True:
                os.chown(directory, uid, gid)
                if recursive is True:
                    for root, dirs, _ in os.walk(directory):
                        for sub_dir in dirs:
                            os.chown(os.path.join(root, sub_dir), uid, gid)
            else:
                recursive_str = '-R' if recursive is True else ''
                self.run('chown {0} {1}:{2} {3}'.format(recursive_str, user, group, directory))

    def symlink(self, links):
        if self.is_local is True:
            for link_name, source in links.iteritems():
                os.symlink(source, link_name)
        else:
            for link_name, source in links.iteritems():
                self.run('ln -s {0} {1}'.format(self._shell_safe(source), self._shell_safe(link_name)))

    def file_create(self, filenames):
        if isinstance(filenames, basestring):
            filenames = [filenames]
        for filename in filenames:
            if not filename.startswith('/'):
                raise ValueError('Absolute path required for filename {0}'.format(filename))

            filename = self._shell_safe(filename)
            if self.is_local is True:
                if not self.dir_exists(directory=os.path.dirname(filename)):
                    self.dir_create(os.path.dirname(filename))
                if not os.path.exists(filename):
                    open(filename, 'a').close()
            else:
                directory = os.path.dirname(filename)
                self.dir_create(directory)
                self.run('touch {0}'.format(filename))

    def file_delete(self, filenames):
        """
        Remove a file (or multiple files) from the remote filesystem
        """
        if isinstance(filenames, basestring):
            filenames = [filenames]
        for filename in filenames:
            filename = self._shell_safe(filename)
            if self.is_local is True:
                if '*' in filename:
                    for fn in glob.glob(filename):
                        os.remove(fn)
                else:
                    if os.path.isfile(filename):
                        os.remove(filename)
            else:
                if '*' in filename:
                    command = """import glob, json
print json.dumps(glob.glob('{0}'))""".format(filename)
                    for fn in json.loads(self.run('python -c """{0}"""'.format(command))):
                        self.run('rm -f "{0}"'.format(fn))
                else:
                    if self.file_exists(filename):
                        self.run('rm -f "{0}"'.format(filename))

    def file_unlink(self, path):
        if self.file_exists(path):
            self.run("unlink {0}".format(self._shell_safe(path)))

    def file_read(self, filename):
        """
        Load a file from the remote end
        """
        if self.is_local is True:
            with open(filename, 'r') as the_file:
                return the_file.read()
        else:
            return self.run('cat "{0}"'.format(filename))

    @connected()
    def file_write(self, filename, contents, mode='w'):
        """
        Writes into a file to the remote end
        """
        if self.is_local is True:
            with open(filename, mode) as the_file:
                the_file.write(contents)
        else:
            handle, temp_filename = tempfile.mkstemp()
            with open(temp_filename, mode) as the_file:
                the_file.write(contents)
            os.close(handle)
            try:
                sftp = self.client.open_sftp()
                sftp.put(temp_filename, filename)
                sftp.close()
            finally:
                os.remove(temp_filename)

    @connected()
    def file_upload(self, remote_filename, local_filename):
        """
        Uploads a file to a remote end
        """
        if self.is_local is True:
            check_output('cp -f "{0}" "{1}"'.format(local_filename, remote_filename), shell=True)
        else:
            sftp = self.client.open_sftp()
            sftp.put(local_filename, remote_filename)

    def file_exists(self, filename):
        """
        Checks if a file exists on a remote host
        """
        if self.is_local is True:
            return os.path.isfile(self._shell_safe(filename))
        else:
            command = """import os, json
print json.dumps(os.path.isfile('{0}'))""".format(self._shell_safe(filename))
            return json.loads(self.run('python -c """{0}"""'.format(command)))

    def file_attribs(self, filename, mode):
        """
        Sets the mode of a remote file
        """
        command = 'chmod {0} "{1}"'.format(mode, filename)
        if self.is_local is True:
            check_output(command, shell=True)
        else:
            self.run(command)

    @connected()
    def config_read(self, key):
        if self.is_local is True:
            from ovs.extensions.generic.configuration import Configuration
            return Configuration.get(key)
        else:
            read = """
import sys, json
sys.path.append('/opt/OpenvStorage')
from ovs.extensions.generic.configuration import Configuration
print json.dumps(Configuration.get('{0}'))
""".format(key)
            return json.loads(self.run('python -c """{0}"""'.format(read)))

    @connected()
    def config_set(self, key, value):
        if self.is_local is True:
            from ovs.extensions.generic.configuration import Configuration
            Configuration.set(key, value)
        else:
            write = """
import sys, json
sys.path.append('/opt/OpenvStorage')
from ovs.extensions.generic.configuration import Configuration
Configuration.set('{0}', json.loads('{1}'))
""".format(key, json.dumps(value).replace('"', '\\"'))
            self.run('python -c """{0}"""'.format(write))

    def rawconfig_read(self, filename):
        contents = self.file_read(filename)
        handle, temp_filename = tempfile.mkstemp()
        with open(temp_filename, 'w') as configfile:
            configfile.write(contents)
        os.close(handle)
        rawconfig = RawConfigParser()
        rawconfig.read(temp_filename)
        os.remove(temp_filename)
        return rawconfig

    def rawconfig_write(self, filename, rawconfig):
        handle, temp_filename = tempfile.mkstemp()
        with open(temp_filename, 'w') as configfile:
            rawconfig.write(configfile)
        with open(temp_filename, 'r') as configfile:
            contents = configfile.read()
            self.file_write(filename, contents)
        os.close(handle)
        os.remove(temp_filename)
