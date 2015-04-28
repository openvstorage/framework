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

import os
import re
import grp
import pwd
import tempfile
import paramiko
import subprocess
from subprocess import check_output
from ConfigParser import RawConfigParser


class SSHClient(object):
    """
    Remote/local client
    """

    def __init__(self, ip, username=None, password=None):
        """
        Initializes an SSHClient
        """
        ip_regex = re.compile('^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$')
        if not re.findall(ip_regex, ip):
            raise ValueError('Incorrect IP {0} specified'.format(ip))

        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.ip = ip
        local_ips = check_output("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1", shell=True).strip().splitlines()
        local_ips = [ip.strip() for ip in local_ips]
        self.is_local = self.ip in local_ips

        current_user = check_output("whoami", shell=True).strip()
        if username is None:
            self.username = current_user
        else:
            self.username = username
            if username != current_user:
                self.is_local = False  # If specified user differs from current executing user, we always use the paramiko SSHClient
        self.password = password

    def _connect(self):
        """
        Connects to the remote end
        """
        if self.is_local is True:
            return

        self.client.connect(self.ip, username=self.username, password=self.password)

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

    def run(self, command):
        """
        Executes a shell command
        """
        if self.is_local is True:
            return check_output(command, shell=True).strip()
        else:
            try:
                self._connect()
                _, stdout, stderr = self.client.exec_command(command)  # stdin, stdout, stderr
                exit_code = stdout.channel.recv_exit_status()
                if exit_code != 0:  # Raise same error as check_output
                    raise subprocess.CalledProcessError(exit_code, command, stderr.readlines())
                return '\n'.join(line.strip() for line in stdout).strip()
            finally:
                self._disconnect()

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
                self.run('mkdir -p "{0}"'.format(directory))

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
                self.run('chmod {0} {1} {2}'.format(recursive_str, mode, directory))

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
                self.run('chown {0} {1} {2}:{3}'.format(recursive_str, directory, user, group))

    def file_create(self, filenames):
        if isinstance(filenames, basestring):
            filenames = [filenames]
        for filename in filenames:
            if not filename.startswith('/'):
                raise ValueError('Absolute path required for filename {0}'.format(filename))

            filename = self._shell_safe(filename)
            if self.is_local is True:
                if not os.path.exists(filename):
                    open(filename, 'a').close()
            else:
                directory = os.path.dirname(filename)
                self.dir_create(directory)
                self.run('touch {0}'.format(filename))

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
                self._connect()
                sftp = self.client.open_sftp()
                sftp.put(temp_filename, filename)
            finally:
                self._disconnect()
            os.remove(temp_filename)

    def file_upload(self, remote_filename, local_filename):
        """
        Uploads a file to a remote end
        """
        if self.is_local is True:
            check_output('cp -f "{0}" "{1}"'.format(local_filename, remote_filename), shell=True)
        else:
            try:
                self._connect()
                sftp = self.client.open_sftp()
                sftp.put(local_filename, remote_filename)
            finally:
                self._disconnect()

    def file_exists(self, filename):
        """
        Checks if a file exists on a remote host
        """
        if self.is_local is True:
            return os.path.isfile(filename)
        else:
            return self.run('[[ -f "{0}" ]] && echo "TRUE" || echo "FALSE"'.format(filename)) == 'TRUE'

    def file_attribs(self, filename, mode):
        """
        Sets the mode of a remote file
        """
        command = 'chmod {0} "{1}"'.format(mode, filename)
        if self.is_local is True:
            check_output(command, shell=True)
        else:
            self.run(command)

    def config_read(self, key):
        if self.is_local is True:
            from ovs.plugin.provider.configuration import Configuration
            return Configuration.get(key)
        else:
            read = """
import sys
sys.path.append('/opt/OpenvStorage')
from ovs.plugin.provider.configuration import Configuration
print Configuration.get('{0}')
""".format(key)
            return self.run('python -c """{0}"""'.format(read))

    def config_set(self, key, value):
        if self.is_local is True:
            from ovs.plugin.provider.configuration import Configuration
            Configuration.set(key, value)
        else:
            write = """
import sys
sys.path.append('/opt/OpenvStorage')
from ovs.plugin.provider.configuration import Configuration
Configuration.set('{0}', '{1}')
""".format(key, value)
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
