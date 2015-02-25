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
import tempfile
import paramiko
from subprocess import check_output


class SSHClient(object):
    """
    Remote/local client
    """

    def __init__(self, ip=None, username=None, password=None):
        """
        Initializes an SSHClient
        """

        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.ip = ip.strip() if ip is not None else None
        self.username = username if username is not None else check_output("whoami", shell=True).strip()
        self.password = password

        local_ips = check_output("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1", shell=True).strip().split('\n')
        local_ips = [ip.strip() for ip in local_ips]
        self.is_local = True if self.ip is None else self.ip in local_ips

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

    def run(self, command):
        """
        Executes a shell command
        """
        if self.is_local is True:
            return check_output(command, shell=True)
        else:
            try:
                self._connect()
                _, stdout, _ = self.client.exec_command(command)  # stdin, stdout, stderr
                return '\n'.join(line for line in stdout)
            finally:
                self._disconnect()

    def file_read(self, filename):
        """
        Load a file from the remote end
        """
        if self.is_local is True:
            with open(filename, 'r') as the_file:
                return the_file.read()
        else:
            return self.run('cat "{0}"'.format(filename))

    def file_write(self, filename, contents):
        """
        Writes into a file to the remote end
        """
        if self.is_local is True:
            with open(filename, 'w') as the_file:
                the_file.write(contents)
        else:
            handle, temp_filename = tempfile.mkstemp()
            with open(temp_filename, 'w') as the_file:
                the_file.write(contents)
            os.close(handle)
            try:
                self._connect()
                sftp = self.client.open_sftp()
                sftp.put(temp_filename, filename)
            finally:
                self._disconnect()
            os.remove(temp_filename)

    def dir_ensure(self, directory):
        """
        Ensures a directory exists on the remote end
        """
        if self.is_local is True:
            if not os.path.exists(directory):
                os.makedirs(directory)
        else:
            self.run('mkdir -p "{0}"'.format(directory))

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
