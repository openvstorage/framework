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
SSHClient module
Used for remote or local command execution
"""

import os
import re
import grp
import pwd
import glob
import json
import time
import types
import select
import socket
import logging
import tempfile
import unicodedata
from subprocess import CalledProcessError, PIPE, Popen
from ovs.dal.helpers import Descriptor
from ovs.extensions.generic.remote import remote
from ovs.log.log_handler import LogHandler


def connected():
    """
    Makes sure a call is executed against a connected client if required
    """

    def wrap(outer_function):
        """
        Wrapper function
        :param outer_function: Function to wrap
        """

        def inner_function(self, *args, **kwargs):
            """
            Wrapped function
            :param self
            """
            try:
                if self._client is not None and not self._client.is_connected():
                    self._connect()
                return outer_function(self, *args, **kwargs)
            except AttributeError as ex:
                if "'NoneType' object has no attribute 'open_session'" in str(ex):
                    self._connect()  # Reconnect
                    return outer_function(self, *args, **kwargs)
                raise

        inner_function.__name__ = outer_function.__name__
        inner_function.__module__ = outer_function.__module__
        return inner_function

    return wrap


def is_connected(self):
    """
    Monkey-patch method to check whether the Paramiko client is connected
    :param self
    """
    return self._transport is not None


class UnableToConnectException(Exception):
    """
    Custom exception thrown when client cannot connect to remote side
    """
    pass


class NotAuthenticatedException(Exception):
    """
    Custom exception thrown when client cannot connect to remote side because SSH keys have not been exchanged
    """
    pass


class SSHClient(object):
    """
    Remote/local client
    """

    _logger = LogHandler.get('extensions', name='sshclient')
    client_cache = {}
    IP_REGEX = re.compile('^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$')
    _run_returns = {}
    _run_recordings = []

    def __init__(self, endpoint, username='ovs', password=None, cached=True):
        """
        Initializes an SSHClient
        """
        from subprocess import check_output
        from ovs.dal.hybrids.storagerouter import StorageRouter
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
        self._client = None
        self.local_ips = [lip.strip() for lip in check_output("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1", shell=True).strip().splitlines()]
        self.is_local = self.ip in self.local_ips
        self.password = password
        self._unittest_mode = os.environ.get('RUNNING_UNITTESTS') == 'True'

        if self.is_local is False and storagerouter is not None and self._unittest_mode is False:
            process_heartbeat = storagerouter.heartbeats.get('process')
            if process_heartbeat is not None:
                if time.time() - process_heartbeat > 300:
                    message = 'StorageRouter {0} process heartbeat > 300s'.format(ip)
                    SSHClient._logger.error(message)
                    raise UnableToConnectException(message)

        current_user = check_output('whoami', shell=True).strip()
        if username is None:
            self.username = current_user
        else:
            self.username = username
            if username != current_user:
                self.is_local = False  # If specified user differs from current executing user, we always use the paramiko SSHClient

        if self._unittest_mode is True:
            self.is_local = True

        if not self.is_local:
            logging.getLogger('paramiko').setLevel(logging.WARNING)
            key = None
            create_new = True
            if cached is True:
                key = '{0}@{1}'.format(self.ip, self.username)
                if key in SSHClient.client_cache:
                    create_new = False
                    self._client = SSHClient.client_cache[key]

            if create_new is True:
                import paramiko
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.is_connected = types.MethodType(is_connected, client)
                if cached is True:
                    SSHClient.client_cache[key] = client
                self._client = client
        self._connect()

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

        from paramiko import AuthenticationException
        try:
            try:
                self._client.connect(self.ip, username=self.username, password=self.password)
            except:
                try:
                    self._client.close()
                except:
                    pass
                raise
        except socket.error as ex:
            message = str(ex)
            if 'No route to host' in message or 'Unable to connect' in message:
                SSHClient._logger.error(message)
                raise UnableToConnectException(message)
            raise
        except AuthenticationException:
            raise NotAuthenticatedException('Authentication failed')

    def _disconnect(self):
        """
        Disconnects from the remote end
        """
        if self.is_local is True:
            return

        self._client.close()

    @staticmethod
    def shell_safe(argument):
        """
        Makes sure that the given path/string is escaped and safe for shell
        :param argument: Argument to make safe for shell
        """
        return "'{0}'".format(argument.replace(r"'", r"'\''"))

    @staticmethod
    def _clean_text(text):
        if type(text) is list:
            text = '\n'.join(line.rstrip() for line in text)
        try:
            # This strip is absolutely necessary. Without it, channel.communicate() is never executed (odd but true)
            cleaned = text.strip()
            # I ? unicode
            if not isinstance(text, unicode):
                cleaned = unicode(cleaned.decode('utf-8', 'replace'))
            for old, new in {u'\u2018': "'",
                             u'\u2019': "'",
                             u'\u201a': "'",
                             u'\u201e': '"',
                             u'\u201c': '"',
                             u'\u25cf': '*'}.iteritems():
                cleaned = cleaned.replace(old, new)
            cleaned = unicodedata.normalize('NFKD', cleaned)
            cleaned = cleaned.encode('ascii', 'ignore')
            return cleaned
        except UnicodeDecodeError:
            SSHClient._logger.error('UnicodeDecodeError with output: {0}'.format(text))
            raise

    @connected()
    def run(self, command, debug=False, suppress_logging=False, allow_nonzero=False, allow_insecure=False, return_stderr=False):
        """
        Executes a shell command
        :param suppress_logging: Do not log anything
        :param command: Command to execute
        :param debug: Extended logging
        :param allow_nonzero: Allow non-zero exit code
        :param allow_insecure: Allow string commands (which might be improperly escaped)
        :param return_stderr: Return stderr
        """
        if self._unittest_mode is True:
            _command = command
            if isinstance(command, list):
                _command = ' '.join(command)
            SSHClient._logger.debug('Executing: {0}'.format(_command))
            SSHClient._run_recordings.append(_command)
            if _command in SSHClient._run_returns:
                SSHClient._logger.debug('Emulating return value')
                return SSHClient._run_returns[_command]

        if not isinstance(command, list) and not allow_insecure:
            raise RuntimeError('The given command must be a list, or the allow_insecure flag must be set')
        if isinstance(command, list):
            command = ' '.join([self.shell_safe(str(entry)) for entry in command])
        if self.is_local is True:
            stderr = None
            try:
                try:
                    if not hasattr(select, 'poll'):
                        import subprocess
                        subprocess._has_poll = False  # Damn 'monkey patching'
                    channel = Popen(command, stdout=PIPE, stderr=PIPE, shell=not isinstance(command, list))
                except OSError as ose:
                    raise CalledProcessError(1, command, str(ose))
                stdout, stderr = channel.communicate()
                stdout = self._clean_text(stdout)
                stderr = self._clean_text(stderr)
                exit_code = channel.returncode
                if exit_code != 0 and allow_nonzero is False:  # Raise same error as check_output
                    raise CalledProcessError(exit_code, command, stdout)
                if debug is True:
                    SSHClient._logger.debug('stdout: {0}'.format(stdout))
                    SSHClient._logger.debug('stderr: {0}'.format(stderr))
                if return_stderr is True:
                    return stdout, stderr
                else:
                    return stdout
            except CalledProcessError as cpe:
                if suppress_logging is False:
                    SSHClient._logger.error('Command "{0}" failed with output "{1}"{2}'.format(
                        command, cpe.output, '' if stderr is None else ' and error "{0}"'.format(stderr)
                    ))
                raise
        else:
            _, stdout, stderr = self._client.exec_command(command)  # stdin, stdout, stderr
            output = self._clean_text(stdout.readlines())
            error = self._clean_text(stderr.readlines())
            exit_code = stdout.channel.recv_exit_status()
            if exit_code != 0 and allow_nonzero is False:  # Raise same error as check_output
                if suppress_logging is False:
                    SSHClient._logger.error('Command "{0}" failed with output "{1}" and error "{2}"'
                                            .format(command, output, error))
                raise CalledProcessError(exit_code, command, output)
            if return_stderr is True:
                return output, error
            else:
                return output

    def dir_create(self, directories):
        """
        Ensures a directory exists on the remote end
        :param directories: Directories to create
        """
        if isinstance(directories, basestring):
            directories = [directories]
        for directory in directories:
            if self.is_local is True:
                if not os.path.exists(directory):
                    os.makedirs(directory)
            else:
                self.run(['mkdir', '-p', directory])

    def dir_delete(self, directories, follow_symlinks=False):
        """
        Remove a directory (or multiple directories) from the remote filesystem recursively
        :param directories: Single directory or list of directories to delete
        :param follow_symlinks: Boolean to indicate if symlinks should be followed and thus be deleted too
        """
        if isinstance(directories, basestring):
            directories = [directories]
        for directory in directories:
            real_path = self.file_read_link(directory)
            if real_path and follow_symlinks is True:
                self.file_unlink(directory.rstrip('/'))
                self.dir_delete(real_path)
            else:
                if self.is_local is True:
                    if os.path.exists(directory):
                        for dirpath, dirnames, filenames in os.walk(directory, topdown=False, followlinks=follow_symlinks):
                            for filename in filenames:
                                os.remove('/'.join([dirpath, filename]))
                            for sub_directory in dirnames:
                                os.rmdir('/'.join([dirpath, sub_directory]))
                        os.rmdir(directory)
                else:
                    if self.dir_exists(directory):
                        self.run(['rm', '-rf', directory])

    def dir_exists(self, directory):
        """
        Checks if a directory exists on a remote host
        :param directory: Directory to check for existence
        """
        if self.is_local is True:
            return os.path.isdir(directory)
        else:
            command = """import os, json
print json.dumps(os.path.isdir('{0}'))""".format(directory)
            return json.loads(self.run(['python', '-c', """{0}""".format(command)]))

    def dir_chmod(self, directories, mode, recursive=False):
        """
        Chmod a or multiple directories
        :param directories: Directories to chmod
        :param mode: Mode to chmod
        :param recursive: Chmod the directories recursively or not
        :return: None
        """
        if not isinstance(mode, int):
            raise ValueError('Mode should be an integer')

        if isinstance(directories, basestring):
            directories = [directories]
        for directory in directories:
            if self.is_local is True:
                os.chmod(directory, mode)
                if recursive is True:
                    for root, dirs, _ in os.walk(directory):
                        for sub_dir in dirs:
                            os.chmod('/'.join([root, sub_dir]), mode)
            else:
                command = ['chmod', oct(mode), directory]
                if recursive is True:
                    command.insert(1, '-R')
                self.run(command)

    def dir_chown(self, directories, user, group, recursive=False):
        """
        Chown a or multiple directories
        :param directories: Directories to chown
        :param user: User to assign to directories
        :param group: Group to assign to directories
        :param recursive: Chown the directories recursively or not
        :return: None
        """
        if self._unittest_mode is True:
            return

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
            if self.is_local is True:
                os.chown(directory, uid, gid)
                if recursive is True:
                    for root, dirs, _ in os.walk(directory):
                        for sub_dir in dirs:
                            os.chown('/'.join([root, sub_dir]), uid, gid)
            else:
                command = ['chown', '{0}:{1}'.format(user, group), directory]
                if recursive is True:
                    command.insert(1, '-R')
                self.run(command)

    def dir_list(self, directory):
        """
        List contents of a directory on a remote host
        :param directory: Directory to list
        """
        if self.is_local is True:
            return os.listdir(directory)
        else:
            command = """import os, json
print json.dumps(os.listdir('{0}'))""".format(directory)
            return json.loads(self.run(['python', '-c', """{0}""".format(command)]))

    def symlink(self, links):
        """
        Create symlink
        :param links: Dictionary containing the absolute path of the files and their link which needs to be created
        :return: None
        """
        if self.is_local is True:
            for link_name, source in links.iteritems():
                os.symlink(source, link_name)
        else:
            for link_name, source in links.iteritems():
                self.run(['ln', '-s', source, link_name])

    def file_create(self, filenames):
        """
        Create a or multiple files
        :param filenames: Files to create
        :return: None
        """
        if isinstance(filenames, basestring):
            filenames = [filenames]
        for filename in filenames:
            if not filename.startswith('/'):
                raise ValueError('Absolute path required for filename {0}'.format(filename))

            if self.is_local is True:
                if not self.dir_exists(directory=os.path.dirname(filename)):
                    self.dir_create(os.path.dirname(filename))
                if not os.path.exists(filename):
                    open(filename, 'a').close()
            else:
                directory = os.path.dirname(filename)
                self.dir_create(directory)
                self.run(['touch', filename])

    def file_delete(self, filenames):
        """
        Remove a file (or multiple files) from the remote filesystem
        :param filenames: File names to delete
        """
        if isinstance(filenames, basestring):
            filenames = [filenames]
        for filename in filenames:
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
                    for fn in json.loads(self.run(['python', '-c', """{0}""".format(command)])):
                        self.run(['rm', '-f', fn])
                else:
                    if self.file_exists(filename):
                        self.run(['rm', '-f', filename])

    def file_unlink(self, path):
        """
        Unlink a file
        :param path: Path of the file to unlink
        :return: None
        """
        if self.is_local is True:
            if os.path.islink(path):
                os.unlink(path)
        else:
            self.run(['unlink', path])

    def file_read_link(self, path):
        """
        Read the symlink of the specified path
        :param path: Path of the symlink
        :return: None
        """
        path = path.rstrip('/')
        if self.is_local is True:
            if os.path.islink(path):
                return os.path.realpath(path)
        else:
            command = """import os, json
if os.path.islink('{0}'):
    print json.dumps(os.path.realpath('{0}'))""".format(path)
            try:
                return json.loads(self.run(['python', '-c', """{0}""".format(command)]))
            except ValueError:
                pass

    def file_read(self, filename):
        """
        Load a file from the remote end
        :param filename: File to read
        """
        if self.is_local is True:
            with open(filename, 'r') as the_file:
                return the_file.read()
        else:
            return self.run(['cat', filename])

    @connected()
    def file_write(self, filename, contents):
        """
        Writes into a file to the remote end
        :param filename: File to write
        :param contents: Contents to write to the file
        """
        temp_filename = '{0}~'.format(filename)
        if self.is_local is True:
            if os.path.isfile(filename):
                # Use .run([cp -pf ...]) here, to make sure owner and other rights are preserved
                self.run(['cp', '-pf', filename, temp_filename])
            with open(temp_filename, 'w') as the_file:
                the_file.write(contents)
                the_file.flush()
                os.fsync(the_file)
            os.rename(temp_filename, filename)
        else:
            handle, local_temp_filename = tempfile.mkstemp()
            with open(local_temp_filename, 'w') as the_file:
                the_file.write(contents)
                the_file.flush()
                os.fsync(the_file)
            os.close(handle)
            try:
                if self.file_exists(filename):
                    self.run(['cp', '-pf', filename, temp_filename])
                sftp = self._client.open_sftp()
                sftp.put(local_temp_filename, temp_filename)
                sftp.close()
                self.run(['mv', '-f', temp_filename, filename])
            finally:
                os.remove(local_temp_filename)

    @connected()
    def file_upload(self, remote_filename, local_filename):
        """
        Uploads a file to a remote end
        :param remote_filename: Name of the file on the remote location
        :param local_filename: Name of the file locally
        """
        temp_remote_filename = '{0}~'.format(remote_filename)
        if self.is_local is True:
            self.run(['cp', '-f', local_filename, temp_remote_filename])
            self.run(['mv', '-f', temp_remote_filename, remote_filename])
        else:
            sftp = self._client.open_sftp()
            sftp.put(local_filename, temp_remote_filename)
            sftp.close()
            self.run(['mv', '-f', temp_remote_filename, remote_filename])

    def file_exists(self, filename):
        """
        Checks if a file exists on a remote host
        :param filename: File to check for existence
        """
        if self.is_local is True:
            return os.path.isfile(filename)
        else:
            command = """import os, json
print json.dumps(os.path.isfile('{0}'))""".format(filename)
            return json.loads(self.run(['python', '-c', """{0}""".format(command)]))

    def file_chmod(self, filename, mode):
        """
        Sets the mode of a remote file
        :param filename: File to chmod
        :param mode: Mode to give to file, eg: 0744
        """
        self.run(['chmod', str(mode), filename])

    def file_chown(self, filenames, user, group):
        """
        Sets the ownership of a remote file
        :param filenames: Files to chown
        :param user: User to set
        :param group: Group to set
        :return: None
        """
        if self._unittest_mode is True:
            return

        all_users = [user_info[0] for user_info in pwd.getpwall()]
        all_groups = [group_info[0] for group_info in grp.getgrall()]

        if user not in all_users:
            raise ValueError('User "{0}" is unknown on the system'.format(user))
        if group not in all_groups:
            raise ValueError('Group "{0}" is unknown on the system'.format(group))

        uid = pwd.getpwnam(user)[2]
        gid = grp.getgrnam(group)[2]
        if isinstance(filenames, basestring):
            filenames = [filenames]
        for filename in filenames:
            if self.file_exists(filename=filename) is False:
                continue
            if self.is_local is True:
                os.chown(filename, uid, gid)
            else:
                self.run(['chown', '{0}:{1}'.format(user, group), filename])

    def file_list(self, directory, abs_path=False, recursive=False):
        """
        List all files in directory
        WARNING: If executed recursively while not locally, this can take quite some time

        :param directory: Directory to list the files in
        :param abs_path: Return the absolute path of the files or only the file names
        :param recursive: Loop through the directories recursively
        :return: List of files in directory
        """
        all_files = []
        if self.is_local is True:
            for root, dirs, files in os.walk(directory):
                for file_name in files:
                    if abs_path is True:
                        all_files.append('/'.join([root, file_name]))
                    else:
                        all_files.append(file_name)
                if recursive is False:
                    break
        else:
            with remote(self.ip, [os], 'root') as rem:
                for root, dirs, files in rem.os.walk(directory):
                    for file_name in files:
                        if abs_path is True:
                            all_files.append('/'.join([root, file_name]))
                        else:
                            all_files.append(file_name)
                    if recursive is False:
                        break
        return all_files

    def is_mounted(self, path):
        """
        Verify whether a mountpoint is mounted
        :param path: Path to check
        :type path: str

        :return: True if mountpoint is mounted
        :rtype: bool
        """
        path = path.rstrip('/')
        if self.is_local is True:
            return os.path.ismount(path)

        command = """import os, json
print json.dumps(os.path.ismount('{0}'))""".format(path)
        try:
            return json.loads(self.run(['python', '-c', """{0}""".format(command)]))
        except ValueError:
            return False

    def get_hostname(self):
        """
        Gets the simple and fq domain name
        """
        short = self.run(['hostname', '-s'])
        try:
            fqdn = self.run(['hostname', '-f'])
        except:
            fqdn = short
        return short, fqdn
