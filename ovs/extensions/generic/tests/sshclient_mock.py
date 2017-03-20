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
Mocked SSHClient Module
"""

import re
import copy
import json
from subprocess import CalledProcessError
from ovs.log.log_handler import LogHandler


class MockedSSHClient(object):
    """
    Class
    """
    _logger = LogHandler.get('extensions', name='sshclient')
    _file_system = {}
    _run_returns = {}
    _run_recordings = {}

    @staticmethod
    def _clean():
        """
        Clean everything up related to the unittests
        """
        MockedSSHClient._file_system = {}
        MockedSSHClient._run_returns = {}
        MockedSSHClient._run_recordings = {}

    @staticmethod
    def _split_last_part_from(path):
        """
        Split the path in parts divided by '/' and return the last part and the combined parts of the rest
        """
        if not path.startswith('/'):
            raise ValueError('In unittest mode, the paths must be absolute')

        parts = [part for part in path.strip('/').split('/') if part]
        first_part = '/{0}'.format('/'.join(parts[:-1]))
        last_part = parts[-1] if len(parts) > 0 else None
        return first_part, last_part

    @staticmethod
    def traverse_file_system(client, path):
        """
        Traverse the filesystem until 'path' has been reached
        """
        if client.ip not in MockedSSHClient._file_system:
            return None

        parts = [part for part in path.strip('/').split('/') if part]
        pointer = MockedSSHClient._file_system[client.ip]['dirs']
        for index, part in enumerate(parts):
            if part not in pointer:
                return None
            if index == len(parts) - 1:
                return pointer[part]
            pointer = pointer[part]['dirs']
        return MockedSSHClient._file_system[client.ip]

    @staticmethod
    def run(client, command, *args, **kwargs):
        """
        Mocked run method
        """
        if isinstance(command, list):
            command = ' '.join(command)
        MockedSSHClient._logger.debug('Executing: {0}'.format(command))
        if client.ip not in MockedSSHClient._run_recordings:
            MockedSSHClient._run_recordings[client.ip] = []
        MockedSSHClient._run_recordings[client.ip].append(command)
        if command in MockedSSHClient._run_returns.get(client.ip, {}):
            MockedSSHClient._logger.debug('Emulating return value')
            return MockedSSHClient._run_returns[client.ip][command]
        return client.original_function(client, command, *args, **kwargs)

    @staticmethod
    def dir_create(client, directories):
        """
        Mocked dir_create method
        """
        if isinstance(directories, basestring):
            directories = [directories]

        for directory in directories:
            if not directory.startswith('/'):
                raise ValueError('In unittest mode, the paths must be absolute')
        if client.ip not in MockedSSHClient._file_system:
            MockedSSHClient._file_system[client.ip] = {'info': {}, 'dirs': {}, 'files': {}}
        for directory in directories:
            parts = [part for part in directory.strip('/').split('/') if part]
            pointer = MockedSSHClient._file_system[client.ip]['dirs']
            for index, part in enumerate(parts):
                if part in pointer:
                    pointer = pointer[part]['dirs']
                else:
                    pointer[part] = {'info': {}, 'dirs': {}, 'files': {}}
                    pointer = pointer[part]['dirs']

    @staticmethod
    def dir_delete(client, directories, follow_symlinks=False):
        """
        Mocked dir_delete method
        """
        _ = follow_symlinks
        if isinstance(directories, basestring):
            directories = [directories]

        for directory in directories:
            first_part, last_part = MockedSSHClient._split_last_part_from(directory)
            pointer = MockedSSHClient.traverse_file_system(client=client,
                                                           path=first_part)
            if pointer is not None:
                if last_part is None:  # Root filesystem
                    MockedSSHClient._file_system[client.ip]['dirs'] = {}
                    MockedSSHClient._file_system[client.ip]['files'] = {}
                else:
                    pointer['dirs'].pop(last_part, None)

    @staticmethod
    def dir_exists(client, directory):
        """
        Mocked dir_exists method
        """
        first_part, last_part = MockedSSHClient._split_last_part_from(directory)
        pointer = MockedSSHClient.traverse_file_system(client=client,
                                                       path=first_part)
        if pointer is None or (last_part is not None and last_part not in pointer['dirs']):
            return False
        return True

    @staticmethod
    def dir_chmod(client, directories, mode, recursive=False):
        """
        Mocked dir_chmod method
        """
        if isinstance(directories, basestring):
            directories = [directories]

        for directory in directories:
            first_part, last_part = MockedSSHClient._split_last_part_from(directory)
            pointer = MockedSSHClient.traverse_file_system(client=client,
                                                           path=first_part)
            if pointer is None or (last_part is not None and last_part not in pointer['dirs']):
                raise OSError("No such file or directory: '{0}'".format(directory))

            if last_part is not None:
                pointer = pointer['dirs'][last_part]

            pointer['info']['mode'] = str(mode)
            if recursive is True:
                for sub_dir in pointer['dirs']:
                    MockedSSHClient.dir_chmod(client=client,
                                              directories='/{0}/{1}'.format(directory, sub_dir),
                                              mode=mode,
                                              recursive=True)

    @staticmethod
    def dir_chown(client, directories, user, group, recursive=False):
        """
        Mocked dir_chown method
        """
        if isinstance(directories, basestring):
            directories = [directories]

        for directory in directories:
            first_part, last_part = MockedSSHClient._split_last_part_from(directory)
            pointer = MockedSSHClient.traverse_file_system(client=client,
                                                           path=first_part)
            if pointer is None or (last_part is not None and last_part not in pointer['dirs']):
                raise OSError("No such file or directory: '{0}'".format(directory))

            if last_part is not None:
                pointer = pointer['dirs'][last_part]

            pointer['info']['user'] = str(user)
            pointer['info']['group'] = str(group)
            if recursive is True:
                for sub_dir in pointer['dirs']:
                    MockedSSHClient.dir_chown(client=client,
                                              directories='/{0}/{1}'.format(directory, sub_dir),
                                              user=user,
                                              group=group,
                                              recursive=True)

    @staticmethod
    def dir_list(client, directory):
        """
        Mocked dir_list method
        """
        first_part, last_part = MockedSSHClient._split_last_part_from(directory)
        pointer = MockedSSHClient.traverse_file_system(client=client,
                                                       path=first_part)
        if pointer is None or (last_part is not None and last_part not in pointer['dirs']):
            raise OSError("No such file or directory: '{0}'".format(directory))

        if last_part is not None:
            pointer = pointer['dirs'][last_part]
        return pointer['dirs'].keys() + pointer['files'].keys()

    @staticmethod
    def symlink(client, links):
        """
        Mocked symlink method
        """
        _ = client, links

    @staticmethod
    def file_create(client, filenames):
        """
        Mocked file_create method
        """
        if isinstance(filenames, basestring):
            filenames = [filenames]

        if client.ip not in MockedSSHClient._file_system:
            MockedSSHClient._file_system[client.ip] = {'info': {}, 'dirs': {}, 'files': {}}
        for file_location in filenames:
            parts = [part for part in file_location.strip('/').split('/') if part]
            pointer = MockedSSHClient._file_system[client.ip]
            for index, part in enumerate(parts):
                if index == len(parts) - 1:
                    pointer['files'][part] = {'contents': ''}
                    break

                pointer = pointer['dirs']
                if part in pointer:
                    pointer = pointer[part]
                else:
                    pointer[part] = {'info': {}, 'dirs': {}, 'files': {}}
                    pointer = pointer[part]

    @staticmethod
    def file_delete(client, filenames):
        """
        Mocked file_delete method
        """
        if client.ip not in MockedSSHClient._file_system:
            return

        if isinstance(filenames, basestring):
            filenames = [filenames]

        for file_location in filenames:
            if not file_location.startswith('/'):
                raise ValueError('In unittest mode, the paths must be absolute')

        for file_location in filenames:
            parts = [part for part in file_location.strip('/').split('/') if part]
            pointer = MockedSSHClient._file_system[client.ip]
            for index, part in enumerate(parts):
                regex = None if '*' not in part else re.compile('^{0}$'.format(part.replace('.', '\.').replace('*', '.*')))
                if index == len(parts) - 1:
                    pointer = pointer['files']
                    if regex is not None:
                        for sub_file in copy.deepcopy(pointer):
                            if regex.match(sub_file):
                                pointer.pop(sub_file)
                    elif part in pointer:
                        pointer.pop(part)
                    break

                pointer = pointer['dirs']
                if regex is not None:
                    for sub_dir in pointer:
                        if regex.match(sub_dir):
                            MockedSSHClient.file_delete(client=client, filenames='/{0}/{1}/{2}'.format('/'.join(parts[:index]), sub_dir, '/'.join(parts[-(len(parts) - index - 1):])))
                if part not in pointer:
                    break
                pointer = pointer[part]

    @staticmethod
    def file_unlink(client, path):
        """
        Mocked file_unlink method
        """
        _ = client, path

    @staticmethod
    def file_read_link(client, path):
        """
        Mocked file_read_link method
        """
        _ = client, path

    @staticmethod
    def file_read(client, filename):
        """
        Mocked file_read method
        """
        first_part, last_part = MockedSSHClient._split_last_part_from(filename)
        pointer = MockedSSHClient.traverse_file_system(client=client,
                                                       path=first_part)
        if pointer is None or last_part not in pointer['files']:
            raise OSError("No such file or directory: '{0}'".format(filename))

        contents = pointer['files'][last_part]['contents']
        try:
            return json.loads(contents)
        except ValueError:
            return contents

    @staticmethod
    def file_write(client, filename, contents):
        """
        Mocked file_write method
        """
        if client.ip not in MockedSSHClient._file_system:
            MockedSSHClient._file_system[client.ip] = {'info': {}, 'dirs': {}, 'files': {}}

        if not filename.startswith('/'):
            raise ValueError('In unittest mode, the paths must be absolute')

        parts = [part for part in filename.strip('/').split('/') if part]
        pointer = MockedSSHClient._file_system[client.ip]
        if isinstance(contents, list) or isinstance(contents, dict):
            contents = json.dumps(contents, indent=4, sort_keys=True)

        for index, part in enumerate(parts):
            if index == len(parts) - 1:
                pointer['files'][part] = {'contents': contents}
                return

            pointer = pointer['dirs']
            if part in pointer:
                pointer = pointer[part]
            else:
                pointer[part] = {'info': {}, 'dirs': {}, 'files': {}}
                pointer = pointer[part]

    @staticmethod
    def file_upload(client, remote_filename, local_filename):
        """
        Mocked file_upload method
        """
        _ = client, remote_filename, local_filename

    @staticmethod
    def file_exists(client, filename):
        """
        Mocked file_exists method
        """
        first_part, last_part = MockedSSHClient._split_last_part_from(filename)
        pointer = MockedSSHClient.traverse_file_system(client=client,
                                                       path=first_part)
        if pointer is None or last_part not in pointer['files']:
            return False
        return True

    @staticmethod
    def file_chmod(client, filename, mode):
        """
        Mocked file_chmod method
        """
        first_part, last_part = MockedSSHClient._split_last_part_from(filename)
        pointer = MockedSSHClient.traverse_file_system(client=client,
                                                       path=first_part)
        if pointer is None or last_part not in pointer['files']:
            raise CalledProcessError(1, 'chmod {0} {1}'.format(str(mode), filename))

        pointer['files'][last_part]['mode'] = str(mode)

    @staticmethod
    def file_chown(client, filenames, user, group):
        """
        Mocked file_chown method
        """
        if isinstance(filenames, basestring):
            filenames = [filenames]

        for filename in filenames:
            first_part, last_part = MockedSSHClient._split_last_part_from(filename)
            pointer = MockedSSHClient.traverse_file_system(client=client,
                                                           path=first_part)
            if pointer is None or last_part not in pointer['files']:
                continue

            pointer['files'][last_part]['user'] = str(user)
            pointer['files'][last_part]['group'] = str(group)

    @staticmethod
    def file_list(client, directory, abs_path=False, recursive=False):
        """
        Mocked file_list method
        """
        first_part, last_part = MockedSSHClient._split_last_part_from(directory)
        pointer = MockedSSHClient.traverse_file_system(client=client,
                                                       path=first_part)
        if pointer is None or (last_part is not None and last_part not in pointer['dirs']):
            return []

        all_files = []
        if last_part is not None:
            pointer = pointer['dirs'][last_part]
        if abs_path is True:
            directory = directory.rstrip('/')
            all_files.extend(['{0}/{1}'.format(directory, file_name) for file_name in pointer['files']])
        else:
            all_files.extend(pointer['files'].keys())

        if recursive is True:
            for sub_dir in pointer['dirs']:
                all_files.extend(MockedSSHClient.file_list(client=client, directory='{0}/{1}'.format(directory, sub_dir), abs_path=abs_path, recursive=True))
        return all_files
