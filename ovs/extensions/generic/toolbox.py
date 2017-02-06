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
Toolbox module
"""


class Toolbox(object):
    """
    Generic class for various methods
    """
    @staticmethod
    def remove_prefix(string, prefix):
        """
        Removes a prefix from the beginning of a string
        :param string: The string to clean
        :param prefix: The prefix to remove
        :return: The cleaned string
        """
        if string.startswith(prefix):
            return string[len(prefix):]
        return string

    @staticmethod
    def edit_version_file(client, package_name, old_service_name, new_service_name=None):
        """
        Edit a run version file in order to mark it for 'reboot' or 'removal'
        :param client: Client on which to edit the version file
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :param package_name: Name of the package to check on in the version file
        :type package_name: str
        :param old_service_name: Name of the service which needs to be edited
        :type old_service_name: str
        :param new_service_name: Name of the service which needs to be written. When specified the old service file will be marked for removal
        :type new_service_name: str
        :return: None
        """
        old_run_file = '/opt/OpenvStorage/run/{0}.version'.format(old_service_name)

        if client.file_exists(filename=old_run_file):
            contents = client.file_read(old_run_file).strip()
            if new_service_name is not None:  # Scenario in which we will mark the old version file for 'removal' and the new version file for 'reboot'
                client.run(['mv', old_run_file, '{0}.remove'.format(old_run_file)])
                run_file = '/opt/OpenvStorage/run/{0}.version'.format(new_service_name)
            else:  # Scenario in which we will mark the old version file for 'reboot'
                run_file = old_run_file

            if '-reboot' not in contents:
                if '=' in contents:
                    contents = ';'.join(['{0}-reboot'.format(part) for part in contents.split(';') if package_name in part])
                else:
                    contents = '{0}-reboot'.format(contents)
                client.file_write(filename=run_file, contents=contents)
                client.file_chown(filenames=[run_file], user='ovs', group='ovs')

    @staticmethod
    def advanced_sort(element, separator):
        """
        Function which can be used to sort names
        Eg: Sorting service_1, service_2, service_10
            will result in service_1, service_2, service_10
            io service_1, service_10, service_2
        :param element: Element to sort
        :type element: str
        :param separator: Separator to split the element on
        :type separator: str
        :return: Element split on separator and digits converted to floats
        :rtype: Tuple
        """
        entries = element.split(separator)
        for index in xrange(len(entries)):
            try:
                entries[index] = float(entries[index])
            except ValueError:
                pass
        return tuple(entries)

