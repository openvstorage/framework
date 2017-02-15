# Copyright (C) 2017 iNuron NV
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
ExtensionsHelper module
"""
import os
from ovs.extensions.generic.toolbox import ExtensionsToolbox


class ExtensionsHelper(object):
    """
    This class contains functionality used by all UnitTest related to the BLL
    """
    @staticmethod
    def extract_dir_structure(directory):
        """
        Builds a dict representing a given directory
        """
        data = {'dirs': {}, 'files': []}
        for current_dir, dirs, files in os.walk(directory):
            current_dir = ExtensionsToolbox.remove_prefix(current_dir, directory)
            if current_dir == '':
                data['dirs'] = dict((entry, {'dirs': {}, 'files': []}) for entry in dirs)
                data['files'] = files
            else:
                dir_entries = current_dir.strip('/').split('/')
                pointer = data['dirs']
                for entry in dir_entries[:-1]:
                    pointer = pointer[entry]['dirs']
                pointer = pointer[dir_entries[-1]]
                pointer['dirs'] = dict((entry, {'dirs': {}, 'files': []}) for entry in dirs)
                pointer['files'] = files
        return data
