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
