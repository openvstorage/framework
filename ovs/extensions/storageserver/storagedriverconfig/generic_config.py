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
Generic storagedriver config
"""
class GenericConfig():

    def get_config(self):
        return vars(self)

    def __eq__(self, other):
        if isinstance(other, type(self)):
            if vars(self) == vars(other):
                return True
        return False

    def __ne__(self, other):
        if isinstance(other, type(self)):
            if vars(self) == vars(other):
                return False
        return True

    def get_difference(self, other):
        diff_keys = {}
        if isinstance(other, type(self)) and self != other:
            diff_keys = other.copy()
            for key, value in vars(other).iteritems():
                if value == getattr(self, key):
                    diff_keys.pop(key)

        return diff_keys



