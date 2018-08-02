# Copyright (C) 2018 iNuron NV
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
Test module for the SSHClient class
"""

import unittest
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.generic.configuration.exceptions import ConfigurationNotFoundException

class ConfigurationTest(unittest.TestCase):
    """
    Test Configuration functionality
    """

    def test_rename_happy(self):
        rename_key_old = 'test_file'
        rename_key_new = 'test_file_changed'
        set_data = [(basestring, rename_key_old, rename_key_old, False)]
        get_data = [(basestring, rename_key_new, rename_key_old, False)]
        self._compare(rename_key_old, rename_key_new, set_data, get_data)
        with self.assertRaises(ConfigurationNotFoundException):
            Configuration.get('test_folder/test_file_changed')  # This key should not be made
        with self.assertRaises(ConfigurationNotFoundException):
            Configuration.get(rename_key_old)   # This key cannot exist anymore

    def test_rename_unhappy(self):
        rename_key_old = 'test_folder'
        rename_key_new = 'test_folder_changed'
        set_data = [(basestring, '{0}_'.format(rename_key_old), rename_key_old, False), (basestring, '{0}_/test_file'.format(rename_key_old), rename_key_old, False)]
        get_data = [(basestring, rename_key_new, rename_key_old, False), (basestring, rename_key_new, rename_key_old, False)]
        self._compare(rename_key_old, rename_key_new, set_data, get_data)

        c = Configuration.get_client()
        entries = c.prefix_entries(rename_key_old)
        self.assertTrue(Configuration.get('{0}_'.format(rename_key_old)) == rename_key_old)  # None of these keys should be renamed
        self.assertTrue(Configuration.get('{0}_/test_file'.format(rename_key_old)) == rename_key_old)
        self.assertFalse(any(['/../' in i[0] for i in entries]), 'Error during joining of prefixes')  # Check if joining error happened in some key


    def test_jeff(self):
        Configuration.set('djef/test', 'test_content')
        Configuration.set('djef_test1', 'test_content')
        Configuration.rename('djef', 'djef2')
        c = Configuration.get_client()
        self.assertListEqual(c.prefix_entries('djef'), [('djef_test1', '"test_content"'), ('djef2/test', '"test_content"')])


    def _compare(self, rename_key_old, rename_key_new, a, b):
            for set_data, get_data in zip(a, b):
                set_data_type, set_key, set_value, raw = set_data
                get_data_type, get_key, get_value, raw = get_data
                Configuration.set(set_key, set_value, raw=raw)
                set_get_value = Configuration.get(set_key, raw=raw)
                self.assertIsInstance(set_get_value, set_data_type)
                self.assertEquals(set_get_value, get_value) # Make sure every key and value is set as should be
            Configuration.rename(rename_key_old, rename_key_new)
