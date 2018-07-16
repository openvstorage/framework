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
Test decorators
"""

import unittest
from ovs_extensions.api.decorators import HTTPRequestDecorators

wrap = HTTPRequestDecorators.proper_wrap

class TestDecorator(unittest.TestCase):

    def test_wrap_request(self):
        key_wrapper = 'key_wrapper'
        value_wrapper = 'value_wrapper'

        @wrap(key_wrapper)
        def test_wrapper_decorator():
            return value_wrapper

        wrapped_dict = test_wrapper_decorator()
        self.assertDictEqual(d1=wrapped_dict, d2= {key_wrapper: value_wrapper})

        @wrap(key_wrapper)
        def test_wrapper_decorator(a='a'):
            return value_wrapper + a

        wrapped_dict = test_wrapper_decorator()
        self.assertDictEqual(d1=wrapped_dict, d2= {key_wrapper: value_wrapper + 'a'})

