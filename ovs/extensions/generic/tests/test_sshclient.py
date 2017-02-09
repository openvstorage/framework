# -*- coding: UTF-8 -*-
#  Copyright (C) 2016 iNuron NV
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
from ovs.extensions.generic.sshclient import SSHClient


class SSHClientTest(unittest.TestCase):
    """
    Test SSHClient functionality
    """
    def test_text_cleanup(self):
        tests = {0: ['foobar', 'foobar'],
                 1: ['aàcçnñ', 'aaccnn'],
                 2: ['foo\u201ebar', 'foo\u201ebar'],  # This isn't an actual unicode character, just the characters \, u, 2, 0, 1 and e
                 3: [u'foobar', 'foobar'],
                 4: [u'foo\u1234bar', 'foobar'],
                 5: [u'foo\u201ebar', 'foo"bar'],
                 6: [u'aàcçnñ', 'aaccnn']}
        for test in sorted(tests.keys()):
            original, cleaned = tests[test]
            try:
                self.assertEqual(SSHClient._clean_text(original), cleaned)
            except:
                print 'Failed test {0}'.format(test)
                raise
