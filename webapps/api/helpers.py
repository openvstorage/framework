#!/usr/bin/env python2
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
Some helpers
"""

from rest_framework.response import Response


class OVSResponse(Response):

    def __init__(self, data=None, status=200,
                 template_name=None, headers=None,
                 exception=False, content_type=None, timings=None):
        super(OVSResponse, self).__init__(data=data,
                                          status=status,
                                          template_name=template_name,
                                          headers=headers,
                                          exception=exception,
                                          content_type=content_type)
        self.timings = timings

    def build_timings(self):
        self['Server-Timing'] = ','.join('{0};dur={1};desc={2}'.format(key, timing_info[0] * 1000, timing_info[1])
                                         for key, timing_info in self.timings.iteritems())
