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
Memcache serializer module
"""
from rest_framework import serializers


class MemcacheSerializer(serializers.Serializer):
    """
    Serializes Memcache information
    """
    pid                  = serializers.Field()
    uptime               = serializers.Field()
    time                 = serializers.Field()
    version              = serializers.Field()
    rusage_user          = serializers.Field()
    rusage_system        = serializers.Field()
    curr_items           = serializers.Field()
    total_items          = serializers.Field()
    bytes                = serializers.Field()
    curr_connections     = serializers.Field()
    total_connections    = serializers.Field()
    cmd_get              = serializers.Field()
    cmd_set              = serializers.Field()
    get_hits             = serializers.Field()
    get_misses           = serializers.Field()
    evictions            = serializers.Field()
    bytes_read           = serializers.Field()
    bytes_written        = serializers.Field()
    limit_maxbytes       = serializers.Field()
    ovs_dal              = serializers.Field()

    class Meta:
        """
        Contains metadata regarding serializing fields
        """
        fields = ('pid', 'uptime', 'time', 'version', 'rusage_user', 'rusage_system', 'curr_items',
                  'total_items', 'bytes', 'curr_connections', 'total_connections', 'cmd_get',
                  'cmd_set', 'ovs_dal', 'get_hits', 'get_misses', 'evictions', 'bytes_read',
                  'bytes_written', 'limit_maxbytes')
