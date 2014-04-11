# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
