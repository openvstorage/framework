from rest_framework import serializers


class MemcacheSerializer(serializers.Serializer):
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

    class Meta:
        fields = ('pid', 'uptime', 'time', 'version', 'rusage_user', 'rusage_system', 'curr_items', 'total_items',
                  'bytes', 'curr_connections', 'total_connections', 'cmd_get', 'cmd_set',
                  'get_hits', 'get_misses', 'evictions', 'bytes_read', 'bytes_written', 'limit_maxbytes');
