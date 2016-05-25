# Logs

The redis log endpoint can be configured using etcd
```json
/ovs/framework/logging = {"type": "console|file|redis"}
```

In the case of redis the following optional params can be specified
```json
{"type": "redis",
 "queue": "/ovs/logging",
 "host": "127.0.0.1",
 "port": 6379 }
```
Where:
```
 "queue" is the name of the redis list key, optional
 "host" is the host/ip of redis endpoint, optional
 "port" is the port of redus endpoint, optional
```
If it would be desired to have every component log to its own queue then configure e.g. ```"queue": "ovs/logging/{0}"```

Using the above logging key the framework will take care of passing the correct logging parameter to each individual process.
One can decide to log to a remote redis queue or a local one. By default every node gets a local redis LRU cache of 128MB to which each component logs using RPUSH. The eviction policy is ```allkeys-lru```.
Different workers subscribed to the redis log channels can consume the log messages using LPOP and dump them into a log management system of your choice like for example papertrail or ElasticSearch/Kibana.

The queue will always start with a slash (```/```), regardless of how the queue will be configured.

If redis is not available log messages will be dropped and get lost.
If the log consumers can't consume fast enough the LRU mechanism will kick in and result in log messages never reaching the log management system. Configuring a larger queue or running more consumers might help.

Dropped messages can be detected by a sequence number in the log messages: each instance logging to Redis maintains a counter that is incremented when a message is logged.

Defined ovs_logging queues
```
ovs_logging_api
ovs_logging_lib
ovs_logging_dal
ovs_logging_celery
ovs_logging_volumedriver
ovs_logging_dtl
ovs_logging_arakoon
ovs_logging_alba-asd
ovs_logging_alba-proxy
...
```

The different components all log in the following format:
```
<timestamp> <sep> <hostname> <sep> <pid/tid> <sep> <component/subcomponent> <sep> <seqnum> <sep> <loglevel> <sep> <message>
```
Where
```
<sep> : -
<timestamp> :	<YYYY-MM-DD HH:MM:SS MICROSECS OFFSET>, 2015-12-23 19:34:53 064105, MICROSECS can be fake but ordered sub second values, OFFSET is the offset in +xxxx or -xxxx. Time is the local time.
<hostname> : node154211
<pid/tid> : 1829/0x00007f8400994700
<component/subcomponent>: volumerouter/BackendConnectionInterface
<seqnum>: 0x0000001F
<loglevel>: trace|debug|info|notification|warning|error|fatal
<message>: Exiting write for e64367f1-592f-4d8a-921e-f270855db30f
```

Example:
```
2016-02-09 13:07:29 00198 +0100 - ovs100 - 18332/140467332515648 - arakoon/pyrakoon - 527 - WARNING - Master not found, retrying in 0.20 seconds
```
