# Framework Troubleshooting

## Watcher is stuck in pre-start
When a watcher is stuck in pre-start, a master service will be down. As a first step, figure out what master service is down by checking the logs under `/var/log/ovs/extenions.log`. This logfile contains a lot, so you might want to grep for `[extensions] - [watcher]`. Every couple of seconds, there will be either a report that everything works correct. In case something is wrong you will find what is wrong exactly.

A few examples:

```
2015-10-06 09:08:50,636 - [6625] - [DEBUG] - [extensions] - [watcher] - [volumedriver]   Error during arakoon (voldrv) test: Could not determine the Arakoon master node
2015-10-06 09:08:51,637 - [6625] - [DEBUG] - [extensions] - [watcher] - [volumedriver]   Arakoon (voldrv) not working correctly
2015-10-06 09:08:51,698 - [6627] - [DEBUG] - [extensions] - [watcher] - [framework]   Error during persistent store test: Could not determine the Arakoon master node
2015-10-06 09:08:52,699 - [6627] - [DEBUG] - [extensions] - [watcher] - [framework]   Persistent store not working correctly
```

In the above case Arakoon isn't running. Note that when a node boots up, the watcher might be started before the master services themself, so it doesn't always indicate a problem when you see this. The time in these lines might indicate whether it's an issue or not. On startup, you will see that everything turns out fine after a while when the master services are finally all started.

```
2015-10-06 12:18:38,884 - [6627] - [DEBUG] - [extensions] - [watcher] - [framework]   Error during rabbitMQ test on node 172.19.5.4:5672: 1
2015-10-06 12:18:38,885 - [6627] - [DEBUG] - [extensions] - [watcher] - [framework]   Error during rabbitMQ test on node 172.19.5.5:5672: 1
2015-10-06 12:18:38,886 - [6627] - [DEBUG] - [extensions] - [watcher] - [framework]   Error during rabbitMQ test on node 172.19.5.6:5672: 1
2015-10-06 12:18:38,886 - [6627] - [DEBUG] - [extensions] - [watcher] - [framework]   No working rabbitMQ node could be found
```

In the above example RabbitMQ isn't running.

## GUI doesn't show any dynamic info
In case the GUI doesn't shown any dynamic info (vDisks, vMachines, ...) or newly created vDisks are not showing up there might be an issue with Celery or RabbitMQ.

The easiest way to detect something is wrong is when the RabbitMQ queues are growing as more and more tasks pile up.
Execute `rabbitmqctl list_queues` and you will see something like

```
root@ctl02:/mnt# rabbitmqctl list_queues
Listing queues ...
celery@cmp01.celery.pidbox      0
celery@cmp02.celery.pidbox      0
celery@cmp03.celery.pidbox      0
celery@cmp04.celery.pidbox      0
celery@cmp05.celery.pidbox      0
celery@cmp06.celery.pidbox      0
celery@ctl01.celery.pidbox      0
celery@ctl02.celery.pidbox      0
celery@ctl03.celery.pidbox      0
celeryev.402b9bcb-077b-4c4f-9f02-3f6649670988   0
celeryev.48481246-563a-48f4-8030-174f893c8353   0
celeryev.5f857e54-6ae0-4a75-b26b-127b0a5486ad   0
celeryev.65a7396e-08b1-44bf-b20b-f84c944d8796   0
celeryev.94d18f5b-cda6-4d66-94bc-ec7a4f1c6624   0
celeryev.ab02cfc0-956c-40f6-a69d-7b91b17325eb   0
celeryev.bb048739-a857-4b5a-ab53-4b1db1a57970   0
celeryev.c19d8a10-405e-4eb7-b955-b58601c19870   0
celeryev.f0fbc8f8-451b-4852-8d0a-2a642ecf5ef6   0
ovs_Bj5FS3YEXrgE3y0z    280
ovs_MB34EuCwYYLeY4rM    314
ovs_OztdNxM47UPPdwSh    268
ovs_SCMQUsrOrBbhTgIX    253
ovs_YpSUVYES5AUaIMfG    291
ovs_generic     0
ovs_kR6b5oIMgbWNUNaY    282
ovs_masters     0
ovs_pPKAdKtMFRRJYDmK    399
ovs_r7oP9Au0BILdABrW    272
ovs_rvwbcrO3qAVKH0uk    347
volumerouter    0
...done.
```

In the above example node ovs_Bj5FS3YEXrgE3y0z has 280 tasks waiting to be executed.

First try to get everything unstuck by restarting the ovs-workers on all nodes:
`restart ovs-workers`

Check `rabbitmqctl list_queues` again and verify that the celery queues are empty:

```
root@ctl02:/mnt# rabbitmqctl list_queues
Listing queues ...
celery@cmp01.celery.pidbox      0
celery@cmp02.celery.pidbox      0
celery@cmp03.celery.pidbox      0
celery@cmp04.celery.pidbox      0
celery@cmp05.celery.pidbox      0
celery@cmp06.celery.pidbox      0
celery@ctl01.celery.pidbox      0
celery@ctl02.celery.pidbox      0
celery@ctl03.celery.pidbox      0
celeryev.402b9bcb-077b-4c4f-9f02-3f6649670988   0
celeryev.48481246-563a-48f4-8030-174f893c8353   0
celeryev.5f857e54-6ae0-4a75-b26b-127b0a5486ad   0
celeryev.65a7396e-08b1-44bf-b20b-f84c944d8796   0
celeryev.94d18f5b-cda6-4d66-94bc-ec7a4f1c6624   0
celeryev.ab02cfc0-956c-40f6-a69d-7b91b17325eb   0
celeryev.bb048739-a857-4b5a-ab53-4b1db1a57970   0
celeryev.c19d8a10-405e-4eb7-b955-b58601c19870   0
celeryev.f0fbc8f8-451b-4852-8d0a-2a642ecf5ef6   0
```

This means Celery is working as expected but in case the amount of tasks in the RabbitMQ queues are still growing, RabbitMQ is still having issues.

A typicall issue with RabbitMQ is network partitions. RabbitMQ will typically determine that a node is down if another node is unable to contact it for a minute or so. If two nodes come back into contact, both having thought the other is down, RabbitMQ will determine that a partition has occurred. This will be written to the RabbitMQ log in a form like:

```
Dec 24 12:53:44 ctl01 rabbit@ctl01.log:  =ERROR REPORT==== 24-Dec-2015::12:53:12 ===
Dec 24 12:53:44 ctl01 rabbit@ctl01.log:  Mnesia(rabbit@ctl01): ** ERROR ** mnesia_event got {inconsistent_database, running_partitioned_network, rabbit@ctl03}
```

Instead of going through the logs you can also run  `rabbitmqctl cluster_status`. Under normal circumstances the partitions section will be empty but in case of a network partition the status will be:
```

root@ctl01:~# rabbitmqctl cluster_status
Cluster status of node rabbit@ctl01 ...
[{nodes,[{disc,[rabbit@ctl01,rabbit@ctl02,rabbit@ctl03]}]},
 {running_nodes,[rabbit@ctl01]},
 {partitions,[{rabbit@ctl01,[rabbit@ctl02,rabbit@ctl03]}]}]
...done.
```

The line `{partitions,[{rabbit@ctl01,[rabbit@ctl02,rabbit@ctl03]}]}]` indicated that there was a partition and the master `rabbit@ctl01` (first one) is not in sync with the slaves `rabbit@ctl02,rabbit@ctl03`.

To recover from a network partition, first stop all slaves

On the first slave root@ctl02:
```
root@ctl02:~# rabbitmqctl stop_app
Stopping node rabbit@ctl02 ...
...done.
```
On the second slave root@ctl0:
```
root@ctl03:~# rabbitmqctl stop_app
Stopping node rabbit@ctl03 ...
...done.
```

Check the cluster status from the master node
```

root@ctl01:~# rabbitmqctl cluster_status
Cluster status of node rabbit@ctl01 ...
[{nodes,[{disc,[rabbit@ctl01,rabbit@ctl02,rabbit@ctl03]}]},
 {running_nodes,[rabbit@ctl01]},
 {partitions,[]}]
...done.
```
The partitions section will now be empty.

Restart all slaves by executing on the slave `rabbitmqctl start_app`.

The cluster status will be updated with additional running nodes:

```
root@ctl01:~# rabbitmqctl cluster_status
Cluster status of node rabbit@ctl01 ...
[{nodes,[{disc,[rabbit@ctl01,rabbit@ctl02,rabbit@ctl03]}]},
 {running_nodes,[rabbit@ctl02,rabbit@ctl03,rabbit@ctl01]},
 {partitions,[]}]
...done.
```

Finally, restart all the nodes in the trusted partition to clear the warning and verify the cluster status.
```
root@ctl01:~# rabbitmqctl stop_app
Stopping node rabbit@ctl01 ...
...done.

root@ctl01:~# rabbitmqctl start_app
Starting node rabbit@ctl01 ...
...done.

root@ctl01:~# rabbitmqctl cluster_status
Cluster status of node rabbit@ctl01 ...
[{nodes,[{disc,[rabbit@ctl01,rabbit@ctl02,rabbit@ctl03]}]},
 {running_nodes,[rabbit@ctl02,rabbit@ctl03,rabbit@ctl01]},
 {partitions,[]}]
...done.
```

## The node ID isn't showing up on the Support page of the GUI.
In case the node ID isn't showing up on the Support page (Administration > Support) of the GUI, restart the workers on the node from which the node ID isn't showing up.
```
restart ovs-workers
```