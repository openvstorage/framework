#### The watchers
Open vStorage is build as a clustered system where nodes can have multiple/different roles. Certain nodes, the so called master nodes, run certain services that are important for all other services (e.g. arakoon (persistent storage), memcache (volatile storage), rabbitmq (message queue)). Other nodes (in fact, all nodes) run also services that depend on these "master services" to be able to work.

Native system services (upstart on Ubuntu, systemd on CentOS) usually only support a service dependency tree within a single node, not across multiple nodes. And that's where the watchers come in.

Within Open vStorage there are 2 watchers: one for the framework and one for the Volume Driver.

These watchers are being start up as soon as possible, and they will run through some kind of state machine:
# During startup (in upstart indicated by `start/pre-start`) they keep on connecting to these master services, one after the other.
#* If one of them fails, the watcher will wait some seconds, and try again.
#* If they all succeed, the startup completes.
#* Please note that this means that if you start a watcher manually (e.g. `start ovs-watcher-framework`), the start command will *block* until it can connect to all defined master services.
# Once it is started (in upstart indicated by `start/running`), it keeps on connecting to the master services every couple of seconds.
# As soon as one of these master services seems to be unavailable, it will exit with an error code.
#* This causes upstart to restart the watcher service, back to 1.

When the watchers connect to these master services they will actually try to consume the service, not just check the service state. E.g. the Arakoon service might indicate it's running, but there might be an issue finding a master node, rendering the cluster useless. The watcher will notice that.

This is as expected also cluster aware. Take nodes 1 to 4. The first three are master and run a master service and a watcher depending service, and the fourth one only runs a watcher depending service. Assuming all is up and running, and the master service is high available and can lose one of its nodes. So as soon as one master service goes down, all 4 watchers can still connect to the service, so their dependent services will keep on running. As soon as the second master service dies, the watchers will notice that and exit with an error code taking down their depending services. All four watchers will go down, as will the four depending services. Upstart will restart the watchers as they behave like they crashed, and the watchers will block in the pre-start phase. As soon as one of the master services comes back online, and the service is usable again, the watcher will complete its startup sequence and their depending services will be started as well (by upstart).

##### Master services

The master services are:
* Arakoon
* Memcache
* RabbitMQ

These are generally speaking cluster services that run on the master nodes. They usually start right after the network is up.


##### Watcher depending services

Typically, these are services that - like stated above - need the master services to work. There are quite some services but one example is the celery workers (service name is `ovs-workers`). If they are started manually they will usually just start fine, but fail in the assigned tasks. They may not be able to connect to the database, they might fail loading work, ... Basically it will make things worse, as certain tasks might fail causing that task to be "lost" (it won't be retried when the master service is back online).

They however are configured to "start on started watcher" and "stop on stopping watcher" which means that they will be started as soon as a watcher has *completed* the startup sequence. This means all services are verified to be up and running. It also means that they will stop before the watcher is stopped (or as soon as it somehow doesn't run anymore because it has exited with an error), or like stated above, once a master service is not running anymore.

##### Other independent services

Open vStorage contains other services that are more or less independent. They run on their own and don't need those master services and can be configured to start at any time.

Examples are f.e. the support agent.


##### How to recognize which service is what.
The watchers are labeled as `ovs-watcher-<something>`.

The services which are not depending on a watcher to startup, the independent services, can be identified by their service definition file (for upstart, that's `/etc/init/<service name>.conf`. These services will typically be started on a set of runlevels or e.g. when the network is up. A few examples:

```
description "Open vStorage support agent"

start on runlevel [2345]
stop on runlevel [016]

...
```

```
description "Arakoon upstart for cluster <CLUSTER>"

start on (local-filesystems and started networking)
stop on runlevel [016]

...
```

The services that are depending on the watcher can be recognized as they are depending on the watcher services. These services shouldn't not be start manually. An example:

```
description "ovs workers"

start on started ovs-watcher-framework
stop on stopping ovs-watcher-framework

...
```
