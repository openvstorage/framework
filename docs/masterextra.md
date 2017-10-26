## Master and Extra Nodes
Within an Open vStorage cluster not all nodes are equal. You have master nodes and extra nodes. You need at least 3 master nodes but in large environments you can promote nodes which are configured as extra to become master. The difference between master and extra nodes and which services should be configured where.

![](/Images/Open-vStorge-Services.png)

### <a name="extra"></a>Extra Nodes
Once you have 3 master nodes in your environment, each additional node will be configured as extra node by default. All nodes, extra nodes and master nodes, run 2 types of services:

* Extra services: the celery workers and the volume router consumer. The celery worker executes the tasks put on the celery queue by RabbitMQ. The volume router consumer watches directories on KVM to detect VM config files and store these in the model.
* vPool services: these services are only available in case a vPool is configured on the node. Per vPool a volume driver service and a Distributed Transaction Log service, the FOC service, are started. Depending on the type of backend, an additional proxy service will be started as well.

### <a name="master"></a>Master Nodes
The master node runs the same services as the extra nodes but has 2 additional types of services:

* Master services: the master nodes run Nginx which is required for the frontend and the API. It also runs instances of our distributed Key/Value store Arakoon: one for the Open vStorage model (vDisk, VMs, Storage Routers, users, …) and one for the volume driver database which stores for example which Storage Router owns a vDisk. A master node also runs Memcache, RabbitMQ and a Task Scheduler.
* ALBA services: these services are related to the Open vStorage Backend, ALBA. Although it is strictly not necessary to run these services on a master node – you can run them on any node – we currently tie these services to the master node. The ALBA services consist of a maintenance and rebalancer service and 2 (or more) Arakoon instances: an NSM and ABM instance. The NameSpace Manager (NSM) stores the metadata of each namespace, a vDisk, on the Alba backend. On large environments there will be multiple NSM instances. The ALBA Backend Manager (ABM) stores the relation between a vDisk and the NSM storing its backend metadata.
