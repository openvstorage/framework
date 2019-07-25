# Volumedriver Events
The volumedriver throws event onto the messagebus. The messagebus is [configured in the StorageDriverConfig](https://github.com/openvstorage/framework/blob/updated_docs/ovs/extensions/storageserver/storagedriverconfig/storagedriver.py#L353)
that [defaults to the configured messagequeue]((https://github.com/openvstorage/framework/blob/updated_docs/ovs/extensions/storageserver/storagedriverconfig/storagedriver.py#L128)) (in our case rabbitmq).

## Handling the events
The Volumedriver events are pushed to the `volumerouter` queue by default. This is queue separated from the `celery workers`.

To process them, the Framework deploys the `volumerouter-consumer` service on the master nodes.
This is a simple [consumer](../../ovs/extensions/rabbitmq/consumer.py) that maps the thrown volumedriver events using the [available mapping](../../ovs/extensions/rabbitmq/mappings/mapping.py).
It uses the [processor](../../ovs/extensions/rabbitmq/processor.py) in order to process the body of the event and offload the handling to celery/something else (dictated by the mapping)
