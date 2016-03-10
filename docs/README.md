### Framework
The Framework is responsible for the orchestration (extending a cluster, service management, ...) and task management, the GUI and the API.

The Framework is built in python and makes use of different technologies:
* [Django](https://www.djangoproject.com/): a python webframework
* [RabbitMQ](https://www.rabbitmq.com/): a distributed message bus

{% include "dal.md" %}

{% include "masterextra.md" %}

{% include "watchers.md" %}

{% include "scrubber.md" %}

{% include "log.md" %}

