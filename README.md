# Open vStorage Framework

The Framework is a set of components and tools which brings the user an interface (GUI / API) to manage the Open vStorage platform.
The main framework pieces are written in python and javascript. It makes use of other open source projects like Arakoon for distributed
configuration management and Celery as distributed task queue.
The intuitive GUI allows easy setup and management of an Open vStorage Backend and link that to a distributed vPool.
Having the ability to interface with hypervisors like KVM and VMware the framework turns the platform into a VM-Centric Storage platform.

The Open vStorage Framework is licensed under the [GNU AFFERO GENERAL PUBLIC LICENSE Version 3](https://www.gnu.org/licenses/agpl.html).

## Get started

Check our gitbook on [how to get started with an installation](https://openvstorage.gitbooks.io/openvstorage/content/Installation/index.html).

## Releases
You can find an overview of the release history on our [Releases page](https://github.com/openvstorage/framework/wiki/releases).

## Documentation
The Framework specific documentation (components, concepts, logs, troubleshooting, ...) can be found  [here](https://www.gitbook.com/book/openvstorage/framework/details). 

## Contribution & Packaging

We welcome contributions, general contribution info/guidelines can be found [here](https://github.com/openvstorage/home/blob/master/CONTRIBUTING.md).
From a Framework perspective packaging your own changes for testing can be done using the [packager module](https://github.com/openvstorage/framework-tools/blob/master/packaging/packager.py)

## File a bug/enhancement
Open vStorage is quality checked to the highest level. Unfortunately we might have overlooked some tiny topics here or there. The Open vStorage Project maintains a [public issue tracker](https://github.com/openvstorage/framework/issues) where you can report bugs and request enhancements. This issue tracker is not a customer support forum but an error, flaw, failure, or fault in the Open vStorage software.

