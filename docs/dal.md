## The Data Abstraction Layer (DAL)
The Data Abstraction model uses hybrids, a template to build object which consists of persistent data, quickly changing data and relations between these objects.

The link between hybrids and their relations can be found in the [Open vStorage ERD](openvstorage-erd.pdf).

### Hybrids

A hybrid is an object consisting of persistent data, and dynamic data. Persistent data is stored in a persistent key-value store, and is read-write. The dynamic properties is code that's cached for a certain amount of time, and contains logic to load the value from 3rd party libraries.

Hybrids also support lazy loading relations. These are all one-to-many. With a junction table, a many-to-many relation is possible as well.

### File structure

All hybrids should be located in `/ovs/dal/hybrids`. Each hybrid should have its own module, having its name as lowercase.

### File contents

**Imports**

```
#!python
from ovs.dal.dataobject import DataObject
# Optionally, dependencies for dynamic properties or relations
```

**Class**

```
#!python
def MyNewClass(DataObject):
```

**Object definition**

```
#!python
__properties = []
__relations = []
__dynamics = []
```

Where `__properties` is a dictionary containing the fieldname as key, and a tuple with 2 or 3 elements as value:

1. Default value (will be used on object initialization, or in an upgrade scenario)
2. Type. Can be: str, float, int, bool, list, dict, or an actual list of possible values (enum)
3. An optional docstring

Where `__relations` is a dictionary containing the fieldname as a key, and a tuple with 2 elements as value:

1. Target object (type) to which the field points
2. The field on the target object (type) that will point to a list of object of the current type

Where `__dynamics` is a dictionary containing the fieldname as a key, and a tuple with 2 elements as value:

1. Integer stating how long the dynamic property will be cached before the load code will be executed
2. Type of the value expected from the fetching code (analogue to the `__properties` type)

**Dynamics**

The dynamics, referred to from the `__dynamics` dictionary each should have a private data loading method with name '_' and the name of the `__dynamic` key. The wrapping into a property with caching will be executed in the backend.

**Code formatting**

Always code pep8 compliant. However, by aligning the `__properties`, `__relations` and `__dynamics` fields, things get a better readability, so breaking the line-too-long rule on that part shouldn't be an issue.

### Example ###
The Backend hybrid can be found below:

```
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation, Dynamic
from ovs.dal.hybrids.backendtype import BackendType


class Backend(DataObject):
    """
    A Backend represents an instance of the supported backend types that has been setup with the OVS GUI
    """
    __properties = [Property('name', str, doc='Name of the Backend.'),
                    Property('status', ['NEW', 'INSTALLING', 'RUNNING', 'STOPPED', 'FAILURE', 'UNKNOWN'], default='NEW', doc='State of the backend')]
    __relations = [Relation('backend_type', BackendType, 'backends', doc='Type of the backend.')]
    __dynamics = [Dynamic('linked_guid', str, 3600),
                  Dynamic('available', bool, 60)]

    def _linked_guid(self):
        """
        Returns the GUID of the detail object that's linked to this particular backend. This depends on the backend type.
        This requires that the backlink from that object to this object is named <backend_type>_backend and is a
        one-to-one relation
        """
        if self.backend_type.has_plugin is False:
            return None
        return getattr(self, '{0}_backend_guid'.format(self.backend_type.code))

    def _available(self):
        """
        Returns True if the backend can be used
        """
        if self.backend_type.has_plugin is False:
            return False
        linked_backend = getattr(self, '{0}_backend'.format(self.backend_type.code))
        if linked_backend is not None:
            return linked_backend.available
        return False

```

### The databases
The hybrids are constructed using multiple databases: a volatile and a non-volatile DB.

#### Volatile - Memcached
Complete hybrids containing both the static information and the quickly changing dynamic information are stored in [Memcached](http://memcached.org/). This is a fast, distributed in-memory object caching system which increases the communication from the slower persistent DB (Arakoon) to the front-end/

An example for a vDisk:

```
__dynamics = [Dynamic('snapshots', list, 60),
              Dynamic('info', dict, 60),
              Dynamic('statistics', dict, 4, locked=True),
              Dynamic('storagedriver_id', str, 60),
              Dynamic('storagerouter_guid', str, 15)]
```

In this situation we can see that the list of the snapshots for this vDisk is refreshed (from the storagedriver) every 60 seconds:
```
Dynamic('snapshots', list, 60)
```

This info get stored in Memcached.

#### Persistent  - Arakoon
The [Arakoon](https://openvstorage.gitbooks.io/Arakoon/content) key-value database (OVSDB) stores data about the Open vStorage model which doesn't frequently change, typically static properties of the object.
