# Scrubber
The scrubber is an essential part of the OpenvStorage cluster. It keeps the data used on the backend consistent by
removing already discarded items.

## Concurrency
The scrubber has been updated to handle concurrent jobs. It will now:
- Register that proxies are in-use
- Register which vDisks are queued for scrubbing
- Re-use existing proxies

All registration happens with metadata from the worker process. This way the scrubber can determine which information is
useful. A scrub job will remove all stale data once it saves its registration data.

## Debugging scrub job
In the event that a scrubber is not behaving as it should, the following debugging steps should be taken:
- Verify that the queues are empty
- Check logging

### Verifying queues
The scrubber maintains a couple of entries within the OVSDB Arakoon.
The entries are dependant on the vdisk information.

```
from ovs.lib.helpers.generic.scrubber import ScrubShared
from ovs.extensions.storage.persistentfactory import PersistentFactory

proxy_name = {REQUIRED_INPUT}
vpool_name = {REQUIRED_INPUT}
proxy_queue_key = ScrubShared._SCRUB_PROXY_KEY.format(proxy_name)
vdisk_queue_key = ScrubShared.__SCRUB_VDISK_KEY.format(vpool_name)
persistent_client = PersistentFactory.get_client()
print persistent_client.get(proxy_queue_key)
print persistent_client.get(vdisk_queue_key)
```

If the queues are not empty: the worker information should be validated for those items.
A queue will consist of entries like:
```
{'vdisk_guid': vdisk_guid,
 'storagerouter_guid': storagerouter.guid,
 'worker_pid': worker_pid,
 'worker_start': worker_start}
```
The `storagerouter_guid` is the storagerouter executing the current scrub job (the ovs-worker host)
The `worker_pid` is the PID of the ovs-worker process. If this has changed after checking the queues: a next scrub job
will discard the information.
The `worker_start` is a UTC string indicating when the process was started. Together with the PID this makes a unique key
to indicate whether information was stale or not.

### Check logging
A scrub job can be traced in the ovs-workers log, the /var/log/ovs/scrubber_VPOOL_NAME.log and if the scrubber would
be stopped abruptly you must check the /var/log/ovs/storagerouterclient.log for any SIGKILL during refresh_lock events

A scrub job is identified by a unique ID which matches the celery job id so all Framework-related logging for scrubbing
can be easily grepped
