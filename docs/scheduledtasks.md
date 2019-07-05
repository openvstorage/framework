### Scheduled Tasks
The Framework is responsible for executing all Open vStorage scheduled tasks. These tasks are kicked of by celery beat,
the task scheduler of Celery. To schedule the exact timing of these tasks, celery schedules are used.
The default hardcoded schedules can be changed or disabled by saving the desired schedule in the [distributed configuration management system](configmgmt.md).  

An optional new key is introduced in the Configuration management: `/ovs/framework/scheduling/celery` which contains a JSON dictionary where the key is the task's name, and the value is:

* null in case the task should be disabled (not automatically executed)
* A dict containing [crontab](http://docs.celeryproject.org/en/latest/reference/celery.schedules.html#celery.schedules.crontab)] keyword arguments

Example configuration:

```
{
    "ovs.generic.execute_scrub": {"minute": "0", "hour": "*"},
    "alba.verify_namespaces": null
}
```


To disable the automatic scrubbing job add `"ovs.generic.execute_scrub": null` to the JSON object. 
In case you want to change the schedule for the ALBA backend verifictaion process which checks the state of each object in the backend, add `"alba.verify_namespaces": {"minute": "0", "hour": "0", "month_of_year": "*/X"}` where X is the amount of months between each run.


In case the configuration cannot be parsed at all (e.g. invalid JSON), the code will fallback to the hardcoded schedule. If the crontab arguments are invalid (e.g. they contain an unsupported key) the task will be disabled.

**NOTE:** Changing the schedules should be done with caution. Setting the frequency of some tasks too high or disabling them, might lead to performance loss or can even lead to instability (f.e. when disabling the scrubber). 
