# Workers
The Framework is a collection of tasks that are interwoven. 
These tasks are generally executed by Celery although in-line execution can occur also.

## Tasks
A `ovs_task` is a wrapper around the `celery.task`.
The wrapper provides some flexibility for
- Scheduling: scheduling information for `celery.beat` to pick up on
- Locking: ensuring a single task instance is running at the time

Tasks are identified by the `@ovs_task` decorator throughout the code. This decorator adds metadata to the function
for plugin and celery beat dynamic loading.

### Celery
The scheduling and execution of a task is done by Celery. Since the `ovs_task` is a wrapper around Celery,
the same ways to trigger the [task](https://docs.celeryproject.org/en/latest/userguide/tasks.html) exist.

You can execute a task in two ways: inline and through workers.

The Framework configures Celery to use rabbitmq as messagebus and arakoon as resultbackend.
The [configuration file](https://github.com/openvstorage/framework/blob/develop/ovs/celery_run.py) is used by the `ovs-worker` services

#### Queues
A task is serialized onto a queue within rabbitmq. The default queue it gets serialized onto is `ovs_generic` 

These queues can be found in the celery config file.
Each worker is configured to listen on certain queues:
- `ovs_generic` routed by `generic.#`
- `ovs_<UNIQUE ID OF THE MACHINE` routed by `sr.<UNIQUE ID OF THE MACHINE>.#`
- `ovs_masters` routed by `masters.#` if the Storagerouter where the worker lives is a master node

#### Inline tasks
Calling a task inline is nothing special. You simple call the function as you normally would or use the `apply` function.
Example:
```
@ovs_task(name='an_example')
def do_example():
    return 'I do something special'
    
result = do_example()
result = do_example.apply()
```

#### Worker tasks
A worker task is just a task you offload asynchronously to Celery. Celery takes care of the serialization
and execution of the task.

A task is serialized onto a queue within rabbitmq. The default queue it gets serialized onto is `ovs_generic` 
The queue can be specified when calling the asynchronously.

Examples:

```
@ovs_task(name='an_example')
def do_example():
    return 'I do something special'

# Run through the workers
async_result = do_example.delay()
# Extra parameters, route to the workers on the master nodes. For more options, check the apply documentation
async_result = do_example.apply_async(routing_key='masters')

```
