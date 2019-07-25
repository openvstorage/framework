# Plugin loading
The Framework is written with plugins in mind. That way we can be flexible when it comes to supporting different backends.

## Plugin list
The current list of maintained plugins is:
- ALBA: Manage an ALBA backend through the Framework
- Iscsi: Add ISCSI support to vDisks

## How does it work
The Framework loads in plugins by loading all files within directories and looking for metadata on the objects (functions/classes/...)

### Workers
The different plugins are separated by code. Every plugin can only load in modules from the plugin itself or the base Framework library.
This avoids importing modules which may not exist.

Registering new tasks for your plugin is alleviated through the use of [\@ovs_task](https://github.com/openvstorage/framework/blob/develop/ovs/lib/helpers/decorators.py#L101).
This decorator registers the function into the celery app instance. The Celery app is then able to translate the task name to the function to execute.

Registering all tasks on start-up is also [offloaded to celery](https://github.com/openvstorage/framework/blob/develop/ovs/celery_run.py#L137).
All directories under `ovs.lib` are [included](https://docs.celeryproject.org/en/latest/userguide/configuration.html#std:setting-include) for task definitions.
#### Hooks
If changes must occur throughout the whole application, you must use hooks.

Hook loading is done by the Framework itself.
By using the [\@add_hooks](https://github.com/openvstorage/framework/blob/develop/ovs/lib/helpers/decorators.py#L1084) decorator, 
metadata is registered on the function. When loading in the function, the wrapper is executed upon importing and the function now contains properties that are query-able.

Loading in hooks is done by the using [fetch_hooks](https://github.com/openvstorage/framework/blob/develop/ovs/lib/helpers/toolbox.py#L51) function.
The code will load in all paths that might contain hooks (in this case all paths under the `ovs.lib` module) and look for hooking metadata to build up a map.

There is no standard regarding arguments that can be supplied to hooks.

### API
The Framework provides the API serialization, authentication and a set of routes out of the box.
Plugins can create additional routes to be picked up by the API service.

The `ovs-webapp-api` service will [discover all routes](https://github.com/openvstorage/framework/blob/develop/webapps/api/urls.py#L288) 
under `webapps.api.backend.views` and add them to the router dynamically. The urls part of the config which invokes the loading is found [here](https://github.com/openvstorage/framework/blob/develop/webapps/api/settings.py#L122)

### Front-end
The front-end does its plugin loading by fetching the available plugins through the API.
It does so when [first opening the application](https://github.com/openvstorage/framework/blob/develop/webapps/frontend/app/viewmodels/shell.js#L53)

When it has loaded the metadata, the [plugin loader](https://github.com/openvstorage/framework/blob/develop/webapps/frontend/lib/ovs/plugins/pluginloader.js#L50) 
code will **pre-load the related pages** and add them to the client-sided router.
This is also a **drawback** as none of the plugins are lazyloaded. 

