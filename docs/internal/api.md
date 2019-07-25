# API
Documentation about the OpenvStorage http API. Please refer to [here](https://github.com/openvstorage/ovs-documentation/tree/master/Administration/usingtheapi)
for a guide to use the API

## Processes
The API is divided into two separate processes.
- Gunicorn: gunicorn django stack with API routing
- Nginx: used a webserver to serve http files and proxy server to pass through Gunicorn

## Nginx
Nginx is configured to forward all http traffic to https. You can find the configs under `config/nginx`

It is configured to 
- proxy all /api traffic to Gunicorn on port 8002.
- proxy all /logging to the specified host
- serve all / requests from `webapps/frontend/`. If the browser gets the '/' resource, it'll fetch the index.html and load the Durandal app

## Django stack
Our django stack is located in the `webapps.api` folder. We only use a subset of the whole Django functionality.
 - Authentication
 - Django rest framework + views
 - serialization: customized, as Django cannot cope too well with our DAL structure. We don't use their ORM for this reason.
 Django relies heavily on their own ORM, which we exclude completely. Due to this, their default serialization cannot be used and we have to implement our own serialization (`FullSerializer`)
 
 This is hammering Django just hard enough to make it fit our stack
 A Better idea would have been to use flask from the start, as this provides way more liberty in implementation and doesn't rely on an ORM.
 Work has been done to migrate from DRF (Django rest-framework) towards flask, since our current DRF version is harder to control and customize, especially when also upgrading to python 3.x.
 This transition has been laborous and is unfinished.
### Default Django files and contents
- wsgi.py
- urls.py: implementation of the Django simplerouter. An own implementation is needed to backport features introduced in newer django versions.
implements static and dynamic list and detail routes, and functions to access these.
The default django `urlpatterns` variable contains routes for the built views:


|routes  | views  |
|---|---|
| ovs api calls  |  `def build_router_urls` |
| authentication, identification, plugin metadata |  `metadata.py`|
| openAPI according to swagger| `openapi.py`|
| relay | `view.py`, `def relay` |
| oath2 token creation and redirection | `oath2` module |
The `PluginController.get_webapps` fetches all viewclasses in `api/backend/views`.

All ovs implemented routes in this module are registered through the base django rest framework router. 
   
- settings.py
- manage.py
