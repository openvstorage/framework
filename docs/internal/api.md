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


