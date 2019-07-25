# Development Environment
How to configure your developer environment to start working on the Framework

## Prerequisites
- Virtual environment
    - Current development way is flawed
        - Topologie requires files to be present which are configured upon package install and setup
        - Applies to unittesting
    - Handy for snapshotting
- An IDE
- Check out of
    - https://github.com/openvstorage/framework: the core Framework repository
    - https://github.com/openvstorage/framework-extensions: the library used by the Framework
    
## Setting up an environment
Please refer to the [install documentation](https://github.com/openvstorage/ovs-documentation/blob/master/Installation/quickinstall.md).
The only thing to watch out is to install from the right repository as dependencies change.

## Uploading code
Our current flawed way is leveraging all testing work to the an environment as opposed to being able to locally unittest our code.
Within our favourite editor Pycharm, we setup deployment paths for the different repositories and use remote-interpreters to run our tests.

The overview list of repository paths to deployment paths is as follows:
- Framework and it's plugins:
    - ovs folder -> /opt/OpenvStorage/ovs
    - webapps folder -> /opt/OpenvStorage/webapps
    - scripts/system folder -> /usr/bin/
    
- Extensions library:
    - src folder -> /usr/lib/python2.7/dist-packages/ovs_extensions/
    
## Testing changes
All changes can be unittested through `ovs unittest`. This will run the whole suite at once.

To test changes on the environment, certain processes need a kick to reload their contents.
A rule of thumb is:
- everything inside ovs: restart `ovs-workers`
- everything inside webapps/api (backend related): restart `ovs-webapp-api`
- everything inside webapps/frontend: reload the browser page
