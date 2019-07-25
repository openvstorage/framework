# Framework structure

## Packaging
Previously redhat (deprecated), now debian is used as default ubuntu.
Contains metadata for the Frameworkt-tools packager to work it's magic.
The packaging folder exposes the different distros and their control files.

The settings.json file contains metadata about the packaging process. The source_contents value is used for the tar command to compress the files.
## Scripts
Contains everything that is a `.sh` file or as script that needs to be run once.

`/system/ovs.sh` contains the entrypoint to the ovs CLI in the Andes releases. This has been reworked though in develop and master, and this is now offloaded to the python click library.


## OVS
### CLI 
_MASTER ONLY_

This folder contains all logic needed for the `click` library to build our refactored CLI.
Every `ovs` subcommand has its subfolder, except for calls used once (-> `misc`)
The calls are built dynamically starting from this folders' `__init__.py`. Check the `__init.py__`, `commands.py` and `entry.py` files for seeing its magic in action.
Adding commands should be done accordingly: 
- Add the module to the main `__init__.py` if necessary
- implement call in `__init__.py` of module
- add call to group

### Constants
_MASTER ONLY_
This module contains a multitude of strings of names, paths and other strings that are used throughout the codebase. Equivalent to regular i8n, its purpose is to prevent much refactoring in case some namestrings should change. And it is cleaner, in our opinion.

Usage is pretty straightforward.

### DAL
Ah, the DAL. A wonderful piece of black magic, obfuscating it design flaws through itsobscurity.
The DAL, in its essence, is a comfort (abstraction)layer on top of a db. 

#### Hybrids
Hybrids are name after their, well, _hybrid_ nature.
They contain 
- properties: static, fixed attributes that shouldn't be reloaded, or at least not often.
- dynamics: dynamic, changing attributes that should reflect state or other frequently changing parameters of an object. 

Now, get ready for some obscure magic.
- relations: These relations reflect relations between 2 DAL objects that are a one-to-one or one-to-many relationship.

But wait, there's more!

- These relations only reflect one-to-one or one-to-many relationships. For many-to-many relationships, `junctions` were introduced. These files can be seen in the list of 
the DAL hybrids as files named according to this format: `^j_.*`. 

All these `properties`, `dynamics`, `relations` and `junctions` are manipulated into the DAL object upon creation with some metaprogramming in the `__init__` of the `DataObject` superclass.

All these attributes can be accessed the same way however: 
`DALObject.property` 

more information can be found [here](https://github.com/openvstorage/framework/blob/develop/docs/dal.md)

#### Lists
Responsible for retrieving lists of data and providing a small query language on top.
- Uses indexes wherever possible
- Does query-ing client sided (not possible through Arakoon)

```
backends =  DataList(Backend, {'type': DataList.where_operator.AND,
                               'items': []})
backends = # add some extra logic here: filtering on names, types, w/e
return backends
```

#### Migration
This section contains code that will be executed when upgrading the ovs framework. When changing from version `x` to `y`, some changes might need to be made on existing (DAL) objects themselves. These objects need to be 'migrated': they need to be manipulated so that they fit the new model. 
Our migration code does this, and will be executed depending on which was the original version of the fwk, and what version one upgrades towards.
This code is summoned from `ovs.lib.update` -> `ovs.lib.migration`
overall, 3 migration codepaths are invoked.
- DAL migration
    
    `ovs.dal.<ovs|iscsi|alba>migration`
    
    Occurs on both manager and framework nodes. Will migrate DAL objects that currently live in the cluster from their old format to their new  format, if `old version < new version`
    Everytime changes are made to the layout of existing dal objects or attributes, these changes have to be reflected in the migration code. This goes for all DAL objects, hence the migration code for managers as well.
   

- Critical migration

    `ovs.extensions.migration.migration.ovsmigrator`
    
    `<iscsi-manager|asd-manager>.source.controller.update`
    
    Contains logic of migration code that is crucial for the migration to proceed. Will raise if anything fails. 
    Executed before the out-of-band migrations in the managers.
    
- Out-of-band migration

    `ovs.lib.migration`
    
    `<iscsi-manager|asd-manager>.source.controller.update`

    Executes async migrations. It doesn't matter too much when they are executed, as long as they get eventually
    executed. This code will typically contain:
    - "dangerous" migration code (it needs certain running services)
    - Migration code depending on a cluster-wide state

#### Extensions
Wraps around the ovs_extensions repository in 90% of the cases and implements the missing pieces of the puzzle of its abstract clients.
### Webapps
Contains all GUI and api related code.


#### Backend
`serializers/serializers.py` contains our serializer, needed for the django rest framework to cope with our DAL datastructure. 

#### Views
Contains all api routes for all fwk DAL objects. Ideally, the views contain as little logic as possible. Logic for the views should be mostly in the DAL lists.
decorators:
 - `log()`: make sure that the api call is logged in `webapp-api.log`
 - `required_roles()`: verify that the apiclient has the correct role for executing this call.
          A list as parameter such as ` required_roles([role_a, role_b])` means that there is an `OR` relationship. Either of these listitem roles can execute this call.
 - `return_list()` or `return_object()` will wrap the return value of the api call in all imaginable metadata needed for pagination, sorting etc.
 - `load()` The @load decorator will consume the primary key of the route and pass it onto the function as the loaded object. The name of the parameter that represent the object must always be the lowercase value of the classname.
          eg. `@load('VDisk')` will return an object of type VDisk
          
#### Oath2 
Contains all logic, decorators and others regarding credentials, token generation and verification etc.

#### Misc


### Frontend
All GUI logic
#### App
contains durandals and other JS models (`viewmodels`), HTML `views` and `widgets` such as dropdownboxes, lazylists used throughout the GUI
#### Locales
Internationalization strings 
#### CSS
- bootstrap.css -> vendor
- durandal.css -> vendor
- ovs.css -> our own
If modication are to be made, they must be made into ovs.css
CSS is loaded by the `index.html`, which kicks in the whole application
