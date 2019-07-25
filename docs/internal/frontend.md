# Frontend stack

Our frontend relies on the [Durandal framework](http://durandaljs.com), which in turn uses Knockout for databinding. The GUI is implemented as an MVC, and this is projected in the structure of the webapps.
Javascript version used is ES4, with certain ES5 backports.

## GUI entry point
`main.js`
 - configure some i8n and plugin settings
 - set application root to `shell.js`

`shell.js`
 - loads in all routes, build navigation model and direct unknown routes
 - loads in layout via css
 - loads in translation
 - loads in backend module if needed
 - activate router
 
`index.js` :
 - adds guarding to routes if necessary 
 - maps routes to modules (via durandal `childrouter.map`)
 - handles logging in
 - directs to landing page `viewmodels/index`
 
## Pluginloader
Loads pages by fetching hooks for all plugins. Supported plugins so far: alba and iSCSI.
The hooks that are found (`systems.acquire(<filename>`)) in the provided folders, are stored in the `routing.extraRoutes` and `routing.extraPatches`
from the `ovs.routing.js` object, as well as saved in a viewcache (`shell.js`)
