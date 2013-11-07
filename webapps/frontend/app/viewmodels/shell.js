/*global define */
define([
    'plugins/router', 'bootstrap',
    'ovs/shared', 'ovs/messaging', 'ovs/generic', 'ovs/tasks', 'ovs/authentication', 'ovs/api', 'ovs/plugins/cssloader'
], function(router, bootstrap, shared, Messaging, generic, Tasks, Authentication, api, cssLoader) {
    "use strict";
    router.map([
               { route: '',              moduleId: 'viewmodels/redirect',   nav: false },
               { route: ':mode*details', moduleId: 'viewmodels/index', nav: false }
           ]).buildNavigationModel()
          .mapUnknownRoutes('viewmodels/404');

    return function() {
        var self = this;

        self.shared = shared;
        self.router = router;
        self.compositionComplete = function() {
            return api.get('branding')
                .then(function(brandings) {
                    var i, brand, css;
                    for (i = 0; i < brandings.length; i += 1) {
                        brand = brandings[i];
                        if (brand.is_default === true) {
                            css = brand.css;
                        }
                    }
                    if (css !== undefined) {
                        cssLoader.removeModuleCss();
                        cssLoader.loadCss('css/' + css);
                    }
                });
        };
        self.activate = function() {
            self.shared.messaging      = new Messaging();
            self.shared.authentication = new Authentication();
            self.shared.tasks          = new Tasks();

            self.shared.authentication.onLoggedIn.push(self.shared.messaging.start);
            return router.activate();
        };
    };
});