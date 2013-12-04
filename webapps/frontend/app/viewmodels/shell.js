// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'plugins/router', 'bootstrap', 'i18next',
    'ovs/shared', 'ovs/routing', 'ovs/messaging', 'ovs/generic', 'ovs/tasks', 'ovs/authentication', 'ovs/api', 'ovs/plugins/cssloader'
], function(router, bootstrap, i18n, shared, routing, Messaging, generic, Tasks, Authentication, api, cssLoader) {
    "use strict";
    router.map(routing.mainRoutes)
          .buildNavigationModel()
          .mapUnknownRoutes('viewmodels/404');

    return function() {
        var self = this;

        self._translate = function() {
            return $.Deferred(function(deferred) {
                i18n.setLng(self.shared.language, function() {
                    $('html').i18n(); // Force retranslation of complete UI
                    deferred.resolve();
                });
            }).promise();
        };

        self.shared = shared;
        self.router = router;
        self.routing = routing;
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
            self.shared.authentication.onLoggedIn.push(function() {
                return api.get('users/' + self.shared.authentication.token)
                    .then(function(data) {
                        self.shared.language = data.language;
                    })
                    .then(self._translate);
            });
            self.shared.authentication.onLoggedOut.push(function() {
                self.shared.language = self.shared.defaultLanguage;
                return self._translate();
            });
            return router.activate();
        };
    };
});
