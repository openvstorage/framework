/*global define */
define([
    'plugins/router', 'bootstrap',
    'ovs/shared', 'ovs/messaging', 'ovs/generic', 'ovs/tasks', 'ovs/authentication'
], function(router, bootstrap, shared, Messaging, generic, Tasks, Authentication) {
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
        self.activate = function() {
            self.shared.messaging      = new Messaging();
            self.shared.authentication = new Authentication();
            self.shared.tasks          = new Tasks();

            self.shared.authentication.onLoggedIn.push(self.shared.messaging.start);
            return router.activate();
        };
    };
});