define([
    'plugins/router', 'durandal/app', 'bootstrap',
    'ovs/shared', 'ovs/messaging', 'ovs/generic', 'ovs/tasks'
], function (router, app, bootstrap, shared, Messaging, generic, Tasks) {
    "use strict";
    router.map([
               { route: '',              moduleId: 'viewmodels/redirect',   nav: false },
               { route: ':mode*details', moduleId: 'viewmodels/index', nav: false }
           ]).buildNavigationModel()
          .mapUnknownRoutes('viewmodels/404')
          .activate();

    return function() {
        var self = this;

        self.shared = shared;
        self.router = router;
        self.activate = function () {
            self.shared.messaging = new Messaging();
            self.shared.messaging.start();
            self.shared.tasks = new Tasks();
        };
    };
});