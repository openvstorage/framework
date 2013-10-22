define(['plugins/router', 'durandal/app', 'ovs/shared', 'ovs/authentication', 'jqp/pnotify'], function (router, app, shared, authentication, $) {
    "use strict";
    var childRouter = router.createChildRouter()
                            .makeRelative({
                                moduleId: 'viewmodels/site',
                                route: ':mode',
                                fromParent: true
                            })
                            .map([
                                // Navigation routes
                                { route: '',            moduleId: 'dashboard',   hash: '#full',             title: 'Dashboard',   nav: true  },
                                { route: 'statistics',  moduleId: 'statistics',  hash: '#full/statistics',  title: 'Statistics',  nav: true  },
                                { route: 'vmachines',   moduleId: 'vmachines',   hash: '#full/vmachines',   title: 'vMachines',   nav: true  },
                                { route: 'clone', moduleId: '../wizards/clone/index', hash: '#full/clone', title: 'Clone', nav: true },
                                // Non-navigation routes
                                { route: 'login',       moduleId: 'login',       hash: '#full/login',       title: 'Login',       nav: false }
                            ])
                            .buildNavigationModel();
    childRouter.mapUnknownRoutes('404');

    return {
        authentication: authentication,
        shared: shared,
        router: childRouter,
        activate: function(mode) {
            // Shared config
            this.shared.mode(mode);
            // Authentication
            authentication.init(mode);
            // Notifications
            $.pnotify.defaults.history = false;
            $.pnotify.defaults.styling = "bootstrap";
        }
    };
});