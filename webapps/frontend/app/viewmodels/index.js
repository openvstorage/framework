define([
    'plugins/router', 'jqp/pnotify',
    'ovs/shared'
], function(router, $, shared) {
    "use strict";
    var childRouter = router.createChildRouter()
                            .makeRelative({
                                moduleId: 'viewmodels/site',
                                route: ':mode',
                                fromParent: true
                            })
                            .map([
                                // Navigation routes
                                { route: '',                moduleId: 'dashboard',   hash: '#full',             title: 'Dashboard',   nav: true  },
                                { route: 'statistics',      moduleId: 'statistics',  hash: '#full/statistics',  title: 'Statistics',  nav: true  },
                                { route: 'vmachines',       moduleId: 'vmachines',   hash: '#full/vmachines',   title: 'vMachines',   nav: true  },
                                // Non-navigation routes
                                { route: 'login',           moduleId: 'login',       hash: '#full/login',       title: 'Login',       nav: false }
                            ])
                            .buildNavigationModel();
    childRouter.guardRoute = function(instance, instruction) {
        if (instance !== undefined && instance.hasOwnProperty('guard')) {
            if (instance.guard.authenticated === true) {
                if (instance.shared.authentication.validate()) {
                    return true;
                }
                window.localStorage.setItem('referrer', instruction.fragment);
                return 'full/login';
            }
        }
        return true;
    };
    childRouter.mapUnknownRoutes('../404');

    return {
        shared: shared,
        router: childRouter,
        activate: function(mode) {
            var self = this;
            // Config
            self.shared.mode(mode);
            self.shared.authentication.init(mode);
            // Notifications
            $.pnotify.defaults.history = false;
            $.pnotify.defaults.styling = "bootstrap";
        }
    };
});