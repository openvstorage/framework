define(['plugins/router', 'durandal/app', 'ovs/shared', 'ovs/authentication'], function (router, app, shared, authentication) {
    "use strict";
    var childRouter = router.createChildRouter()
                            .makeRelative({
                                moduleId: 'viewmodels/site',
                                route: ':mode',
                                fromParent: true
                            })
                            .map([
                                { route: '',           moduleId: 'dashboard',  hash: '#full',            title: 'Dashboard',  nav: true  },
                                { route: 'statistics', moduleId: 'statistics', hash: '#full/statistics', title: 'Statistics', nav: true  },
                                { route: 'login',      moduleId: 'login',      hash: '#full/login',      title: 'Login',      nav: false }
                            ])
                            .buildNavigationModel();
    childRouter.mapUnknownRoutes('404');

    return {
        shared: shared,
        router: childRouter,
        activate: function(mode) {
            this.shared.mode(mode);
            authentication.init(mode);
        }
    };
});