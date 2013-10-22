define(['plugins/router', 'durandal/app', 'ovs/shared', 'bootstrap'], function (router, app, shared) {
    "use strict";
    router.map([
               { route: '',              moduleId: 'viewmodels/redirect',   nav: false },
               { route: ':mode*details', moduleId: 'viewmodels/index', nav: false }
           ]).buildNavigationModel()
          .mapUnknownRoutes('viewmodels/404')
          .activate();

    return {
        shared: shared,
        router: router,
        activate: function () { }
    };
});