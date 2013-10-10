define(['plugins/router', 'durandal/app'], function (router, app) {
    "use strict";
    return {
        router: router,
        activate: function () {
            router.map([
                { route: '', title: 'Dashboard', moduleId: 'viewmodels/dashboard', nav: true },
                { route: 'statistics', title: 'Statistics', moduleId: 'viewmodels/statistics', nav: true }
            ]).buildNavigationModel();
            return router.activate();
        }
    };
});