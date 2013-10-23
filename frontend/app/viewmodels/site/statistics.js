define([
    'plugins/router', 'durandal/app',
    'ovs/shared', 'ovs/authentication', 'ovs/refresher',
    '../containers/memcached'
], function (router, app, shared, authentication, Refresher, Memcached) {
    "use strict";
    return function () {
        var self = this;

        // System
        self.shared = shared;
        self.refresher = new Refresher();

        // Data
        self.displayname = 'Statistics';
        self.description = 'The page contains various system statistics';
        self.memcached = new Memcached();

        // Functions
        self.refresh = function () {
            self.memcached.refresh();
        };

        // Durandal
        self.canActivate = function() { return authentication.validate(); };
        self.activate = function () {
            self.refresher.init(self.refresh, 1000);
            self.refresher.run();
            self.refresher.start();
        };
        self.deactivate = function () {
            self.refresher.stop();
        };
    };
});