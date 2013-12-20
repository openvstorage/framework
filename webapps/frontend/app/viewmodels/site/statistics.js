// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'ovs/shared', 'ovs/refresher',
    '../containers/memcached'
], function(shared, Refresher, Memcached) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared    = shared;
        self.guard     = { authenticated: true };
        self.refresher = new Refresher();

        // Data
        self.memcached = new Memcached();

        // Functions
        self.refresh = function() {
            self.memcached.refresh();
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.refresh, 1000);
            self.refresher.run();
            self.refresher.start();
        };
        self.deactivate = function() {
            self.refresher.stop();
        };
    };
});