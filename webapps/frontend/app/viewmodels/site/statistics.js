// Copyright (C) 2016 iNuron NV
//
// This file is part of Open vStorage Open Source Edition (OSE),
// as available from
//
//      http://www.openvstorage.org and
//      http://www.openvstorage.com.
//
// This file is free software; you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
// as published by the Free Software Foundation, in version 3 as it comes
// in the LICENSE.txt file of the Open vStorage OSE distribution.
//
// Open vStorage is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY of any kind.
/*global define */
define([
    'ovs/shared', 'ovs/refresher',
    '../containers/memcached'
], function(shared, Refresher, Memcached) {
    "use strict";
    return function() {
        var self = this;

        // Variables
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
