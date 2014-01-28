// Copyright 2014 CloudFounders NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
