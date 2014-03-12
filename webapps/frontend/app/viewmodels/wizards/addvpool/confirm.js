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
    'jquery', 'knockout',
    '../../containers/vmachine', './data'
], function($, ko, VMachine, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data = data;

        // Computed
        self.canContinue = ko.computed(function() {
            return { value: true, reasons: [], fields: [] };
        });

        // Functions
        self.finish = function() {
            // 'vpool_name', 'backend_type', 'connection_host', 'connection_port', 'connection_timeout',
            // 'connection_username', 'connection_password', 'mountpoint_temp', 'mountpoint_dfs', 'mountpoint_md',
            // 'mountpoint_cacahe', 'storage_ip', 'vrouter_port', 'vsa_password'
        };
    };
});
