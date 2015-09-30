// Copyright 2014 Open vStorage NV
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
    'plugins/router',
    'ovs/api', 'ovs/shared', 'ovs/generic',
    './data',
    '../../containers/backend', '../../containers/backendtype'
], function($, ko, router, api, shared, generic, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data                   = data;
        self.shared                 = shared;

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
            return { value: valid, reasons: reasons, fields: fields };
        });

        // Functions
        self.isEmpty = generic.isEmpty;
        self.finish = function() {
            return $.Deferred(function(deferred) {
                deferred.resolve();
            }).promise();
        };

        // Durandal
        self.activate = function() {
            api.post('storagerouters/' + self.data.storageRouter().guid() + '/get_metadata')
                .then(self.shared.tasks.wait)
                .done(function(metadata) {
                    self.data.currentUsage(metadata.partitions);
                    self.data.roles([]);
                    $.each(metadata.partitions, function(role, partitions) {
                        $.each(partitions, function(index, partition) {
                            if (partition.guid === self.data.partition().guid()) {
                                self.data.roles.push({name: role.toLowerCase()});
                            }
                        })
                    });
                });
        };
    };
});
