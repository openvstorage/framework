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
    'knockout', 'jquery',
    'ovs/shared', 'ovs/generic', 'ovs/api',
    '../containers/vmachine'
], function(ko, $, shared, generic, api, VMachine) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared = shared;
        self.guard  = { authenticated: true };
        self.query  = {
            query: {
                type: 'AND',
                items: [['is_internal', 'EQUALS', true]]
            }
        };
        self.loading = ko.observable(true);

        // Observables
        self.vSAs = ko.observableArray([]);

        // Computed
        self.version = ko.computed(function() {
            var versions = [];
            $.each(self.vSAs(), function(index, vsa) {
                if (vsa.versions() !== undefined && $.inArray(vsa.versions().openvstorage, versions) === -1) {
                    versions.push(vsa.versions().openvstorage);
                }
            });
            if (versions.length > 0) {
                return versions.join(',');
            }
            return '';
        });

        // Functions
        self.fetchVSAs = function() {
            return $.Deferred(function(deferred) {
                var options = {
                    sort: 'name',
                    full: true,
                    contents: ''
                };
                api.post('vmachines/filter', self.query, options)
                    .done(function(data) {
                        var guids = [], vsadata = {};
                        $.each(data, function(index, item) {
                            guids.push(item.guid);
                            vsadata[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.vSAs,
                            function(guid) {
                                var vmachine = new VMachine(guid);
                                if ($.inArray(guid, guids) !== -1) {
                                    vmachine.fillData(vsadata[guid]);
                                }
                                vmachine.versions = ko.observable();
                                vmachine.loading(true);
                                api.get('vmachines/' + guid + '/get_version_info')
                                    .then(self.shared.tasks.wait)
                                    .done(function(data) {
                                        $.each(self.vSAs(), function(index, vsa) {
                                           if (vsa.guid() === data.vsa_guid) {
                                               vsa.versions(data.versions);
                                               vsa.loading(false);
                                           }
                                        });
                                    });
                                return vmachine;
                            }, 'guid'
                        );
                        self.loading(false);
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };

        // Durandal
        self.activate = function() {
            self.fetchVSAs();
        };
    };
});
