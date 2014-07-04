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
    '../containers/storageappliance'
], function(ko, $, shared, generic, api, StorageAppliance) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared = shared;
        self.guard  = { authenticated: true };
        self.loading = ko.observable(true);

        // Observables
        self.storageAppliances = ko.observableArray([]);

        // Computed
        self.version = ko.computed(function() {
            var versions = [];
            $.each(self.storageAppliances(), function(index, storageAppliance) {
                if (storageAppliance.versions() !== undefined && $.inArray(storageAppliance.versions().openvstorage, versions) === -1) {
                    versions.push(storageAppliance.versions().openvstorage);
                }
            });
            if (versions.length > 0) {
                return versions.join(',');
            }
            return '';
        });

        // Functions
        self.fetchStorageAppliances = function() {
            return $.Deferred(function(deferred) {
                var options = {
                    sort: 'name',
                    contents: ''
                };
                api.get('storageappliances', undefined, options)
                    .done(function(data) {
                        var guids = [], sadata = {};
                        $.each(data, function(index, item) {
                            guids.push(item.guid);
                            sadata[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.storageAppliances,
                            function(guid) {
                                var sa = new StorageAppliance(guid);
                                if ($.inArray(guid, guids) !== -1) {
                                    sa.fillData(sadata[guid]);
                                }
                                sa.versions = ko.observable();
                                sa.loading(true);
                                api.get('storageappliances/' + guid + '/get_version_info')
                                    .then(self.shared.tasks.wait)
                                    .done(function(data) {
                                        $.each(self.storageAppliances(), function(index, storageAppliance) {
                                           if (storageAppliance.guid() === data.storageappliance_guid) {
                                               storageAppliance.versions(data.versions);
                                               storageAppliance.loading(false);
                                           }
                                        });
                                    });
                                return sa;
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
            self.fetchStorageAppliances();
        };
    };
});
