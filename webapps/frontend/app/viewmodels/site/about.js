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
    '../containers/storagerouter'
], function(ko, $, shared, generic, api, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared  = shared;
        self.guard   = { authenticated: true };
        self.loading = ko.observable(true);

        // Observables
        self.storageRouters = ko.observableArray([]);

        // Computed
        self.version = ko.computed(function() {
            var versions = [];
            $.each(self.storageRouters(), function(index, storageRouter) {
                if (storageRouter.versions() !== undefined && $.inArray(storageRouter.versions().openvstorage, versions) === -1) {
                    versions.push(storageRouter.versions().openvstorage);
                }
            });
            if (versions.length > 0) {
                return versions.join(',');
            }
            return '';
        });

        // Functions
        self.fetchStorageRouters = function() {
            return $.Deferred(function(deferred) {
                var options = {
                    sort: 'name',
                    contents: ''
                };
                api.get('storagerouters', undefined, options)
                    .done(function(data) {
                        var guids = [], sadata = {};
                        $.each(data, function(index, item) {
                            guids.push(item.guid);
                            sadata[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.storageRouters,
                            function(guid) {
                                var sa = new StorageRouter(guid);
                                if ($.inArray(guid, guids) !== -1) {
                                    sa.fillData(sadata[guid]);
                                }
                                sa.versions = ko.observable();
                                sa.loading(true);
                                api.get('storagerouters/' + guid + '/get_version_info')
                                    .then(self.shared.tasks.wait)
                                    .done(function(data) {
                                        $.each(self.storageRouters(), function(index, storageRouter) {
                                           if (storageRouter.guid() === data.storagerouter_guid) {
                                               storageRouter.versions(data.versions);
                                               storageRouter.loading(false);
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
            self.fetchStorageRouters();
        };
    };
});
