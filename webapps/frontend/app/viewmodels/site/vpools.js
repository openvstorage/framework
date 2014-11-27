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
    'jquery', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vpool',
    '../wizards/addvpool/index'
], function($, dialog, ko, shared, generic, Refresher, api, VPool, AddVPoolWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared       = shared;
        self.guard        = { authenticated: true };
        self.refresher    = new Refresher();
        self.widgets      = [];
        self.vPoolHeaders = [
            { key: 'name',              value: $.t('ovs:generic.name'),             width: 200       },
            { key: 'storedData',        value: $.t('ovs:generic.storeddata'),       width: 150       },
            { key: 'cacheRatio',        value: $.t('ovs:generic.cache'),            width: 100       },
            { key: 'iops',              value: $.t('ovs:generic.iops'),             width: 100       },
            { key: 'backendType',       value: $.t('ovs:vpools.backendtype'),       width: 150       },
            { key: 'backendConnection', value: $.t('ovs:vpools.backendconnection'), width: 100       },
            { key: 'backendLogin',      value: $.t('ovs:vpools.backendlogin'),      width: undefined }
        ];
        self.vPoolCache = {};

        // Handles
        self.vPoolsHandle = {};

        // Observables
        self.vPools = ko.observableArray([]);

        // Functions
        self.loadVPools = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.vPoolsHandle[page])) {
                    var options = {
                        sort: 'name',
                        page: page,
                        contents: '_dynamics,backend_type'
                    };
                    self.vPoolsHandle[page] = api.get('vpools', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    if (!self.vPoolCache.hasOwnProperty(guid)) {
                                        self.vPoolCache[guid] = new VPool(guid);
                                    }
                                    return self.vPoolCache[guid];
                                },
                                dependencyLoader: function(item) {
                                    item.loadBackendType();
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.addVPool = function() {
            dialog.show(new AddVPoolWizard({
                modal: true
            }));
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(function() {
                if (generic.xhrCompleted(self.vPoolsHandle[undefined])) {
                    self.vPoolsHandle[undefined] = api.get('vpools', { queryparams: {contents: 'statistics,stored_data,backend_type' }})
                        .done(function(data) {
                            var guids = [], vpdata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                vpdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.vPools,
                                function(guid) {
                                    if (!self.vPoolCache.hasOwnProperty(guid)) {
                                         self.vPoolCache[guid] = new VPool(guid);
                                    }
                                    return self.vPoolCache[guid];
                                }, 'guid'
                            );
                            $.each(self.vPools(), function(index, item) {
                                if (vpdata.hasOwnProperty(item.guid())) {
                                    item.fillData(vpdata[item.guid()]);
                                }
                            });
                        });
                }
            }, 5000);
            self.refresher.start();
            self.refresher.run();
            self.shared.footerData(self.vPools);
        };
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
            self.refresher.stop();
            self.shared.footerData(ko.observable());
        };
    };
});
