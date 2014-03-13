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
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vpool',
    '../wizards/addvpool/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, VPool, AddVPoolWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared       = shared;
        self.guard        = { authenticated: true };
        self.refresher    = new Refresher();
        self.widgets      = [];
        self.vPoolHeaders = [
            { key: 'name',              value: $.t('ovs:generic.name'),             width: 150       },
            { key: 'storedData',        value: $.t('ovs:generic.storeddata'),       width: 150       },
            { key: 'freeSpace',         value: $.t('ovs:vpools.freespace'),         width: 150       },
            { key: 'cacheRatio',        value: $.t('ovs:generic.cache'),            width: 100       },
            { key: 'iops',              value: $.t('ovs:generic.iops'),             width: 55        },
            { key: 'backendType',       value: $.t('ovs:vpools.backendtype'),       width: 100       },
            { key: 'backendConnection', value: $.t('ovs:vpools.backendconnection'), width: 100       },
            { key: 'backendLogin',      value: $.t('ovs:vpools.backendlogin'),      width: undefined }
        ];

        // Observables
        self.vPools            = ko.observableArray([]);
        self.vPoolsInitialLoad = ko.observable(true);

        // Handles
        self.loadVPoolsHandle    = undefined;
        self.refreshVPoolsHandle = {};

        // Functions
        self.fetchVPools = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVPoolsHandle)) {
                    var options = {
                        sort: 'name',
                        full: true,
                        contents: ''
                    };
                    self.loadVPoolsHandle = api.get('vpools', undefined, options)
                        .done(function(data) {
                            var guids = [], vpdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vpdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.vPools,
                                function(guid) {
                                    var vpool = new VPool(guid);
                                    if ($.inArray(guid, guids) !== -1) {
                                        vpool.fillData(vpdata[guid]);
                                    }
                                    vpool.loading(true);
                                    return vpool;
                                }, 'guid'
                            );
                            self.vPoolsInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.refreshVPools = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.refreshVPoolsHandle[page])) {
                    var options = {
                        sort: 'name',
                        full: true,
                        page: page,
                        contents: '_dynamics'
                    };
                    self.refreshVPoolsHandle[page] = api.get('vpools', {}, options)
                        .done(function(data) {
                            var guids = [], vpdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vpdata[item.guid] = item;
                            });
                            $.each(self.vPools(), function(index, vpool) {
                                if ($.inArray(vpool.guid(), guids) !== -1) {
                                    vpool.fillData(vpdata[vpool.guid()]);
                                }
                            });
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
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
            self.refresher.init(self.fetchVPools, 5000);
            self.refresher.start();
            self.shared.footerData(self.vPools);

            self.fetchVPools().then(function() {
                self.refreshVPools(1);
            });
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
