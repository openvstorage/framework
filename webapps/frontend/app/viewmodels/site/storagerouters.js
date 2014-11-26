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
/*global define*/
define([
    'jquery', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vpool', '../containers/storagerouter', '../containers/pmachine'
], function($, ko, shared, generic, Refresher, api, VPool, StorageRouter, PMachine) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared                = shared;
        self.guard                 = { authenticated: true };
        self.refresher             = new Refresher();
        self.widgets               = [];
        self.pMachineCache         = {};
        self.storageRoutersHeaders = [
            { key: 'status',     value: $.t('ovs:generic.status'),     width: 55  },
            { key: 'name',       value: $.t('ovs:generic.name'),       width: 100 },
            { key: 'ip',         value: $.t('ovs:generic.ip'),         width: 100 },
            { key: 'host',       value: $.t('ovs:generic.host'),       width: 55  },
            { key: 'type',       value: $.t('ovs:generic.type'),       width: 55  },
            { key: 'vdisks',     value: $.t('ovs:generic.vdisks'),     width: 55  },
            { key: 'storedData', value: $.t('ovs:generic.storeddata'), width: 100 },
            { key: 'cacheRatio', value: $.t('ovs:generic.cache'),      width: 100 },
            { key: 'iops',       value: $.t('ovs:generic.iops'),       width: 55  },
            { key: 'readSpeed',  value: $.t('ovs:generic.read'),       width: 100 },
            { key: 'writeSpeed', value: $.t('ovs:generic.write'),      width: 100 }
        ];

        // Observables
        self.vPools = ko.observableArray([]);

        // Handles
        self.storageRoutersHandle = {};
        self.vPoolsHandle         = undefined;

        // Functions
        self.loadStorageRouters = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.storageRoutersHandle[page])) {
                    var options = {
                        sort: 'name',
                        page: page,
                        contents: '_relations,statistics,stored_data,vdisks_guids,status'
                    };
                    self.storageRoutersHandle[page] = api.get('storagerouters', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    return new StorageRouter(guid);
                                },
                                dependencyLoader: function(item) {
                                    var pMachineGuid = item.pMachineGuid(), pm;
                                    if (pMachineGuid && (item.pMachine() === undefined || item.pMachine().guid() !== pMachineGuid)) {
                                        if (!self.pMachineCache.hasOwnProperty(pMachineGuid)) {
                                            pm = new PMachine(pMachineGuid);
                                            pm.load();
                                            self.pMachineCache[pMachineGuid] = pm;
                                        }
                                        item.pMachine(self.pMachineCache[pMachineGuid]);
                                    }
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(function() {
                if (generic.xhrCompleted(self.vPoolsHandle)) {
                    self.vPoolsHandle = api.get('vpools', { contents: 'statistics,stored_data' })
                        .done(function(data) {
                            var guids = [], vpdata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                vpdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.vPools,
                                function(guid) {
                                    return new VPool(guid);
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
