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
    '../containers/storagerouter', '../containers/pmachine'
], function($, ko, shared, generic, Refresher, api, StorageRouter, PMachine) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared                  = shared;
        self.guard                   = { authenticated: true };
        self.refresher               = new Refresher();
        self.widgets                 = [];
        self.pMachineCache           = {};
        self.storageRoutersHeaders   = [
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
        self.storageRouters            = ko.observableArray([]);
        self.storageRoutersInitialLoad = ko.observable(true);

        // Handles
        self.loadStorageRoutersHandle    = undefined;
        self.refreshStorageRoutersHandle = {};

        // Functions
        self.fetchStorageRouters = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadStorageRoutersHandle)) {
                    var options = {
                        sort: 'name',
                        contents: 'stored_data'
                    };
                    self.loadStorageRoutersHandle = api.get('storagerouters', undefined, options)
                        .done(function(data) {
                            var guids = [], srdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                srdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.storageRouters,
                                function(guid) {
                                    var sr = new StorageRouter(guid);
                                    if ($.inArray(guid, guids) !== -1) {
                                        sr.fillData(srdata[guid]);
                                    }
                                    sr.loading(true);
                                    return sr;
                                }, 'guid'
                            );
                            self.storageRoutersInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.refreshStorageRouters = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.refreshStorageRoutersHandle[page])) {
                    var options = {
                        sort: 'name',
                        page: page,
                        contents: '_relations,statistics,stored_data,vdisks_guids'
                    };
                    self.refreshStorageRoutersHandle[page] = api.get('storagerouters', undefined, options)
                        .done(function(data) {
                            var guids = [], srdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                srdata[item.guid] = item;
                            });
                            $.each(self.storageRouters(), function(index, storageRouter) {
                                if ($.inArray(storageRouter.guid(), guids) !== -1) {
                                    storageRouter.fillData(srdata[storageRouter.guid()]);
                                    var pMachineGuid = storageRouter.pMachineGuid(), pm;
                                    if (pMachineGuid && (storageRouter.pMachine() === undefined || storageRouter.pMachine().guid() !== pMachineGuid)) {
                                        if (!self.pMachineCache.hasOwnProperty(pMachineGuid)) {
                                            pm = new PMachine(pMachineGuid);
                                            pm.load();
                                            self.pMachineCache[pMachineGuid] = pm;
                                        }
                                        storageRouter.pMachine(self.pMachineCache[pMachineGuid]);
                                    }
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

        // Durandal
        self.activate = function() {
            self.refresher.init(self.fetchStorageRouters, 5000);
            self.refresher.start();
            self.shared.footerData(self.storageRouters);

            self.fetchStorageRouters().then(function() {
                self.refreshStorageRouters(1);
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
