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
/*global define, window */
define([
    'jquery', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vdisk', '../containers/vmachine', '../containers/vpool', '../containers/storagerouter'
], function($, ko, shared, generic, Refresher, api, VDisk, VMachine, VPool, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared              = shared;
        self.guard               = { authenticated: true };
        self.refresher           = new Refresher();
        self.widgets             = [];
        self.vMachineCache       = {};
        self.storageRouterCache  = {};
        self.vPoolCache          = {};
        self.vDiskHeaders        = [
            { key: 'name',          value: $.t('ovs:generic.name'),          width: undefined },
            { key: 'vmachine',      value: $.t('ovs:generic.vmachine'),      width: 110       },
            { key: 'vpool',         value: $.t('ovs:generic.vpool'),         width: 110       },
            { key: 'storagerouter', value: $.t('ovs:generic.storagerouter'), width: 150       },
            { key: 'size',          value: $.t('ovs:generic.size'),          width: 100       },
            { key: 'storedData',    value: $.t('ovs:generic.storeddata'),    width: 110       },
            { key: 'cacheRatio',    value: $.t('ovs:generic.cache'),         width: 100       },
            { key: 'iops',          value: $.t('ovs:generic.iops'),          width: 55        },
            { key: 'readSpeed',     value: $.t('ovs:generic.read'),          width: 100       },
            { key: 'writeSpeed',    value: $.t('ovs:generic.write'),         width: 100       },
            { key: 'failoverMode',  value: $.t('ovs:generic.focstatus'),     width: 50        }
        ];

        // Handles
        self.vDisksHandle = {};
        self.vPoolsHandle = undefined;

        // Observables
        self.vPools = ko.observableArray([]);

        // Functions
        self.loadVDisks = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.vDisksHandle[page])) {
                    var options = {
                        sort: 'vpool_guid,devicename',  // Aka, sorted by vpool, machinename, diskname
                        page: page,
                        contents: '_dynamics,_relations,-snapshots'
                    };
                    self.vDisksHandle[page] = api.get('vdisks', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    return new VDisk(guid);
                                },
                                dependencyLoader: function(item) {
                                    var vm, sr, pool,
                                        storageRouterGuid = item.storageRouterGuid(),
                                        vMachineGuid = item.vMachineGuid(),
                                        vPoolGuid = item.vpoolGuid();
                                    if (storageRouterGuid && (item.storageRouter() === undefined || item.storageRouter().guid() !== storageRouterGuid)) {
                                        if (!self.storageRouterCache.hasOwnProperty(storageRouterGuid)) {
                                            sr = new StorageRouter(storageRouterGuid);
                                            sr.load();
                                            self.storageRouterCache[storageRouterGuid] = sr;
                                        }
                                        item.storageRouter(self.storageRouterCache[storageRouterGuid]);
                                    }
                                    if (vMachineGuid && (item.vMachine() === undefined || item.vMachine().guid() !== vMachineGuid)) {
                                        if (!self.vMachineCache.hasOwnProperty(vMachineGuid)) {
                                            vm = new VMachine(vMachineGuid);
                                            vm.load();
                                            self.vMachineCache[vMachineGuid] = vm;
                                        }
                                        item.vMachine(self.vMachineCache[vMachineGuid]);
                                    }
                                    if (vPoolGuid && (item.vpool() === undefined || item.vpool().guid() !== vPoolGuid)) {
                                        if (!self.vPoolCache.hasOwnProperty(vPoolGuid)) {
                                            pool = new VPool(vPoolGuid);
                                            pool.load();
                                            self.vPoolCache[vPoolGuid] = pool;
                                        }
                                        item.vpool(self.vPoolCache[vPoolGuid]);
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
