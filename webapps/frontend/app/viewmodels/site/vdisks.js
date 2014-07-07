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
            { key: 'name',             value: $.t('ovs:generic.name'),             width: undefined },
            { key: 'vmachine',         value: $.t('ovs:generic.vmachine'),         width: 110       },
            { key: 'vpool',            value: $.t('ovs:generic.vpool'),            width: 110       },
            { key: 'storagerouter', value: $.t('ovs:generic.storagerouter'), width: 150       },
            { key: 'size',             value: $.t('ovs:generic.size'),             width: 100       },
            { key: 'storedData',       value: $.t('ovs:generic.storeddata'),       width: 110       },
            { key: 'cacheRatio',       value: $.t('ovs:generic.cache'),            width: 100       },
            { key: 'iops',             value: $.t('ovs:generic.iops'),             width: 55        },
            { key: 'readSpeed',        value: $.t('ovs:generic.read'),             width: 100       },
            { key: 'writeSpeed',       value: $.t('ovs:generic.write'),            width: 100       },
            { key: 'failoverMode',     value: $.t('ovs:generic.focstatus'),        width: 50        }
        ];

        // Handles
        self.loadVDisksHandle    = undefined;
        self.refreshVDisksHandle = {};

        // Observables
        self.vDisks            = ko.observableArray([]);
        self.vDisksInitialLoad = ko.observable(true);

        // Functions
        self.fetchVDisks = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVDisksHandle)) {
                    var options = {
                        sort: 'vpool_guid,devicename',  // Aka, sorted by vpool, machinename, diskname
                        contents: ''
                    };
                    self.loadVDisksHandle = api.get('vdisks', undefined, options)
                        .done(function(data) {
                            var guids = [], vddata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vddata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.vDisks,
                                function(guid) {
                                    var vdisk = new VDisk(guid);
                                    if ($.inArray(guid, guids) !== -1) {
                                        vdisk.fillData(vddata[guid]);
                                    }
                                    vdisk.loading(true);
                                    return vdisk;
                                }, 'guid'
                            );
                            self.vDisksInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.refreshVDisks = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.refreshVDisksHandle[page])) {
                    var options = {
                        sort: 'vpool_guid,devicename',  // Aka, sorted by vpool, machinename, diskname
                        page: page,
                        contents: '_dynamics,_relations,-snapshots'
                    };
                    self.refreshVDisksHandle[page] = api.get('vdisks', {}, options)
                        .done(function(data) {
                            var guids = [], vddata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vddata[item.guid] = item;
                            });
                            $.each(self.vDisks(), function(index, vdisk) {
                                if ($.inArray(vdisk.guid(), guids) !== -1) {
                                    vdisk.fillData(vddata[vdisk.guid()]);
                                    var vm, sa, pool,
                                        storageRouterGuid = vdisk.storageRouterGuid(),
                                        vMachineGuid = vdisk.vMachineGuid(),
                                        vPoolGuid = vdisk.vpoolGuid();
                                    if (storageRouterGuid && (vdisk.storageRouter() === undefined || vdisk.storageRouter().guid() !== storageRouterGuid)) {
                                        if (!self.storageRouterCache.hasOwnProperty(storageRouterGuid)) {
                                            sa = new StorageRouter(storageRouterGuid);
                                            sa.load();
                                            self.storageRouterCache[storageRouterGuid] = sa;
                                        }
                                        vdisk.storageRouter(self.storageRouterCache[storageRouterGuid]);
                                    }
                                    if (vMachineGuid && (vdisk.vMachine() === undefined || vdisk.vMachine().guid() !== vMachineGuid)) {
                                        if (!self.vMachineCache.hasOwnProperty(vMachineGuid)) {
                                            vm = new VMachine(vMachineGuid);
                                            vm.load();
                                            self.vMachineCache[vMachineGuid] = vm;
                                        }
                                        vdisk.vMachine(self.vMachineCache[vMachineGuid]);
                                    }
                                    if (vPoolGuid && (vdisk.vpool() === undefined || vdisk.vpool().guid() !== vPoolGuid)) {
                                        if (!self.vPoolCache.hasOwnProperty(vPoolGuid)) {
                                            pool = new VPool(vPoolGuid);
                                            pool.load();
                                            self.vPoolCache[vPoolGuid] = pool;
                                        }
                                        vdisk.vpool(self.vPoolCache[vPoolGuid]);
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
            self.refresher.init(self.fetchVDisks, 5000);
            self.refresher.start();
            self.shared.footerData(self.vDisks);

            api.get('vmachines', {}, { contents: ''})
                .done(function(data) {
                    $.each(data, function(index, item) {
                        if (!self.vMachineCache.hasOwnProperty(item.guid)) {
                            var vm = new VMachine(item.guid);
                            vm.fillData(item);
                            self.vMachineCache[item.guid] = vm;
                        }
                    });
                });

            self.fetchVDisks().then(function() {
                self.refreshVDisks(1);
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
