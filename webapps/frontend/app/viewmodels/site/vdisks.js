// Copyright (C) 2016 iNuron NV
//
// This file is part of Open vStorage Open Source Edition (OSE),
// as available from
//
//      http://www.openvstorage.org and
//      http://www.openvstorage.com.
//
// This file is free software; you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
// as published by the Free Software Foundation, in version 3 as it comes
// in the LICENSE.txt file of the Open vStorage OSE distribution.
//
// Open vStorage is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY of any kind.
/*global define, window */
define([
    'jquery', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vdisk', '../containers/vmachine', '../containers/vpool', '../containers/storagerouter',
    '../wizards/addvdisk/index'
], function($, dialog, ko, shared, generic, Refresher, api, VDisk, VMachine, VPool, StorageRouter, AddVDiskWizard) {
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
        self.query               = {
            type: 'AND',
            items: [['is_vtemplate', 'EQUALS', false]]
        };
        self.vDiskHeaders        = [
            { key: 'name',          value: $.t('ovs:generic.name'),          width: undefined },
            { key: 'vmachine',      value: $.t('ovs:generic.vmachine'),      width: 110       },
            { key: 'vpool',         value: $.t('ovs:generic.vpool'),         width: 110       },
            { key: 'storagerouter', value: $.t('ovs:generic.storagerouter'), width: 150       },
            { key: 'size',          value: $.t('ovs:generic.size'),          width: 100       },
            { key: 'storedData',    value: $.t('ovs:generic.storeddata'),    width: 135       },
            { key: 'iops',          value: $.t('ovs:generic.iops'),          width: 90        },
            { key: 'readSpeed',     value: $.t('ovs:generic.read'),          width: 125       },
            { key: 'writeSpeed',    value: $.t('ovs:generic.write'),         width: 125       },
            { key: 'dtlStatus',     value: $.t('ovs:generic.dtl_status'),    width: 50        }
        ];

        // Handles
        self.vDisksHandle = {};
        self.vPoolsHandle = undefined;

        // Observables
        self.vPools = ko.observableArray([]);

        // Functions
        self.addVDisk = function() {
            dialog.show(new AddVDiskWizard({
                modal: true
            }));
        };

        self.loadVDisks = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.vDisksHandle[options.page])) {
                    options.sort = 'devicename';
                    options.contents = '_dynamics,_relations,-snapshots';
                    options.query = JSON.stringify(self.query);
                    self.vDisksHandle[options.page] = api.get('vdisks', { queryparams: options })
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
                                            sr.load('');
                                            self.storageRouterCache[storageRouterGuid] = sr;
                                        }
                                        item.storageRouter(self.storageRouterCache[storageRouterGuid]);
                                    }
                                    if (vMachineGuid && (item.vMachine() === undefined || item.vMachine().guid() !== vMachineGuid)) {
                                        if (!self.vMachineCache.hasOwnProperty(vMachineGuid)) {
                                            vm = new VMachine(vMachineGuid);
                                            vm.load('');
                                            self.vMachineCache[vMachineGuid] = vm;
                                        }
                                        item.vMachine(self.vMachineCache[vMachineGuid]);
                                    }
                                    if (vPoolGuid && (item.vpool() === undefined || item.vpool().guid() !== vPoolGuid)) {
                                        if (!self.vPoolCache.hasOwnProperty(vPoolGuid)) {
                                            pool = new VPool(vPoolGuid);
                                            pool.load('');
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
                    self.vPoolsHandle = api.get('vpools', { queryparams: { contents: '' }})
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
            }, 60000);
            self.refresher.start();
            self.refresher.run();
        };
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
            self.refresher.stop();
        };
    };
});
