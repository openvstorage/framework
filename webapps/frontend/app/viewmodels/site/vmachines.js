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
    'jquery', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vmachine', '../containers/vpool', '../containers/storagerouter'
], function($, ko, shared, generic, Refresher, api, VMachine, VPool, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared             = shared;
        self.guard              = { authenticated: true };
        self.refresher          = new Refresher();
        self.widgets            = [];
        self.vPoolCache         = {};
        self.storageRouterCache = {};
        self.query              = {
            type: 'AND',
            items: [['is_vtemplate', 'EQUALS', false],
                    ['status', 'NOT_EQUALS', 'CREATED']]
        };
        self.vMachineHeaders    = [
            { key: 'name',          value: $.t('ovs:generic.name'),          width: undefined },
            { key: 'vpool',         value: $.t('ovs:generic.vpool'),         width: 150       },
            { key: 'storagerouter', value: $.t('ovs:generic.storagerouter'), width: 150       },
            { key: undefined,       value: $.t('ovs:generic.vdisks'),        width: 60        },
            { key: 'storedData',    value: $.t('ovs:generic.storeddata'),    width: 110       },
            { key: 'cacheRatio',    value: $.t('ovs:generic.cache'),         width: 100       },
            { key: 'iops',          value: $.t('ovs:generic.iops'),          width: 55        },
            { key: 'readSpeed',     value: $.t('ovs:generic.read'),          width: 120       },
            { key: 'writeSpeed',    value: $.t('ovs:generic.write'),         width: 120       },
            { key: 'failoverMode',  value: $.t('ovs:generic.focstatus'),     width: 50        }
        ];

        // Handles
        self.loadVMachinesHandle    = undefined;
        self.refreshVMachinesHandle = {};

        // Observables
        self.vMachines            = ko.observableArray([]);
        self.vMachinesInitialLoad = ko.observable(true);

        // Functions
        self.fetchVMachines = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVMachinesHandle)) {
                    var options = {
                        sort: 'name',
                        contents: '',
                        query: JSON.stringify(self.query)
                    };
                    self.loadVMachinesHandle = api.get('vmachines', {}, options)
                        .done(function(data) {
                            var guids = [], vmdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vmdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.vMachines,
                                function(guid) {
                                    var vmachine = new VMachine(guid);
                                    if ($.inArray(guid, guids) !== -1) {
                                        vmachine.fillData(vmdata[guid]);
                                    }
                                    vmachine.loading(true);
                                    return vmachine;
                                }, 'guid'
                            );
                            self.vMachinesInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.refreshVMachines = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.refreshVMachinesHandle[page])) {
                    var options = {
                        sort: 'name',
                        page: page,
                        contents: '_dynamics,_relations,-snapshots,-hypervisor_status',
                        query: JSON.stringify(self.query)
                    };
                    self.refreshVMachinesHandle[page] = api.get('vmachines', {}, options)
                        .done(function(data) {
                            var guids = [], vmdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vmdata[item.guid] = item;
                            });
                            $.each(self.vMachines(), function(index, vm) {
                                if ($.inArray(vm.guid(), guids) !== -1) {
                                    vm.fillData(vmdata[vm.guid()]);
                                    generic.crossFiller(
                                        vm.storageRouterGuids, vm.storageRouters,
                                        function(guid) {
                                            if (!self.storageRouterCache.hasOwnProperty(guid)) {
                                                var sr = new StorageRouter(guid);
                                                sr.load('');
                                                self.storageRouterCache[guid] = sr;
                                            }
                                            return self.storageRouterCache[guid];
                                        }, 'guid'
                                    );
                                    generic.crossFiller(
                                        vm.vPoolGuids, vm.vPools,
                                        function(guid) {
                                            if (!self.vPoolCache.hasOwnProperty(guid)) {
                                                var vp = new VPool(guid);
                                                vp.load('');
                                                self.vPoolCache[guid] = vp;
                                            }
                                            return self.vPoolCache[guid];
                                        }, 'guid'
                                    );
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
            self.refresher.init(self.fetchVMachines, 5000);
            self.refresher.start();
            self.shared.footerData(self.vMachines);

            self.fetchVMachines().then(function() {
                self.refreshVMachines(1);
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
