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
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout', 'plugins/router',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vpool', '../containers/vmachine', '../containers/storagerouter',
    '../wizards/storageroutertovpool/index'
], function($, app, dialog, ko, router, shared, generic, Refresher, api, VPool, VMachine, StorageRouter, StorageRouterToVPoolWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared             = shared;
        self.guard              = { authenticated: true };
        self.refresher          = new Refresher();
        self.widgets            = [];
        self.storageRouterCache = {};
        self.vMachineCache      = {};
        self.checksInit         = false;
        self.vDiskHeaders       = [
            { key: 'name',         value: $.t('ovs:generic.name'),       width: undefined },
            { key: 'vmachine',     value: $.t('ovs:generic.vmachine'),   width: 110       },
            { key: 'size',         value: $.t('ovs:generic.size'),       width: 100       },
            { key: 'storedData',   value: $.t('ovs:generic.storeddata'), width: 110       },
            { key: 'cacheRatio',   value: $.t('ovs:generic.cache'),      width: 100       },
            { key: 'iops',         value: $.t('ovs:generic.iops'),       width: 55        },
            { key: 'readSpeed',    value: $.t('ovs:generic.read'),       width: 100       },
            { key: 'writeSpeed',   value: $.t('ovs:generic.write'),      width: 100       },
            { key: 'failoverMode', value: $.t('ovs:generic.focstatus'),  width: 50        }
        ];
        self.vMachineHeaders    = [
            { key: 'name',          value: $.t('ovs:generic.name'),          width: undefined },
            { key: 'storagerouter', value: $.t('ovs:generic.storagerouter'), width: 200       },
            { key: undefined,       value: $.t('ovs:generic.vdisks'),        width: 60        },
            { key: 'storedData',    value: $.t('ovs:generic.storeddata'),    width: 110       },
            { key: 'cacheRatio',    value: $.t('ovs:generic.cache'),         width: 100       },
            { key: 'iops',          value: $.t('ovs:generic.iops'),          width: 55        },
            { key: 'readSpeed',     value: $.t('ovs:generic.read'),          width: 120       },
            { key: 'writeSpeed',    value: $.t('ovs:generic.write'),         width: 120       },
            { key: 'failoverMode',  value: $.t('ovs:generic.focstatus'),     width: 50        }
        ];

        // Handles
        self.loadVDisksHandle         = undefined;
        self.refreshVDisksHandle      = {};
        self.loadVMachinesHandle      = undefined;
        self.refreshVMachinesHandle   = {};
        self.loadStorageRoutersHandle = undefined;

        // Observables
        self.vDisksInitialLoad         = ko.observable(true);
        self.vMachinesInitialLoad      = ko.observable(true);
        self.storageRoutersLoaded      = ko.observable(false);
        self.updatingStorageRouters    = ko.observable(false);
        self.vPool                     = ko.observable();
        self.storageRouters            = ko.observableArray([]);
        self.checkedStorageRouterGuids = ko.observableArray([]);

        // Computed
        self.pendingStorageRouters = ko.computed(function() {
            var storageRouters = [];
            $.each(self.storageRouters(), function(index, storageRouter) {
                if ($.inArray(storageRouter.guid(), self.checkedStorageRouterGuids()) !== -1 &&
                        $.inArray(storageRouter.guid(), self.vPool().storageRouterGuids()) === -1) {
                    storageRouters.push(storageRouter);
                }
            });
            return storageRouters;
        });
        self.removingStorageRouters = ko.computed(function() {
            var storageRouters = [];
            $.each(self.storageRouters(), function(index, storageRouter) {
                if ($.inArray(storageRouter.guid(), self.checkedStorageRouterGuids()) === -1 &&
                        $.inArray(storageRouter.guid(), self.vPool().storageRouterGuids()) !== -1) {
                    storageRouters.push(storageRouter);
                }
            });
            return storageRouters;
        });

        // Functions
        self.load = function() {
            return $.Deferred(function (deferred) {
                var vpool = self.vPool();
                self.vDisksInitialLoad(false);
                self.vMachinesInitialLoad(false);
                $.when.apply($, [
                    vpool.load('storagedrivers,_dynamics', { skipDisks: true }),
                    vpool.loadStorageRouters()
                        .then(function() {
                            if (self.checksInit === false) {
                                self.checkedStorageRouterGuids(self.vPool().storageRouterGuids().slice());
                                self.checksInit = true;
                            }
                        }),
                    vpool.loadVDisks(),
                    self.loadStorageRouters()
                ])
                    .fail(function(error) {
                        if (error.status === 404) {
                            router.navigate(shared.routing.loadHash('vpools'));
                        }
                    })
                    .always(deferred.resolve);
            }).promise();
        };
        self.loadStorageRouters = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadStorageRoutersHandle)) {
                    var options = {
                        sort: 'name',
                        contents: 'storagedrivers'
                    };
                    self.loadStorageRoutersHandle = api.get('storagerouters', { queryparams: options })
                        .done(function(data) {
                            var guids = [], sadata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                sadata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.storageRouters,
                                function(guid) {
                                    return new StorageRouter(guid);
                                }, 'guid'
                            );
                            $.each(self.storageRouters(), function(index, storageRouter) {
                                if (sadata.hasOwnProperty(storageRouter.guid())) {
                                    storageRouter.fillData(sadata[storageRouter.guid()]);
                                }
                            });
                            self.storageRoutersLoaded(true);
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
                        sort: 'devicename',
                        page: page,
                        contents: '_dynamics,_relations,-snapshots',
                        vpoolguid: self.vPool().guid()
                    };
                    self.refreshVDisksHandle[page] = api.get('vdisks', { queryparams: options })
                        .done(function(data) {
                            var guids = [], vddata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vddata[item.guid] = item;
                            });
                            $.each(self.vPool().vDisks(), function(index, vdisk) {
                                if ($.inArray(vdisk.guid(), guids) !== -1) {
                                    vdisk.fillData(vddata[vdisk.guid()]);
                                    var vm, vMachineGuid = vdisk.vMachineGuid();
                                    if (vMachineGuid && (vdisk.vMachine() === undefined || vdisk.vMachine().guid() !== vMachineGuid)) {
                                        if (!self.vMachineCache.hasOwnProperty(vMachineGuid)) {
                                            vm = new VMachine(vMachineGuid);
                                            vm.load('');
                                            self.vMachineCache[vMachineGuid] = vm;
                                        }
                                        vdisk.vMachine(self.vMachineCache[vMachineGuid]);
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
        self.refreshVMachines = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.refreshVMachinesHandle[page])) {
                    var options = {
                        sort: 'name',
                        page: page,
                        contents: '_dynamics,-snapshots,-hypervisor_status',
                        vpoolguid: self.vPool().guid()
                    };
                    self.refreshVMachinesHandle[page] = api.get('vmachines', { queryparams: options })
                        .done(function(data) {
                            var guids = [], vmdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vmdata[item.guid] = item;
                            });
                            $.each(self.vPool().vMachines(), function(index, vmachine) {
                                if ($.inArray(vmachine.guid(), guids) !== -1) {
                                    if (!self.vMachineCache.hasOwnProperty(vmachine.guid())) {
                                        self.vMachineCache[vmachine.guid()] = vmachine;
                                    }
                                    vmachine.fillData(vmdata[vmachine.guid()]);
                                    generic.crossFiller(
                                        vmachine.storageRouterGuids, vmachine.storageRouters,
                                        function(guid) {
                                            if (!self.storageRouterCache.hasOwnProperty(guid)) {
                                                var sr = new StorageRouter(guid);
                                                sr.load('');
                                                self.storageRouterCache[guid] = sr;
                                            }
                                            return self.storageRouterCache[guid];
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
        self.sync = function() {
            if (self.vPool() !== undefined) {
                var vp = self.vPool();
                app.showMessage(
                        $.t('ovs:vpools.sync.warning'),
                        $.t('ovs:vpools.sync.title', { what: vp.name() }),
                        [$.t('ovs:vpools.sync.no'), $.t('ovs:vpools.sync.yes')]
                    )
                    .done(function(answer) {
                        if (answer === $.t('ovs:vpools.sync.yes')) {
                            generic.alertInfo(
                                $.t('ovs:vpools.sync.marked'),
                                $.t('ovs:vpools.sync.markedmsg', { what: vp.name() })
                            );
                            api.post('vpools/' + vp.guid() + '/sync_vmachines')
                                .then(self.shared.tasks.wait)
                                .done(function() {
                                    generic.alertSuccess(
                                        $.t('ovs:vpools.sync.done'),
                                        $.t('ovs:vpools.sync.donemsg', { what: vp.name() })
                                    );
                                })
                                .fail(function(error) {
                                    generic.alertError(
                                        $.t('ovs:generic.error'),
                                        $.t('ovs:generic.messages.errorwhile', {
                                            context: 'error',
                                            what: $.t('ovs:vpools.sync.errormsg', { what: vp.name() }),
                                            error: error.responseText
                                        })
                                    );
                                });
                        }
                    });
            }
        };
        self.updateStorageRouterServing = function() {
            self.updatingStorageRouters(true);
            var deferred = $.Deferred(), wizard;
            wizard = new StorageRouterToVPoolWizard({
                modal: true,
                completed: deferred,
                vPool: self.vPool(),
                pendingStorageRouters: self.pendingStorageRouters,
                removingStorageRouters: self.removingStorageRouters
            });
            wizard.closing.always(function() {
                self.load();
                deferred.resolve();
            });
            dialog.show(wizard);
            deferred.always(function() {
                self.updatingStorageRouters(false);
            });
        };
        self.saveConfiguration = function() {
            if (self.vPool() !== undefined) {
                var vp = self.vPool();
                api.post('vpools/' + vp.guid() + '/set_configparams', {
                    data: { configparams: vp._configuration() }
                })
                    .then(self.shared.tasks.wait)
                    .done(function () {
                        generic.alertSuccess(
                            $.t('ovs:vpools.saveconfig.done'),
                            $.t('ovs:vpools.saveconfig.donemsg', { what: vp.name() })
                        );
                    })
                    .fail(function (error) {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:generic.messages.errorwhile', {
                                context: 'error',
                                what: $.t('ovs:vpools.saveconfig.errormsg', { what: vp.name() }),
                                error: error.responseText
                            })
                        );
                    });
            }
        };

        // Durandal
        self.activate = function(mode, guid) {
            self.vPool(new VPool(guid));
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
            self.shared.footerData(self.vPool);
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
