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
    '../containers/vmachine', '../containers/pmachine', '../containers/vpool', '../containers/storagerouter',
    '../wizards/rollback/index', '../wizards/snapshot/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, VMachine, PMachine, VPool, StorageRouter, RollbackWizard, SnapshotWizard) {
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
        self.pMachineCache      = {};
        self.vDiskHeaders       = [
            { key: 'name',         value: $.t('ovs:generic.name'),         width: undefined },
            { key: 'size',         value: $.t('ovs:generic.size'),         width: 100       },
            { key: 'storedData',   value: $.t('ovs:generic.storeddata'),   width: 110       },
            { key: 'cacheRatio',   value: $.t('ovs:generic.cache'),        width: 100       },
            { key: 'iops',         value: $.t('ovs:generic.iops'),         width: 55        },
            { key: 'readSpeed',    value: $.t('ovs:generic.read'),         width: 100       },
            { key: 'writeSpeed',   value: $.t('ovs:generic.write'),        width: 100       },
            { key: 'failoverMode', value: $.t('ovs:generic.focstatus'),    width: 50        }
        ];
        self.snapshotHeaders    = [
            { key: 'label',         value: $.t('ovs:generic.description'), width: undefined },
            { key: 'timestamp',     value: $.t('ovs:generic.datetime'),    width: 200       },
            { key: 'stored',        value: $.t('ovs:generic.storeddata'),  width: 110       },
            { key: 'is_automatic',  value: $.t('ovs:generic.type'),        width: 110       },
            { key: 'is_consistent', value: $.t('ovs:generic.consistent'),  width: 100       }
        ];

        // Handles
        self.loadVDisksHandle    = undefined;
        self.refreshVDisksHandle = {};

        // Observables
        self.vDisksInitialLoad    = ko.observable(true);
        self.snapshotsInitialLoad = ko.observable(true);
        self.vMachine             = ko.observable();

        // Functions
        self.load = function() {
            return $.Deferred(function (deferred) {
                var vm = self.vMachine();
                vm.load()
                    .then(function() {
                        self.vDisksInitialLoad(false);
                        self.snapshotsInitialLoad(false);
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
                        var pMachineGuid = vm.pMachineGuid(), pm;
                        if (pMachineGuid && (vm.pMachine() === undefined || vm.pMachine().guid() !== pMachineGuid)) {
                            if (!self.pMachineCache.hasOwnProperty(pMachineGuid)) {
                                pm = new PMachine(pMachineGuid);
                                pm.load();
                                self.pMachineCache[pMachineGuid] = pm;
                            }
                            vm.pMachine(self.pMachineCache[pMachineGuid]);
                        }
                    })
                    .always(deferred.resolve);
            }).promise();
        };
        self.refreshSnapshots = function() {
            // Not un use, for mapping only
        };
        self.refreshVDisks = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.refreshVDisksHandle[page])) {
                    var options = {
                        sort: 'name',
                        page: page,
                        contents: '_dynamics,-snapshots',
                        vmachineguid: self.vMachine().guid()
                    };
                    self.refreshVDisksHandle[page] = api.get('vdisks', { queryparams: options })
                        .done(function(data) {
                            var guids = [], vddata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vddata[item.guid] = item;
                            });
                            $.each(self.vMachine().vDisks(), function(index, vdisk) {
                                if ($.inArray(vdisk.guid(), guids) !== -1) {
                                    vdisk.fillData(vddata[vdisk.guid()]);
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
        self.formatBytes = function(value) {
            return generic.formatBytes(value);
        };
        self.rollback = function() {
            if (self.vMachine() !== undefined) {
                var vm = self.vMachine();
                if (!vm.isRunning()) {
                    dialog.show(new RollbackWizard({
                        modal: true,
                        type: 'vmachine',
                        guid: vm.guid()
                    }));
                }
            }
        };
        self.snapshot = function() {
            if (self.vMachine() !== undefined) {
                var vm = self.vMachine();
                dialog.show(new SnapshotWizard({
                    modal: true,
                    machineguid: vm.guid()
                }));
            }
        };
        self.setAsTemplate = function() {
            if (self.vMachine() !== undefined) {
                var vm = self.vMachine();
                if (!vm.isRunning()) {
                    app.showMessage(
                            $.t('ovs:vmachines.setastemplate.warning'),
                            $.t('ovs:vmachines.setastemplate.title', { what: vm.name() }),
                            [$.t('ovs:vmachines.setastemplate.no'), $.t('ovs:vmachines.setastemplate.yes')]
                        )
                        .done(function(answer) {
                            if (answer === $.t('ovs:vmachines.setastemplate.yes')) {
                                generic.alertInfo(
                                    $.t('ovs:vmachines.setastemplate.marked'),
                                    $.t('ovs:vmachines.setastemplate.markedmsg', { what: vm.name() })
                                );
                                api.post('vmachines/' + vm.guid() + '/set_as_template')
                                    .then(self.shared.tasks.wait)
                                    .done(function() {
                                        generic.alertSuccess(
                                            $.t('ovs:vmachines.setastemplate.done'),
                                            $.t('ovs:vmachines.setastemplate.donemsg', { what: vm.name() })
                                        );
                                    })
                                    .fail(function(error) {
                                        generic.alertError(
                                            $.t('ovs:generic.error'),
                                            $.t('ovs:generic.messages.errorwhile', {
                                                context: 'error',
                                                what: $.t('ovs:vmachines.setastemplate.errormsg', { what: vm.name() }),
                                                error: error.responseText
                                            })
                                        );
                                    });
                            }
                        });
                }
            }
        };

        // Durandal
        self.activate = function(mode, guid) {
            self.vMachine(new VMachine(guid));
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
            self.shared.footerData(self.vMachine);
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
