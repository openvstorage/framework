// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/OVS_NON_COMMERCIAL
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
    '../containers/vdisk', '../containers/vmachine', '../containers/vpool', '../containers/storagerouter',
    '../wizards/rollback/index', '../wizards/snapshot/index'
], function($, dialog, ko, shared, generic, Refresher, api, VDisk, VMachine, VPool, StorageRouter, RollbackWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared            = shared;
        self.guard             = { authenticated: true };
        self.refresher         = new Refresher();
        self.widgets           = [];
        self.snapshotHeaders   = [
            { key: 'label',         value: $.t('ovs:generic.description'), width: undefined },
            { key: 'timestamp',     value: $.t('ovs:generic.datetime'),    width: 200       },
            { key: 'stored',        value: $.t('ovs:generic.storeddata'),  width: 110       },
            { key: 'is_automatic',  value: $.t('ovs:generic.type'),        width: 110       },
            { key: 'is_consistent', value: $.t('ovs:generic.consistent'),  width: 100       }
        ];

        // Observables
        self.snapshotsInitialLoad = ko.observable(true);
        self.vDisk                = ko.observable();

        // Functions
        self.load = function() {
            return $.Deferred(function (deferred) {
                var vdisk = self.vDisk();
                vdisk.load()
                    .then(function() {
                        self.snapshotsInitialLoad(false);
                        var vm, sr, pool,
                            storageRouterGuid = vdisk.storageRouterGuid(),
                            vMachineGuid = vdisk.vMachineGuid(),
                            vPoolGuid = vdisk.vpoolGuid();
                        if (storageRouterGuid && (vdisk.storageRouter() === undefined || vdisk.storageRouter().guid() !== storageRouterGuid)) {
                            sr = new StorageRouter(storageRouterGuid);
                            sr.load();
                            vdisk.storageRouter(sr);
                        }
                        if (vMachineGuid && (vdisk.vMachine() === undefined || vdisk.vMachine().guid() !== vMachineGuid)) {
                            vm = new VMachine(vMachineGuid);
                            vm.load();
                            vdisk.vMachine(vm);
                        }
                        if (vPoolGuid && (vdisk.vpool() === undefined || vdisk.vpool().guid() !== vPoolGuid)) {
                            pool = new VPool(vPoolGuid);
                            pool.load();
                            vdisk.vpool(pool);
                        }
                    })
                    .always(deferred.resolve);
            }).promise();
        };
        self.refreshSnapshots = function() {
            // Not un use, for mapping only
        };
        self.formatBytes = function(value) {
            return generic.formatBytes(value);
        };
        self.rollback = function() {
            if (self.vDisk() !== undefined) {
                var vdisk = self.vDisk();
                if (vdisk.vMachine() === undefined || !vdisk.vMachine().isRunning()) {
                    dialog.show(new RollbackWizard({
                        modal: true,
                        type: 'vdisk',
                        guid: vdisk.guid()
                    }));
                }
            }
        };
        self.saveConfiguration = function() {
            if (self.vDisk() !== undefined) {
                var vd = self.vDisk();
                api.post('vdisks/' + vd.guid() + '/set_config_params', {
                    data: { new_config_params: vd.configuration(),
                            old_config_params: vd.oldConfiguration() }
                })
                    .then(self.shared.tasks.wait)
                    .done(function () {
                        generic.alertSuccess(
                            $.t('ovs:vdisks.saveconfig.done'),
                            $.t('ovs:vdisks.saveconfig.donemsg', { what: vd.name() })
                        );
                    })
                    .fail(function (error) {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:generic.messages.errorwhile', {
                                what: $.t('ovs:vdisks.saveconfig.errormsg', { what: vd.name() })
                            })
                        );
                    })
                    .always(function() {
                        vd.loadConfiguration(true);
                    });
                vd.oldConfiguration($.extend({}, vd.configuration()));
            }
        };

        // Durandal
        self.activate = function(mode, guid) {
            self.vDisk(new VDisk(guid));
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
            self.shared.footerData(self.vDisk);
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
