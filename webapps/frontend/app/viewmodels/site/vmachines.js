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
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vmachine', '../containers/vpool', '../wizards/rollback/index', '../wizards/snapshot/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, VMachine, VPool, RollbackWizard, SnapshotWizard) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared      = shared;
        self.guard       = { authenticated: true };
        self.refresher   = new Refresher();
        self.widgets     = [];

        // Data
        self.vMachineHeaders = [
            { key: 'name',         value: $.t('ovs:generic.name'),       width: 150       },
            { key: 'vpool',        value: $.t('ovs:generic.vpool'),      width: 150       },
            { key: 'vsa',          value: $.t('ovs:generic.vsa'),        width: 100       },
            { key: undefined,      value: $.t('ovs:generic.vdisks'),     width: 60        },
            { key: 'storedData',   value: $.t('ovs:generic.storeddata'), width: 110       },
            { key: 'cacheRatio',   value: $.t('ovs:generic.cache'),      width: 100       },
            { key: 'iops',         value: $.t('ovs:generic.iops'),       width: 55        },
            { key: 'readSpeed',    value: $.t('ovs:generic.read'),       width: 120       },
            { key: 'writeSpeed',   value: $.t('ovs:generic.write'),      width: 120       },
            { key: 'failoverMode', value: $.t('ovs:generic.focstatus'),  width: undefined },
            { key: undefined,      value: $.t('ovs:generic.actions'),    width: 100       }
        ];
        self.vMachines = ko.observableArray([]);
        self.vPoolCache = {};
        self.vsaCache = {};
        self.vMachinesInitialLoad = ko.observable(true);

        // Variables
        self.loadVMachinesHandle = undefined;
        self.refreshVMachinesHandle = {};
        self.query = {
            query: {
                type: 'AND',
                items: [['is_internal', 'EQUALS', false],
                        ['is_vtemplate', 'EQUALS', false],
                        ['status', 'NOT_EQUALS', 'CREATED']]
            }
        };

        // Functions
        self.fetchVMachines = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVMachinesHandle)) {
                    self.loadVMachinesHandle = api.post('vmachines/filter', self.query, { sort: 'name' })
                        .done(function(data) {
                            var guids = [];
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                            });
                            generic.crossFiller(
                                guids, self.vMachines,
                                function(guid) {
                                    var vmachine = new VMachine(guid);
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
        self.refreshVMachines = function(page, abort) {
            abort = abort || false;
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.refreshVMachinesHandle[page]) || abort) {
                    if (abort) {
                        generic.xhrAbort(self.refreshVMachinesHandle[page]);
                    }
                    var options = {
                        sort: 'name',
                        full: true,
                        page: page
                    };
                    self.refreshVMachinesHandle[page] = api.post('vmachines/filter', self.query, options)
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
                                        vm.vSAGuids, vm.vSAs,
                                        function(guid) {
                                            if (!self.vsaCache.hasOwnProperty(guid)) {
                                                var vm = new VMachine(guid);
                                                vm.load();
                                                self.vsaCache[guid] = vm;
                                            }
                                            return self.vsaCache[guid];
                                        }, 'guid'
                                    );
                                    generic.crossFiller(
                                        vm.vPoolGuids, vm.vPools,
                                        function(guid) {
                                            if (!self.vPoolCache.hasOwnProperty(guid)) {
                                                var vp = new VPool(guid);
                                                vp.load();
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
                    deferred.reject();
                }
            }).promise();
        };
        self.rollback = function(guid) {
            $.each(self.vMachines(), function(index, vm) {
                if (vm.guid() === guid && !vm.isRunning()) {
                    dialog.show(new RollbackWizard({
                        modal: true,
                        type: 'vmachine',
                        guid: guid
                    }));
                }
            });
        };
        self.snapshot = function(guid) {
            dialog.show(new SnapshotWizard({
                modal: true,
                machineguid: guid
            }));
        };
        self.setAsTemplate = function(guid) {
            $.each(self.vMachines(), function(index, vm) {
                if (vm.guid() === guid && !vm.isRunning()) {
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
                                        self.vMachines.destroy(vm);
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
                                                error: error
                                            })
                                        );
                                    });
                            }
                        });
                }
            });
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
