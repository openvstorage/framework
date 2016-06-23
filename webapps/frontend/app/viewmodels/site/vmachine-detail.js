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
/*global define */
define([
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout', 'plugins/router',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vdisk', '../containers/vmachine', '../containers/pmachine',
    '../containers/vpool', '../containers/storagerouter',
    '../wizards/rollback/index', '../wizards/snapshot/index'
], function($, app, dialog, ko, router, shared, generic, Refresher, api, VDisk, VMachine, PMachine, VPool, StorageRouter, RollbackWizard, SnapshotWizard) {
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
            { key: 'name',       value: $.t('ovs:generic.name'),         width: undefined },
            { key: 'size',       value: $.t('ovs:generic.size'),         width: 100       },
            { key: 'storedData', value: $.t('ovs:generic.storeddata'),   width: 135       },
            { key: 'iops',       value: $.t('ovs:generic.iops'),         width: 90        },
            { key: 'readSpeed',  value: $.t('ovs:generic.read'),         width: 125       },
            { key: 'writeSpeed', value: $.t('ovs:generic.write'),        width: 125       },
            { key: 'dtlStatus',  value: $.t('ovs:generic.dtl_status'),   width: 50        }
        ];
        self.snapshotHeaders    = [
            { key: 'label',         value: $.t('ovs:generic.description'), width: undefined },
            { key: 'timestamp',     value: $.t('ovs:generic.datetime'),    width: 200       },
            { key: 'stored',        value: $.t('ovs:generic.storeddata'),  width: 110       },
            { key: 'is_automatic',  value: $.t('ovs:generic.type'),        width: 110       },
            { key: 'is_consistent', value: $.t('ovs:generic.consistent'),  width: 100       },
            { key: 'is_sticky',     value: $.t('ovs:generic.sticky'),      width: 100       }
        ];

        // Handles
        self.vDisksHandle = {};

        // Observables
        self.convertingToTemplate = ko.observable(false);
        self.snapshotsInitialLoad = ko.observable(true);
        self.vMachine             = ko.observable();

        // Functions
        self.load = function() {
            return $.Deferred(function (deferred) {
                var vm = self.vMachine();
                vm.load()
                    .then(function() {
                        if (self.vMachine().isVTemplate()) {
                            router.navigate(shared.routing.loadHash('vmachines'));
                            return deferred.reject();
                        }
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
                    .fail(function(error) {
                        if (error !== undefined && error.status === 404) {
                            router.navigate(shared.routing.loadHash('vmachines'));
                        }
                    })
                    .always(deferred.resolve);
            }).promise();
        };
        self.refreshSnapshots = function() {
            // Not in use, for mapping only
        };
        self.loadVDisks = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.vDisksHandle[options.page])) {
                    options.sort = 'name';
                    options.contents = '_dynamics,-snapshots';
                    options.vmachineguid = self.vMachine().guid();
                    self.vDisksHandle[options.page] = api.get('vdisks', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    return new VDisk(guid);
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
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
                dialog.show(new SnapshotWizard({
                    modal: true,
                    mode: 'vmachine',
                    guid: self.vMachine().guid()
                }));
            }
        };
        self.setAsTemplate = function() {
            if (self.vMachine() !== undefined) {
                var vm = self.vMachine();
                if (!vm.isRunning()) {
                    self.convertingToTemplate(true);
                    app.showMessage(
                            $.t('ovs:vmachines.set_as_template.warning'),
                            $.t('ovs:vmachines.set_as_template.title', { what: vm.name() }),
                            [$.t('ovs:vmachines.set_as_template.no'), $.t('ovs:vmachines.set_as_template.yes')]
                        )
                        .done(function(answer) {
                            if (answer === $.t('ovs:vmachines.set_as_template.yes')) {
                                generic.alertInfo(
                                    $.t('ovs:vmachines.set_as_template.marked'),
                                    $.t('ovs:vmachines.set_as_template.marked_msg', { what: vm.name() })
                                );
                                api.post('vmachines/' + vm.guid() + '/set_as_template')
                                    .then(self.shared.tasks.wait)
                                    .done(function() {
                                        generic.alertSuccess(
                                            $.t('ovs:vmachines.set_as_template.done'),
                                            $.t('ovs:vmachines.set_as_template.done_msg', { what: vm.name() })
                                        );
                                        router.navigate(shared.routing.loadHash('vtemplates'));
                                    })
                                    .fail(function(error) {
                                        generic.alertError(
                                            $.t('ovs:generic.error'),
                                            $.t('ovs:generic.messages.errorwhile', {what: $.t('ovs:vmachines.set_as_template.error_msg', {what: vm.name(), error: error})}));
                                    })
                                    .always(function() {
                                        self.convertingToTemplate(false);
                                    });
                            } else {
                                self.convertingToTemplate(true);
                            }
                        });
                }
            }
        };

        // Durandal
        self.activate = function(mode, guid) {
            self.vMachine(new VMachine(guid));
            self.refresher.init(self.load, 5000);
            self.refresher.start();
            self.load()
                .then(function() {
                    self.loadVDisks({page: 1});
                });
        };
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
            self.refresher.stop();
        };
    };
});
