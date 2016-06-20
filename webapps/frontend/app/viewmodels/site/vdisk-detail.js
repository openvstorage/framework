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
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vdisk', '../containers/vmachine', '../containers/vpool', '../containers/storagerouter', '../containers/domain',
    '../wizards/rollback/index', '../wizards/clone/index'
], function($, app, dialog, ko, shared, generic, Refresher, api, VDisk, VMachine, VPool, StorageRouter, Domain, RollbackWizard, CloneWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.domainCache     = {};
        self.shared          = shared;
        self.guard           = { authenticated: true };
        self.refresher       = new Refresher();
        self.widgets         = [];
        self.snapshotHeaders = [
            { key: 'label',         value: $.t('ovs:generic.description'), width: undefined },
            { key: 'timestamp',     value: $.t('ovs:generic.datetime'),    width: 200       },
            { key: 'stored',        value: $.t('ovs:generic.storeddata'),  width: 110       },
            { key: 'is_automatic',  value: $.t('ovs:generic.type'),        width: 110       },
            { key: 'is_consistent', value: $.t('ovs:generic.consistent'),  width: 100       },
            { key: 'is_sticky',     value: $.t('ovs:generic.sticky'),      width: 100       },
            { key: undefined,       value: $.t('ovs:generic.actions'),     width: 60        }
        ];

        // Observables
        self.domains              = ko.observableArray([]);
        self.snapshotsInitialLoad = ko.observable(true);
        self.vDisk                = ko.observable();

        // Handles
        self.loadDomainHandle         = undefined;
        self.loadStorageRoutersHandle = undefined;

        // Functions
        self.load = function() {
            return $.Deferred(function (deferred) {
                self.vDisk().load()
                    .then(self.loadStorageRouters)
                    .then(self.loadDomains)
                    .then(function() {
                        self.snapshotsInitialLoad(false);
                        var vm, sr, pool, vdisk = self.vDisk(),
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
        self.loadStorageRouters = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadStorageRouterHandle)) {
                    self.loadStorageRouterHandle = api.get('storagerouters', {queryparams: {contents: ''}})
                        .done(function (data) {
                            var guids = [];
                            $.each(data.data, function (index, item) {
                                guids.push(item.guid);
                            });
                            self.vDisk().storageRouterGuids(guids);
                        })
                        .always(deferred.resolve());
                }
            }).promise();
        };
        self.loadDomains = function() {
            return $.Deferred(function(deferred) {
                var vdisk = self.vDisk();
                if (vdisk !== undefined && generic.xhrCompleted(self.loadDomainHandle)) {
                    self.loadDomainHandle = api.get('domains', { queryparams: { contents: 'storage_router_layout' }})
                        .done(function(data) {
                            var guids = [], ddata = {}, domainsPresent = false;
                            $.each(data.data, function(index, item) {
                                if (item.storage_router_layout.regular.contains(self.vDisk().storageRouterGuid())) {
                                    domainsPresent = true;
                                    if (item.storage_router_layout.regular.length > 1) {
                                        guids.push(item.guid);
                                        ddata[item.guid] = item;
                                    }
                                } else if (item.storage_router_layout.regular.length > 0)  {
                                    domainsPresent = true;
                                    guids.push(item.guid);
                                    ddata[item.guid] = item;
                                }
                            });
                            self.vDisk().domainsPresent(domainsPresent);
                            self.vDisk().dtlTargets(guids);
                            generic.crossFiller(
                                guids, self.domains,
                                function(guid) {
                                    return new Domain(guid);
                                }, 'guid'
                            );
                            $.each(self.domains(), function(index, domain) {
                                if (!self.domainCache.hasOwnProperty(domain.guid())) {
                                    self.domainCache[domain.guid()] = domain;
                                }
                                if (ddata.hasOwnProperty(domain.guid())) {
                                    domain.fillData(ddata[domain.guid()]);
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
        self.refreshSnapshots = function() {
            // Not in use, for mapping only
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
        self.clone = function() {
            if (self.vDisk() !== undefined) {
                dialog.show(new CloneWizard({
                    modal: true,
                    vdisk: self.vDisk()
                }));
            }
        };
        self.saveConfiguration = function() {
            if (self.vDisk() !== undefined) {
                var vd = self.vDisk();
                api.post('vdisks/' + vd.guid() + '/set_config_params', {
                    data: { new_config_params: vd.configuration() }
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
        self.removeSnapshot = function(snapshotid) {
            app.showMessage(
                $.t('ovs:vdisks.removesnapshot.delete', { what: snapshotid }),
                $.t('ovs:generic.areyousure'),
                [$.t('ovs:generic.no'), $.t('ovs:generic.yes')]
            )
            .done(function(answer) {
                if (answer === $.t('ovs:generic.yes')) {
                    api.post('vdisks/' + self.vDisk().guid() + '/removesnapshot', {
                        data: { snapshot_id: snapshotid }
                    })
                        .then(self.shared.tasks.wait)
                        .done(function () {
                            generic.alertSuccess(
                                $.t('ovs:vdisks.removesnapshot.done'),
                                $.t('ovs:vdisks.removesnapshot.donemsg', { what: snapshotid })
                            );
                        })
                        .fail(function (error) {
                            generic.alertError(
                                $.t('ovs:generic.error'),
                                $.t('ovs:generic.messages.errorwhile', {
                                    what: $.t('ovs:vdisks.removesnapshot.errormsg', { what: snapshotid })
                                })
                            );
                        })
                        .always(function () {
                            self.load();
                        });
            }});
        };

        // Durandal
        self.activate = function(mode, guid) {
            self.vDisk(new VDisk(guid));
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
        };
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
            self.refresher.stop();
        };
    };
});
