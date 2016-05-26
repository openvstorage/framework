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
    'jquery', 'knockout', 'plugins/dialog',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/storagerouter', '../containers/pmachine', '../containers/vpool', '../containers/storagedriver', '../containers/domain',
    '../wizards/configurepartition/index'
], function($, ko, dialog, shared, generic, Refresher, api, StorageRouter, PMachine, VPool, StorageDriver, Domain, ConfigurePartitionWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared                   = shared;
        self.guard                    = { authenticated: true };
        self.refresher                = new Refresher();
        self.widgets                  = [];
        self.domainCache              = {};
        self.loadVPoolsHandle         = undefined;
        self.loadStorageDriversHandle = {};
        self.loadDomainsHandle        = undefined;

        // Observables
        self.domains           = ko.observableArray([]);
        self.domainsLoaded     = ko.observable(false);
        self.refreshing        = ko.observable(false);
        self.storageRouter     = ko.observable();
        self.vPools            = ko.observableArray([]);
        self.vPoolsLoaded      = ko.observable(false);
        self.checkedVPoolGuids = ko.observableArray([]);

        // Computed
        self.domainGuids = ko.computed(function() {
            var guids = [], storageRouter = self.storageRouter();
            if (storageRouter === undefined) {
                return guids;
            }
            $.each(self.domains(), function(index, domain) {
                if (!storageRouter.recoveryDomainGuids().contains(domain.guid())) {
                    guids.push(domain.guid());
                }
            });
            return guids;
        });
        self.recoveryDomainGuids = ko.computed(function() {
            var guids = [], storageRouter = self.storageRouter();
            if (storageRouter === undefined) {
                return guids;
            }
            $.each(self.domains(), function(index, domain) {
                if (!storageRouter.domainGuids().contains(domain.guid())) {
                    guids.push(domain.guid());
                }
            });
            return guids;
        });

        // Functions
        self.load = function() {
            return $.Deferred(function (deferred) {
                var storageRouter = self.storageRouter(), calls = [];
                if (!storageRouter.edit()) {
                    calls.push(storageRouter.load('_dynamics,_relations'))
                }
                calls.push(storageRouter.getAvailableActions());
                calls.push(storageRouter.getDisks());
                calls.push(self.loadDomains());
                $.when.apply($, calls)
                    .then(self.loadStorageDrivers)
                    .then(self.loadVPools)
                    .done(function() {
                        self.checkedVPoolGuids(self.storageRouter().vPoolGuids());
                        if (storageRouter.pMachine() !== undefined && !storageRouter.pMachine().loaded()) {
                            storageRouter.pMachine().load();
                        }
                        // Move child guids to the observables for easy display
                        storageRouter.vPools(storageRouter.vPoolGuids());
                        storageRouter.vMachines(storageRouter.vMachineGuids);
                    })
                    .always(deferred.resolve);
            }).promise();
        };
        self.loadVPools = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVPoolsHandle)) {
                    self.loadVPoolsHandle = api.get('vpools', {queryparams: {sort: 'name', contents: 'storagedrivers'}})
                        .done(function(data) {
                            var guids = [], vpdata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                vpdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.vPools,
                                function(guid) {
                                    var vpool = new VPool(guid);
                                    vpool.fillData(vpdata[guid]);
                                    return vpool;
                                }, 'guid'
                            );
                            self.vPoolsLoaded(true);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.loadDomains = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadDomainsHandle)) {
                    self.loadDomainsHandle = api.get('domains', {
                        queryparams: { sort: 'name', contents: '' }
                    })
                        .done(function(data) {
                            var guids = [], ddata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                ddata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.domains,
                                function(guid) {
                                    return new Domain(guid);
                                }, 'guid'
                            );
                            $.each(self.domains(), function(index, domain) {
                                if (ddata.hasOwnProperty(domain.guid())) {
                                    domain.fillData(ddata[domain.guid()]);
                                }
                                self.domainCache[domain.guid()] = domain;
                            });
                            self.domains.sort(function(dom1, dom2) {
                                return dom1.name() < dom2.name() ? -1 : 1;
                            });
                            self.domainsLoaded(true);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.loadStorageDrivers = function() {
            return $.Deferred(function(deferred) {
                $.each(self.storageRouter().storageDriverGuids, function(index, guid) {
                    if (generic.xhrCompleted(self.loadStorageDriversHandle[guid])) {
                        self.loadStorageDriversHandle[guid] = api.get('storagedrivers/' + guid)
                            .done(function(data) {
                                var storageDriverFound = false, storageDriver;
                                $.each(self.storageRouter().storageDrivers(), function(vindex, storageDriver) {
                                    if (storageDriver.guid() === guid) {
                                        storageDriver.fillData(data);
                                        storageDriverFound = true;
                                        return false;
                                    }
                                    return true;
                                });
                                if (storageDriverFound === false) {
                                    storageDriver = new StorageDriver(data.guid);
                                    storageDriver.fillData(data);
                                    self.storageRouter().storageDrivers.push(storageDriver);
                                }
                            });
                    }
                });
                deferred.resolve();
            }).promise();
        };
        self.isEmpty = generic.isEmpty;
        self.configureRoles = function(partition, disk) {
            if (self.shared.user.roles().contains('manage')) {
                dialog.show(new ConfigurePartitionWizard({
                    modal: true,
                    partition: partition,
                    disk: disk,
                    storageRouter: self.storageRouter()
                }));
            }
        };
        self.rescanDisks = function() {
            api.post('storagerouters/' + self.storageRouter().guid() + '/rescan_disks')
                .then(shared.tasks.wait)
                .done(function() {
                    generic.alertSuccess($.t('ovs:generic.saved'), $.t('ovs:storagerouters.detail.disks.rescan.success'));
                })
                .fail(function() {
                    generic.alertError($.t('ovs:generic.error'), $.t('ovs:generic.messages.errorwhile', { what: $.t('ovs:storagerouters.detail.disks.rescan.scanning') }));
                });
            generic.alertInfo($.t('ovs:storagerouters.detail.disks.rescan.started'), $.t('ovs:storagerouters.detail.disks.rescan.inprogress'));
        };
        self.refresh = function() {
            self.refreshing(true);
            api.post('storagerouters/' + self.storageRouter().guid() + '/refresh_hardware')
                .then(shared.tasks.wait)
                .done(function() {
                    generic.alertSuccess($.t('ovs:generic.saved'), $.t('ovs:storagerouters.detail.refresh.success'));
                })
                .fail(function() {
                    generic.alertError($.t('ovs:generic.error'), $.t('ovs:generic.messages.errorwhile', { what: $.t('ovs:storagerouters.detail.refresh.refreshing') }));
                })
                .always(function() {
                    self.refreshing(false);
                });
            generic.alertInfo($.t('ovs:storagerouters.detail.refresh.started'), $.t('ovs:storagerouters.detail.refresh.inprogress'));
        };

        // Durandal
        self.activate = function(mode, guid) {
            self.storageRouter(new StorageRouter(guid));
            self.storageRouter().storageDrivers = ko.observableArray();
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
            self.shared.footerData(self.storageRouter);
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
