// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Modified Apache License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'jquery', 'knockout', 'plugins/dialog',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/storagerouter', '../containers/pmachine', '../containers/vpool', '../containers/storagedriver', '../containers/failuredomain',
    '../wizards/configurepartition/index'
], function($, ko, dialog, shared, generic, Refresher, api, StorageRouter, PMachine, VPool, StorageDriver, FailureDomain, ConfigurePartitionWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared                   = shared;
        self.guard                    = { authenticated: true, registered: true };
        self.refresher                = new Refresher();
        self.widgets                  = [];
        self.fdCache                  = {};
        self.pMachineCache            = {};
        self.vPoolCache               = {};
        self.vMachineCache            = {};
        self.loadVPoolsHandle         = undefined;
        self.loadStorageDriversHandle = {};
        self.loadFailureDomainsHandle = undefined;

        // Observables
        self.refreshing              = ko.observable(false);
        self.storageRouter           = ko.observable();
        self.vPoolsLoaded            = ko.observable(false);
        self.vPools                  = ko.observableArray([]);
        self.checkedVPoolGuids       = ko.observableArray([]);
        self.failureDomains          = ko.observableArray([]);
        self.secondaryFailureDomains = ko.observableArray([]);

        // Computed
        self.availableSecondaryFailureDomains = ko.computed(function() {
            var domains = [undefined], primary_guid, storageRouter = self.storageRouter(), secondary, guids;
            if (storageRouter !== undefined) {
                secondary = storageRouter.secondaryFailureDomainGuid;
                $.each(self.secondaryFailureDomains(), function (index, domain) {
                    primary_guid = storageRouter.primaryFailureDomainGuid();
                    if (domain.guid() === primary_guid) {
                        if (!domain.primarySRGuids().contains(storageRouter.guid())) {
                            domain.primarySRGuids.push(storageRouter.guid());
                        }
                    } else {
                        guids = domain.primarySRGuids();
                        guids.remove(storageRouter.guid());
                        domain.primarySRGuids(guids);
                    }
                    domain.disabled(
                        domain.guid() === storageRouter.primaryFailureDomainGuid() ||
                        domain.primarySRGuids().length === 0
                    );
                    domains.push(domain);
                    if (secondary() !== undefined && domain.guid() === secondary() && domain.disabled()) {
                        secondary(undefined);
                    }
                });
            }
            return domains;
        });
        self.canChangePFD = ko.computed(function() {
            var storageRouter = self.storageRouter(), primary;
            if (storageRouter === undefined) {
                return true;
            }
            primary = storageRouter.primaryFailureDomain();
            if (primary === undefined) {
                return true;
            }
            return !(
                primary.primarySRGuids().length === 1 &&
                primary.primarySRGuids()[0] == storageRouter.guid() &&
                primary.secondarySRGuids().length > 0
            );
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
                $.when.apply($, calls)
                    .then(self.loadStorageDrivers)
                    .then(self.loadVPools)
                    .then(self.loadFailureDomains)
                    .done(function() {
                        self.checkedVPoolGuids(self.storageRouter().vPoolGuids);
                        var pMachineGuid = storageRouter.pMachineGuid(), pm, pfd, sfd,
                            primaryFailureDomainGuid = storageRouter.primaryFailureDomainGuid(),
                            secondaryFailureDomainGuid = storageRouter.secondaryFailureDomainGuid();
                        if (pMachineGuid && (storageRouter.pMachine() === undefined || storageRouter.pMachine().guid() !== pMachineGuid)) {
                            if (!self.pMachineCache.hasOwnProperty(pMachineGuid)) {
                                pm = new PMachine(pMachineGuid);
                                pm.load();
                                self.pMachineCache[pMachineGuid] = pm;
                            }
                            storageRouter.pMachine(self.pMachineCache[pMachineGuid]);
                        } else if (pMachineGuid && storageRouter.pMachine() !== undefined && storageRouter.pMachine().loaded() === false) {
                            if (!self.pMachineCache.hasOwnProperty(storageRouter.pMachine().guid())) {
                                self.pMachineCache[storageRouter.pMachine().guid()] = storageRouter.pMachine();
                            }
                            storageRouter.pMachine().load();
                        }
                        if (primaryFailureDomainGuid && (storageRouter.primaryFailureDomain() === undefined || storageRouter.primaryFailureDomainGuid() !== primaryFailureDomainGuid)) {
                            if (!self.fdCache.hasOwnProperty(primaryFailureDomainGuid)) {
                                pfd = new FailureDomain(primaryFailureDomainGuid);
                                pfd.load();
                                self.fdCache[primaryFailureDomainGuid] = pfd;
                            }
                            storageRouter.primaryFailureDomain(self.fdCache[primaryFailureDomainGuid]);
                        }
                        if (secondaryFailureDomainGuid && (storageRouter.secondaryFailureDomain() === undefined || storageRouter.secondaryFailureDomainGuid() !== secondaryFailureDomainGuid)) {
                            if (!self.fdCache.hasOwnProperty(secondaryFailureDomainGuid)) {
                                sfd = new FailureDomain(secondaryFailureDomainGuid);
                                sfd.load();
                                self.fdCache[secondaryFailureDomainGuid] = sfd;
                            }
                            storageRouter.secondaryFailureDomain(self.fdCache[secondaryFailureDomainGuid]);
                        }
                        // Move child guids to the observables for easy display
                        storageRouter.vPools(storageRouter.vPoolGuids);
                        storageRouter.vMachines(storageRouter.vMachineGuids);
                    })
                    .always(deferred.resolve);
            }).promise();
        };
        self.loadVPools = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVPoolsHandle)) {
                    self.loadVPoolsHandle = api.get('vpools', {
                        queryparams: {
                            sort: 'name',
                            contents: 'storagedrivers'
                        }
                    })
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
        self.loadFailureDomains = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadFailureDomainsHandle)) {
                    self.loadFailureDomainsHandle = api.get('failure_domains', {
                        queryparams: {
                            sort: 'name',
                            contents: '_relations'
                        }
                    })
                        .done(function(data) {
                            var guids = [], fddata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                fddata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.failureDomains,
                                function(guid) {
                                    var domain = new FailureDomain(guid);
                                    domain.fillData(fddata[guid]);
                                    return domain;
                                }, 'guid'
                            );
                            generic.crossFiller(
                                guids, self.secondaryFailureDomains,
                                function(guid) {
                                    var domain = new FailureDomain(guid);
                                    domain.fillData(fddata[guid]);
                                    return domain;
                                }, 'guid'
                            );
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
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
