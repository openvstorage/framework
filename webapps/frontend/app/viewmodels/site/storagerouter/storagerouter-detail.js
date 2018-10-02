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
    'jquery', 'durandal/app', 'knockout', 'plugins/dialog',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    'ovs/services/authentication',
    'viewmodels/containers/storagerouter/storagerouter', 'viewmodels/containers/vpool/vpool', 'viewmodels/containers/storagedriver/storagedriver',
    'viewmodels/containers/domain/domain', 'viewmodels/containers/vdisk/vdisk',
    'viewmodels/wizards/configurepartition/index'
], function(
    $, app, ko, dialog, shared,
    generic, Refresher, api,
    authentication,
    StorageRouter, VPool, StorageDriver, Domain, VDisk,
    ConfigurePartitionWizard
) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared                   = shared;
        self.guard                    = { authenticated: true };
        self.refresher                = new Refresher();
        self.widgets                  = [];
        self.loadVPoolsHandle         = undefined;
        self.loadStorageDriversHandle = {};
        self.loadDomainsHandle        = undefined;
        self.vDiskCache               = {};
        self.vDisksHandle             = {};
        self.vDiskHeaders             = [
            { key: 'status',     value: '',                            width: 30        },
            { key: 'name',       value: $.t('ovs:generic.name'),       width: undefined },
            { key: 'size',       value: $.t('ovs:generic.size'),       width: 100       },
            { key: 'storedData', value: $.t('ovs:generic.storeddata'), width: 110       },
            { key: 'iops',       value: $.t('ovs:generic.iops'),       width: 90        },
            { key: 'readSpeed',  value: $.t('ovs:generic.read'),       width: 125       },
            { key: 'writeSpeed', value: $.t('ovs:generic.write'),      width: 125       },
            { key: 'dtlStatus',  value: $.t('ovs:generic.dtl_status'), width: 50        }
        ];
        self.vDiskQuery               = JSON.stringify({
            type: 'AND',
            items: [['is_vtemplate', 'EQUALS', false]]
        });

        // Observables
        self.checkedVPoolGuids   = ko.observableArray([]);
        self.domainCache         = ko.observable({});
        self.domains             = ko.observableArray([]);
        self.domainsLoaded       = ko.observable(false);
        self.markingOffline      = ko.observable(false);
        self.refreshing          = ko.observable(false);
        self.storageRouter       = ko.observable();
        self.storageRouterLoaded = ko.observable(false);
        self.vPools              = ko.observableArray([]);
        self.vPoolsLoaded        = ko.observable(false);

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
        self.badRecoveryDomains = ko.computed(function() {
            var domains = [], storageRouter = self.storageRouter(), cache = self.domainCache();
            if (storageRouter === undefined) {
                return domains;
            }
            $.each(storageRouter.recoveryDomainGuids(), function(index, guid) {
                if (cache[guid] !== undefined && cache[guid].storageRouterLayout() !== undefined &&
                    (cache[guid].storageRouterLayout()['regular'].length === 0 ||
                     (cache[guid].storageRouterLayout()['regular'].length === 1 && cache[guid].storageRouterLayout()['regular'][0] === storageRouter.guid()))) {
                    domains.push(cache[guid]);
                }
            });
            return domains;
        });

        self.canManage = ko.pureComputed(function() {
            return authentication.user.canManage()
        });
        self.canEdit = ko.pureComputed(function() {
            return self.storageRouter().loaded() && self.canManage() && !self.markingOffline() && !self.refreshing()
        });
        self.canRefresh = ko.pureComputed(function() {
            return self.canManage() && !self.markingOffline()
        });
        self.canMarkOffline = ko.pureComputed(function() {
            return self.canManage() && !self.refreshing()
        });

        // Functions
        self.load = function() {
            return $.Deferred(function (deferred) {
                var storageRouter = self.storageRouter(), calls = [];
                if (!storageRouter.edit()) {
                    calls.push(storageRouter.load('_dynamics,_relations'))
                }
                calls.push(storageRouter.getDisks());
                calls.push(self.loadDomains());
                $.when.apply($, calls)
                    .then(self.loadStorageDrivers)
                    .then(self.loadVPools)
                    .done(function() {
                        self.checkedVPoolGuids(self.storageRouter().vPoolGuids());
                        // Move child guids to the observables for easy display
                        storageRouter.vPools(storageRouter.vPoolGuids());
                    })
                    .always(function() {
                        self.storageRouterLoaded(true);
                        deferred.resolve();
                    });
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
                    self.loadDomainsHandle = api.get('domains', {queryparams: {sort: 'name', contents: 'storage_router_layout'}})
                        .done(function(data) {
                            var guids = [], ddata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                ddata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.domains,
                                function(guid) {
                                    var domain = new Domain(guid),
                                        cache = self.domainCache();
                                    cache[guid] = domain;
                                    self.domainCache(cache);
                                    if (ddata.hasOwnProperty(guid)) {
                                        domain.fillData(ddata[guid]);
                                    }
                                    return domain;
                                }, 'guid'
                            );
                            $.each(self.domains(), function(index, domain) {
                                if (ddata.hasOwnProperty(domain.guid())) {
                                    domain.fillData(ddata[domain.guid()]);
                                }
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
        self.loadVDisks = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.vDisksHandle[options.page])) {
                    options.sort = 'devicename';
                    options.contents = '_dynamics,_relations,-snapshots';
                    options.storagerouterguid = self.storageRouter().guid();
                    options.query = self.vDiskQuery;
                    self.vDisksHandle[options.page] = api.get('vdisks', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    if (!self.vDiskCache.hasOwnProperty(guid)) {
                                        self.vDiskCache[guid] = new VDisk(guid);
                                    }
                                    return self.vDiskCache[guid];
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
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
                    generic.alertSuccess($.t('ovs:generic.finished'), $.t('ovs:storagerouters.detail.refresh.success'));
                })
                .fail(function() {
                    generic.alertError($.t('ovs:generic.error'), $.t('ovs:generic.messages.errorwhile', { what: $.t('ovs:storagerouters.detail.refresh.refreshing') }));
                })
                .always(function() {
                    self.refreshing(false);
                });
            generic.alertInfo($.t('ovs:storagerouters.detail.refresh.started'), $.t('ovs:storagerouters.detail.refresh.inprogress'));
        };
        self.markoffline = function() {
            self.markingOffline(true);
            app.showMessage(
                    $.t('ovs:storagerouters.detail.offline.warning'),
                    $.t('ovs:storagerouters.detail.offline.title'),
                    [$.t('ovs:storagerouters.detail.offline.no'), $.t('ovs:storagerouters.detail.offline.yes')]
                )
                .done(function(answer) {
                    if (answer === $.t('ovs:storagerouters.detail.offline.yes')) {
                        api.post('storagerouters/' + self.storageRouter().guid() + '/mark_offline')
                            .then(self.shared.tasks.wait)
                            .done(function() {
                                generic.alertSuccess(
                                    $.t('ovs:storagerouters.detail.offline.done'),
                                    $.t('ovs:storagerouters.detail.offline.done_msg')
                                );
                            })
                            .fail(function(error) {
                                error = generic.extractErrorMessage(error);
                                generic.alertError(
                                    $.t('ovs:generic.error'),
                                    $.t('ovs:generic.messages.errorwhile', {
                                        context: 'error',
                                        what: $.t('ovs:storagerouters.detail.offline.error_msg'),
                                        error: error
                                    })
                                )
                            })
                            .always(function() {
                                self.markingOffline(false);
                            });
                        generic.alertInfo(
                            $.t('ovs:storagerouters.detail.offline.pending'),
                            $.t('ovs:storagerouters.detail.offline.pendingmsg')
                        );
                    } else {
                        self.markingOffline(false);
                    }
                });
        };

        // Durandal
        self.activate = function(mode, guid) {
            self.storageRouter(new StorageRouter(guid));
            self.storageRouter().storageDrivers = ko.observableArray();
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
