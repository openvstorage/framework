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
/*global define, window */
define([
    'jquery', 'knockout',
    'ovs/generic', 'ovs/api', 'ovs/shared',
    'viewmodels/containers/shared/base_container', 'viewmodels/containers/vdisk/vdisk', 'viewmodels/containers/disk/disk'
], function($, ko,
            generic, api, shared,
            BaseContainer, VDisk, Disk) {
    "use strict";
    function StorageRouter(guid) {
        var self = this;

        BaseContainer.call(this);
        // Variables
        self.shared             = shared;
        self.storageDriverGuids = [];

        // Handles
        self.loadActions        = undefined;
        self.loadDisks          = undefined;
        self.loadLogFiles       = undefined;
        self.loadHandle         = undefined;
        self.loadUpdateMetadata = undefined;

        // External dependencies
        self.domains         = ko.observableArray([]);
        self.packageInfo     = ko.observableArray([]);
        self.recoveryDomains = ko.observableArray([]);
        self.vPools          = ko.observableArray([]);

        // Observables
        self.backendRead         = ko.observable().extend({smooth: {}}).extend({format: generic.formatBytes});
        self.backendWritten      = ko.observable().extend({smooth: {}}).extend({format: generic.formatBytes});
        self.bandwidthSaved      = ko.observable().extend({smooth: {}}).extend({format: generic.formatBytes});
        self.cacheHits           = ko.observable().extend({smooth: {}}).extend({format: generic.formatNumber});
        self.cacheMisses         = ko.observable().extend({smooth: {}}).extend({format: generic.formatNumber});
        self.disks               = ko.observableArray([]);
        self.disksLoaded         = ko.observable(false);
        self.domainGuids         = ko.observableArray([]);
        self.downLoadingLogs     = ko.observable(false);
        self.downloadLogState    = ko.observable($.t('ovs:support.information.download_logs'));
        self.edit                = ko.observable(false);
        self.expanded            = ko.observable(true);
        self.features            = ko.observable(undefined);
        self.guid                = ko.observable(guid);
        self.iops                = ko.observable().extend({smooth: {}}).extend({format: generic.formatNumber});
        self.ipAddress           = ko.observable();
        self.lastHeartbeat       = ko.observable();
        self.loaded              = ko.observable(false);
        self.loading             = ko.observable(false);
        self.machineId           = ko.observable();
        self.name                = ko.observable();
        self.nodeType            = ko.observable();
        self.recoveryDomainGuids = ko.observableArray([]);
        self.rdmaCapable         = ko.observable(false);
        self.readSpeed           = ko.observable().extend({smooth: {}}).extend({format: generic.formatSpeed});
        self.saving              = ko.observable(false);
        self.scrubCapable        = ko.observable(false);
        self.status              = ko.observable();
        self.storedData          = ko.observable().extend({smooth: {}}).extend({format: generic.formatBytes});
        self.totalCacheHits      = ko.observable().extend({smooth: {}}).extend({format: generic.formatNumber});
        self.updateMetadata      = ko.observable();
        self.vDisks              = ko.observableArray([]);
        self.vPoolGuids          = ko.observableArray([]);
        self.writeSpeed          = ko.observable().extend({smooth: {}}).extend({format: generic.formatSpeed});

        // Computed
        self.bandwidth = ko.computed(function () {
            if (self.readSpeed() === undefined || self.writeSpeed() === undefined) {
                return undefined;
            }
            var total = (self.readSpeed.raw() || 0) + (self.writeSpeed.raw() || 0);
            return generic.formatSpeed(total);
        });
        self.statusColor = ko.computed(function () {
            if (self.status() === 'ok') {
                return 'green';
            }
            if (self.status() === 'failure') {
                return 'red';
            }
            if (self.status() === 'warning') {
                return 'orange';
            }
            return 'lightgrey';
        });
        self.updatesAvailable = ko.computed(function() {
            var updatesFound = false;
            $.each(self.packageInfo(), function(index, comp) {
                if (comp.packages().length > 0) {
                    updatesFound = true;
                    return false;
                }
            });
            return updatesFound;
        });
        self.pageHash = ko.pureComputed(function() {
            // Returns the hashed URL to this objects own page
            return shared.routing.loadHash('storagerouter-detail', { guid: self.guid()})
        });
        // Feature Computed
        self.supportsBlockCache = ko.pureComputed(function() {
            var features = self.features();
            return features !== undefined && features.alba.features.contains('block-cache')
        });
        self.supportsCacheQuota = ko.pureComputed(function() {
            var features = self.features();
            return features !== undefined && features.alba.features.contains('cache-quota');
        });
        self.isEnterpriseEdition = ko.pureComputed(function() {
            var features = self.features();
            return features !== undefined && features.alba.edition === 'enterprise';
        });

        // Functions
        self.getUpdateMetadata = function () {
            return $.Deferred(function (deferred) {
                if (generic.xhrCompleted(self.loadUpdateMetadata)) {
                    self.loadUpdateMetadata = api.get('storagerouters/' + self.guid() + '/get_update_metadata')
                        .then(self.shared.tasks.wait)
                        .done(function (data) {
                            self.updateMetadata(data);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                }
            }).promise();
        };
        self.getDisks = function () {
            return $.Deferred(function (deferred) {
                if (generic.xhrCompleted(self.loadDisks)) {
                    self.loadDisks = api.get('disks', {
                        queryparams: {
                            storagerouterguid: self.guid(),
                            contents: '_relations',
                            sort: 'name'
                        }
                    })
                        .done(function (data) {
                            var guids = [], ddata = {};
                            $.each(data.data, function (index, item) {
                                guids.push(item.guid);
                                ddata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.disks,
                                function (guid) {
                                    var d = new Disk(guid);
                                    d.loading(true);
                                    return d;
                                }, 'guid'
                            );
                            $.each(self.disks(), function (index, disk) {
                                if (ddata.hasOwnProperty(disk.guid())) {
                                    disk.fillData(ddata[disk.guid()]);
                                    disk.getPartitions();
                                }
                            });
                            self.disks.sort(function (a, b) {
                                return a.name() < b.name() ? -1 : (a.name() > b.name() ? 1 : 0);
                            });
                            self.disksLoaded(true);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.downloadLogfiles = function () {
            if (self.downLoadingLogs() === true) {
                return;
            }
            if (generic.xhrCompleted(self.loadLogFiles)) {
                self.downLoadingLogs(true);
                self.downloadLogState($.t('ovs:support.information.downloading_logs'));
                self.loadLogFiles = api.get('storagerouters/' + self.guid() + '/get_logfiles')
                    .then(self.shared.tasks.wait)
                    .done(function (data) {
                        window.location.href = 'downloads/' + data;
                    })
                    .always(function () {
                        self.downLoadingLogs(false);
                        self.downloadLogState($.t('ovs:support.information.download_logs'));
                    });
            }
        };
        self.fillData = function (data) {
            data = data || {};
            if (self.edit()) {
                self.loading(false);
                return;
            }
            generic.trySet(self.ipAddress, data, 'ip');
            generic.trySet(self.machineId, data, 'machineid');
            generic.trySet(self.name, data, 'name');
            generic.trySet(self.nodeType, data, 'node_type');
            generic.trySet(self.rdmaCapable, data, 'rdma_capable');
            generic.trySet(self.status, data, 'status', 'failure');
            generic.trySet(self.features, data, 'features');
            if (data.hasOwnProperty('recovery_domains')) {
                self.recoveryDomainGuids(data.recovery_domains);
            }
            if (data.hasOwnProperty('regular_domains')) {
                self.domainGuids(data.regular_domains);
            }
            if (data.hasOwnProperty('last_heartbeat')) {
                self.lastHeartbeat(data.last_heartbeat === null ? undefined : data.last_heartbeat);
            }
            if (data.hasOwnProperty('vpools_guids')) {
                self.vPoolGuids(data.vpools_guids);
            }
            if (data.hasOwnProperty('storagedrivers_guids')) {
                self.storageDriverGuids = data.storagedrivers_guids;
            }
            if (data.hasOwnProperty('vdisks_guids')) {
                generic.crossFiller(
                    data.vdisks_guids, self.vDisks,
                    function (guid) {
                        var vd = new VDisk(guid);
                        vd.loading(true);
                        return vd;
                    }, 'guid'
                );
            }
            if (data.hasOwnProperty('disks_guids')) {
                generic.crossFiller(
                    data.disks_guids, self.disks,
                    function (guid) {
                        var d = new Disk(guid);
                        d.loading(true);
                        return d;
                    }, 'guid'
                );
            }
            if (data.hasOwnProperty('statistics')) {
                var stats = data.statistics;
                self.storedData(stats.stored);
                self.iops(stats['4k_operations_ps']);
                self.cacheHits(stats.cache_hits_ps);
                self.cacheMisses(stats.cache_misses_ps);
                self.readSpeed(stats.data_read_ps);
                self.totalCacheHits(stats.cache_hits);
                self.writeSpeed(stats.data_written_ps);
                self.backendWritten(stats.backend_data_written);
                self.backendRead(stats.backend_data_read);
                self.bandwidthSaved(Math.max(0, stats.data_read - stats.backend_data_read));
            }
            if (data.hasOwnProperty('partition_config')) {
                var part_config = data.partition_config;
                if (part_config.hasOwnProperty('SCRUB') && part_config.SCRUB.length > 0) {
                    self.scrubCapable(true);
                } else {
                    self.scrubCapable(false);
                }
            }
            self.loaded(true);
            self.loading(false);
        };
        self.load = function (contents) {
            return $.Deferred(function (deferred) {
                self.loading(true);
                if (generic.xhrCompleted(self.loadHandle)) {
                    var options = {};
                    if (contents !== undefined) {
                        options.contents = contents;
                    }
                    self.loadHandle = api.get('storagerouters/' + self.guid(), {queryparams: options})
                        .done(function (data) {
                            self.fillData(data);
                            self.loaded(true);
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function () {
                            self.loading(false);
                        });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.save = function() {
            return $.Deferred(function(deferred) {
                self.saving(true);
                var data = {
                    domain_guids: self.domainGuids(),
                    recovery_domain_guids: self.recoveryDomainGuids()
                };
                api.post('storagerouters/' + self.guid() + '/set_domains', { data: data })
                    .done(function() {
                        generic.alertSuccess($.t('ovs:generic.saved'), $.t('ovs:storagerouters.detail.save.success'));
                        deferred.resolve();
                    })
                    .fail(function() {
                        generic.alertError($.t('ovs:generic.error'), $.t('ovs:storagerouters.detail.save.failure'));
                        deferred.reject();
                    })
                    .always(function() {
                        self.edit(false);
                        self.saving(false);
                    });
            }).promise();
        };
    }
    StorageRouter.prototype = $.extend({}, BaseContainer.prototype)
    return StorageRouter
});
