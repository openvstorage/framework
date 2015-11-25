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
/*global define, window */
define([
    'jquery', 'knockout',
    'ovs/generic', 'ovs/api', 'ovs/shared',
    'viewmodels/containers/vdisk', 'viewmodels/containers/disk', 'viewmodels/containers/pmachine', 'viewmodels/containers/failuredomain'
], function($, ko, generic, api, shared, VDisk, Disk, PMachine, FailureDomain) {
    "use strict";
    return function(guid) {
        var self = this;

        // Variables
        self.shared             = shared;
        self.vPoolGuids         = [];
        self.vMachineGuids      = [];
        self.storageDriverGuids = [];

        // Handles
        self.loadHandle  = undefined;
        self.loadActions = undefined;
        self.loadDisks   = undefined;

        // External dependencies
        self.pMachine               = ko.observable();
        self.primaryFailureDomain   = ko.observable();
        self.secondaryFailureDomain = ko.observable();
        self.vPools                 = ko.observableArray([]);
        self.vMachines              = ko.observableArray([]);

        // Observables
        self.availableActions           = ko.observableArray([]);
        self.backendRead                = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendWritten             = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.bandwidthSaved             = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.cacheHits                  = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.cacheMisses                = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.disks                      = ko.observableArray([]);
        self.disksLoaded                = ko.observable(false);
        self.downloadLogState           = ko.observable($.t('ovs:support.downloadlogs'));
        self.dtlMode                    = ko.observable();
        self.edit                       = ko.observable(false);
        self.guid                       = ko.observable(guid);
        self.iops                       = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.ipAddress                  = ko.observable();
        self.lastHeartbeat              = ko.observable();
        self.loaded                     = ko.observable(false);
        self.loading                    = ko.observable(false);
        self.machineId                  = ko.observable();
        self.name                       = ko.observable();
        self.nodeType                   = ko.observable();
        self.pMachineGuid               = ko.observable();
        self.primaryFailureDomainGuid   = ko.observable();
        self.rdmaCapable                = ko.observable(false);
        self.readSpeed                  = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.saving                     = ko.observable(false);
        self.scrubCapable               = ko.observable(false);
        self.secondaryFailureDomainGuid = ko.observable();
        self.status                     = ko.observable();
        self.storedData                 = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.updates                    = ko.observable();
        self.vDisks                     = ko.observableArray([]);
        self.versions                   = ko.observable();
        self.writeSpeed                 = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });

        // Computed
        self.cacheRatio = ko.computed(function() {
            if (self.cacheHits() === undefined || self.cacheMisses() === undefined) {
                return undefined;
            }
            var total = (self.cacheHits.raw() || 0) + (self.cacheMisses.raw() || 0);
            if (total === 0) {
                total = 1;
            }
            return generic.formatRatio((self.cacheHits.raw() || 0) / total * 100);
        });
        self.bandwidth = ko.computed(function() {
            if (self.readSpeed() === undefined || self.writeSpeed() === undefined) {
                return undefined;
            }
            var total = (self.readSpeed.raw() || 0) + (self.writeSpeed.raw() || 0);
            return generic.formatSpeed(total);
        });
        self.statusColor = ko.computed(function() {
            if (self.status() === "ok") {
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

        // Functions
        self.getUpdates = function() {
            return $.Deferred(function(deferred) {
                api.get('storagerouters/' + self.guid() + '/get_update_status')
                    .then(self.shared.tasks.wait)
                    .done(function(data) {
                        if (data.framework.length > 0) {
                            data.framework.sort(function(a, b) {
                                return a.name < b.name ? -1 : (a.name > b.name ? 1 : 0);
                            });
                        }
                        if (data.volumedriver.length > 0) {
                            data.volumedriver.sort(function(a, b) {
                                return a.name < b.name ? -1 : (a.name > b.name ? 1 : 0);
                            });
                        }
                        self.updates(data);
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.getDisks = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadDisks)) {
                    self.loadDisks = api.get('disks', { queryparams: {
                        storagerouterguid: self.guid(),
                        contents: '_relations',
                        sort: 'name'
                    }})
                        .done(function(data) {
                            var guids = [], ddata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                ddata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.disks,
                                function(guid) {
                                    var d = new Disk(guid);
                                    d.loading(true);
                                    return d;
                                }, 'guid'
                            );
                            $.each(self.disks(), function(index, disk) {
                                if (ddata.hasOwnProperty(disk.guid())) {
                                    disk.fillData(ddata[disk.guid()]);
                                    disk.getPartitions();
                                }
                            });
                            self.disks.sort(function(a, b) {
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
        self.getAvailableActions = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadActions)) {
                    self.loadActions = api.get('storagerouters/' + self.guid() + '/get_available_actions')
                        .done(function(data) {
                            self.availableActions(data);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.downloadLogfiles = function() {
            self.downloadLogState($.t('ovs:support.downloadinglogs'));
            api.get('storagerouters/' + self.guid() + '/get_logfiles')
                .then(self.shared.tasks.wait)
                .done(function(data) {
                    window.location.href = 'downloads/' + data;
                })
                .always(function() {
                    self.downloadLogState($.t('ovs:support.downloadlogs'));
                });
        };
        self.fillData = function(data) {
            generic.trySet(self.name, data, 'name');
            generic.trySet(self.storedData, data, 'stored_data');
            generic.trySet(self.ipAddress, data, 'ip');
            generic.trySet(self.machineId, data, 'machineid');
            generic.trySet(self.status, data, 'status', generic.lower);
            generic.trySet(self.dtlMode, data, 'dtl_mode', generic.lower);
            generic.trySet(self.nodeType, data, 'node_type');
            generic.trySet(self.rdmaCapable, data, 'rdma_capable');
            if (data.hasOwnProperty('last_heartheat')) {
                self.lastHeartbeat(data.last_heartheat === null ? undefined : data.last_heartheat);
            }
            if (data.hasOwnProperty('vpools_guids')) {
                self.vPoolGuids = data.vpools_guids;
            }
            if (data.hasOwnProperty('pmachine_guid')) {
                if (data.pmachine_guid !== self.pMachineGuid()) {
                    self.pMachineGuid(data.pmachine_guid);
                    self.pMachine(new PMachine(data.pmachine_guid))
                }
            }
            if (data.hasOwnProperty('storagedrivers_guids')) {
                self.storageDriverGuids = data.storagedrivers_guids;
            }
            if (data.hasOwnProperty('vmachine_guids')) {
                self.vMachineGuids = data.vmachine_guids;
            }
            if (data.hasOwnProperty('vdisks_guids')) {
                generic.crossFiller(
                    data.vdisks_guids, self.vDisks,
                    function(guid) {
                        var vd = new VDisk(guid);
                        vd.loading(true);
                        return vd;
                    }, 'guid'
                );
            }
            if (data.hasOwnProperty('disks_guids')) {
                generic.crossFiller(
                    data.disks_guids, self.disks,
                    function(guid) {
                        var d = new Disk(guid);
                        d.loading(true);
                        return d;
                    }, 'guid'
                );
            }
            if (data.hasOwnProperty('primary_failure_domain_guid')) {
                var pfdGuid = data.primary_failure_domain_guid;
                if (pfdGuid === null || pfdGuid === undefined) {
                    self.primaryFailureDomainGuid(undefined);
                } else {
                    if (self.primaryFailureDomainGuid() === undefined || self.primaryFailureDomainGuid() !== pfdGuid) {
                        self.primaryFailureDomainGuid(pfdGuid);
                    }
                }
            }
            if (data.hasOwnProperty('secondary_failure_domain_guid')) {
                var sfdGuid = data.secondary_failure_domain_guid;
                if (sfdGuid === null || sfdGuid === undefined) {
                    self.secondaryFailureDomainGuid(undefined);
                } else {
                    if (self.secondaryFailureDomainGuid() === undefined || self.secondaryFailureDomainGuid() !== sfdGuid) {
                        self.secondaryFailureDomainGuid(sfdGuid);
                    }
                }
            }
            if (data.hasOwnProperty('statistics')) {
                var stats = data.statistics;
                self.iops(stats['4k_operations_ps']);
                self.cacheHits(stats.cache_hits_ps);
                self.cacheMisses(stats.cache_misses_ps);
                self.readSpeed(stats.data_read_ps);
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
        self.load = function(contents) {
            return $.Deferred(function(deferred) {
                self.loading(true);
                if (generic.xhrCompleted(self.loadHandle)) {
                    var options = {};
                    if (contents !== undefined) {
                        options.contents = contents;
                    }
                    self.loadHandle = api.get('storagerouters/' + self.guid(), { queryparams: options })
                        .done(function(data) {
                            self.fillData(data);
                            self.loaded(true);
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function() {
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
                    primary_failure_domain_guid: self.primaryFailureDomain().guid(),
                    secondary_failure_domain_guid: self.secondaryFailureDomain() === undefined ? null : self.secondaryFailureDomain().guid()
                };
                api.patch('storagerouters/' + self.guid(), { data: data })
                    .done(deferred.resolve)
                    .fail(deferred.reject)
                    .always(function() {
                        self.edit(false);
                        self.saving(false);
                    });
            }).promise();
        };
    };
});
