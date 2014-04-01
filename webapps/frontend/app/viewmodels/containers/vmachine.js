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
    'jquery', 'knockout',
    'ovs/generic', 'ovs/api', 'ovs/shared',
    'viewmodels/containers/vdisk'
], function($, ko, generic, api, shared, VDisk) {
    "use strict";
    return function(guid) {
        var self = this;

        // Variables
        self.shared        = shared;
        self.vSAGuids      = [];
        self.vPoolGuids    = [];
        self.vMachineGuids = [];

        // Handles
        self.loadVDisksHandle  = undefined;
        self.loadVSAGuid       = undefined;
        self.loadHandle        = undefined;
        self.loadVpoolGuid     = undefined;
        self.loadChildrenGuid  = undefined;
        self.loadSChildrenGuid = undefined;

        // External dependencies
        self.pMachine  = ko.observable();
        self.vSAs      = ko.observableArray([]);
        self.vPools    = ko.observableArray([]);
        self.vMachines = ko.observableArray([]);

        // Observables
        self.guid                  = ko.observable(guid);
        self.loading               = ko.observable(false);
        self.loaded                = ko.observable(false);
        self.pMachineGuid          = ko.observable();
        self.name                  = ko.observable();
        self.hypervisorStatus      = ko.observable();
        self.ipAddress             = ko.observable();
        self.isInternal            = ko.observable();
        self.isVTemplate           = ko.observable();
        self.status                = ko.observable();
        self.iops                  = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.storedData            = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.cacheHits             = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.cacheMisses           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.readSpeed             = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.writeSpeed            = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendReads          = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.backendWritten        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendRead           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.bandwidthSaved        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.failoverMode          = ko.observable();
        self.snapshots             = ko.observableArray([]);
        self.vDisks                = ko.observableArray([]);
        self.templateChildrenGuids = ko.observableArray([]);
        self.availableActions      = ko.observableArray([]);

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
        self.isRunning = ko.computed(function() {
            return self.hypervisorStatus() === 'RUNNING';
        });
        self.bandwidth = ko.computed(function() {
            if (self.readSpeed() === undefined || self.writeSpeed() === undefined) {
                return undefined;
            }
            var total = (self.readSpeed.raw() || 0) + (self.writeSpeed.raw() || 0);
            return generic.formatSpeed(total);
        });

        // Functions
        self.fetchServedChildren = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadSChildrenGuid)) {
                    self.loadSChildrenGuid = api.get('vmachines/' + self.guid() + '/get_served_children')
                        .done(function(data) {
                            self.vPoolGuids = data.vpool_guids;
                            self.vMachineGuids = data.vmachine_guids;
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.fetchTemplateChildrenGuids = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadChildrenGuid)) {
                    self.loadChildrenGuid = api.get('vmachines/' + self.guid() + '/get_children')
                        .done(function(data) {
                            self.templateChildrenGuids(data);
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
                if (generic.xhrCompleted(self.loadChildrenGuid)) {
                    self.loadChildrenGuid = api.get('vmachines/' + self.guid() + '/get_available_actions')
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
        self.loadDisks = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVDisksHandle)) {
                    self.loadVDisksHandle = api.get('vdisks', undefined, {vmachineguid: self.guid()})
                        .done(function(data) {
                            var guids = [];
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                            });
                            generic.crossFiller(
                                guids, self.vDisks,
                                function(guid) {
                                    return new VDisk(guid);
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
        self.fillData = function(data) {
            generic.trySet(self.name, data, 'name');
            generic.trySet(self.hypervisorStatus, data, 'hypervisor_status');
            generic.trySet(self.storedData, data, 'stored_data');
            generic.trySet(self.ipAddress, data, 'ip');
            generic.trySet(self.isInternal, data, 'is_internal');
            generic.trySet(self.isVTemplate, data, 'is_vtemplate');
            generic.trySet(self.snapshots, data, 'snapshots');
            generic.trySet(self.status, data, 'status', generic.lower);
            generic.trySet(self.failoverMode, data, 'failover_mode', generic.lower);
            generic.trySet(self.pMachineGuid, data, 'pmachine_guid');
            if (data.hasOwnProperty('vsas_guids')) {
                self.vSAGuids = data.vsas_guids;
            }
            if (data.hasOwnProperty('vpools_guids')) {
                self.vPoolGuids = data.vpools_guids;
            }
            // The vdisks from a full object data is only valid for non-VSA vMachines, since those
            // don't have disks, but only serve disks.
            if (data.hasOwnProperty('vdisks_guids') && self.isInternal() === false) {
                generic.crossFiller(
                    data.vdisks_guids, self.vDisks,
                    function(guid) {
                        var vd = new VDisk(guid);
                        vd.loading(true);
                        return vd;
                    }, 'guid'
                );
            }
            if (data.hasOwnProperty('statistics')) {
                var stats = data.statistics;
                self.iops(stats.write_operations_ps + stats.read_operations_ps);
                self.cacheHits(stats.sco_cache_hits_ps + stats.cluster_cache_hits_ps);
                self.cacheMisses(stats.sco_cache_misses_ps);
                self.readSpeed(stats.data_read_ps);
                self.writeSpeed(stats.data_written_ps);
                self.backendWritten(stats.data_written);
                self.backendRead(stats.data_read);
                self.backendReads(stats.backend_read_operations);
                self.bandwidthSaved(stats.data_read - stats.backend_data_read);
            }

            self.snapshots.sort(function(a, b) {
                // Newest first
                return b.timestamp - a.timestamp;
            });

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
                    self.loadHandle = api.get('vmachines/' + self.guid(), undefined, options)
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
    };
});
