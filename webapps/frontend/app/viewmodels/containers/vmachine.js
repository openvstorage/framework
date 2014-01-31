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
        self.shared = shared;
        self.loadVDisksHandle  = undefined;
        self.loadVSAGuid       = undefined;
        self.loadHandle        = undefined;
        self.loadVpoolGuid     = undefined;
        self.loadChildrenGuid  = undefined;
        self.loadSChildrenGuid = undefined;

        // External dependencies
        self.vsas             = ko.observableArray([]);
        self.vpools           = ko.observableArray([]);
        self.vMachines        = ko.observableArray([]);
        self.pMachine         = ko.observable();

        // Observables
        self.loading          = ko.observable(false);
        self.loaded           = ko.observable(false);

        self.guid             = ko.observable(guid);
        self.vSAGuids         = [];
        self.vPoolGuids       = [];
        self.vMachineGuids    = [];
        self.pMachineGuid     = ko.observable();
        self.name             = ko.observable();
        self.hypervisorStatus = ko.observable();
        self.ipAddress        = ko.observable();
        self.isInternal       = ko.observable();
        self.isVTemplate      = ko.observable();
        self.snapshots        = ko.observableArray([]);
        self.status           = ko.observable();
        self.iops             = ko.smoothDeltaObservable(generic.formatNumber);
        self.storedData       = ko.smoothObservable(undefined, generic.formatBytes);
        self.cacheHits        = ko.smoothDeltaObservable();
        self.cacheMisses      = ko.smoothDeltaObservable();
        self.readSpeed        = ko.smoothDeltaObservable(generic.formatSpeed);
        self.writeSpeed       = ko.smoothDeltaObservable(generic.formatSpeed);
        self.backendReads     = ko.smoothObservable(undefined, generic.formatNumber);
        self.backendWritten   = ko.smoothObservable(undefined, generic.formatBytes);
        self.backendRead      = ko.smoothObservable(undefined, generic.formatBytes);
        self.bandwidthSaved   = ko.smoothObservable(undefined, generic.formatBytes);
        self.failoverMode     = ko.observable();
        self.cacheRatio       = ko.computed(function() {
            var total = (self.cacheHits.raw() || 0) + (self.cacheMisses.raw() || 0);
            if (total === 0) {
                total = 1;
            }
            return generic.formatRatio((self.cacheHits.raw() || 0) / total * 100);
        });
        self.isRunning        = ko.computed(function() {
            return self.hypervisorStatus() === 'RUNNING';
        });

        self.vDisks                = ko.observableArray([]);
        self.templateChildrenGuids = ko.observableArray([]);

        self._bandwidth = ko.computed(function() {
            var total = (self.readSpeed.raw() || 0) + (self.writeSpeed.raw() || 0),
                initialized = self.readSpeed.initialized() && self.writeSpeed.initialized();
            return {
                value: generic.formatSpeed(total),
                initialized: initialized
            };
        });
        self.bandwidth = ko.computed(function() {
            return self._bandwidth().value;
        });
        self.bandwidth.initialized = ko.computed(function() {
            return self._bandwidth().initialized;
        });

        // Functions
        self.fetchVSAGuids = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVSAGuid)) {
                    self.loadVSAGuid = api.get('vmachines/' + self.guid() + '/get_vsas')
                        .done(function(data) {
                            self.vSAGuids = data;
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
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
        self.fetchVPoolGuids = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVpoolGuid)) {
                    self.loadVpoolGuid = api.get('vmachines/' + self.guid() + '/get_vpools')
                        .done(function(data) {
                            self.vPoolGuids = data;
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
        self.loadDisks = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVDisksHandle)) {
                    self.loadVDisksHandle = api.get('vdisks', undefined, {vmachineguid: self.guid()})
                        .done(function(data) {
                            var i, guids = [];
                            for (i = 0; i < data.length; i += 1) {
                                guids.push(data[i].guid);
                            }
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
            var stats = data.statistics,
                statsTime = Math.round(stats.timestamp * 1000);
            self.name(data.name);
            self.hypervisorStatus(data.hypervisor_status);
            self.iops({ value: stats.write_operations + stats.read_operations, timestamp: statsTime });
            self.storedData(data.stored_data);
            self.cacheHits({ value: stats.sco_cache_hits + stats.cluster_cache_hits, timestamp: statsTime });
            self.cacheMisses({ value: stats.sco_cache_misses, timestamp: statsTime });
            self.readSpeed({ value: stats.data_read, timestamp: statsTime });
            self.writeSpeed({ value: stats.data_written, timestamp: statsTime });
            self.backendWritten(stats.data_written);
            self.backendRead(stats.data_read);
            self.backendReads(stats.backend_read_operations);
            self.bandwidthSaved(stats.data_read - stats.backend_data_read);
            self.ipAddress(data.ip);
            self.isInternal(data.is_internal);
            self.isVTemplate(data.is_vtemplate);
            self.snapshots(data.snapshots);
            self.status(data.status.toLowerCase());
            self.failoverMode(data.failover_mode.toLowerCase());
            self.pMachineGuid(data.pmachine_guid);

            self.snapshots.sort(function(a, b) {
                // Newest first
                return b.timestamp - a.timestamp;
            });

            self.loaded(true);
            self.loading(false);
        };
        self.load = function(reduced, onlyextends) {
            reduced = reduced || false;
            onlyextends = onlyextends || false;
            if (onlyextends) {
                reduced = false;
            }
            return $.Deferred(function(deferred) {
                self.loading(true);
                var calls = [];
                if (!onlyextends) {
                    calls.push($.Deferred(function(deferred) {
                        if (generic.xhrCompleted(self.loadHandle)) {
                            self.loadHandle = api.get('vmachines/' + self.guid())
                                .done(function(data) {
                                    self.fillData(data);
                                    deferred.resolve();
                                })
                                .fail(deferred.reject);
                        } else {
                            deferred.reject();
                        }
                    }).promise());
                }
                if (!reduced) {
                    calls.push(self.loadDisks());
                }
                $.when.apply($, calls)
                    .done(function() {
                        self.loaded(true);
                        deferred.resolve();
                    })
                    .fail(deferred.reject)
                    .always(function() {
                        self.loading(false);
                    });
            }).promise();
        };
    };
});
