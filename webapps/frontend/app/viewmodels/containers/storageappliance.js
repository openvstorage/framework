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
        self.shared             = shared;
        self.vPoolGuids         = [];
        self.vMachineGuids      = [];
        self.storageDriverGuids = [];

        // Handles
        self.loadHandle  = undefined;
        self.loadActions = undefined;

        // External dependencies
        self.pMachine  = ko.observable();
        self.vPools    = ko.observableArray([]);
        self.vMachines = ko.observableArray([]);

        // Observables
        self.guid                  = ko.observable(guid);
        self.loading               = ko.observable(false);
        self.loaded                = ko.observable(false);
        self.pMachineGuid          = ko.observable();
        self.name                  = ko.observable();
        self.machineid             = ko.observable();
        self.ipAddress             = ko.observable();
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
        self.vDisks                = ko.observableArray([]);
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
        self.bandwidth = ko.computed(function() {
            if (self.readSpeed() === undefined || self.writeSpeed() === undefined) {
                return undefined;
            }
            var total = (self.readSpeed.raw() || 0) + (self.writeSpeed.raw() || 0);
            return generic.formatSpeed(total);
        });

        // Functions
        self.getAvailableActions = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadActions)) {
                    self.loadActions = api.get('storageappliances/' + self.guid() + '/get_available_actions')
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
        self.fillData = function(data) {
            generic.trySet(self.name, data, 'name');
            generic.trySet(self.storedData, data, 'stored_data');
            generic.trySet(self.ipAddress, data, 'ip');
            generic.trySet(self.machineid, data, 'machineid');
            generic.trySet(self.status, data, 'status', generic.lower);
            generic.trySet(self.failoverMode, data, 'failover_mode', generic.lower);
            generic.trySet(self.pMachineGuid, data, 'pmachine_guid');
            if (data.hasOwnProperty('vpools_guids')) {
                self.vPoolGuids = data.vpools_guids;
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
            if (data.hasOwnProperty('statistics')) {
                var stats = data.statistics;
                self.iops(stats.write_operations_ps + stats.read_operations_ps);
                self.cacheHits(stats.sco_cache_hits_ps + stats.cluster_cache_hits_ps);
                self.cacheMisses(stats.sco_cache_misses_ps);
                self.readSpeed(stats.data_read_ps);
                self.writeSpeed(stats.data_written_ps);
                self.backendWritten(stats.backend_data_written);
                self.backendRead(stats.backend_data_read);
                self.backendReads(stats.sco_cache_hits + stats.cluster_cache_hits);
                self.bandwidthSaved(Math.max(0, stats.data_read - stats.backend_data_read));
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
                    self.loadHandle = api.get('storageappliances/' + self.guid(), undefined, options)
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
