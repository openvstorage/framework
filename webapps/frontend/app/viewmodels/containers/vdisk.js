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
    'ovs/generic', 'ovs/api'
], function($, ko, generic, api) {
    "use strict";
    return function(guid) {
        var self = this;

        // Handles
        self.loadHandle             = undefined;
        self.loadVSAGuidHandle      = undefined;
        self.loadVMachineGuidHandle = undefined;

        // External dependencies
        self.vsa      = ko.observable();
        self.vMachine = ko.observable();
        self.vpool    = ko.observable();

        // Observables
        self.loading        = ko.observable(false);
        self.loaded         = ko.observable(false);
        self.guid           = ko.observable(guid);
        self.name           = ko.observable();
        self.order          = ko.observable(0);
        self.snapshots      = ko.observableArray([]);
        self.size           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.storedData     = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.cacheHits      = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.cacheMisses    = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.iops           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.readSpeed      = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.writeSpeed     = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendReads   = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.backendWritten = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendRead    = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.bandwidthSaved = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.vsaGuid        = ko.observable();
        self.vpoolGuid      = ko.observable();
        self.vMachineGuid   = ko.observable();
        self.failoverMode   = ko.observable();

        // Computed
        self.cacheRatio = ko.computed(function() {
            var total = (self.cacheHits.raw() || 0) + (self.cacheMisses.raw() || 0);
            if (total === 0) {
                total = 1;
            }
            return generic.formatRatio((self.cacheHits.raw() || 0) / total * 100);
        });

        // Functions
        self.fillData = function(data) {
            generic.trySet(self.name, data, 'name');
            generic.trySet(self.order, data, 'order');
            generic.trySet(self.snapshots, data, 'snapshots');
            generic.trySet(self.size, data, 'size');
            generic.trySet(self.vpoolGuid, data, 'vpool_guid');
            generic.trySet(self.vMachineGuid, data, 'vmachine_guid');
            generic.trySet(self.vsaGuid, data, 'vsa_guid');
            if (data.hasOwnProperty('info')) {
                self.storedData(data.info.stored);
                self.failoverMode(data.info.failover_mode.toLowerCase() || 'unknown');
            }
            if (data.hasOwnProperty('statistics')) {
                var stats = data.statistics;
                self.iops(stats.operations_ps);
                self.cacheHits(stats.cache_hits_ps);
                self.cacheMisses(stats.sco_cache_misses_ps);
                self.readSpeed(stats.data_read_ps);
                self.writeSpeed(stats.data_written_ps);
                self.backendWritten(stats.data_written);
                self.backendRead(stats.data_read);
                self.backendReads(stats.backend_read_operations);
                self.bandwidthSaved(stats.data_read - stats.backend_data_read);
            }

            self.snapshots.sort(function(a, b) {
                // Sorting based on newest first
                return b.timestamp - a.timestamp;
            });

            self.loaded(true);
            self.loading(false);
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                if (generic.xhrCompleted(self.loadHandle)) {
                    self.loadHandle = api.get('vdisks/' + self.guid())
                        .done(function(data) {
                            self.fillData(data);
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            self.loading(false);
                        });
                } else {
                    deferred.reject();
                }
            }).promise();
        };
    };
});
