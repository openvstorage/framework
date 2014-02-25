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
        self.loadHandle    = undefined;
        self.diskHandle    = undefined;
        self.machineHandle = undefined;

        // Observables
        self.loading           = ko.observable(false);
        self.loaded            = ko.observable(false);
        self.guid              = ko.observable(guid);
        self.name              = ko.observable();
        self.size              = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.iops              = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.storedData        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.cacheHits         = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.cacheMisses       = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.numberOfDisks     = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.numberOfMachines  = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.readSpeed         = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.writeSpeed        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendWriteSpeed = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendReadSpeed  = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendReads      = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.backendWritten    = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendRead       = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.bandwidthSaved    = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendType       = ko.observable();
        self.backendConnection = ko.observable();
        self.backendLogin      = ko.observable();

        // Computed
        self.cacheRatio = ko.computed(function() {
            var total = (self.cacheHits.raw() || 0) + (self.cacheMisses.raw() || 0);
            if (total === 0) {
                total = 1;
            }
            return generic.formatRatio((self.cacheHits.raw() || 0) / total * 100);
        });
        self.freeSpace = ko.computed(function() {
            if ((self.size.raw() || 0) === 0) {
                return generic.formatRatio(0);
            }
            return generic.formatRatio((self.size.raw() - (self.storedData.raw() || 0)) / self.size.raw() * 100);
        });
        self.bandwidth = ko.computed(function() {
            var total = (self.readSpeed.raw() || 0) + (self.writeSpeed.raw() || 0);
            return generic.formatSpeed(total);
        });

        // Functions
        self.fillData = function(data) {
            generic.trySet(self.name, data, 'name');
            generic.trySet(self.storedData, data, 'stored_data');
            generic.trySet(self.size, data, 'size');
            if (data.hasOwnProperty('backend_type')) {
                self.backendType($.t('ovs:vpools.backendtypes.' + data.backend_type));
            } else {
                self.backendType('');
            }
            if (data.hasOwnProperty('vdisks_guids')) {
                self.numberOfDisks(data.vdisks_guids.length);
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
                self.backendReadSpeed(stats.backend_data_read_ps);
                self.backendWriteSpeed(stats.backend_data_written_ps);
                self.backendConnection(data.backend_connection);
                self.backendLogin(data.backend_login);
            }

            self.loaded(true);
            self.loading(false);
        };
        self.load = function(contents) {
            self.loading(true);
            return $.Deferred(function(deferred) {
                var calls = [
                    $.Deferred(function(mainDeferred) {
                        if (generic.xhrCompleted(self.loadHandle)) {
                            var options = {};
                            if (contents !== undefined) {
                                options.contents = contents;
                            }
                            self.loadHandle = api.get('vpools/' + self.guid(), undefined, options)
                                .done(function(data) {
                                    self.fillData(data);
                                    mainDeferred.resolve();
                                })
                                .fail(mainDeferred.reject);
                        } else {
                            mainDeferred.reject();
                        }
                    }).promise(),
                    $.Deferred(function(machineDeferred) {
                        if (generic.xhrCompleted(self.machineHandle)) {
                            self.machineHandle = api.get('vpools/' + self.guid() + '/count_machines')
                                .done(function(data) {
                                    self.numberOfMachines(data);
                                    machineDeferred.resolve();
                                })
                                .fail(machineDeferred.reject);
                        } else {
                            machineDeferred.reject();
                        }
                    }).promise()];
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
