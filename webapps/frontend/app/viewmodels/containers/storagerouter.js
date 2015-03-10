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
/*global define, window */
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
        self.guid             = ko.observable(guid);
        self.loading          = ko.observable(false);
        self.loaded           = ko.observable(false);
        self.pMachineGuid     = ko.observable();
        self.name             = ko.observable();
        self.machineId        = ko.observable();
        self.ipAddress        = ko.observable();
        self.status           = ko.observable();
        self.iops             = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.storedData       = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.cacheHits        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.cacheMisses      = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.readSpeed        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.writeSpeed       = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendWritten   = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendRead      = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.bandwidthSaved   = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.failoverMode     = ko.observable();
        self.vDisks           = ko.observableArray([]);
        self.availableActions = ko.observableArray([]);
        self.downloadLogState = ko.observable($.t('ovs:support.downloadlogs'));

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
                self.iops(stats.operations_ps);
                self.cacheHits(stats.cache_hits_ps);
                self.cacheMisses(stats.cache_misses_ps);
                self.readSpeed(stats.data_read_ps);
                self.writeSpeed(stats.data_written_ps);
                self.backendWritten(stats.backend_data_written);
                self.backendRead(stats.backend_data_read);
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
    };
});
