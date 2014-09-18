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
        self.loadHandle = undefined;

        // External dependencies
        self.storageRouter = ko.observable();
        self.vMachine      = ko.observable();
        self.vpool         = ko.observable();

        // Observables
        self.loading               = ko.observable(false);
        self.loaded                = ko.observable(false);
        self.guid                  = ko.observable(guid);
        self.name                  = ko.observable();
        self.order                 = ko.observable(0);
        self.snapshots             = ko.observableArray([]);
        self.configuration         = ko.observable();
        self._configuration        = ko.observable();
        self.resolvedConfiguration = ko.observable();
        self.size                  = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.storedData            = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.cacheHits             = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.cacheMisses           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.iops                  = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.readSpeed             = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.writeSpeed            = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.backendReads          = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.backendWritten        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendRead           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.bandwidthSaved        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.namespace             = ko.observable();
        self.storageRouterGuid     = ko.observable();
        self.vpoolGuid             = ko.observable();
        self.vMachineGuid          = ko.observable();
        self.failoverMode          = ko.observable();
        self.cacheStrategies       = ko.observableArray([undefined, { name: 'onread' }, { name: 'onwrite' }, { name: 'none' }]);
        self.hasFOC                = ko.observableArray([undefined, { value: true }, { value: false }]);

        // Computed
        self.cacheRatio = ko.computed(function() {
            var total = (self.cacheHits.raw() || 0) + (self.cacheMisses.raw() || 0);
            if (total === 0) {
                total = 1;
            }
            return generic.formatRatio((self.cacheHits.raw() || 0) / total * 100);
        });
        self.configIops = ko.computed({
            read: function() {
                if (self._configuration() !== undefined && self._configuration().hasOwnProperty('iops')) {
                    return self._configuration().iops;
                }
                return undefined;
            },
            write: function(value) {
                var target = self._configuration();
                if (value === '') {
                    delete target.iops;
                } else {
                    target.iops = parseInt(value, 10);
                    if (isNaN(target.iops)) {
                        delete target.iops;
                    }
                }
                self._configuration(target);
            }
        }).extend({ notify: 'always' });
        self.configCacheStrategy = ko.computed({
            read: function() {
                if (self._configuration() !== undefined && self._configuration().hasOwnProperty('cache_strategy')) {
                    return { name: self._configuration().cache_strategy };
                }
                return undefined;
            },
            write: function(value) {
                var target = self._configuration();
                if (value === undefined) {
                    delete target.cache_strategy;
                } else {
                    target.cache_strategy = value.name;
                }
                self._configuration(target);
            }
        });
        self.configCacheSize = ko.computed({
            read: function() {
                if (self._configuration() !== undefined && self._configuration().hasOwnProperty('cache_size')) {
                    return self._configuration().cache_size;
                }
                return undefined;
            },
            write: function(value) {
                var target = self._configuration();
                if (value === '') {
                    delete target.cache_size;
                } else {
                    target.cache_size = parseInt(value, 10);
                    if (isNaN(target.cache_size)) {
                        delete target.cache_size;
                    }
                }
                self._configuration(target);
            }
        }).extend({ notify: 'always' });
        self.configFoc = ko.computed({
            read: function() {
                if (self._configuration() !== undefined && self._configuration().hasOwnProperty('foc')) {
                    return { value: self._configuration().foc };
                }
                return undefined;
            },
            write: function(value) {
                var target = self._configuration();
                if (value === undefined) {
                    delete target.foc;
                } else {
                    target.foc = value.value;
                }
                self._configuration(target);
            }
        });
        self.configChanged = ko.computed(function() {
            var changed = false;
            if (self._configuration() !== undefined && self.configuration() !== undefined) {
                $.each(['iops', 'cache_strategy', 'cache_size', 'foc'], function (i, key) {
                    if (self._configuration()[key] !== self.configuration()[key]) {
                        changed = true;
                        return false;
                    }
                });
            }
            return changed;
        });

        // Functions
        self.fillData = function(data) {
            generic.trySet(self.name, data, 'name');
            generic.trySet(self.order, data, 'order');
            generic.trySet(self.snapshots, data, 'snapshots');
            generic.trySet(self.size, data, 'size');
            generic.trySet(self.vpoolGuid, data, 'vpool_guid');
            generic.trySet(self.vMachineGuid, data, 'vmachine_guid');
            generic.trySet(self.storageRouterGuid, data, 'storagerouter_guid');
            generic.trySet(self.resolvedConfiguration, data, 'resolved_configuration');
            if (data.hasOwnProperty('configuration')) {
                if (self._configuration() === undefined) {
                    self._configuration($.extend({}, data.configuration));
                }
                if (self.configuration() === undefined) {
                    self.configuration($.extend({}, data.configuration));
                }
                var target = self._configuration();
                generic.merge(self.configuration(), data.configuration, target, ['iops', 'cache_strategy', 'cache_size', 'foc']);
                self._configuration(target);
                self.configuration($.extend({}, data.configuration));
            }
            if (data.hasOwnProperty('info')) {
                self.storedData(data.info.stored);
                self.failoverMode(data.info.failover_mode.toLowerCase() || 'unknown');
                self.namespace(data.info.namespace);
            }
            if (data.hasOwnProperty('statistics')) {
                var stats = data.statistics;
                self.iops(stats.operations_ps);
                self.cacheHits(stats.cache_hits_ps);
                self.cacheMisses(stats.sco_cache_misses_ps);
                self.readSpeed(stats.data_read_ps);
                self.writeSpeed(stats.data_written_ps);
                self.backendWritten(stats.backend_data_written);
                self.backendRead(stats.backend_data_read);
                self.backendReads(stats.sco_cache_hits + stats.cluster_cache_hits);
                self.bandwidthSaved(Math.max(0, stats.data_read - stats.backend_data_read));
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
