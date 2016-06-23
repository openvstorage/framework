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
    'jquery', 'knockout',
    'ovs/generic', 'ovs/api', 'ovs/shared',
    'viewmodels/containers/vdisk'
], function($, ko, generic, api, shared, VDisk) {
    "use strict";
    return function(guid) {
        var self = this;

        // Variables
        self.shared        = shared;
        self.vMachineGuids = [];
        self.vPoolGuids    = [];

        // Handles
        self.loadChildrenGuid   = undefined;
        self.loadVMachineHandle = undefined;

        // External dependencies
        self.pMachine       = ko.observable();
        self.storageRouters = ko.observableArray([]);
        self.vPools         = ko.observableArray([]);
        self.vMachines      = ko.observableArray([]);

        // Observables
        self.backendRead           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.backendWritten        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.bandwidthSaved        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.cacheHits             = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.cacheMisses           = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.dtlStatus             = ko.observable();
        self.guid                  = ko.observable(guid);
        self.hypervisorStatus      = ko.observable();
        self.iops                  = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.ipAddress             = ko.observable();
        self.isVTemplate           = ko.observable();
        self.loaded                = ko.observable(false);
        self.loading               = ko.observable(false);
        self.name                  = ko.observable();
        self.pMachineGuid          = ko.observable();
        self.readSpeed             = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });
        self.snapshots             = ko.observableArray([]);
        self.status                = ko.observable();
        self.storedData            = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatBytes });
        self.templateChildrenGuids = ko.observableArray([]);
        self.totalCacheHits        = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatNumber });
        self.vDisks                = ko.observableArray([]);
        self.writeSpeed            = ko.observable().extend({ smooth: {} }).extend({ format: generic.formatSpeed });

        // Computed
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
        self.fetchTemplateChildrenGuids = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadChildrenGuid)) {
                    self.loadChildrenGuid = api.get('vmachines/' + self.guid() + '/get_children')
                        .done(function(data) {
                            self.templateChildrenGuids(data.data);
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
            generic.trySet(self.isVTemplate, data, 'is_vtemplate');
            if (data.hasOwnProperty('snapshots')) {
                var snapshots = [];
                $.each(data.snapshots, function(index, snapshot) {
                    if (snapshot.in_backend) {
                        snapshots.push(snapshot);
                    }
                });
                self.snapshots(snapshots);
            }
            generic.trySet(self.status, data, 'status', generic.lower);
            generic.trySet(self.dtlStatus, data, 'dtl_status');
            generic.trySet(self.pMachineGuid, data, 'pmachine_guid');
            if (data.hasOwnProperty('storagerouters_guids')) {
                self.storageRouterGuids = data.storagerouters_guids;
            }
            if (data.hasOwnProperty('vpools_guids')) {
                self.vPoolGuids = data.vpools_guids;
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
                self.iops(stats['4k_operations_ps']);
                self.cacheHits(stats.cache_hits_ps);
                self.cacheMisses(stats.cache_misses_ps);
                self.totalCacheHits(stats.cache_hits);
                self.readSpeed(stats.data_read_ps);
                self.writeSpeed(stats.data_written_ps);
                self.backendWritten(stats.backend_data_written);
                self.backendRead(stats.backend_data_read);
                self.bandwidthSaved(Math.max(0, stats.data_read - stats.backend_data_read));
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
                if (generic.xhrCompleted(self.loadVMachineHandle)) {
                    var options = {};
                    if (contents !== undefined) {
                        options.contents = contents;
                    }
                    self.loadVMachineHandle = api.get('vmachines/' + self.guid(), { queryparams: options })
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
