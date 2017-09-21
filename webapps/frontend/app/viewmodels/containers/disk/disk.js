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
    'ovs/generic', 'ovs/api',
    'viewmodels/containers/disk/diskpartition'
], function($, ko, generic, api, Partition) {
    "use strict";
    return function(guid) {
        var self = this;

        // Handles
        self.loadHandle     = undefined;
        self.loadPartitions = undefined;

        // External dependencies
        self.storageRouter = ko.observable();

        // Observables
        self.diskModel         = ko.observable();
        self.guid              = ko.observable(guid);
        self.isSsd             = ko.observable();
        self.loaded            = ko.observable(false);
        self.loading           = ko.observable(false);
        self.name              = ko.observable();
        self.partitions        = ko.observableArray([]);
        self.partitionsLoaded  = ko.observable(false);
        self.aliases           = ko.observable();
        self.size              = ko.observable().extend({ format: generic.formatBytes });
        self.state             = ko.observable();
        self.storageRouterGuid = ko.observable();
        self.trigger           = ko.observable();

        // Computed
        self.fullPartitions = ko.computed(function() {
            var data = [], minSize = 3,
                previousPartition, partition, newPartition, runningIndex;
            if (self.partitions().length > 0) {
                $.each(self.partitions(), function (index, partition) {
                    if (previousPartition === undefined) {
                        if (partition.offset.raw() !== 0) {
                            newPartition = new Partition();
                            newPartition.state('RAW');
                            newPartition.offset(0);
                            newPartition.size(partition.offset.raw());
                            data.push(newPartition);
                        }
                    } else if (previousPartition.offset.raw() + previousPartition.size.raw() < partition.offset.raw()) {
                        newPartition = new Partition();
                        newPartition.state('RAW');
                        newPartition.offset(previousPartition.offset.raw() + previousPartition.size.raw());
                        newPartition.size(partition.offset.raw() - previousPartition.offset.raw() - previousPartition.size.raw());
                        data.push(newPartition);
                    }
                    data.push(partition);
                    previousPartition = partition;
                });
                partition = self.partitions()[self.partitions().length - 1];
                if (partition.offset.raw() + partition.size.raw() < self.size.raw()) {
                    newPartition = new Partition();
                    newPartition.state('RAW');
                    newPartition.offset(partition.offset.raw() + partition.size.raw());
                    newPartition.size(self.size.raw() - partition.offset.raw() - partition.size.raw());
                    data.push(newPartition);
                }
            } else {
                newPartition = new Partition();
                newPartition.state('RAW');
                newPartition.offset(0);
                newPartition.size(self.size.raw());
                data.push(newPartition);
            }
            $.each(data, function(index, partition) {
                partition.relativeSize(Math.round(partition.size.raw() / self.size.raw() * 100));
                partition.small = false;
            });
            if (data.length > 1) {
                $.each(data, function (index, partition) {
                    if (partition.relativeSize() < minSize) {
                        runningIndex = index + 1;
                        while(runningIndex !== index && partition.relativeSize() < minSize) {
                            if (runningIndex === data.length) {
                                runningIndex = 0;
                            }
                            if (runningIndex !== index && data[runningIndex].relativeSize() >= (minSize * 2)) {
                                data[runningIndex].relativeSize(data[runningIndex].relativeSize() - (minSize - partition.relativeSize()));
                                partition.relativeSize(minSize);
                                partition.small = true;
                            }
                            runningIndex += 1;
                        }
                    }
                });
            }
            return data;
        }).extend({ rateLimit: { method: 'notifyWhenChangesStop', timeout: 100 } });

        // Functions
        self.fillData = function(data) {
            generic.trySet(self.aliases, data, 'aliases');
            generic.trySet(self.diskModel, data, 'model');
            generic.trySet(self.state, data, 'state');
            generic.trySet(self.name, data, 'name');
            generic.trySet(self.size, data, 'size');
            generic.trySet(self.isSsd, data, 'is_ssd');
            generic.trySet(self.storageRouterGuid, data, 'storagerouter_guid');

            self.loaded(true);
            self.loading(false);
            self.trigger(generic.getTimestamp());
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                if (generic.xhrCompleted(self.loadHandle)) {
                    self.loadHandle = api.get('disks/' + self.guid())
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
        self.getPartitions = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadPartitions)) {
                    self.loadPartitions = api.get('diskpartitions', { queryparams: {
                        diskguid: self.guid(),
                        contents: '_relations,_dynamics',
                        sort: 'offset'
                    }})
                        .done(function(data) {
                            var guids = [], pdata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                pdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.partitions,
                                function(guid) {
                                    var p = new Partition(guid, self);
                                    p.loading(true);
                                    return p;
                                }, 'guid'
                            );
                            $.each(self.partitions(), function(index, partition) {
                                if (pdata.hasOwnProperty(partition.guid())) {
                                    partition.fillData(pdata[partition.guid()]);
                                }
                                // Show partitions as failed if disk state is in error or missing
                                if (['FAILURE', 'MISSING'].contains(self.state())) {
                                    partition.state('FAILURE');
                                }
                            });
                            self.partitions.sort(function(a, b) {
                                return a.offset.raw() - b.offset.raw();
                            });
                            self.partitionsLoaded(true);
                            self.trigger(generic.getTimestamp());
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
    };
});
