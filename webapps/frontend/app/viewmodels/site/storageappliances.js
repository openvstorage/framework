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
/*global define*/
define([
    'jquery', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/storageappliance', '../containers/pmachine'
], function($, ko, shared, generic, Refresher, api, StorageAppliance, PMachine) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared                     = shared;
        self.guard                      = { authenticated: true };
        self.refresher                  = new Refresher();
        self.widgets                    = [];
        self.pMachineCache              = {};
        self.storageAppliancesHeaders   = [
            { key: 'status',     value: $.t('ovs:generic.status'),     width: 55  },
            { key: 'name',       value: $.t('ovs:generic.name'),       width: 100 },
            { key: 'ip',         value: $.t('ovs:generic.ip'),         width: 100 },
            { key: 'host',       value: $.t('ovs:generic.host'),       width: 55  },
            { key: 'type',       value: $.t('ovs:generic.type'),       width: 55  },
            { key: 'vdisks',     value: $.t('ovs:generic.vdisks'),     width: 55  },
            { key: 'storedData', value: $.t('ovs:generic.storeddata'), width: 100 },
            { key: 'cacheRatio', value: $.t('ovs:generic.cache'),      width: 100 },
            { key: 'iops',       value: $.t('ovs:generic.iops'),       width: 55  },
            { key: 'readSpeed',  value: $.t('ovs:generic.read'),       width: 100 },
            { key: 'writeSpeed', value: $.t('ovs:generic.write'),      width: 100 }
        ];

        // Observables
        self.storageAppliances            = ko.observableArray([]);
        self.storageAppliancesInitialLoad = ko.observable(true);

        // Handles
        self.loadStorageAppliancesHandle    = undefined;
        self.refreshStorageAppliancesHandle = {};

        // Functions
        self.fetchStorageAppliances = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadStorageAppliancesHandle)) {
                    var options = {
                        sort: 'name',
                        contents: 'stored_data'
                    };
                    self.loadStorageAppliancesHandle = api.get('storageappliances', undefined, options)
                        .done(function(data) {
                            var guids = [], sadata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                sadata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.storageAppliances,
                                function(guid) {
                                    var sa = new StorageAppliance(guid);
                                    if ($.inArray(guid, guids) !== -1) {
                                        sa.fillData(sadata[guid]);
                                    }
                                    sa.loading(true);
                                    return sa;
                                }, 'guid'
                            );
                            self.storageAppliancesInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.refreshStorageAppliances = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.refreshStorageAppliancesHandle[page])) {
                    var options = {
                        sort: 'name',
                        page: page,
                        contents: '_relations,statistics,stored_data,vdisks_guids'
                    };
                    self.refreshStorageAppliancesHandle[page] = api.get('storageappliances', undefined, options)
                        .done(function(data) {
                            var guids = [], sadata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                sadata[item.guid] = item;
                            });
                            $.each(self.storageAppliances(), function(index, storageAppliance) {
                                if ($.inArray(storageAppliance.guid(), guids) !== -1) {
                                    storageAppliance.fillData(sadata[storageAppliance.guid()]);
                                    var pMachineGuid = storageAppliance.pMachineGuid(), pm;
                                    if (pMachineGuid && (storageAppliance.pMachine() === undefined || storageAppliance.pMachine().guid() !== pMachineGuid)) {
                                        if (!self.pMachineCache.hasOwnProperty(pMachineGuid)) {
                                            pm = new PMachine(pMachineGuid);
                                            pm.load();
                                            self.pMachineCache[pMachineGuid] = pm;
                                        }
                                        storageAppliance.pMachine(self.pMachineCache[pMachineGuid]);
                                    }
                                }
                            });
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.resolve();
                }
            }).promise();
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.fetchStorageAppliances, 5000);
            self.refresher.start();
            self.shared.footerData(self.storageAppliances);

            self.fetchStorageAppliances().then(function() {
                self.refreshStorageAppliances(1);
            });
        };
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
            self.refresher.stop();
            self.shared.footerData(ko.observable());
        };
    };
});
