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
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vmachine', '../containers/pmachine'
], function($, app, dialog, ko, shared, generic, Refresher, api, VMachine, PMachine) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared      = shared;
        self.guard       = { authenticated: true };
        self.refresher   = new Refresher();
        self.widgets     = [];
        self.updateSort  = false;
        self.sortTimeout = undefined;

        // Data
        self.vSAsHeaders = [
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
        self.vSAs = ko.observableArray([]);
        self.vSAsInitialLoad = ko.observable(true);

        // Variables
        self.loadVsasHandle = undefined;
        self.pMachineCache = {};

        // Functions
        self.load = function(full) {
            full = full || false;
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVsasHandle)) {
                    var query = {
                        query: {
                            type: 'AND',
                            items: [['is_internal', 'EQUALS', true]]
                        }
                    }, filter = {};
                    if (full) {
                        filter.full = true;
                    }
                    self.loadVsasHandle = api.post('vmachines/filter', query, filter)
                        .done(function(data) {
                            var i, guids = [], vmdata = {};
                            for (i = 0; i < data.length; i += 1) {
                                guids.push(data[i].guid);
                                vmdata[data[i].guid] = data[i];
                            }
                            generic.crossFiller(
                                guids, self.vSAs,
                                function(guid) {
                                    var vm = new VMachine(guid);
                                    if (full) {
                                        vm.fillData(vmdata[guid]);
                                    }
                                    return vm;
                                }, 'guid'
                            );
                            self.vSAsInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.loadVSA = function(vsa) {
            return $.Deferred(function(deferred) {
                vsa.load()
                    .done(function() {
                        var pMachineGuid = vsa.pMachineGuid(), pm;
                        if (pMachineGuid && (vsa.pMachine() === undefined || vsa.pMachine().guid() !== pMachineGuid)) {
                            if (!self.pMachineCache.hasOwnProperty(pMachineGuid)) {
                                pm = new PMachine(pMachineGuid);
                                pm.load();
                                self.pMachineCache[pMachineGuid] = pm;
                            }
                            vsa.pMachine(self.pMachineCache[pMachineGuid]);
                        }
                        // (Re)sort VSAs
                        if (self.updateSort) {
                            self.sort();
                        }
                    })
                    .always(deferred.resolve);
            }).promise();
        };
        self.sort = function() {
            if (self.sortTimeout) {
                window.clearTimeout(self.sortTimeout);
            }
            self.sortTimeout = window.setTimeout(function() { generic.advancedSort(self.vSAs, ['name', 'guid']); }, 250);
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.load, 5000);
            self.shared.footerData(self.vSAs);

            self.load(true)
                .always(function() {
                    self.sort();
                    self.updateSort = true;
                    self.refresher.start();
                });
        };
        self.deactivate = function() {
            var i;
            for (i = 0; i < self.widgets.length; i += 2) {
                self.widgets[i].deactivate();
            }
            self.refresher.stop();
            self.shared.footerData(ko.observable());
        };
    };
});
