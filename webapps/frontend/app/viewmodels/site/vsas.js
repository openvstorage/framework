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

        // Variables
        self.shared        = shared;
        self.guard         = { authenticated: true };
        self.refresher     = new Refresher();
        self.widgets       = [];
        self.pMachineCache = {};
        self.query         = {
            query: {
                type: 'AND',
                items: [['is_internal', 'EQUALS', true]]
            }
        };
        self.vSAsHeaders   = [
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
        self.vSAs = ko.observableArray([]);
        self.vSAsInitialLoad = ko.observable(true);

        // Handles
        self.loadVSAsHandle = undefined;
        self.refreshVSAsHandle = {};

        // Functions
        self.fetchVSAs = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVSAsHandle)) {
                    var options = {
                        sort: 'name',
                        full: true,
                        contents: 'stored_data'
                    };
                    self.loadVSAsHandle = api.post('vmachines/filter', self.query, options)
                        .done(function(data) {
                            var guids = [], vsadata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vsadata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.vSAs,
                                function(guid) {
                                    var vmachine = new VMachine(guid);
                                    if ($.inArray(guid, guids) !== -1) {
                                        vmachine.fillData(vsadata[guid]);
                                    }
                                    vmachine.loading(true);
                                    return vmachine;
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
        self.refreshVSAs = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.refreshVSAsHandle[page])) {
                    var options = {
                        sort: 'name',
                        full: true,
                        page: page,
                        contents: '_relations,statistics,stored_data'
                    };
                    self.refreshVSAsHandle[page] = api.post('vmachines/filter', self.query, options)
                        .done(function(data) {
                            var guids = [], vsadata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vsadata[item.guid] = item;
                            });
                            $.each(self.vSAs(), function(index, vsa) {
                                if ($.inArray(vsa.guid(), guids) !== -1) {
                                    vsa.fillData(vsadata[vsa.guid()]);
                                    vsa.loadDisks();
                                    var pMachineGuid = vsa.pMachineGuid(), pm;
                                    if (pMachineGuid && (vsa.pMachine() === undefined || vsa.pMachine().guid() !== pMachineGuid)) {
                                        if (!self.pMachineCache.hasOwnProperty(pMachineGuid)) {
                                            pm = new PMachine(pMachineGuid);
                                            pm.load();
                                            self.pMachineCache[pMachineGuid] = pm;
                                        }
                                        vsa.pMachine(self.pMachineCache[pMachineGuid]);
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
            self.refresher.init(self.fetchVSAs, 5000);
            self.refresher.start();
            self.shared.footerData(self.vSAs);

            self.fetchVSAs().then(function() {
                self.refreshVSAs(1);
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
