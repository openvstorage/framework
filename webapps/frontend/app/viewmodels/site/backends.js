// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Modified Apache License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'jquery', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/backend', '../containers/backendtype', '../wizards/addbackend/index'
], function($, dialog, ko, shared, generic, Refresher, api, Backend, BackendType, AddBackendWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared           = shared;
        self.guard            = { authenticated: true, registered: true };
        self.refresher        = new Refresher();
        self.widgets          = [];
        self.backendHeaders   = [
            { key: 'name',     value: $.t('ovs:generic.name'),        width: 250       },
            { key: undefined,  value: $.t('ovs:generic.backendtype'), width: undefined },
            { key: undefined,  value: $.t('ovs:generic.status'),      width: 80        }
        ];
        self.backendTypeCache = {};

        // Observables
        self.backends                = ko.observableArray([]);
        self.backendsInitialLoad     = ko.observable(true);
        self.newBackend              = ko.observable(new Backend());
        self.backendTypes            = ko.observableArray([]);
        self.backendTypeMapping      = ko.observable({});
        self.backendTypesInitialLoad = ko.observable(true);

        // Handles
        self.loadBackendTypesHandle = undefined;
        self.backendsHandle         = {};

        // Functions
        self.loadBackends = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.backendsHandle[page])) {
                    var options = {
                        sort: 'name',
                        page: page,
                        contents: '_relations'
                    };
                    self.backendsHandle[page] = api.get('backends', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    return new Backend(guid);
                                },
                                dependencyLoader: function(item) {
                                    var backendTypeGuid = item.backendTypeGuid(), bt;
                                    if (backendTypeGuid && (item.backendType() === undefined || item.backendType().guid() !== backendTypeGuid)) {
                                        if (!self.backendTypeCache.hasOwnProperty(backendTypeGuid)) {
                                            bt = new BackendType(backendTypeGuid);
                                            bt.load();
                                            self.backendTypeCache[backendTypeGuid] = bt;
                                        }
                                        item.backendType(self.backendTypeCache[backendTypeGuid]);
                                    }
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.fetchBackendTypes = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadBackendTypesHandle)) {
                    var options = {
                        sort: 'name',
                        contents: '',
                        query: JSON.stringify({
                            type: 'AND',
                            items: [['has_plugin', 'EQUALS', true]]
                        })
                    };
                    self.loadBackendTypesHandle = api.get('backendtypes', { queryparams: options })
                        .done(function(data) {
                            var guids = [], btdata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                btdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.backendTypes,
                                function(guid) {
                                    var backendType = new BackendType(guid),
                                        mapping = self.backendTypeMapping();
                                    if (!mapping.hasOwnProperty(guid)) {
                                        mapping[guid] = backendType;
                                        self.backendTypeMapping(mapping);
                                    }
                                    return backendType;
                                }, 'guid'
                            );
                            $.each(self.backendTypes(), function(index, backendType) {
                                if ($.inArray(backendType.guid(), guids) !== -1) {
                                    backendType.fillData(btdata[backendType.guid()]);
                                }
                            });
                            self.backendTypesInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.addBackend = function() {
            dialog.show(new AddBackendWizard({
                modal: true
            }));
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.fetchBackendTypes, 5000);
            self.refresher.start();
            return self.fetchBackendTypes();
        };
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
            self.refresher.stop();
        };
    };
});
