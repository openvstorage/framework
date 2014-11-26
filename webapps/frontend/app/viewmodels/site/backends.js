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
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/backend', '../containers/backendtype'
], function($, ko, shared, generic, Refresher, api, Backend, BackendType) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared           = shared;
        self.guard            = { authenticated: true };
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
        self.loadBackendsHandle     = undefined;
        self.refreshBackendsHandle  = {};
        self.loadBackendTypesHandle = undefined;

        // Functions
        self.fetchBackends = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadBackendsHandle)) {
                    var options = {
                        sort: 'name',
                        contents: '_relations'
                    };
                    self.loadBackendsHandle = api.get('backends', { queryparams: options })
                        .done(function(data) {
                            var guids = [], bdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                bdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.backends,
                                function(guid) {
                                    var backend = new Backend(guid), bt, backendTypeGuid;
                                    if ($.inArray(guid, guids) !== -1) {
                                        backend.fillData(bdata[guid]);
                                        backendTypeGuid = backend.backendTypeGuid();
                                        if (backendTypeGuid && (backend.backendType() === undefined || backend.backendType().guid() !== backendTypeGuid)) {
                                            if (!self.backendTypeCache.hasOwnProperty(backendTypeGuid)) {
                                                bt = new BackendType(backendTypeGuid);
                                                bt.load();
                                                self.backendTypeCache[backendTypeGuid] = bt;
                                            }
                                            backend.backendType(self.backendTypeCache[backendTypeGuid]);
                                        }
                                    }
                                    backend.loading(true);
                                    return backend;
                                }, 'guid'
                            );
                            self.backendsInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.refreshBackends = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.refreshBackendsHandle[page])) {
                    var options = {
                        sort: 'name',
                        page: page,
                        contents: '_relations'
                    };
                    self.refreshBackendsHandle[page] = api.get('backends', { queryparams: options })
                        .done(function(data) {
                            var guids = [], bdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                bdata[item.guid] = item;
                            });
                            $.each(self.backends(), function(index, be) {
                                if ($.inArray(be.guid(), guids) !== -1) {
                                    be.fillData(bdata[be.guid()]);
                                    var bt, backendTypeGuid = be.backendTypeGuid();
                                    if (backendTypeGuid && (be.backendType() === undefined || be.backendType().guid() !== backendTypeGuid)) {
                                        if (!self.backendTypeCache.hasOwnProperty(backendTypeGuid)) {
                                            bt = new BackendType(backendTypeGuid);
                                            bt.load();
                                            self.backendTypeCache[backendTypeGuid] = bt;
                                        }
                                        be.backendType(self.backendTypeCache[backendTypeGuid]);
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
                            $.each(data, function(index, item) {
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
        self.saveBackend = function() {
            api.post('backends', {
                name: self.newBackend().name(),
                backend_type_guid: self.newBackend().backendType().guid()
            })
                .done(function() {
                    generic.alertSuccess(
                        $.t('ovs:backends.new.complete'),
                        $.t('ovs:backends.new.addsuccess')
                    );
                    self.fetchBackends();
                })
                .fail(function(error) {
                    error = $.parseJSON(error.responseText);
                    generic.alertError(
                        $.t('ovs:generic.error'),
                        $.t('ovs:backends.new.addfailed', { why: error.detail })
                    );
                })
                .always(function() {
                    self.newBackend(new Backend());
                });
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(function() {
                self.fetchBackends();
                self.fetchBackendTypes();
            }, 5000);
            self.refresher.start();

            self.fetchBackends().then(function() {
                self.refreshBackends(1);
            });
            self.fetchBackendTypes();
        };
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
            self.refresher.stop();
        };
    };
});
