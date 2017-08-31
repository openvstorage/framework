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
    'jquery', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../../containers/backend', '../../containers/backendtype', '../../containers/domain',
    '../../wizards/addbackend/index'
], function($, dialog, ko, shared, generic, Refresher, api, Backend, BackendType, Domain, AddBackendWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.domainCache      = {};
        self.shared           = shared;
        self.guard            = { authenticated: true };
        self.refresher        = new Refresher();
        self.widgets          = [];
        self.backendHeaders   = [
            { key: undefined,  value: '',                              width: 30        },
            { key: 'name',     value: $.t('ovs:generic.name'),         width: 250       },
            { key: undefined,  value: $.t('ovs:generic.backend_type'), width: 250       },
            { key: 'domain',   value: $.t('ovs:generic.domains'),      width: undefined }
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
        self.domainsHandle          = undefined;

        // Functions
        self.loadBackends = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.backendsHandle[options.page])) {
                    options.sort = 'name';
                    options.contents = '_relations,regular_domains';
                    if (options.progressive === false) {
                        options.contents += ',live_status';
                    }
                    self.backendsHandle[options.page] = api.get('backends', { queryparams: options })
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
                                    generic.crossFiller(
                                        item.domainGuids(), item.domains,
                                        function(guid) {
                                            if (!self.domainCache.hasOwnProperty(guid)) {
                                                var domain = new Domain(guid);
                                                domain.load();
                                                self.domainCache[guid] = domain;
                                            }
                                            return self.domainCache[guid];
                                        }, 'guid'
                                    );
                                    item.domains.sort(function(dom1, dom2) {
                                        return dom1.name() < dom2.name() ? -1 : 1;
                                    });
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
        self.loadDomains = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.domainsHandle)) {
                    self.domainsHandle = api.get('domains', {queryparams: {contents: ''}})
                        .done(function(data) {
                            $.each(data.data, function(index, item) {
                                if (!self.domainCache.hasOwnProperty(item.guid)) {
                                    self.domainCache[item.guid] = new Domain(item.guid);
                                }
                                self.domainCache[item.guid].fillData(item);
                            });
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
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
