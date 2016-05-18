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
    '../containers/vpool',
    '../wizards/addvpool/index'
], function($, dialog, ko, shared, generic, Refresher, api, VPool, AddVPoolWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared       = shared;
        self.guard        = { authenticated: true };
        self.refresher    = new Refresher();
        self.widgets      = [];
        self.vPoolHeaders = [
            { key: 'name',              value: $.t('ovs:generic.name'),                   width: 200       },
            { key: 'storedData',        value: $.t('ovs:generic.storeddata'),             width: 150       },
            { key: 'cacheRatio',        value: $.t('ovs:generic.cache'),                  width: 100       },
            { key: 'iops',              value: $.t('ovs:generic.iops'),                   width: 100       },
            { key: 'backendType',       value: $.t('ovs:vpools.backendtype'),             width: 180       },
            { key: 'backendConnection', value: $.t('ovs:vpools.backendconnectionpreset'), width: 230       },
            { key: 'backendLogin',      value: $.t('ovs:vpools.backendlogin'),            width: undefined },
            { key: 'status',            value: $.t('ovs:generic.status'),                 width: 80        }
        ];
        self.vPoolCache = {};

        // Handles
        self.vPoolsHandle = {};

        // Observables
        self.vPools = ko.observableArray([]);

        // Functions
        self.loadVPools = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.vPoolsHandle[options.page])) {
                    options.sort = 'name';
                    options.contents = '_dynamics,backend_type';
                    self.vPoolsHandle[options.page] = api.get('vpools', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    if (!self.vPoolCache.hasOwnProperty(guid)) {
                                        self.vPoolCache[guid] = new VPool(guid);
                                    }
                                    return self.vPoolCache[guid];
                                },
                                dependencyLoader: function(item) {
                                    item.loadBackendType(false);
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.addVPool = function() {
            dialog.show(new AddVPoolWizard({
                modal: true
            }));
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(function() {
                if (generic.xhrCompleted(self.vPoolsHandle[undefined])) {
                    self.vPoolsHandle[undefined] = api.get('vpools', { queryparams: { contents: 'statistics,stored_data,backend_type' }})
                        .done(function(data) {
                            var guids = [], vpdata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                vpdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.vPools,
                                function(guid) {
                                    if (!self.vPoolCache.hasOwnProperty(guid)) {
                                         self.vPoolCache[guid] = new VPool(guid);
                                    }
                                    return self.vPoolCache[guid];
                                }, 'guid'
                            );
                            $.each(self.vPools(), function(index, item) {
                                if (vpdata.hasOwnProperty(item.guid())) {
                                    item.fillData(vpdata[item.guid()]);
                                }
                            });
                        });
                }
            }, 60000);
            self.refresher.start();
            self.refresher.run();
            self.shared.footerData(self.vPools);
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
