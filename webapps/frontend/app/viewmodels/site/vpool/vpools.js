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
    '../../containers/vpool',
    '../../wizards/addvpool/index'
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
            { key: 'status',     value: '',                               width: 30        },
            { key: 'name',       value: $.t('ovs:generic.name'),          width: undefined },
            { key: 'backend',    value: $.t('ovs:vpools.backend_preset'), width: 250       },
            { key: 'storedData', value: $.t('ovs:generic.storeddata'),    width: 150       },
            { key: 'iops',       value: $.t('ovs:generic.iops'),          width: 150       }
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
                    options.contents = '_dynamics';
                    self.vPoolsHandle[options.page] = api.get('vpools', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    if (!self.vPoolCache.hasOwnProperty(guid)) {
                                        self.vPoolCache[guid] = new VPool(guid);
                                    }
                                    return self.vPoolCache[guid];
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
                    self.vPoolsHandle[undefined] = api.get('vpools', { queryparams: { contents: 'statistics' }})
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
        };
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
            self.refresher.stop();
        };
    };
});
