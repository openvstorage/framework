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
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vmachine', '../containers/vpool', '../containers/storagerouter'
], function($, ko, shared, generic, Refresher, api, VMachine, VPool, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared             = shared;
        self.guard              = { authenticated: true };
        self.refresher          = new Refresher();
        self.widgets            = [];
        self.vPoolCache         = {};
        self.storageRouterCache = {};
        self.query              = {
            type: 'AND',
            items: [['is_vtemplate', 'EQUALS', false],
                    ['status', 'NOT_EQUALS', 'CREATED']]
        };
        self.vMachineHeaders    = [
            { key: 'name',          value: $.t('ovs:generic.name'),          width: undefined },
            { key: 'vpool',         value: $.t('ovs:generic.vpool'),         width: 150       },
            { key: 'storagerouter', value: $.t('ovs:generic.storagerouter'), width: 150       },
            { key: undefined,       value: $.t('ovs:generic.vdisks'),        width: 60        },
            { key: 'storedData',    value: $.t('ovs:generic.storeddata'),    width: 110       },
            { key: 'cacheRatio',    value: $.t('ovs:generic.cache'),         width: 100       },
            { key: 'iops',          value: $.t('ovs:generic.iops'),          width: 55        },
            { key: 'readSpeed',     value: $.t('ovs:generic.read'),          width: 120       },
            { key: 'writeSpeed',    value: $.t('ovs:generic.write'),         width: 120       },
            { key: 'dtlStatus',     value: $.t('ovs:generic.dtl_status'),    width: 50        }
        ];

        // Handles
        self.vMachinesHandle = {};
        self.vPoolsHandle    = undefined;

        // Observables
        self.vPools = ko.observableArray([]);

        // Functions
        self.loadVMachines = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.vMachinesHandle[options.page])) {
                    options.sort = 'name';
                    options.contents = '_dynamics,_relations,-snapshots,-hypervisor_status';
                    options.query = JSON.stringify(self.query);
                    self.vMachinesHandle[options.page] = api.get('vmachines', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    return new VMachine(guid);
                                },
                                dependencyLoader: function(item) {
                                    generic.crossFiller(
                                        item.storageRouterGuids, item.storageRouters,
                                        function(guid) {
                                            if (!self.storageRouterCache.hasOwnProperty(guid)) {
                                                var sr = new StorageRouter(guid);
                                                sr.load('');
                                                self.storageRouterCache[guid] = sr;
                                            }
                                            return self.storageRouterCache[guid];
                                        }, 'guid'
                                    );
                                    generic.crossFiller(
                                        item.vPoolGuids, item.vPools,
                                        function(guid) {
                                            if (!self.vPoolCache.hasOwnProperty(guid)) {
                                                var vp = new VPool(guid);
                                                vp.load('');
                                                self.vPoolCache[guid] = vp;
                                            }
                                            return self.vPoolCache[guid];
                                        }, 'guid'
                                    );
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(function() {
                if (generic.xhrCompleted(self.vPoolsHandle)) {
                    self.vPoolsHandle = api.get('vpools', { queryparams: { contents: '' }})
                        .done(function(data) {
                            var guids = [], vpdata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                vpdata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.vPools,
                                function(guid) {
                                    return new VPool(guid);
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
