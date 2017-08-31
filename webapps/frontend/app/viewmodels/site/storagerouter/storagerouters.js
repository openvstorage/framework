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
/*global define*/
define([
    'jquery', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../../containers/domain', '../../containers/storagerouter'
], function($, ko, shared, generic, Refresher, api, Domain, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.domainCache           = {};
        self.shared                = shared;
        self.guard                 = { authenticated: true };
        self.refresher             = new Refresher();
        self.widgets               = [];
        self.storageRoutersHeaders = [
            { key: 'status',         value: '',                                  width: 30        },
            { key: 'name',           value: $.t('ovs:generic.name'),             width: 180       },
            { key: 'ip',             value: $.t('ovs:generic.ip'),               width: 125       },
            { key: 'vdisks',         value: $.t('ovs:generic.vdisks'),           width: 60        },
            { key: 'iops',           value: $.t('ovs:generic.iops'),             width: 60        },
            { key: 'storedData',     value: $.t('ovs:generic.storeddata'),       width: 100       },
            { key: 'readSpeed',      value: $.t('ovs:generic.read'),             width: 100       },
            { key: 'writeSpeed',     value: $.t('ovs:generic.write'),            width: 100       },
            { key: 'domain',         value: $.t('ovs:generic.domains'),          width: 200       },
            { key: 'recoveryDomain', value: $.t('ovs:generic.recovery_domains'), width: undefined }
        ];

        // Handles
        self.domainsHandle        = undefined;
        self.storageRoutersHandle = {};

        // Functions
        self.loadStorageRouters = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.storageRoutersHandle[options.page])) {
                    options.sort = 'name';
                    options.contents = '_relations,statistics,vdisks_guids,status,partition_config,regular_domains,recovery_domains';
                    self.storageRoutersHandle[options.page] = api.get('storagerouters', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    return new StorageRouter(guid);
                                },
                                dependencyLoader: function(item) {
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
                                    generic.crossFiller(
                                        item.recoveryDomainGuids(), item.recoveryDomains,
                                        function(guid) {
                                            if (!self.domainCache.hasOwnProperty(guid)) {
                                                var domain = new Domain(guid);
                                                domain.load();
                                                self.domainCache[guid] = domain;
                                            }
                                            return self.domainCache[guid];
                                        }, 'guid'
                                    );
                                    item.recoveryDomains.sort(function(dom1, dom2) {
                                        return dom1.name() < dom2.name() ? -1 : 1;
                                    });
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
            self.refresher.init(self.loadDomains, 5000);
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
