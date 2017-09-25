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
    'knockout', 'jquery',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../../containers/storagerouter'
], function(ko, $, shared, generic, Refresher, api, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.widgets   = [];
        self.shared    = shared;
        self.guard     = { authenticated: true };
        self.refresher = new Refresher();

        // Observables
        self.storageRouters = ko.observableArray([]);
        self.clusterid      = ko.observable();
        self.dirty          = ko.observable(false);
        self._enable        = ko.observable();
        self._enableSupport = ko.observable();
        self.releaseName    = ko.observable(shared.releaseName);

        // Handles
        self.supportInfoHandle     = {};
        self.supportMetadataHandle = {};
        self.versionInfoHandle     = {};

        // Computed
        self.enableSupport = ko.computed({
            write: function(value) {
                self.dirty(true);
                if (self.enable() !== undefined && self.enable() === true) {
                    self._enableSupport(value);
                } else {
                    self._enableSupport(false);
                }
            },
            read: function() {
                return self._enableSupport();
            }
        });
        self.enable = ko.computed({
            write: function(value) {
                self.dirty(true);
                self._enable(value);
                if (value === false) {
                    self.enableSupport(false);
                }
            },
            read: function() {
                return self._enable();
            }
        });
        self.lastHeartbeat = ko.computed(function() {
            var timestamp = undefined, currentTimestamp;
            $.each(self.storageRouters(), function(index, storageRouter) {
                currentTimestamp = storageRouter.lastHeartbeat();
                if (currentTimestamp !== undefined && (timestamp === undefined || currentTimestamp > timestamp)) {
                    timestamp = currentTimestamp;
                }
            });
            return timestamp
        });

        // Functions
        self.save = function() {
            if (self.storageRouters().length > 0) {
                var data = {
                    enable: self.enable(),
                    enable_support: self.enableSupport()
                };
                generic.alertInfo(
                    $.t('ovs:support.saving'),
                    $.t('ovs:support.savingmsg')
                );
                api.post('storagerouters/' + self.storageRouters()[0].guid() + '/configure_support', { data: data })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:support.saved'),
                            $.t('ovs:support.savedmsg')
                        );
                        self.dirty(false);
                    })
                    .fail(function() {
                        generic.alertError(
                            $.t('ovs:support.failed'),
                            $.t('ovs:support.failedmsg')
                        );
                    });
            }
        };
        self.loadStorageRouters = function() {
            return $.Deferred(function(deferred) {
                var options = {
                    sort: 'name',
                    contents: ''
                };
                api.get('storagerouters', { queryparams: options })
                    .done(function(data) {
                        var guids = [], sadata = {};
                        $.each(data.data, function(index, item) {
                            guids.push(item.guid);
                            sadata[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.storageRouters,
                            function(guid) {
                                var sr = new StorageRouter(guid);
                                sr.nodeid = ko.observable();
                                sr.metadata = ko.observable('');
                                sr.versions = ko.observable({});
                                sr.packageNames = ko.observableArray([]);
                                return sr;
                            }, 'guid'
                        );
                        $.each(self.storageRouters(), function(index, storageRouter) {
                            if (guids.contains(storageRouter.guid())) {
                                storageRouter.fillData(sadata[storageRouter.guid()]);
                            }
                            storageRouter.loading(true);
                            var calls = [];
                            if (generic.xhrCompleted(self.supportInfoHandle[storageRouter.guid()])) {
                                self.supportInfoHandle[storageRouter.guid()] = api.get('storagerouters/' + storageRouter.guid() + '/get_support_info')
                                    .then(self.shared.tasks.wait)
                                    .then(function (data) {
                                        storageRouter.nodeid(data.nodeid);
                                        self.clusterid(data.clusterid);
                                        if (self._enable() === undefined) {
                                            self._enable(data.enabled);
                                            self._enableSupport(data.enablesupport);
                                        }
                                    });
                                calls.push(self.supportInfoHandle[storageRouter.guid()]);
                            }
                            if (generic.xhrCompleted(self.supportMetadataHandle[storageRouter.guid()])) {
                                self.supportMetadataHandle[storageRouter.guid()] = api.get('storagerouters/' + storageRouter.guid() + '/get_support_metadata')
                                    .then(self.shared.tasks.wait)
                                    .then(function (data) {
                                        storageRouter.metadata(data);
                                    });
                                calls.push(self.supportMetadataHandle[storageRouter.guid()]);
                            }
                            if (generic.xhrCompleted(self.versionInfoHandle[storageRouter.guid()])) {
                                self.versionInfoHandle[storageRouter.guid()] = api.get('storagerouters/' + storageRouter.guid() + '/get_version_info')
                                    .then(self.shared.tasks.wait)
                                    .then(function (data) {
                                        storageRouter.packageNames(generic.keys(data.versions));
                                        storageRouter.versions(data.versions);
                                        storageRouter.packageNames.sort(function(name1, name2) {
                                            return name1 < name2 ? -1 : 1;
                                        });
                                    });
                                calls.push(self.versionInfoHandle[storageRouter.guid()]);
                            }
                            $.when.apply($, calls)
                                .always(function() {
                                    storageRouter.loading(false);
                                })
                        });
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };

        // Durandal
        self.activate = function() {
            $.each(shared.hooks.pages, function(pageType, pages) {
                if (pageType === 'support') {
                    $.each(pages, function(index, page) {
                        page.activator.activateItem(page.module);
                    })
                }
            });
            self.refresher.init(self.loadStorageRouters, 5000);
            self.refresher.start();
            self.refresher.run();
        };
        self.deactivate = function() {
            $.each(shared.hooks.pages, function(pageType, pages) {
                if (pageType === 'support') {
                    $.each(pages, function(index, page) {
                        page.activator.deactivateItem(page.module);
                    });
                }
            });
            $.each(self.widgets, function(i, item) {
                item.deactivate();
            });
            self.refresher.stop();
        };
    };
});
