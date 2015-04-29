// Copyright 2015 CloudFounders NV
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
    'knockout', 'jquery',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/storagerouter'
], function(ko, $, shared, generic, Refresher, api, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.widgets       = [];
        self.shared        = shared;
        self.guard         = { authenticated: true };
        self.refresher     = new Refresher();

        // Observables
        self.storageRouters = ko.observableArray([]);
        self.clusterid = ko.observable();
        self.dirty = ko.observable(false);
        self._enable = ko.observable();
        self._enableSupport = ko.observable();

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
        self.fetchStorageRouters = function() {
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
                                if ($.inArray(guid, guids) !== -1) {
                                    sr.fillData(sadata[guid]);
                                }
                                sr.nodeid = ko.observable();
                                sr.metadata = ko.observable('');
                                return sr;
                            }, 'guid'
                        );
                        $.each(self.storageRouters(), function(index, storageRouter) {
                            storageRouter.loading(true);
                            api.get('storagerouters/' + storageRouter.guid() + '/get_support_info')
                                .then(self.shared.tasks.wait)
                                .done(function(data) {
                                    storageRouter.nodeid(data.nodeid);
                                    self.clusterid(data.clusterid);
                                    if (self._enable() === undefined) {
                                        self._enable(data.enabled);
                                        self._enableSupport(data.enablesupport);
                                    }
                                });
                            api.get('storagerouters/' + storageRouter.guid() + '/get_support_metadata')
                                .then(self.shared.tasks.wait)
                                .done(function(data) {
                                    storageRouter.metadata(data);
                                });
                        });
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(function() {
                self.fetchStorageRouters();
            }, 5000);
            self.refresher.start();
            self.refresher.run();
        };
        self.deactivate = function() {
            $.each(self.widgets, function(i, item) {
                item.deactivate();
            });
            self.refresher.stop();
        };
    };
});
