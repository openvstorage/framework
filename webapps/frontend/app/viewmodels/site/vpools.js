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
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vpool'
], function($, app, dialog, ko, shared, generic, Refresher, api, VPool) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared       = shared;
        self.guard        = { authenticated: true };
        self.refresher    = new Refresher();
        self.widgets      = [];
        self.vPoolHeaders = [
            { key: 'name',              value: $.t('ovs:generic.name'),             width: 150       },
            { key: 'storedData',        value: $.t('ovs:generic.storeddata'),       width: 100       },
            { key: 'freeSpace',         value: $.t('ovs:vpools.freespace'),         width: 100       },
            { key: 'cacheRatio',        value: $.t('ovs:generic.cache'),            width: 100       },
            { key: 'iops',              value: $.t('ovs:generic.iops'),             width: 55        },
            { key: 'backendType',       value: $.t('ovs:vpools.backendtype'),       width: 100       },
            { key: 'backendConnection', value: $.t('ovs:vpools.backendconnection'), width: 100       },
            { key: 'backendLogin',      value: $.t('ovs:vpools.backendlogin'),      width: undefined },
            { key: undefined,           value: $.t('ovs:generic.actions'),          width: 100       }
        ];

        // Observables
        self.vPools            = ko.observableArray([]);
        self.vPoolsInitialLoad = ko.observable(true);

        // Handles
        self.loadVPoolsHandle    = undefined;
        self.refreshVPoolsHandle = {};

        // Functions
        self.fetchVPools = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadVPoolsHandle)) {
                    self.loadVPoolsHandle = api.get('vpools', {}, { sort: 'name' })
                        .done(function(data) {
                            var guids = [];
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                            });
                            generic.crossFiller(
                                guids, self.vPools,
                                function(guid) {
                                    var vpool = new VPool(guid);
                                    vpool.loading(true);
                                    return vpool;
                                }, 'guid'
                            );
                            self.vPoolsInitialLoad(false);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.refreshVPools = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.refreshVPoolsHandle[page])) {
                    var options = {
                        sort: 'name',
                        full: true,
                        page: page
                    };
                    self.refreshVPoolsHandle[page] = api.get('vpools', {}, options)
                        .done(function(data) {
                            var guids = [], vpdata = {};
                            $.each(data, function(index, item) {
                                guids.push(item.guid);
                                vpdata[item.guid] = item;
                            });
                            $.each(self.vPools(), function(index, vpool) {
                                if ($.inArray(vpool.guid(), guids) !== -1) {
                                    vpool.fillData(vpdata[vpool.guid()]);
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
        self.sync = function(guid) {
            $.each(self.vPools(), function(index, vp) {
                if (vp.guid() === guid) {
                    app.showMessage(
                            $.t('ovs:vpools.sync.warning'),
                            $.t('ovs:vpools.sync.title', { what: vp.name() }),
                            [$.t('ovs:vpools.sync.no'), $.t('ovs:vpools.sync.yes')]
                        )
                        .done(function(answer) {
                            if (answer === $.t('ovs:vpools.sync.yes')) {
                                generic.alertInfo(
                                    $.t('ovs:vpools.sync.marked'),
                                    $.t('ovs:vpools.sync.markedmsg', { what: vp.name() })
                                );
                                api.post('vpools/' + vp.guid() + '/sync_vmachines')
                                    .then(self.shared.tasks.wait)
                                    .done(function() {
                                        generic.alertSuccess(
                                            $.t('ovs:vpools.sync.done'),
                                            $.t('ovs:vpools.sync.donemsg', { what: vp.name() })
                                        );
                                    })
                                    .fail(function(error) {
                                        generic.alertError(
                                            $.t('ovs:generic.error'),
                                            $.t('ovs:generic.messages.errorwhile', {
                                                context: 'error',
                                                what: $.t('ovs:vpools.sync.errormsg', { what: vp.name() }),
                                                error: error
                                            })
                                        );
                                    });
                            }
                        });
                }
            });
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.fetchVPools, 5000);
            self.refresher.start();
            self.shared.footerData(self.vPools);

            self.fetchVPools().then(function() {
                self.refreshVPools(1);
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
