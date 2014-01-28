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
/*global define, window */
define([
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vpool'
], function($, app, dialog, ko, shared, generic, Refresher, api, VPool) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared      = shared;
        self.guard       = { authenticated: true };
        self.refresher   = new Refresher();
        self.widgets     = [];
        self.sortTimeout = undefined;

        // Data
        self.vPoolHeaders = [
            { key: 'name',              value: $.t('ovs:generic.name'),             width: 150,       colspan: undefined },
            { key: 'storedData',        value: $.t('ovs:generic.storeddata'),       width: 100,       colspan: undefined },
            { key: 'freeSpace',         value: $.t('ovs:vpools.freespace'),         width: 100,       colspan: undefined },
            { key: 'cacheRatio',        value: $.t('ovs:generic.cache'),            width: 100,       colspan: undefined },
            { key: 'iops',              value: $.t('ovs:generic.iops'),             width: 55,        colspan: undefined },
            { key: 'backendType',       value: $.t('ovs:vpools.backendtype'),       width: 100,       colspan: undefined },
            { key: 'backendConnection', value: $.t('ovs:vpools.backendconnection'), width: 100,       colspan: undefined },
            { key: 'backendLogin',      value: $.t('ovs:vpools.backendlogin'),      width: undefined, colspan: undefined },
            { key: undefined,           value: $.t('ovs:generic.actions'),          width: 100,       colspan: undefined }
        ];
        self.vPools = ko.observableArray([]);
        self.vPoolGuids = [];

        // Variables
        self.loadVPoolsHandle = undefined;

        // Functions
        self.load = function() {
            return $.Deferred(function(deferred) {
                generic.xhrAbort(self.loadVPoolsHandle);
                self.loadVPoolsHandle = api.get('vpools')
                    .done(function(data) {
                        var i, guids = [];
                        for (i = 0; i < data.length; i += 1) {
                            guids.push(data[i].guid);
                        }
                        generic.crossFiller(
                            guids, self.vPoolGuids, self.vPools,
                            function(guid) {
                                return new VPool(guid);
                            }, 'guid'
                        );
                        deferred.resolve();
                    })
                    .fail(deferred.reject);
            }).promise();
        };
        self.loadVPool = function(vpool) {
            return $.Deferred(function(deferred) {
                vpool.load()
                    .done(function() {
                        // (Re)sort vPools
                        if (self.sortTimeout) {
                            window.clearTimeout(self.sortTimeout);
                        }
                        self.sortTimeout = window.setTimeout(function() { generic.advancedSort(self.vPools, ['name', 'guid']); }, 250);
                    })
                    .always(deferred.resolve);
            }).promise();
        };
        self.sync = function(guid) {
            var i, vpools = self.vPools(), vp;
            for (i = 0; i < vpools.length; i += 1) {
                if (vpools[i].guid() === guid) {
                    vp = vpools[i];
                }
            }
            if (vp !== undefined) {
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
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.load, 5000);
            self.refresher.start();
            self.shared.footerData(self.vPools);

            self.load()
                .done(function() {
                    var i, vpools = self.vPools();
                    for (i = 0; i < vpools.length; i += 1) {
                        self.loadVPool(vpools[i]);
                    }
                });
        };
        self.deactivate = function() {
            var i;
            for (i = 0; i < self.widgets.length; i += 2) {
                self.widgets[i].deactivate();
            }
            self.refresher.stop();
            self.shared.footerData(ko.observable());
        };
    };
});
