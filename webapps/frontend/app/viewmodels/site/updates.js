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
    '../containers/pmachine', '../containers/storagerouter'
], function($, app, dialog, ko, shared, generic, Refresher, api, PMachine, StorageRouter) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared               = shared;
        self.guard                = { authenticated: true };
        self.updating             = ko.observable(false);
        self.widgets              = [];
        self.storageRouterHeaders = [
            { key: 'name',         value: $.t('ovs:updates.name'),         width: undefined },
            { key: 'framework',    value: $.t('ovs:updates.framework'),    width: 250 },
            { key: 'volumedriver', value: $.t('ovs:updates.volumedriver'), width: 250 },
        ];

        // Handles
        self.storageRoutersHandle = {};

        // Observables
        self.storageRouters = ko.observableArray([]);

        // Computed
        self.updates = ko.computed(function() {
            var updates_data = {'framework': false,
                                'volumedriver': false};
            $.each(self.storageRouters(), function(index, storageRouter) {
                var item = storageRouter.updates();
                if (item !== undefined && item.framework !== null) {
                    updates_data.framework = true;
                }
                else if (item !== undefined && item.volumedriver !== null) {
                    updates_data.volumedriver = true;
                }
            });
            return updates_data;
        });

        // Functions
        self.loadStorageRouters = function(page) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.storageRoutersHandle[page])) {
                    var options = {
                        sort: 'name',
                        page: page,
                        'contents': '_relations'
                    };
                    self.storageRoutersHandle[page] = api.get('storagerouters', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    return new StorageRouter(guid);
                                },
                                dependencyLoader: function(item) {
                                    item.getUpdates();
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.updateFramework = function() {
            if (self.updating() === true) {
                return;
            }
            self.updating(true);

            return $.Deferred(function(deferred) {
                app.showMessage(
                    $.t('ovs:updates.start_update_question', { what: $.t('ovs:updates.framework') }).trim(),
                    $.t('ovs:generic.areyousure'),
                    [$.t('ovs:generic.no'), $.t('ovs:generic.yes')]
                )
                    .done(function(answer) {
                        if (answer === $.t('ovs:generic.yes')) {
                            generic.alertSuccess($.t('ovs:updates.start_update'), '');
                            api.post('storagerouters/' + self.storageRouters()[0].guid() + '/update_framework')
                                .then(self.shared.tasks.wait)
                                .done(function() {
                                    generic.alertSuccess(
                                        $.t('ovs:updates.complete'),
                                        $.t('ovs:updates.success')
                                    );
                                    deferred.resolve();
                                    self.updating(false);
                                })
                                .fail(function(error) {
                                    generic.alertError(
                                        $.t('ovs:generic.error'),
                                        $.t('ovs:updates.failed', { why: error })
                                    );
                                    deferred.reject();
                                    self.updating(false);
                                });
                        } else {
                            deferred.reject();
                            self.updating(false);
                        }
                    })
            }).promise();
        };
        self.updateVolumedriver = function() {
            if (self.updating() === true) return;
            self.updating(true);
        }
    };
});
