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
            { key: 'name',         value: $.t('ovs:updates.name'),         width: 300 },
            { key: 'framework',    value: $.t('ovs:updates.framework'),    width: undefined },
            { key: 'volumedriver', value: $.t('ovs:updates.volumedriver'), width: 400 },
        ];

        // Handles
        self.storageRoutersHandle = {};

        // Observables
        self.storageRouters     = ko.observableArray([]);
        self.upgradeOngoing     = ko.observable(false);
        self.frameworkUpdate    = ko.observable(false);
        self.volumedriverUpdate = ko.observable(false);

        // Computed
        self.updates = ko.computed(function() {
            var any_framework_update = false;
            var any_volumedriver_update = false;
            var updates_data = {'framework': {'update': false,
                                              'downtime': []},
                                'volumedriver': {'update': false,
                                                 'downtime': []}};
            $.each(self.storageRouters(), function(index, storageRouter) {
                var item = storageRouter.updates();
                if (item !== undefined) {
                    if (item.framework.length > 0) {
                        any_framework_update = true;
                        updates_data.framework.update = true;
                        $.each(item.framework, function(a_index, framework_info) {
                            $.each(framework_info.downtime, function(b_index, downtime) {
                                if (!downtime.nestedIn(updates_data.framework.downtime)) {
                                    updates_data.framework.downtime.push(downtime);
                                }
                            });
                        });
                    }
                    if (item.volumedriver.length > 0) {
                        any_volumedriver_update = true;
                        updates_data.volumedriver.update = true;
                        $.each(item.volumedriver, function(a_index, volumedriver_info) {
                            $.each(volumedriver_info.downtime, function(b_index, downtime) {
                                if (!downtime.nestedIn(updates_data.volumedriver.downtime)) {
                                    updates_data.volumedriver.downtime.push(downtime);
                                }
                            });
                        });
                    }
                    if (item.upgrade_ongoing === true) {
                        self.upgradeOngoing(true);
                    }
                }
            });
            self.frameworkUpdate(any_framework_update);
            self.volumedriverUpdate(any_volumedriver_update);
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
                        .fail(deferred.reject);
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.updateFramework = function() {
            if (self.updating() === true) {  // Cleared by refreshing page, kept in memory only
                return;
            }
            else if (self.upgradeOngoing() === true) {  // Checked by presence of file /etc/upgrade_ongoing on any of the storagerouters
                return;
            }
            self.updating(true);

            return $.Deferred(function(deferred) {
                var downtimes = [];
                $.each(self.updates().framework.downtime, function(index, downtime) {
                    if (downtime[2] === null) {
                        downtimes.push($.t(downtime[0] + ':downtime.' + downtime[1]))
                    } else {
                        downtimes.push($.t(downtime[0] + ':downtime.' + downtime[1]) + ': ' + downtime[2])
                    }
                });
                $.each(self.updates().volumedriver.downtime, function(index, downtime) {
                    if (downtime[2] === null) {
                        downtimes.push($.t(downtime[0] + ':downtime.' + downtime[1]))
                    } else {
                        downtimes.push($.t(downtime[0] + ':downtime.' + downtime[1]) + ': ' + downtime[2])
                    }
                });

                var downtimeMessage = '<br /><br />' + $.t('ovs:downtime.general', { multiple: downtimes.length > 1 ? 's': '' }) + '<ul><li>' + downtimes.join('</li><li>') + '</li></ul>';
                app.showMessage(
                    $.t('ovs:updates.start_update_question', { what: $.t('ovs:updates.framework'), downtime: downtimeMessage }).trim(),
                    $.t('ovs:generic.areyousure'),
                    [$.t('ovs:generic.no'), $.t('ovs:generic.yes')]
                )
                    .done(function(answer) {
                        if (answer === $.t('ovs:generic.yes')) {
                            generic.alertSuccess($.t('ovs:updates.start_update'), $.t('ovs:updates.start_update_extra'));
                            $.each(self.storageRouters(), function(index, storageRouter) {
                                if (storageRouter.nodeType() == 'MASTER') {
                                    self.upgradeOngoing(true);
                                    api.post('storagerouters/' + storageRouter.guid() + '/update_framework')
                                        .then(function(taskID) {
                                            self.updating(false);  // Update itself will stop all services, so celery should not reach done status
                                            return self.shared.tasks.wait(taskID);
                                        })
                                        .done(function() {
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
                                            self.upgradeOngoing(false);
                                        });
                                    return false;  // break out of $.each loop
                                }
                            });
                        } else {
                            deferred.reject();
                            self.updating(false);
                            self.upgradeOngoing(false);
                        }
                    })
            }).promise();
        };
        self.updateVolumedriver = function() {
            return;
        };

        // Durandal
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
        };
    };
});
