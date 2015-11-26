// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Modified Apache License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/license
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
        self.guard                = { authenticated: true, registered: true };
        self.counter              = 0;
        self.widgets              = [];
        self.fileSystemChecked    = false;  // Whether the filesystem has been checked for presence of upgrade lock
        self.storageRouterHeaders = [
            { key: 'name',         value: $.t('ovs:updates.name'),               width: 300 },
            { key: 'framework',    value: $.t('ovs:updates.framework.title'),    width: undefined },
            { key: 'volumedriver', value: $.t('ovs:updates.volumedriver.title'), width: 400 },
        ];

        // Handles
        self.storageRoutersHandle = {};

        // Observables
        self.storageRouters     = ko.observableArray([]);
        self.upgradeOngoing     = ko.observable(false);  // Whether any upgrade is ongoing (framework or volumedriver)
        self.frameworkUpdate    = ko.observable(false);  // Whether a framework update is available
        self.volumedriverUpdate = ko.observable(false);  // Whether a volumedriver update is available

        // Computed
        self.updates = ko.computed(function() {
            var any_framework_update = false;
            var any_volumedriver_update = false;
            var updates_data = {'framework': {'update': false,
                                              'guiDown': false,
                                              'downtime': [],
                                              'prerequisites': []},
                                'volumedriver': {'update': false,
                                                 'downtime': [],
                                                 'prerequisites': []}};
            $.each(self.storageRouters(), function(index, storageRouter) {
                var item = storageRouter.updates();
                if (item !== undefined) {
                    if (item.framework.length > 0) {
                        any_framework_update = true;
                        updates_data.framework.update = true;
                        $.each(item.framework, function(a_index, framework_info) {
                            if (framework_info.gui_down === true) {
                                updates_data.framework.guiDown = true;
                            }
                            $.each(framework_info.downtime, function(b_index, downtime) {
                                if (!downtime.nestedIn(updates_data.framework.downtime)) {
                                    updates_data.framework.downtime.push(downtime);
                                }
                            });
                            $.each(framework_info.prerequisites, function(c_index, prereq) {
                                if (!prereq.nestedIn(updates_data.framework.prerequisites)) {
                                    updates_data.framework.prerequisites.push(prereq);
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
                            $.each(volumedriver_info.prerequisites, function(c_index, prereq) {
                                if (!prereq.nestedIn(updates_data.volumedriver.prerequisites)) {
                                    updates_data.volumedriver.prerequisites.push(prereq);
                                }
                            });
                        });
                    }
                    if (item.upgrade_ongoing === true) {
                        self.upgradeOngoing(item.upgrade_ongoing);
                        self.counter = 0;
                        self.fileSystemChecked = true;
                    } else {
                        if (self.fileSystemChecked === true || self.counter === 4) {
                            self.upgradeOngoing(item.upgrade_ongoing);
                            self.counter = 0;
                        } else {
                            self.counter += 1;
                        }
                        self.fileSystemChecked = false;
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
            if (self.upgradeOngoing() === true) {  // Checked by presence of file /etc/upgrade_ongoing on any of the storagerouters
                return;
            }
            self.upgradeOngoing(true);
            return $.Deferred(function(deferred) {
                var guiDown = false;
                var downtimes = [];
                var prerequisites = [];
                $.each(self.updates().framework.downtime, function(index, downtime) {
                    if (downtime[2] === null) {
                        downtimes.push($.t(downtime[0] + ':downtime.' + downtime[1]))
                    } else {
                        downtimes.push($.t(downtime[0] + ':downtime.' + downtime[1]) + ': ' + downtime[2])
                    }
                });
                $.each(self.updates().framework.prerequisites, function(index, prereq) {
                    if (prereq[2] === null) {
                        prerequisites.push($.t(prereq[0] + ':prerequisites.' + prereq[1]))
                    } else {
                        prerequisites.push($.t(prereq[0] + ':prerequisites.' + prereq[1]) + ': ' + prereq[2])
                    }
                });

                var guiDownMessage = self.updates().framework.guiDown === true ? '<br /><br />' + $.t('ovs:updates.framework.gui_unavailable') : '';
                var downtimeMessage = downtimes.length === 0 ? '' : '<br /><br />' + $.t('ovs:downtime.general', { multiple: downtimes.length > 1 ? 's': '' }) + '<ul><li>' + downtimes.join('</li><li>') + '</li></ul>';
                var prereqMessage = prerequisites.length === 0 ? '' : '<br /><br />' + (prerequisites.length !== 1 ? $.t('ovs:prerequisites.multiple') : $.t('ovs:prerequisites.singular')) + '<ul><li>' + prerequisites.join('</li><li>') + '</li></ul>';
                var button_options = prerequisites.length === 0 ? [$.t('ovs:generic.no'), $.t('ovs:generic.yes')] : [$.t('ovs:generic.cancel')]
                app.showMessage(
                    $.t('ovs:updates.framework.start_update_question', { what: $.t('ovs:updates.framework.title'), guidown: guiDownMessage, downtime: downtimeMessage, prerequisites: prereqMessage }).trim(),
                    $.t('ovs:generic.areyousure'),
                    button_options
                )
                    .done(function(answer) {
                        if (answer === $.t('ovs:generic.yes')) {
                            generic.alertSuccess($.t('ovs:updates.start_update'), $.t('ovs:updates.start_update_extra'));
                            var masterStorageRouters = [];
                            $.each(self.storageRouters(), function(index, storageRouter) {
                                if (storageRouter.nodeType() === 'MASTER') {
                                    masterStorageRouters.push(storageRouter);
                                }
                            });
                            var sortedStorageRouters = masterStorageRouters.sort(function(a, b) {
                                return a.ipAddress() < b.ipAddress() ? 1 : -1;
                            });
                            api.post('storagerouters/' + sortedStorageRouters[0].guid() + '/update_framework')
                                .then(self.shared.tasks.wait)
                                .done(function() {
                                    deferred.resolve();
                                })
                                .fail(function(error) {
                                    generic.alertError(
                                        $.t('ovs:generic.error'),
                                        $.t('ovs:updates.failed', { why: error })
                                    );
                                    deferred.reject();
                                    self.upgradeOngoing(false);
                                });
                        } else {
                            deferred.reject();
                            self.upgradeOngoing(false);
                        }
                    })
            }).promise();
        };

        self.updateVolumedriver = function() {
            if (self.upgradeOngoing() === true) {  // Checked by presence of file /etc/upgrade_ongoing on any of the storagerouters
                return;
            }
            self.upgradeOngoing(true);
            return $.Deferred(function(deferred) {
                var downtimes = [];
                var prerequisites = [];
                $.each(self.updates().volumedriver.downtime, function(index, downtime) {
                    if (downtime[2] === null) {
                        downtimes.push($.t(downtime[0] + ':downtime.' + downtime[1]))
                    } else {
                        downtimes.push($.t(downtime[0] + ':downtime.' + downtime[1]) + ': ' + downtime[2])
                    }
                });
                $.each(self.updates().volumedriver.prerequisites, function(index, prereq) {
                    if (prereq[2] === null) {
                        prerequisites.push($.t(prereq[0] + ':prerequisites.' + prereq[1]))
                    } else {
                        prerequisites.push($.t(prereq[0] + ':prerequisites.' + prereq[1]) + ' ' + prereq[2])
                    }
                });

                var downtimeMessage = downtimes.length === 0 ? '' : '<br /><br />' + $.t('ovs:downtime.general', { multiple: downtimes.length > 1 ? 's': '' }) + '<ul><li>' + downtimes.join('</li><li>') + '</li></ul>';
                var prereqMessage = prerequisites.length === 0 ? '' : '<br /><br />' + (prerequisites.length !== 1 ? $.t('ovs:prerequisites.multiple') : $.t('ovs:prerequisites.singular')) + '<ul><li>' + prerequisites.join('</li><li>') + '</li></ul>';
                var button_options = prerequisites.length === 0 ? [$.t('ovs:generic.no'), $.t('ovs:generic.yes')] : [$.t('ovs:generic.cancel')]
                app.showMessage(
                    $.t('ovs:updates.volumedriver.start_update_question', { what: $.t('ovs:updates.volumedriver.title'), downtime: downtimeMessage, prerequisites: prereqMessage }).trim(),
                    $.t('ovs:generic.areyousure'),
                    button_options
                )
                    .done(function(answer) {
                        if (answer === $.t('ovs:generic.yes')) {
                            generic.alertSuccess($.t('ovs:updates.start_update'), $.t('ovs:updates.start_update_extra'));
                            var masterStorageRouters = [];
                            $.each(self.storageRouters(), function(index, storageRouter) {
                                if (storageRouter.nodeType() === 'MASTER') {
                                    masterStorageRouters.push(storageRouter);
                                }
                            });
                            var sortedStorageRouters = masterStorageRouters.sort(function(a, b) {
                                return a.ipAddress() < b.ipAddress() ? 1 : -1;
                            });
                            api.post('storagerouters/' + sortedStorageRouters[0].guid() + '/update_volumedriver')
                                .then(self.shared.tasks.wait)
                                .done(function() {
                                    deferred.resolve();
                                })
                                .fail(function(error) {
                                    generic.alertError(
                                        $.t('ovs:generic.error'),
                                        $.t('ovs:updates.failed', { why: error })
                                    );
                                    deferred.reject();
                                    self.upgradeOngoing(false);
                                });
                        } else {
                            deferred.reject();
                            self.upgradeOngoing(false);
                        }
                    })
            }).promise();
        };

        // Durandal
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
        };
    };
});
