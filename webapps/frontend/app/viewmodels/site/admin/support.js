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
    'knockout', 'jquery', 'plugins/dialog',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../../containers/support/statsmonkey', '../../containers/storagerouter/storagerouter', '../../wizards/statsmonkeyconfigure/index'
], function(ko, $, dialog, shared, generic, Refresher, api, StatsMonkeyConfigVM, StorageRouter, StatsMonkeyConfigureWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.guard     = { authenticated: true };
        self.refresher = new Refresher();
        self.shared    = shared;
        self.widgets   = [];

        // View Models
        self.newStatsMonkeyConfig  = new StatsMonkeyConfigVM();  // New configured settings for the stats monkey
        self.origStatsMonkeyConfig = new StatsMonkeyConfigVM();  // Currently configured settings for the stats monkey

        // Observables
        self.allSupportSettings      = ko.observableArray([]);  // All possible configurable settings regarding support agent, stats monkey and remote access for the entire cluster
        self.clusterID               = ko.observable();
        self.oldSupportSettings      = ko.observableArray([]);  // Currently enabled settings regarding support agent, stats monkey and remote access for the entire cluster
        self.releaseName             = ko.observable(shared.releaseName);
        self.saving                  = ko.observable(false);    // Used to disable the 'Save Settings' button when save action is ongoing
        self.selectedSupportSettings = ko.observableArray([]);  // Settings selected in GUI, used for comparison between currently configured settings and selected settings in GUI
        self.storageRouters          = ko.observableArray([]);

        // Handles
        self.supportMetadataHandle = {};
        self.supportSettingsHandle = undefined;

        // Computed
        self.supportSettingsChanged = ko.pureComputed(function() {
            // Computed used for enabling/disabling the 'Save Settings' button
            return !self.selectedSupportSettings().sort().equals(self.oldSupportSettings().sort()) ||
                   (self.newStatsMonkeyConfig.isInitialized() && self.newStatsMonkeyConfig.toJSON() !== self.origStatsMonkeyConfig.toJSON());
        });
        self.lastHeartbeat = ko.computed(function() {
            var timestamp = undefined, currentTimestamp;
            $.each(self.storageRouters(), function(index, storageRouter) {
                currentTimestamp = storageRouter.lastHeartbeat();
                if (currentTimestamp !== undefined && (timestamp === undefined || currentTimestamp > timestamp)) {
                    timestamp = currentTimestamp;
                }
            });
            return timestamp;
        });

        // Functions
        self.disableSupportSetting = function(name) {
            return ko.computed(function() {
                // Disable remote access when support agent is not selected
                return !(name === 'remote_access' && !self.selectedSupportSettings().contains('support_agent'));
            });
        };
        self.getFunction = function(name) {
            if (name === 'stats_monkey') {
                return self.configureStatsMonkey;
            }
        };
        self.configureStatsMonkey = function(edit) {
            var showWizard = false;
            if (edit === true) {
                showWizard = true;
            } else {
                if (!self.selectedSupportSettings().contains('stats_monkey') && self.origStatsMonkeyConfig.isInitialized() === false) {
                    showWizard = true;
                }
            }

            if (showWizard === false) {
                return true;  // Critical to return True, because returning true makes sure that the default 'click' handler gets triggered too (required for the 'checked' handler
            }
            var wizard = new StatsMonkeyConfigureWizard({
                modal: true,
                newConfig: self.newStatsMonkeyConfig,
                origConfig: new StatsMonkeyConfigVM(self.origStatsMonkeyConfig.toJS())
            });
            wizard.closing.always(function() {
                if (self.origStatsMonkeyConfig.isInitialized() === false) {
                    var index = self.selectedSupportSettings.indexOf('stats_monkey');
                    if (index > -1) {
                        self.selectedSupportSettings.splice(index, 1);
                    }
                }
            });
            wizard.finishing.always(function() {
                if (self.supportSettingsChanged() === true) {
                    self.save();
                }
            });
            dialog.show(wizard);
            return true;  // Critical to return True, otherwise knockout can't handle a click + checked event
        };
        self.save = function() {
            if (self.storageRouters().length > 0) {
                var data = {
                    support_info: {
                        stats_monkey: self.selectedSupportSettings().contains('stats_monkey'),
                        support_agent: self.selectedSupportSettings().contains('support_agent'),
                        remote_access: self.selectedSupportSettings().contains('remote_access'),
                        stats_monkey_config: self.newStatsMonkeyConfig.toJS()
                    }
                };
                generic.alertInfo(
                    $.t('ovs:support.settings.saving'),
                    $.t('ovs:support.settings.saving_msg')
                );
                self.saving(true);
                api.post('storagerouters/' + self.storageRouters()[0].guid() + '/configure_support', { data: data })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:support.settings.saved'),
                            $.t('ovs:support.settings.saved_msg')
                        );
                        // Make sure the 'Save Settings' button gets disabled again
                        self.oldSupportSettings([]);
                        $.each(self.selectedSupportSettings(), function(index, settingName) {
                            self.oldSupportSettings.push(settingName);
                        });
                        self.origStatsMonkeyConfig.update(self.newStatsMonkeyConfig.toJS());
                    })
                    .fail(function(error) {
                        generic.alertError(
                            $.t('ovs:support.settings.failed'),
                            $.t('ovs:support.settings.failed_msg', {error: generic.extractErrorMessage(error)})
                        );
                        // In case of failure, remove the selected settings again and restore the original configuration
                        self.selectedSupportSettings(self.oldSupportSettings());
                        self.newStatsMonkeyConfig.update(self.origStatsMonkeyConfig.toJS());
                    })
                    .always(function() {
                        self.saving(false);
                    });
            }
        };
        self.loadStorageRouters = function() {
            return $.Deferred(function(deferred) {
                api.get('storagerouters', {queryparams: {sort: 'name', contents: ''}})
                    .done(function(data) {
                        var guids = [], srData = {};
                        $.each(data.data, function(index, item) {
                            guids.push(item.guid);
                            srData[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.storageRouters,
                            function(guid) {
                                var sr = new StorageRouter(guid);
                                sr.metadata = ko.observable('');
                                sr.versions = ko.observable({});
                                sr.packageNames = ko.observableArray([]);
                                return sr;
                            }, 'guid'
                        );
                        $.each(self.storageRouters(), function(index, storageRouter) {
                            if (guids.contains(storageRouter.guid())) {
                                storageRouter.fillData(srData[storageRouter.guid()]);
                            }
                            storageRouter.loading(true);
                            var calls = [];
                            if (index === 0) {  // Support information are cluster-wide settings, so only retrieving for 1st StorageRouter
                                if (generic.xhrCompleted(self.supportSettingsHandle)) {
                                    self.supportSettingsHandle = api.get('storagerouters/' + storageRouter.guid() + '/get_support_info')
                                        .then(self.shared.tasks.wait)
                                        .then(function(data) {
                                            self.clusterID(data.cluster_id);
                                            self.origStatsMonkeyConfig.update(data.stats_monkey_config);
                                            if (self.newStatsMonkeyConfig.isInitialized() === false) {
                                                self.newStatsMonkeyConfig.update(data.stats_monkey_config);
                                            }
                                            delete data.cluster_id;
                                            delete data.stats_monkey_config;

                                            if (self.oldSupportSettings().length === 0) {
                                                $.each(data, function(key, boolValue) {
                                                    self.allSupportSettings.push({name: key, func: self.getFunction(key), enable: self.disableSupportSetting(key)});
                                                    if (boolValue === true) {
                                                        self.selectedSupportSettings.push(key);
                                                        self.oldSupportSettings.push(key);
                                                    }
                                                });
                                            }
                                        });
                                }
                            }
                            if (generic.xhrCompleted(self.supportMetadataHandle[storageRouter.guid()])) {
                                self.supportMetadataHandle[storageRouter.guid()] = api.get('storagerouters/' + storageRouter.guid() + '/get_support_metadata')
                                    .then(self.shared.tasks.wait)
                                    .then(function(data) {
                                        storageRouter.metadata(data);
                                        storageRouter.versions(data.metadata.versions);
                                        storageRouter.packageNames(generic.keys(data.metadata.versions));
                                    });
                                calls.push(self.supportMetadataHandle[storageRouter.guid()]);
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

        // Subscriptions
        self.selectedSupportSettings.subscribe(function(currentlySelectedSettings) {
            if (!currentlySelectedSettings.contains('support_agent')) {
                var index = self.selectedSupportSettings().indexOf('remote_access');
                if (index > -1) {
                    self.selectedSupportSettings.splice(index, 1);
                }
            }
        });

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
