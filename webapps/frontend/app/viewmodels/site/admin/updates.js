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
    'jquery', 'plugins/dialog', 'knockout',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../../containers/storagerouter',
    '../../wizards/update/index'
], function($, dialog, ko, shared, generic, Refresher, api, StorageRouter, UpdateWizard) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.guard     = { authenticated: true };
        self.refresher = new Refresher();
        self.shared    = shared;
        self.widgets   = [];

        // Handles
        self.mergePackageInfo         = undefined;
        self.loadStorageRoutersHandle = undefined;
        self.refreshPackageInfoHandle = undefined;

        // Observables
        self.expanded        = ko.observable(false);
        self.loadedInfo      = ko.observable(false);
        self.refreshing      = ko.observable(false);
        self.storageNodes    = ko.observableArray([]);
        self.storageRouters  = ko.observableArray([]);
        self.updateInitiated = ko.observable(false);

        // Functions
        self.loadStorageRouters = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadStorageRoutersHandle)) {
                    self.loadStorageRoutersHandle = api.get('storagerouters', {queryparams: {'sort': 'name', 'contents': ''}})
                        .done(function(data) {
                            var guids = [], sadata = {};
                            $.each(data.data, function(index, item) {
                                guids.push(item.guid);
                                sadata[item.guid] = item;
                            });
                            generic.crossFiller(
                                guids, self.storageRouters,
                                function(guid) {
                                    return new StorageRouter(guid);
                                }, 'guid'
                            );
                            $.each(self.storageRouters(), function(index, storageRouter) {
                                storageRouter.fillData(sadata[storageRouter.guid()]);
                                storageRouter.getUpdateMetadata();
                            });
                            self.mergePackageInformation();
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.reject();
                }
            }).promise();
        };
        self.mergePackageInformation = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.mergePackageInfo)) {
                    self.mergePackageInfo = api.get('storagerouters/' + self.storageRouters()[0].guid() + '/merge_package_information')
                        .then(self.shared.tasks.wait)
                        .done(function(data) {
                            $.each(self.storageRouters(), function(index, sr) {
                                if (data.hasOwnProperty(sr.ipAddress())) {
                                    var packageInfo = [], expandedMap = {}, srData = data[sr.ipAddress()], packages = ko.observableArray([]);
                                    $.each(sr.packageInfo(), function(index, comp) {
                                        expandedMap[comp.component] = comp.expanded;
                                    });
                                    if (srData.hasOwnProperty('framework')) {
                                        $.each(srData.framework, function(packageName, packageInfo) {
                                            var pkg = {};
                                            pkg.name = packageName;
                                            pkg.candidate = packageInfo.candidate;
                                            pkg.installed = packageInfo.installed.replace('-reboot', '');
                                            packages.push(pkg);
                                        });
                                        packages.sort(function(pkg1, pkg2) {
                                            return pkg1.name < pkg2.name ? -1 : 1;
                                        });
                                        var framework = {};
                                        framework.expanded = expandedMap.hasOwnProperty('framework') ? expandedMap.framework : ko.observable(false);
                                        framework.packages = packages;
                                        framework.namespace = 'ovs';
                                        framework.component = 'framework';
                                        packageInfo.push(framework);
                                        delete srData.framework;
                                    }
                                    if (srData.hasOwnProperty('storagedriver')) {
                                        packages = ko.observableArray([]);
                                        $.each(srData.storagedriver, function(packageName, packageInfo) {
                                            var pkg = {};
                                            pkg.name = packageName;
                                            pkg.candidate = packageInfo.candidate;
                                            pkg.installed = packageInfo.installed.replace('-reboot', '');
                                            packages.push(pkg);
                                        });
                                        packages.sort(function(pkg1, pkg2) {
                                            return pkg1.name < pkg2.name ? -1 : 1;
                                        });
                                        var storagedriver = {};
                                        storagedriver.expanded = expandedMap.hasOwnProperty('storagedriver') ? expandedMap.storagedriver : ko.observable(false);
                                        storagedriver.packages = packages;
                                        storagedriver.namespace = 'ovs';
                                        storagedriver.component = 'storagedriver';
                                        packageInfo.push(storagedriver);
                                        delete srData.storagedriver;
                                    }
                                    var plugins = [];
                                    $.each(srData, function(pluginName, pluginInfo) {
                                        packages = ko.observableArray([]);
                                        $.each(pluginInfo, function(packageName, packageInfo) {
                                            var pkg = {};
                                            pkg.name = packageName;
                                            pkg.candidate = packageInfo.candidate;
                                            pkg.installed = packageInfo.installed.replace('-reboot', '');
                                            packages.push(pkg);
                                        });
                                        packages.sort(function(pkg1, pkg2) {
                                            return pkg1.name < pkg2.name ? -1 : 1;
                                        });
                                        var plugin = {};
                                        plugin.expanded = expandedMap.hasOwnProperty(pluginName) ? expandedMap[pluginName] : ko.observable(false);
                                        plugin.packages = packages;
                                        plugin.namespace = pluginName;
                                        plugin.component = pluginName;
                                        plugins.push(plugin);
                                    });
                                    plugins.sort(function(plugin1, plugin2) {
                                        return plugin1.namespace < plugin2.namespace ? -1 : 1;
                                    });
                                    $.each(plugins, function(index, plugin) {
                                        packageInfo.push(plugin);
                                    });
                                    delete data[sr.ipAddress()];
                                    sr.packageInfo(packageInfo);
                                }
                            });

                            // Leftovers in data are storage nodes (SDM nodes)
                            var expandedMap = {};
                            $.each(self.storageNodes(), function(index, sdmNode) {
                                expandedMap[sdmNode.ip] = sdmNode.expanded;
                                $.each(sdmNode.plugins(), function(jndex, plugin) {
                                    expandedMap[sdmNode.ip + plugin.namespace] = plugin.expanded;
                                })
                            });
                            var sdmNodes = [];
                            $.each(data, function(ip, nodeInfo) {
                                var plugins = ko.observableArray([]);
                                $.each(nodeInfo, function(pluginName, pluginInfo) {
                                    var packages = ko.observableArray([]);
                                    $.each(pluginInfo, function(packageName, packageInfo) {
                                        var pkg = {};
                                        pkg.name = packageName;
                                        pkg.candidate = packageInfo.candidate;
                                        pkg.installed = packageInfo.installed.replace('-reboot', '');
                                        packages.push(pkg);
                                    });
                                    packages.sort(function(pkg1, pkg2) {
                                        return pkg1.name < pkg2.name ? -1 : 1;
                                    });
                                    var plugin = {};
                                    plugin.expanded = expandedMap.hasOwnProperty(ip + pluginName) ? expandedMap[ip + pluginName] : ko.observable(false);
                                    plugin.packages = packages;
                                    plugin.namespace = pluginName;
                                    plugin.component = pluginName;
                                    plugins.push(plugin);
                                });
                                plugins.sort(function(plugin1, plugin2) {
                                    return plugin1.namespace < plugin2.namespace ? -1 : 1;
                                });
                                var sdmNode = {};
                                sdmNode.ip = ip;
                                sdmNode.plugins = plugins;
                                sdmNode.expanded = expandedMap.hasOwnProperty(ip) ? expandedMap[ip] : ko.observable(true);
                                sdmNodes.push(sdmNode);
                            });
                            self.storageNodes(sdmNodes);
                            // Sort nodes by IP
                            self.storageNodes.sort(function(sn1, sn2) {
                                return sn1.ip < sn2.ip ? -1 : 1;
                            });
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            if (self.loadedInfo() === false) {
                                self.loadedInfo(true);
                            }
                        })
                }
            }).promise();
        };
        self.refresh = function() {
            self.refreshing(true);
            if (generic.xhrCompleted(self.refreshPackageInfoHandle)) {
                self.refreshPackageInfoHandle = api.get('storagerouters/' + self.storageRouters()[0].guid() + '/refresh_package_information')
                    .then(shared.tasks.wait)
                    .done(function () {
                        generic.alertSuccess(
                            $.t('ovs:updates.refresh.success'),
                            $.t('ovs:updates.refresh.success_msg')
                        );
                        self.mergePackageInformation();
                    })
                    .fail(function (error) {
                        error = generic.extractErrorMessage(error);
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:updates.refresh.failure_msg', {error: error})
                        );
                    })
                    .always(function () {
                        self.refreshing(false);
                    });
                generic.alertInfo(
                    $.t('ovs:updates.refresh.started'),
                    $.t('ovs:updates.refresh.started_msg')
                );
            }
        };
        self.expandCollapseAll = function(value) {
            $.each(self.storageRouters(), function(index, sr) {
                sr.expanded(value);
                $.each(sr.packageInfo(), function(index, comp) {
                    comp.expanded(value);
                });
            });
            $.each(self.storageNodes(), function(index, sn) {
                sn.expanded(value);
                $.each(sn.plugins(), function(index, plugin) {
                    plugin.expanded(value);
                });
            });
            self.expanded(value);
        };
        self.collectiveStatus = function() {
            var atFunctional = true, updatesOngoing = false, updatesChecking = false, updatesAvailable = false;
            $.each(self.storageRouters(), function(index, sr) {
                if (sr.updateMetadata() === undefined) {
                    updatesChecking = true;
                } else {
                    if (sr.updateMetadata().at_ok === false) {
                        atFunctional = false;
                    }
                    if (sr.updateMetadata().update_ongoing === true) {
                        self.updateInitiated(false);
                        updatesOngoing = true;
                    }
                }
                if (sr.updatesAvailable() === true) {
                    updatesAvailable = true;
                }
            });
            $.each(self.storageNodes(), function(index, sn) {
                $.each(sn.plugins(), function(jndex, plugin) {
                    if (plugin.packages().length > 0) {
                        updatesAvailable = true;
                        return false;
                    }
                });
                if (updatesAvailable === true) {
                    return false;
                }
            });
            return {'atFunctional': atFunctional,
                    'updatesOngoing': updatesOngoing,
                    'updatesChecking': updatesChecking,
                    'updatesAvailable': updatesAvailable};
        };
        self.showUpdateWizard = function() {
            var wizard = new UpdateWizard({
                modal: true,
                storagerouter: self.storageRouters()[0]
            });
            wizard.finishing.always(function() {
                self.updateInitiated(true);
            });
            dialog.show(wizard);
        };

        // Durandal
        self.activate = function() {
            self.refresher.init(self.loadStorageRouters, 10000);
            self.refresher.start();
            return self.loadStorageRouters();
        };
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
            self.refresher.stop();
        };
    };
});
