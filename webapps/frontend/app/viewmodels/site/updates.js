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
    '../containers/storagerouter',
    '../wizards/update/index'
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
        self.loadStorageNodes         = undefined;
        self.loadStorageRoutersHandle = undefined;
        self.refreshPackageInfoHandle = undefined;

        // Observables
        self.expandedAll    = ko.observable(false);
        self.loadedInfo     = ko.observable(false);
        self.refreshing     = ko.observable(false);
        self.storageNodes   = ko.observableArray([]);
        self.storageRouters = ko.observableArray([]);

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
                if (generic.xhrCompleted(self.loadStorageNodes)) {
                    self.loadStorageNodes = api.get('storagerouters/' + self.storageRouters()[0].guid() + '/merge_package_information')
                        .then(self.shared.tasks.wait)
                        .done(function(data) {
                            $.each(self.storageRouters(), function(index, sr) {
                                if (data.hasOwnProperty(sr.ipAddress())) {
                                    var packageNames = [], srData = data[sr.ipAddress()];
                                    if (srData.hasOwnProperty('framework')) {
                                        sr.componentFwk([]);
                                        $.each(srData.framework, function(index, pkgInfo) {
                                            if (!packageNames.contains(pkgInfo.name)) {
                                                packageNames.push(pkgInfo.name);
                                                sr.componentFwk.push(pkgInfo);
                                            }
                                        });
                                        sr.componentFwk.sort(function(package1, package2) {
                                            return package1.name < package2.name ? -1 : 1;
                                        });
                                        delete srData.framework;
                                    }
                                    if (srData.hasOwnProperty('storagedriver')) {
                                        packageNames = [];
                                        sr.componentSd([]);
                                        $.each(srData.storagedriver, function(index, pkgInfo) {
                                            if (!packageNames.contains(pkgInfo.name)) {
                                                packageNames.push(pkgInfo.name);
                                                sr.componentSd.push(pkgInfo);
                                            }
                                        });
                                        sr.componentSd.sort(function(package1, package2) {
                                            return package1.name < package2.name ? -1 : 1;
                                        });
                                        delete srData.storagedriver;
                                    }
                                    var i, j, currentKeyList = [],
                                        newKeyList = generic.keys(srData);
                                    for (i = 0; i < sr.componentPlugins().length; i += 1) {
                                        currentKeyList.push(sr.componentPlugins()[i]().namespace);
                                    }
                                    for (i = 0; i < newKeyList.length; i += 1) {
                                        if (!currentKeyList.contains(newKeyList[i]) && srData[newKeyList[i]].length) {
                                            var plugin = {};
                                            plugin.expanded = ko.observable(false);
                                            plugin.packages = ko.observableArray(srData[newKeyList[i]]);
                                            plugin.namespace = newKeyList[i];
                                            sr.componentPlugins.push(ko.observable(plugin));
                                        }
                                    }
                                    for (i = 0; i < currentKeyList.length; i += 1) {
                                        if (!newKeyList.contains(currentKeyList[i])) {
                                            for (j = 0; j < sr.componentPlugins().length; j += 1) {
                                                if (sr.componentPlugins()[j].namespace === currentKeyList[i]) {
                                                    sr.componentPlugins.splice(j, 1);
                                                    break;
                                                }
                                            }
                                        } else {
                                            $.each(sr.componentPlugins(), function(index, plugin) {
                                                if (plugin().namespace === currentKeyList[i]) {
                                                    generic.removeElement(sr.componentPlugins(), plugin);
                                                    var temp = plugin();
                                                    temp.packages = ko.observableArray(srData[currentKeyList[i]]);
                                                    plugin(temp);
                                                    sr.componentPlugins.push(plugin);
                                                }
                                            });
                                        }
                                    }
                                    $.each(sr.componentPlugins(), function(index, plugin) {
                                        plugin().packages().sort(function(package1, package2) {
                                            return package1.name < package2.name ? -1 : 1;
                                        });
                                    });
                                    delete data[sr.ipAddress()];
                                }
                            });
                            // Leftovers in data are storage nodes (SDM nodes)
                            var i, j, currentIPList = [],
                                newIPList = generic.keys(data);
                            for (i = 0; i < self.storageNodes().length; i += 1) {
                                currentIPList.push(self.storageNodes()[i]().ip);
                            }
                            for (i = 0; i < newIPList.length; i += 1) {
                                if (!currentIPList.contains(newIPList[i])) {
                                    var temp = {}, plugins = ko.observableArray([]), sdmNode = ko.observable();
                                    $.each(data[newIPList[i]], function(namespace, pkgInfo) {
                                        if (pkgInfo.length > 0) {
                                            var plugin = {};
                                            plugin.expanded = ko.observable(false);
                                            plugin.packages = ko.observableArray(pkgInfo);
                                            plugin.namespace = namespace;
                                            plugins.push(plugin);
                                        }
                                    });
                                    temp.ip = newIPList[i];
                                    temp.plugins = plugins;
                                    temp.expanded = ko.observable(true);
                                    sdmNode(temp);
                                    self.storageNodes.push(sdmNode);
                                }
                            }
                            for (i = 0; i < currentIPList.length; i += 1) {
                                if (!newIPList.contains(currentIPList[i])) {
                                    for (j = 0; j < self.storageNodes().length; j += 1) {
                                        if (self.storageNodes()[j].ip === currentIPList[i]) {
                                            self.storageNodes.splice(j, 1);
                                            break;
                                        }
                                    }
                                } else {
                                    $.each(self.storageNodes(), function(index, sdmNode) {
                                        if (sdmNode().ip === currentIPList[i]) {
                                            var expanded = sdmNode().expanded(), expandedMap = {}, plugins = ko.observableArray([]);
                                            $.each(sdmNode().plugins(), function(index, plugin) {
                                                expandedMap[plugin.namespace] = plugin.expanded();
                                            });
                                            generic.removeElement(self.storageNodes(), sdmNode);
                                            var temp = sdmNode();
                                            $.each(data[currentIPList[i]], function(namespace, pkgInfo) {
                                                if (pkgInfo.length > 0) {
                                                    var plugin = {};
                                                    plugin.expanded = ko.observable(expandedMap.hasOwnProperty(namespace) ? expandedMap[namespace] : false);
                                                    plugin.packages = ko.observableArray(pkgInfo);
                                                    plugin.namespace = namespace;
                                                    plugins.push(plugin);
                                                }
                                            });
                                            temp.plugins = plugins;
                                            temp.expanded = ko.observable(expanded);
                                            sdmNode(temp);
                                            self.storageNodes.push(sdmNode);
                                        }
                                    });
                                }
                            }
                            $.each(self.storageNodes(), function(index, sn) {
                                // Sort plugins
                                sn().plugins().sort(function (plugin1, plugin2) {
                                    return plugin1.namespace < plugin2.namespace ? -1 : 1;
                                });
                                // Sort packages
                                $.each(sn().plugins(), function (index, plugin) {
                                    plugin.packages.sort(function (package1, package2) {
                                        return package1.name < package2.name ? -1 : 1;
                                    });
                                });
                            });
                            // Sort nodes by IP
                            self.storageNodes.sort(function(sn1, sn2) {
                                return sn1().ip < sn2().ip ? -1 : 1;
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
                sr.componentSdExpanded(value);
                sr.componentFwkExpanded(value);
                $.each(sr.componentPlugins(), function(jndex, plugin) {
                    plugin().expanded(value);
                });
            });
            $.each(self.storageNodes(), function(index, sn) {
                sn().expanded(value);
                $.each(sn().plugins(), function(index, plugin) {
                    plugin.expanded(value);
                });
            });
            self.expandedAll(value);
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
                        updatesOngoing = true;
                    }
                }
                if (sr.updatesAvailable() === true) {
                    updatesAvailable = true;
                }
            });
            $.each(self.storageNodes(), function(index, sn) {
                $.each(sn().plugins(), function(jndex, plugin) {
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
            dialog.show(new UpdateWizard({
                modal: true,
                storagerouter: self.storageRouters()[0]
            }));
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
