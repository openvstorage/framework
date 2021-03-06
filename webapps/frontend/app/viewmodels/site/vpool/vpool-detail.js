﻿// Copyright (C) 2016 iNuron NV
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
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout', 'plugins/router',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api', 'ovs/services/authentication',
    'viewmodels/containers/shared/base_container',
    'viewmodels/services/vdisk', 'viewmodels/services/vpool', 'viewmodels/services/storagerouter','viewmodels/services/storagedriver',
    'viewmodels/containers/vpool/vpool', 'viewmodels/containers/storagedriver/storagedriver',
    'viewmodels/containers/storagerouter/storagerouter', 'viewmodels/containers/vdisk/vdisk',
    'viewmodels/wizards/addvpool/index', 'viewmodels/wizards/createhprmconfigs/index', 'viewmodels/wizards/reconfigurevpool/index'
], function($, app, dialog, ko, router,
            shared, generic, Refresher, api, authentication,
            BaseContainer,
            VDiskService, VPoolService, StoragerouterService, StoragedriverService,
            VPool, StorageDriver, StorageRouter, VDisk,
            ExtendVPool, CreateHPRMConfigsWizard, ReconfigureVPool) {
    "use strict";
    var viewMapping = Object.freeze({
        'storageRouters': {
            key: function (data) {  // For relation updates: check if the GUID has changed before discarding a model
                return ko.utils.unwrapObservable(data.guid)
            },
            create: function (options) {  // This object has not yet been converted to work with ko.mapping thus manually overriden the create
                var storagerouter;
                if (options.data === null) {
                    storagerouter = new StorageRouter(null);
                    storagerouter.loaded(true);
                    return storagerouter
                }
                storagerouter = new StorageRouter(ko.utils.unwrapObservable(options.data.guid));
                storagerouter.fillData((ko.utils.unwrapObservable(options.data)));
                storagerouter.loaded(true);
                return storagerouter
            },
            update: function (options) {
                options.target.fillData(options.data);
                return options.target
            }
        },
        'storageDrivers': {
            key: function (data) {  // For relation updates: check if the GUID has changed before discarding a model
                return ko.utils.unwrapObservable(data.guid)
            },
            create: function (options) {  // This object has not yet been converted to work with ko.mapping thus manually overriden the create
                var storagedriver;
                if (options.data === null) {
                    storagedriver = new StorageDriver(null);
                    storagedriver.loaded(true);
                    return storagedriver
                }
                storagedriver = new StorageDriver(ko.utils.unwrapObservable(options.data.guid));
                storagedriver.fillData((ko.utils.unwrapObservable(options.data)));
                storagedriver.loaded(true);
                return storagedriver
            },
            update: function (options) {
                options.target.fillData(options.data);
                return options.target
            }

        }
    });
    function VPoolDetail(data) {
        var self = this;

        BaseContainer.call(this);

        // Variables
        self.shared             = shared;
        self.guard              = { authenticated: true };
        self.refresher          = new Refresher();
        self.widgets            = [];
        self.storageRouterCache = {};
        self.vDiskCache         = {};
        self.vDiskHeaders       = [
            { key: 'status',     value: '',                            width: 30        },
            { key: 'name',       value: $.t('ovs:generic.name'),       width: undefined },
            { key: 'size',       value: $.t('ovs:generic.size'),       width: 100       },
            { key: 'storedData', value: $.t('ovs:generic.storeddata'), width: 110       },
            { key: 'iops',       value: $.t('ovs:generic.iops'),       width: 90        },
            { key: 'readSpeed',  value: $.t('ovs:generic.read'),       width: 125       },
            { key: 'writeSpeed', value: $.t('ovs:generic.write'),      width: 125       },
            { key: 'dtlStatus',  value: $.t('ovs:generic.dtl_status'), width: 50        }
        ];
        self.vDiskQuery         = JSON.stringify({
            type: 'AND',
            items: [['is_vtemplate', 'EQUALS', false]]
        });

        // Handles
        self.loadConfigFiles          = undefined;
        self.loadStorageDriversHandle = undefined;
        self.loadStorageRoutersHandle = undefined;
        self.vDisksHandle             = {};

        // Observables
        self.generatingConfigs      = ko.observable(false);
        self.refreshList            = ko.observableArray([]);
        self.storageDrivers         = ko.observableArray([]);
        self.storageRouters         = ko.observableArray([]);
        self.updatingStorageRouters = ko.observable(false);
        self.vPool                  = ko.observable();

        // Computed
        self.expanded = ko.computed({
            write: function(value) {
                $.each(self.storageRouters(), function(index, storagerouter) {
                    storagerouter.expanded(value);
                });
            },
            read: function() {
                var expanded = false;
                $.each(self.storageRouters(), function(index, storagerouter) {
                    expanded |= storagerouter.expanded();  // Bitwise or, |= is correct.
                });
                return expanded;
            }
        });
        self.anyCollapsed = ko.computed(function() {
            /**
             * Check if any node is collapsed
             * Different than the expanded check in the way this will return true when any are collapsed as opposed to all
              */
            var collapsed = false;
            $.each(self.storageRouters(), function(index, storagerouter) {
                if (storagerouter.expanded() === false) {
                    collapsed = true;
                    return false;
                }
            });
            return collapsed;
        });


        var vmData = $.extend({
            'storageRouters': [],
            'storageDrivers ': []
        }, data || {});

        ko.mapping.fromJS(vmData, viewMapping, self);  // Bind the data into this


        self.canManage = ko.pureComputed(function() {
            return authentication.user.canManage()
        });

        self.srSDMap = ko.pureComputed(function() {
            return self.storageDrivers().reduce(function(acc, storagedriver) {
                acc[storagedriver.storageRouterGuid()] = storagedriver;
                return acc
            }, {});
        });

        // Functions
        self.load = function() {
            return $.Deferred(function (deferred) {
                var vpool = self.vPool();
                $.when.apply($, [
                    vpool.load('storagedrivers,vdisks,_dynamics'),
                    self.loadStorageRouters(),
                    self.loadStorageDrivers()
                ])
                .fail(function(error) {
                    if (error !== undefined && error.status === 404) {
                        router.navigateBack();
                    }
                })
                .always(deferred.resolve);
            }).promise();
        };
        self.loadStorageRouters = function() {
            return $.when().then(function () {
                if (generic.xhrCompleted(self.loadStorageRoutersHandle)) {
                    var options = {
                        sort: 'name',
                        contents: 'storagedrivers,features'
                    };
                    return self.loadStorageRoutersHandle = StoragerouterService.loadStorageRouters(options)
                        .then(function (data) {
                            self.update({storageRouters: data.data});
                            return data
                        })
                }
            })
        };
        self.loadStorageDrivers = function() {
            return $.when().then(function () {
                if (generic.xhrCompleted(self.loadStorageDriversHandle)) {
                    return self.loadStorageDriversHandle = StoragedriverService.loadStorageDrivers({
                            vpool_guid: self.vPool().guid(),
                            contents: 'storagerouter,vpool_backend_info,vdisks_guids,alba_proxies,proxy_summary'
                        })
                        .then(function(data) {
                            self.update({storageDrivers: data.data});
                            return data;
                        });
                }
            })
        };
        self.loadVDisks = function(options) {
            return $.when().then(function() {
                if (generic.xhrCompleted(self.vDisksHandle[options.page])) {
                    options.sort = 'devicename';
                    options.contents = '_dynamics,_relations,-snapshots';
                    options.vpoolguid = self.vPool().guid();
                    options.query = self.vDiskQuery;
                    return self.vDisksHandle[options.page] = VDiskService.loadVDisks(options)
                        .then(function(data) {
                            return {
                                data: data,
                                loader: function(guid) {
                                    if (!self.vDiskCache.hasOwnProperty(guid)) {
                                        self.vDiskCache[guid] = new VDisk(guid);
                                    }
                                    return self.vDiskCache[guid];
                                }
                            }
                    })
                }
            })
        };

        self.formatBytes = function(value) {
            return generic.formatBytes(value);
        };
        self.addStorageRouter = function(sr) {
            self.updatingStorageRouters(true);
            if (self.vPool().extensible()) {
                var wizard = new ExtendVPool({
                    modal: true,
                    vPool: self.vPool(),
                    storageRouter: sr});
                // Setup listener for when the Modal is closed
                wizard.closing.fail(function() {
                    self.updatingStorageRouters(false);
                });
                wizard.completed.always(function() {
                    self.updatingStorageRouters(false);
                });
                dialog.show(wizard);
            } else {
                var reasons = self.vPool().notExtensibleReasons(),
                    message = $.t('ovs:wizards.extend_vpool.prohibited.message', {name: self.vPool().name(), multi: reasons.length === 1 ? '' : 's'});
                $.each(reasons, function(index, reason) {
                    message += '<li>' + $.t('ovs:wizards.extend_vpool.prohibited.reasons.' + reason) + '</li>';
                });
                message += '</ul>';
                app.showMessage(
                    message,
                    $.t('ovs:wizards.extend_vpool.prohibited.title', {name: self.vPool().name()}),
                    [$.t('ovs:generic.ok')]
                )
                .always(function() {
                    self.updatingStorageRouters(false);
                });
            }
        };
        self.reconfigureStorageRouter = function(sr, sd) {
            self.updatingStorageRouters(true);
            if (self.srSDMap().hasOwnProperty(sr.guid())) {
                var wizard = new ReconfigureVPool({
                        modal: true,
                        vPool: self.vPool(),
                        storageRouter: sr,
                        storageDriver: sd
                    });
                wizard.closing.fail(function() {
                    self.updatingStorageRouters(false);
                });
                wizard.completed.always(function(){
                    self.updatingStorageRouters(false);
                });
                dialog.show(wizard);
            }
        };
        self.generateHPRMConfigFiles = function(sr) {
            self.updatingStorageRouters(true);
            var wizard = new CreateHPRMConfigsWizard({
                modal: true,
                vPool: self.vPool(),
                storageRouter: sr
            });
            dialog.show(wizard);
            wizard.completed.always(function() {
                self.updatingStorageRouters(false);
            });
        };
        self.removeStorageRouter = function(sr) {
            var single = Object.keys(self.srSDMap()).length === 1;
            if (self.srSDMap().hasOwnProperty(sr.guid()) && self.srSDMap()[sr.guid()].canBeDeleted() === true && self.refreshList().length === 0) {
                self.updatingStorageRouters(true);
                app.showMessage(
                    $.t('ovs:wizards.shrink_vpool.confirm.remove_' + (single === true ? 'single' : 'multi'), { what: sr.name() }),
                    $.t('ovs:generic.are_you_sure'),
                    [$.t('ovs:generic.yes'), $.t('ovs:generic.no')]
                )
                    .done(function(answer) {
                        if (answer === $.t('ovs:generic.no')) {
                            self.updatingStorageRouters(false);
                        } else {
                            if (single === true) {
                                generic.alertInfo(
                                    $.t('ovs:wizards.shrink_vpool.confirm.started'),
                                    $.t('ovs:wizards.shrink_vpool.confirm.inprogress_single')
                                );
                            } else {
                                generic.alertInfo(
                                    $.t('ovs:wizards.shrink_vpool.confirm.started'),
                                    $.t('ovs:wizards.shrink_vpool.confirm.inprogress_multi')
                                );
                            }
                            VPoolService.shrinkVPool(self.vPool().guid(), sr.guid())
                                .done(function() {
                                    if (single === true) {
                                        generic.alertSuccess(
                                            $.t('ovs:wizards.shrink_vpool.confirm.complete'),
                                            $.t('ovs:wizards.shrink_vpool.confirm.success_single')
                                        );
                                    } else {
                                        generic.alertSuccess(
                                            $.t('ovs:wizards.shrink_vpool.confirm.complete'),
                                            $.t('ovs:wizards.shrink_vpool.confirm.success_multi')
                                        );
                                    }
                                })
                                .fail(function() {
                                    if (single === true) {
                                        generic.alertError(
                                            $.t('ovs:generic.error'),
                                            $.t('ovs:wizards.shrink_vpool.confirm.failed_single')
                                        );
                                    } else {
                                        generic.alertError(
                                            $.t('ovs:generic.error'),
                                            $.t('ovs:wizards.shrink_vpool.confirm.failed_multi')
                                        );
                                    }
                                })
                                .always(function() {
                                    self.updatingStorageRouters(false);
                                });
                        }
                    });
            }
        };
        self.refreshConfiguration = function(sr) {
            if (self.srSDMap().hasOwnProperty(sr.guid()) && !self.refreshList().contains(sr.guid())) {
                self.refreshList.push(sr.guid());
                generic.alertInfo(
                    $.t('ovs:vpools.detail.refresh.started'),
                    $.t('ovs:vpools.detail.refresh.started_msg', { sr: sr.name(), vpool: self.vPool().name() })
                );
                StoragedriverService.refreshConfiguration(self.srSDMap()[sr.guid()].guid())
                    .done(function(data) {

                        if (data === 0) {
                            generic.alertWarning(
                                $.t('ovs:vpools.detail.refresh.warning'),
                                $.t('ovs:vpools.detail.refresh.warning_msg', { sr: sr.name(), vpool: self.vPool().name() })
                            )
                        } else {
                            generic.alertSuccess(
                                $.t('ovs:vpools.detail.refresh.success'),
                                $.t('ovs:vpools.detail.refresh.success_msg', { sr: sr.name(), vpool: self.vPool().name() })
                            );
                        }
                    })
                    .fail(function(error) {
                        error = generic.extractErrorMessage(error);
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:vpools.detail.refresh.failed_msg', { sr: sr.name(), vpool: self.vPool().name(), why: error})
                        );
                    })
                    .always(function() {
                        var refreshList = self.refreshList();
                        refreshList.remove(sr.guid());
                        self.refreshList(refreshList);
                    });
            }
        };

        // Durandal
        self.activate = function(mode, guid) {
            self.vPool(new VPool(guid));
            self.refresher.init(self.load, 5000);
            self.refresher.run();
            self.refresher.start();
        };
        self.deactivate = function() {
            $.each(self.widgets, function(index, item) {
                item.deactivate();
            });
            self.refresher.stop();
        };
    }

    VPoolDetail.prototype = $.extend({}, BaseContainer.prototype);
    return VPoolDetail
});
