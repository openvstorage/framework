// Copyright 2016 iNuron NV
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
    'jquery', 'durandal/app', 'plugins/dialog', 'knockout', 'plugins/router',
    'ovs/shared', 'ovs/generic', 'ovs/refresher', 'ovs/api',
    '../containers/vpool', '../containers/vmachine', '../containers/storagedriver', '../containers/storagerouter', '../containers/vdisk',
    '../wizards/addvpool/index'
], function($, app, dialog, ko, router,
            shared, generic, Refresher, api,
            VPool, VMachine, StorageDriver, StorageRouter, VDisk,
            ExtendVPool) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared             = shared;
        self.guard              = { authenticated: true, registered: true };
        self.refresher          = new Refresher();
        self.widgets            = [];
        self.storageRouterCache = {};
        self.vMachineCache      = {};
        self.vDiskCache         = {};
        self.vDiskHeaders       = [
            { key: 'name',       value: $.t('ovs:generic.name'),       width: undefined },
            { key: 'vmachine',   value: $.t('ovs:generic.vmachine'),   width: 110       },
            { key: 'size',       value: $.t('ovs:generic.size'),       width: 100       },
            { key: 'storedData', value: $.t('ovs:generic.storeddata'), width: 110       },
            { key: 'cacheRatio', value: $.t('ovs:generic.cache'),      width: 100       },
            { key: 'iops',       value: $.t('ovs:generic.iops'),       width: 55        },
            { key: 'readSpeed',  value: $.t('ovs:generic.read'),       width: 100       },
            { key: 'writeSpeed', value: $.t('ovs:generic.write'),      width: 100       },
            { key: 'dtlStatus',  value: $.t('ovs:generic.dtl_status'), width: 50        }
        ];
        self.vMachineHeaders    = [
            { key: 'name',          value: $.t('ovs:generic.name'),          width: undefined },
            { key: 'storagerouter', value: $.t('ovs:generic.storagerouter'), width: 200       },
            { key: undefined,       value: $.t('ovs:generic.vdisks'),        width: 60        },
            { key: 'storedData',    value: $.t('ovs:generic.storeddata'),    width: 110       },
            { key: 'cacheRatio',    value: $.t('ovs:generic.cache'),         width: 100       },
            { key: 'iops',          value: $.t('ovs:generic.iops'),          width: 55        },
            { key: 'readSpeed',     value: $.t('ovs:generic.read'),          width: 120       },
            { key: 'writeSpeed',    value: $.t('ovs:generic.write'),         width: 120       },
            { key: 'dtlStatus',     value: $.t('ovs:generic.dtl_status'),    width: 50        }
        ];

        // Handles
        self.vDisksHandle             = {};
        self.vMachinesHandle          = {};
        self.loadStorageDriversHandle = undefined;
        self.loadStorageRoutersHandle = undefined;

        // Observables
        self.storageRoutersLoaded      = ko.observable(false);
        self.updatingStorageRouters    = ko.observable(false);
        self.vPool                     = ko.observable();
        self.srCanDeleteMap            = ko.observable();
        self.storageDrivers            = ko.observableArray([]);
        self.storageRouters            = ko.observableArray([]);

        // Functions
        self.load = function() {
            return $.Deferred(function (deferred) {
                var vpool = self.vPool();
                $.when.apply($, [
                    vpool.load('storagedrivers,vdisks,_dynamics,backend_type'),
                    vpool.loadConfiguration(),
                    vpool.loadStorageRouters(),
                    self.loadStorageRouters()
                ])
                    .then(vpool.loadBackendType)
                    .then(self.loadStorageDriverInfo)
                    .fail(function(error) {
                        if (error !== undefined && error.status === 404) {
                            router.navigate(shared.routing.loadHash('vpools'));
                        }
                    })
                    .always(deferred.resolve);
            }).promise();
        };
        self.loadStorageDriverInfo = function() {
            return $.Deferred(function (deferred) {
                var calls = [];
                generic.crossFiller(
                    self.vPool().storageDriverGuids(), self.storageDrivers,
                    function(guid) {
                        var storageDriver = new StorageDriver(guid);
                        calls.push(storageDriver.load());
                        return storageDriver;
                    }, 'guid'
                );
                $.each(self.storageDrivers(), function(_, sd) {
                    calls.push(sd.canBeDeleted());
                });
                $.when.apply($, calls)
                    .done(function() {
                        var map = self.srCanDeleteMap();
                        if (map === undefined) {
                            map = {};
                        }
                        $.each(self.storageRouters(), function(_, sr) {
                            var srGuid = sr.guid();
                            if (map[srGuid] === undefined) {
                                map[srGuid] = null;
                            }
                            var found = false;
                            $.each(self.storageDrivers(), function(_, sd) {
                                if (sd.storageRouterGuid() === srGuid) {
                                    map[srGuid] = sd.canDelete();
                                    found = true;
                                    return false;
                                }
                            });
                            if (found === false) {
                                map[srGuid] = null;
                            }
                        });
                        self.srCanDeleteMap(map);
                        deferred.resolve();
                    })
                    .always(deferred.resolve);
            }).promise();
        };
        self.loadStorageRouters = function() {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.loadStorageRoutersHandle)) {
                    var options = {
                        sort: 'name',
                        contents: 'storagedrivers,pmachine'
                    };
                    self.loadStorageRoutersHandle = api.get('storagerouters', { queryparams: options })
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
                                if (sadata.hasOwnProperty(storageRouter.guid())) {
                                    storageRouter.fillData(sadata[storageRouter.guid()]);
                                    if (storageRouter.pMachine() !== undefined) {
                                        storageRouter.pMachine().load();
                                        storageRouter.pMachine().loadVPoolConfigurationState(self.vPool().guid());
                                    }
                                }
                            });
                            self.storageRoutersLoaded(true);
                            deferred.resolve();
                        })
                        .fail(deferred.reject);
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.loadVDisks = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.vDisksHandle[options.page])) {
                    options.sort = 'devicename';
                    options.contents = '_dynamics,_relations,-snapshots';
                    options.vpoolguid = self.vPool().guid();
                    self.vDisksHandle[options.page] = api.get('vdisks', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    if (!self.vDiskCache.hasOwnProperty(guid)) {
                                        self.vDiskCache[guid] = new VDisk(guid);
                                    }
                                    return self.vDiskCache[guid];
                                },
                                dependencyLoader: function(item) {
                                    var guid = item.vMachineGuid();
                                    if (self.vMachineCache.hasOwnProperty(guid)) {
                                        item.vMachine(self.vMachineCache[guid]);
                                    }
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.loadVMachines = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.vMachinesHandle[options.page])) {
                    options.sort = 'name';
                    options.contents = 'vdisks,_dynamics,-snapshots,-hypervisor_status';
                    options.vpoolguid = self.vPool().guid();
                    self.vMachinesHandle[options.page] = api.get('vmachines', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    if (!self.vMachineCache.hasOwnProperty(guid)) {
                                        self.vMachineCache[guid] = new VMachine(guid);
                                    }
                                    return self.vMachineCache[guid];
                                },
                                dependencyLoader: function(item) {
                                    generic.crossFiller(
                                        item.storageRouterGuids, item.storageRouters,
                                        function(guid) {
                                            if (!self.storageRouterCache.hasOwnProperty(guid)) {
                                                var sr = new StorageRouter(guid);
                                                sr.load('');
                                                self.storageRouterCache[guid] = sr;
                                            }
                                            return self.storageRouterCache[guid];
                                        }, 'guid'
                                    );
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };
        self.sync = function() {
            if (self.vPool() !== undefined) {
                var vp = self.vPool();
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
                                            error: error.responseText
                                        })
                                    );
                                });
                        }
                    });
            }
        };
        self.reconfigurePmachine = function(sr_guid, configure) {
            self.updatingStorageRouters(true);
            var pmachine_guid;
            $.each(self.storageRouters(), function(index, storagerouter) {
                if (storagerouter.guid() === sr_guid) {
                    pmachine_guid = storagerouter.pMachineGuid();
                }
            });
            if (pmachine_guid !== undefined) {
                app.showMessage(
                    $.t('ovs:pmachines.configure.vpool.warning', { what: configure === true ? 'configure' : 'unconfigure' }),
                    $.t('ovs:generic.areyousure'),
                    [$.t('ovs:generic.no'), $.t('ovs:generic.yes')]
                )
                .done(function(answer) {
                    if (answer === $.t('ovs:generic.yes')) {
                        var action = configure === true ? '/configure_vpool_for_host' : '/unconfigure_vpool_for_host';
                        generic.alertInfo(
                            $.t('ovs:pmachines.configure.vpool.started'),
                            $.t('ovs:pmachines.configure.vpool.started_msg', { which: (action === '/configure_vpool_for_host' ? 'Configuration' : 'Unconfiguration') })
                        );
                        api.post('pmachines/' + pmachine_guid + action, {
                            data: { vpool_guid: self.vPool().guid() }
                        })
                        .then(shared.tasks.wait)
                        .done(function() {
                            generic.alertSuccess(
                                $.t('ovs:pmachines.configure.vpool.success'),
                                $.t('ovs:pmachines.configure.vpool.success_msg', { which: (action === '/configure_vpool_for_host' ? 'configured' : 'unconfigured') })
                            );
                        })
                        .fail(function(error) {
                            generic.alertError(
                                $.t('ovs:generic.error'),
                                $.t('ovs:pmachines.configure.vpool.failed', {
                                    which: (action === '/configure_vpool_for_host' ? 'configure' : 'unconfigure'),
                                    why: error
                                })
                            );
                        });
                    }
                });
            }
            self.updatingStorageRouters(false);
        };
        self.addStorageRouter = function(sr) {
            self.updatingStorageRouters(true);

            var deferred = $.Deferred(),
                wizard = new ExtendVPool({
                    modal: true,
                    completed: deferred,
                    vPool: self.vPool(),
                    storageRouter: sr
                });
            wizard.closing.always(function() {
                deferred.resolve();
            });
            dialog.show(wizard);
            deferred.always(function() {
                self.updatingStorageRouters(false);
            });
        };
        self.removeStorageRouter = function(sr) {
            var single = self.vPool().storageRouterGuids().length === 1;
            if (self.srCanDeleteMap() !== undefined && self.srCanDeleteMap()[sr.guid()] === true) {
                self.updatingStorageRouters(true);
                app.showMessage(
                    $.t('ovs:wizards.shrink_vpool.confirm.remove_' + (single === true ? 'single' : 'multi'), { what: sr.name() }),
                    $.t('ovs:generic.areyousure'),
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
                            api.post('vpools/' + self.vPool().guid() + '/shrink_vpool', { data: { storagerouter_guid: sr.guid() } })
                                .then(self.shared.tasks.wait)
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
                                    var map = self.srCanDeleteMap();
                                    map[sr.guid()] = null;
                                    self.srCanDeleteMap(map);
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
                                    var map = self.srCanDeleteMap();
                                    map[sr.guid()] = true;
                                    self.srCanDeleteMap(map);
                                })
                                .always(function() {
                                    self.updatingStorageRouters(false);
                                });
                        }
                    });
            }
        };

        // Durandal
        self.activate = function(mode, guid) {
            self.vPool(new VPool(guid));
            self.refresher.init(self.load, 10000);
            self.refresher.run();
            self.refresher.start();
            self.shared.footerData(self.vPool);
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
