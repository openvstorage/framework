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
    'jquery', 'knockout',
    'ovs/api', 'ovs/generic', 'ovs/shared', 'ovs/refresher',
    '../../containers/storagedriver', './data'
], function($, ko, api, generic, shared, Refresher, StorageDriver, data) {
    "use strict";
    return function(parent) {
        var self = this;

        // Variables
        self.shared               = shared;
        self.storageDriverLoading = $.Deferred();
        self.refresher            = new Refresher();

        // Observables
        self.storageDriver = ko.observable();

        // Objects
        self.RemoveValidation = function(storageDriverGuid) {
            var self = this;

            // Variables
            self.storageDriverGuid = storageDriverGuid;

            // Observables
            self.loaded = ko.observable(false);
            self.inUse  = ko.observable(false);

            // Computed
            self.validationState = ko.computed(function() {
                var valid = true, reasons = [];
                if (self.loaded() === false) {
                    valid = undefined;
                } else if (self.loaded() === undefined) {
                    valid = false;
                    reasons.push($.t('ovs:wizards.storageroutertovpool.confirm.errorvalidating'));
                } else if (self.inUse() === false) {
                    valid = false;
                    reasons.push($.t('ovs:wizards.storageroutertovpool.confirm.inuse'));
                }
                return { valid: valid, reasons: reasons, fields: [] };
            });

            // Functions
            self.validate = function() {
                api.post('storagedrivers/' + self.storageDriverGuid + '/can_be_deleted')
                    .done(function(data) {
                        self.loaded(true);
                        self.inUse(data);
                    })
                    .fail(function() {
                        self.loaded(undefined);
                    });
            };
        };
        self.AddValidation = function(storageDriver, storageRouter, storageDriverDeferred, data) {
            var self = this;

            // Variables
            self.storageRouter         = storageRouter;
            self.storageDriver         = storageDriver;
            self.shared                = shared;
            self.storageDriverDeferred = storageDriverDeferred;
            self.data                  = data;

            // Observables
            self.loaded         = ko.observable(false);
            self.allowVPool     = ko.observable(true);
            self.mtptOK         = ko.observable(true);
            self.storageDrivers = ko.observableArray([]);
            self.mountpoints    = ko.observableArray([]);
            self.ipAddresses    = ko.observableArray([]);

            // Computed
            self.validationState = ko.computed(function() {
                var valid = true, reasons = [], fields = [];
                if (self.loaded() === false) {
                    valid = undefined;
                } else if (self.loaded() === undefined) {
                    valid = false;
                    reasons.push($.t('ovs:wizards.storageroutertovpool.confirm.errorvalidating'));
                } else {
                    $.each(self.storageDrivers(), function(index, storageDriver) {
                        if (self.storageDriver().mountpointReadCache1() === storageDriver.mountpointReadCache1() && $.inArray('readcache1', fields) === -1) {
                            valid = false;
                            fields.push('readcache1');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
                        }
                        if (self.storageDriver().mountpointReadCache2() === storageDriver.mountpointReadCache2() && $.inArray('readcache2', fields) === -1) {
                            valid = false;
                            fields.push('readcache2');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
                        }
                        if (self.storageDriver().mountpointWriteCache() === storageDriver.mountpointWriteCache() && $.inArray('writecache', fields) === -1) {
                            valid = false;
                            fields.push('writecache');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
                        }
                        if (self.storageDriver().mountpointFOC() === storageDriver.mountpointFOC() && $.inArray('foc', fields) === -1) {
                            valid = false;
                            fields.push('foc');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
                        }
                        if (self.storageDriver().mountpointBFS() === storageDriver.mountpointBFS() && $.inArray('bfs', fields) === -1 &&
                            (self.data.vPool().backendType() === 'local' || self.data.vPool().backendType() === 'distributed')) {
                            valid = false;
                            fields.push('bfs');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.bfs') }));
                        }
                        if (self.storageDriver().mountpointMD() === storageDriver.mountpointMD() && $.inArray('md', fields) === -1) {
                            valid = false;
                            fields.push('md');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.mdfs') }));
                        }
                        if (self.storageDriver().mountpointTemp() === storageDriver.mountpointTemp() && $.inArray('temp', fields) === -1) {
                            valid = false;
                            fields.push('temp');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.tempfs') }));
                        }
                        if (generic.overlap(self.storageDriver().ports(), storageDriver.ports()) && $.inArray('port', fields) === -1) {
                            valid = false;
                            fields.push('port');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.portinuse'));
                        }
                        return true;
                    });
                    if ($.inArray(self.storageDriver().storageIP(), self.ipAddresses()) === -1 && $.inArray('ip', fields) === -1) {
                        valid = false;
                        fields.push('ip');
                        reasons.push($.t('ovs:wizards.storageroutertovpool.confirm.ipnotavailable', { what: self.storageDriver().storageIP() }));
                    }
                    if (!self.allowVPool() && $.inArray('vpool', fields) === -1) {
                        valid = false;
                        fields.push('vpool');
                        reasons.push($.t('ovs:wizards.storageroutertovpool.confirm.vpoolnotallowed'));
                    }
                    if (!self.mtptOK()) {
                        valid = false;
                        fields.push('mtpt');
                        reasons.push($.t('ovs:wizards.storageroutertovpool.confirm.mtptinuse', { what: self.data.vPool().name() }));
                    }
                }
                return { valid: valid, reasons: reasons, fields: fields };
            });

            // Functions
            self.validate = function() {
                var calls = [
                    $.Deferred(function(physicalDeferred) {
                        api.post('storagerouters/' + self.storageRouter.guid() + '/get_physical_metadata')
                            .then(self.shared.tasks.wait)
                            .then(function(data) {
                                self.mountpoints(data.mountpoints);
                                self.ipAddresses(data.ipaddresses);
                                self.allowVPool(data.allow_vpool);
                            })
                            .done(physicalDeferred.resolve)
                            .fail(physicalDeferred.reject);
                    }).promise(),
                    $.Deferred(function(mtptDeferred) {
                        var postData = {
                            name: self.data.vPool().name()
                        };
                        api.post('storagerouters/' + self.storageRouter.guid() + '/check_mtpt', { data: postData })
                            .then(self.shared.tasks.wait)
                            .done(function(data) {
                                self.mtptOK(data);
                                mtptDeferred.resolve();
                            })
                            .fail(mtptDeferred.reject);
                    }).promise(),
                    self.storageDriverDeferred.promise()
                ];
                generic.crossFiller(
                    self.storageRouter.storageDriverGuids, self.storageDrivers,
                    function(guid) {
                        var storageDriver = new StorageDriver(guid);
                        calls.push($.Deferred(function(deferred) {
                            api.get('storagedrivers/' + guid)
                                .done(function(storageDriverData) {
                                    storageDriver.fillData(storageDriverData);
                                    deferred.resolve();
                                })
                                .fail(deferred.reject);
                        }).promise());
                        return storageDriver;
                    }, 'guid'
                );
                $.when.apply($, calls)
                    .done(function() {
                        self.loaded(true);
                    })
                    .fail(function() {
                        self.loaded(undefined);
                    });
            };
        };

        // Variables
        self.data               = data;
        self._addValidations    = {};
        self._removeValidations = {};

        // Computed
        self.addValidations = ko.computed(function() {
            $.each(self.data.pendingStorageRouters(), function(index, storageRouter) {
                if (!self._addValidations.hasOwnProperty(storageRouter.guid())) {
                    self._addValidations[storageRouter.guid()] = new self.AddValidation(self.storageDriver, storageRouter, self.storageDriverLoading, self.data);
                    self._addValidations[storageRouter.guid()].validate();
                }
            });
            return self._addValidations;
        });
        self.removeValidations = ko.computed(function() {
            $.each(self.data.removingStorageRouters(), function(index, storageRouter) {
                var foundStorageDriverGuid;
                $.each(storageRouter.storageDriverGuids, function(storageDriverIndex, storageDriverGuid) {
                    $.each(self.data.vPool().storageDriverGuids(), function(pIndex, pStorageDriverGuid) {
                        if (pStorageDriverGuid === storageDriverGuid) {
                            foundStorageDriverGuid = storageDriverGuid;
                            return false;
                        }
                        return true;
                    });
                });
                if (!self._removeValidations.hasOwnProperty(storageRouter.guid())) {
                    self._removeValidations[storageRouter.guid()] = new self.RemoveValidation(foundStorageDriverGuid);
                    self._removeValidations[storageRouter.guid()].validate();
                }
            });
            return self._removeValidations;
        });
        self.canContinue = ko.computed(function() {
            var valid = true, hasValid = false;
            $.each(self.addValidations(), function(index, validation) {
                if (validation.validationState().valid === undefined) {
                    valid = false;
                    return false;
                }
                if (validation.validationState().valid === true) {
                    hasValid = true;
                }
                return true;
            });
            $.each(self.removeValidations(), function(index, validation) {
                if (validation.validationState().valid === undefined) {
                    valid = false;
                    return false;
                }
                if (validation.validationState().valid === true) {
                    hasValid = true;
                }
                return true;
            });
            return { value: valid && hasValid, reasons: [], fields: [] };
        });

        // Functions
        self.finish = function() {
            return $.Deferred(function(deferred) {
                var storageRouterGuids = [], storageDriverGuids = [];
                $.each(self.addValidations(), function(storageRouterGuid, validation) {
                    if (validation.validationState().valid === true) {
                        storageRouterGuids.push(storageRouterGuid);
                    }
                });
                $.each(self.removeValidations(), function(storageRouterGuid, validation) {
                    if (validation.validationState().valid === true) {
                        storageDriverGuids.push(validation.storageDriverGuid);
                    }
                });
                api.post('vpools/' + self.data.vPool().guid() + '/update_storagedrivers', {
                    data: {
                        storagedriver_guid: self.storageDriver().guid(),
                        storagerouter_guids: storageRouterGuids.join(','),
                        storagedriver_guids: storageDriverGuids.join(',')
                    }
                })
                    .then(self.shared.tasks.wait)
                    .done(function(data) {
                        if (data === true) {
                            generic.alertSuccess(
                                $.t('ovs:wizards.storageroutertovpool.confirm.complete'),
                                $.t('ovs:wizards.storageroutertovpool.confirm.success')
                            );
                        } else {
                            generic.alert(
                                $.t('ovs:wizards.storageroutertovpool.confirm.complete'),
                                $.t('ovs:wizards.storageroutertovpool.confirm.somefailed')
                            );
                        }
                    })
                    .fail(function() {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.storageroutertovpool.confirm.allfailed')
                        );
                    })
                    .always(function() {
                        if (self.data.completed !== undefined) {
                            self.data.completed.resolve(true);
                        }
                    });
                generic.alertInfo($.t('ovs:wizards.storageroutertovpool.confirm.started'), $.t('ovs:wizards.storageroutertovpool.confirm.inprogress'));
                deferred.resolve();
            }).promise();
        };

        // Durandal
        self.activate = function() {
            self.data.vPool().load('storageDrivers', { skipDisks: true })
                .then(function() {
                    self.storageDriver(new StorageDriver(self.data.vPool().storageDriverGuids()[0]));
                    api.get('storagedrivers/' + self.storageDriver().guid())
                        .done(function(storageDriverData) {
                            self.storageDriver().fillData(storageDriverData);
                            self.storageDriverLoading.resolve();
                        })
                        .fail(function() {
                            self.storageDriverLoading.reject();
                        });
                })
                .fail(function() {
                    self.storageDriverLoading.reject();
                });
            self.refresher.init(function() {
                $.each(self.data.pendingStorageRouters(), function(index, storageRouter) {
                    self.addValidations()[storageRouter.guid()].validate();
                });
                $.each(self.data.removingStorageRouters(), function(index, storageRouter) {
                    self.removeValidations()[storageRouter.guid()].validate();
                });
            }, 5000);
            self.refresher.run();
            self.refresher.start();
            parent.closing.always(function() {
                self.refresher.stop();
            });
            parent.finishing.always(function() {
                self.refresher.stop();
            });
        };
    };
});
