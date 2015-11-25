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
    'jquery', 'knockout',
    'ovs/shared', 'ovs/api', 'ovs/generic',
    '../../containers/storagerouter', '../../containers/storagedriver', '../../containers/vpool', './data'
], function($, ko, shared, api, generic, StorageRouter, StorageDriver, VPool, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared                   = shared;
        self.data                     = data;

        //Handles
        self.checkS3Handle            = undefined;
        self.checkMtptHandle          = undefined;
        self.fetchAlbaVPoolHandle     = undefined;
        self.loadStorageRouterHandle  = undefined;
        self.loadStorageRoutersHandle = undefined;
        self.loadStorageDriversHandle = {};

        // Observables
        self.preValidateResult  = ko.observable({ valid: true, reasons: [], fields: [] });
        self.albaBackendLoading = ko.observable(false);
        self.invalidAlbaInfo    = ko.observable(false);

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, showErrors = false, reasons = [], fields = [], preValidation = self.preValidateResult();
            if (!self.data.name.valid()) {
                valid = false;
                fields.push('name');
                reasons.push($.t('ovs:wizards.addvpool.gathervpool.invalidname'));
            }
            else {
                $.each(self.data.vPools(), function(index, vpool) {
                    if (vpool.name() === self.data.name()) {
                        valid = false;
                        fields.push('name');
                        reasons.push($.t('ovs:wizards.addvpool.gathervpool.duplicatename'));
                    }
                });
            }
            if (self.data.backend().match(/^.+_s3$/)) {
                if (!self.data.host.valid()) {
                    valid = false;
                    fields.push('host');
                    reasons.push($.t('ovs:wizards.addvpool.gathervpool.invalidhost'));
                }
                if (self.data.accesskey() === '' || self.data.secretkey() === '') {
                    valid = false;
                    fields.push('accesskey');
                    fields.push('secretkey');
                    reasons.push($.t('ovs:wizards.addvpool.gathervpool.nocredentials'));
                }
            }
            if (self.data.backend() === 'alba') {
                if (self.data.albaBackend() === undefined) {
                    valid = false;
                    reasons.push($.t('ovs:wizards.addvpool.gathervpool.choosebackend'));
                    fields.push('backend');
                }
                if (self.invalidAlbaInfo()) {
                    valid = false;
                    reasons.push($.t('ovs:wizards.addvpool.gathervpool.invalidalbainfo'));
                    fields.push('clientid');
                    fields.push('clientsecret');
                    fields.push('host');
                }
            }
            if (preValidation.valid === false) {
                showErrors = true;
                reasons = reasons.concat(preValidation.reasons);
                fields = fields.concat(preValidation.fields);
            }
            return { value: valid, showErrors: showErrors, reasons: reasons, fields: fields };
        });

        // Functions
        self.preValidate = function() {
            var validationResult = { valid: true, reasons: [], fields: [] };
            return $.Deferred(function(deferred) {
                $.when.apply($, [
                    $.Deferred(function(s3deferred) {
                        if (self.data.backend().match(/^.+_s3$/)) {
                            generic.xhrAbort(self.checkS3Handle);
                            var postData = {
                                host: self.data.host(),
                                port: self.data.port(),
                                accesskey: self.data.accesskey(),
                                secretkey: self.data.secretkey()
                            };
                            self.checkS3Handle = api.post('storagerouters/' + self.data.target().guid() + '/check_s3', { data: postData })
                                .then(self.shared.tasks.wait)
                                .done(function(data) {
                                    if (!data) {
                                        validationResult.valid = false;
                                        validationResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.invalids3info'));
                                        validationResult.fields.push('accesskey');
                                        validationResult.fields.push('secretkey');
                                        validationResult.fields.push('host');
                                    }
                                    s3deferred.resolve();
                                })
                                .fail(s3deferred.reject);
                        } else {
                            s3deferred.resolve();
                        }
                    }).promise(),
                    $.Deferred(function(mtptDeferred) {
                        generic.xhrAbort(self.checkMtptHandle);
                        var postData = {
                            name: self.data.name()
                        };
                        self.checkMtptHandle = api.post('storagerouters/' + self.data.target().guid() + '/check_mtpt', { data: postData })
                            .then(self.shared.tasks.wait)
                            .done(function(data) {
                                if (!data) {
                                    validationResult.valid = false;
                                    validationResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.mtptinuse', { what: self.data.name() }));
                                    validationResult.fields.push('name');
                                }
                                mtptDeferred.resolve();
                            })
                            .fail(mtptDeferred.reject);
                    }).promise(),
                    $.Deferred(function(physicalMetadataDeferred) {
                        generic.xhrAbort(self.loadStorageRouterHandle);
                        self.loadStorageRouterHandle = api.post('storagerouters/' + self.data.target().guid() + '/get_metadata')
                            .then(self.shared.tasks.wait)
                            .then(function(data) {
                                var write;
                                self.data.mountpoints(data.mountpoints);
                                self.data.partitions(data.partitions);
                                self.data.ipAddresses(data.ipaddresses);
                                self.data.arakoonFound(data.arakoon_found);
                                self.data.sharedSize(data.shared_size);
                                self.data.scrubAvailable(data.scrub_available);
                                self.data.readCacheAvailableSize(data.readcache_size);
                                self.data.writeCacheAvailableSize(data.writecache_size);
                                self.data.readCacheSize(Math.floor(data.readcache_size / 1024 / 1024 / 1024));
                                if (self.data.readCacheAvailableSize() === 0) {
                                    write = Math.floor((data.writecache_size + data.shared_size) / 1024 / 1024 / 1024) - 1;
                                } else {
                                    write = Math.floor((data.writecache_size + data.shared_size) / 1024 / 1024 / 1024);
                                }
                                self.data.writeCacheSize(write);
                            })
                            .done(function() {
                                var requiredRoles = ['READ', 'WRITE'];
                                if (self.data.arakoonFound() === false) {
                                    requiredRoles.push('DB');
                                }
                                $.each(self.data.partitions(), function(role, partitions) {
                                   if (requiredRoles.contains(role) && partitions.length > 0) {
                                       generic.removeElement(requiredRoles, role);
                                   }
                                });
                                if (requiredRoles.contains('DB')) {
                                    validationResult.valid = false;
                                    validationResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.missing_arakoon'));
                                    generic.removeElement(requiredRoles, 'DB');
                                }
                                $.each(requiredRoles, function(index, role) {
                                    validationResult.valid = false;
                                    validationResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.missing_role', { what: role }));
                                });
                                if (self.data.backend() === 'distributed' && self.data.mountpoints().length === 0) {
                                    validationResult.valid = false;
                                    validationResult.reasons.push($.t('ovs:wizards.addvpool.gathervpool.missing_mountpoints'));
                                }
                                physicalMetadataDeferred.resolve();
                            })
                            .fail(physicalMetadataDeferred.reject);
                    }).promise()
                ])
                    .always(function() {
                        self.preValidateResult(validationResult);
                        if (validationResult.valid) {
                            deferred.resolve();
                        } else {
                            deferred.reject();
                        }
                    });
            }).promise();
        };
        self.next = function() {
            return $.Deferred(function(deferred) {
                var calls = [];
                generic.crossFiller(
                    self.data.target().storageDriverGuids, self.data.storageDrivers,
                    function(guid) {
                        var storageDriver = new StorageDriver(guid);
                        calls.push($.Deferred(function(deferred) {
                            generic.xhrAbort(self.loadStorageDriversHandle[guid]);
                            self.loadStorageDriversHandle[guid] = api.get('storagedrivers/' + guid)
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
                    .done(deferred.resolve)
                    .fail(deferred.reject);
            });
        };
        self.loadAlbaBackends = function() {
            return $.Deferred(function(albaDeferred) {
                generic.xhrAbort(self.fetchAlbaVPoolHandle);
                var getData, relay = '', remoteInfo = {};
                getData = {
                    backend_type: 'alba',
                    contents: '_dynamics'
                };
                if (!self.data.localHost()) {
                    relay = 'relay/';
                    remoteInfo.ip = self.data.host();
                    remoteInfo.port = self.data.port();
                    remoteInfo.client_id = self.data.accesskey();
                    remoteInfo.client_secret = self.data.secretkey();
                }
                $.extend(getData, remoteInfo);
                self.albaBackendLoading(true);
                self.invalidAlbaInfo(false);
                self.fetchAlbaVPoolHandle = api.get(relay + 'backends', { queryparams: getData })
                    .done(function(data) {
                        var available_backends = [], calls = [];
                        $.each(data.data, function (index, item) {
                            if (item.available === true) {
                                calls.push(
                                    api.get(relay + 'alba/backends/' + item.linked_guid + '/', { queryparams: getData })
                                        .then(function(data) {
                                            data.presetNames = ko.observableArray(data.presets.filter(function(preset) {
                                                return preset.is_available === true;
                                            }));
                                            if (data.available === true && data.presetNames().length > 0) {
                                                available_backends.push(data);
                                            }
                                        })
                                );
                            }
                        });
                        $.when.apply($, calls)
                            .then(function() {
                                if (available_backends.length > 0) {
                                    self.data.albaBackends(available_backends);
                                    self.data.albaBackend(available_backends[0]);
                                    self.data.albaPreset(available_backends[0].presetNames()[0]);
                                } else {
                                    self.data.albaBackends(undefined);
                                    self.data.albaBackend(undefined);
                                    self.data.albaPreset(undefined);
                                }
                                self.albaBackendLoading(false);
                            })
                            .done(albaDeferred.resolve)
                            .fail(function() {
                                self.data.albaBackends(undefined);
                                self.data.albaBackend(undefined);
                                self.data.albaPreset(undefined);
                                self.albaBackendLoading(false);
                                self.invalidAlbaInfo(true);
                                albaDeferred.reject();
                            });
                    })
                    .fail(function() {
                        self.data.albaBackends(undefined);
                        self.data.albaBackend(undefined);
                        self.albaBackendLoading(false);
                        self.invalidAlbaInfo(true);
                        albaDeferred.reject();
                    });
            }).promise();
        };

        // Durandal
        self.activate = function() {
            generic.xhrAbort(self.loadStorageRoutersHandle);
            self.loadStorageRoutersHandle = api.get('storagerouters', {
                    queryparams: {
                    contents: 'storagedrivers',
                    sort: 'name'
                }
            })
                .done(function(data) {
                    var guids = [], srdata = {};
                    $.each(data.data, function(index, item) {
                        guids.push(item.guid);
                        srdata[item.guid] = item;
                    });
                    generic.crossFiller(
                        guids, self.data.storageRouters,
                        function(guid) {
                            return new StorageRouter(guid);
                        }, 'guid'
                    );
                    $.each(self.data.storageRouters(), function(index, storageRouter) {
                        storageRouter.fillData(srdata[storageRouter.guid()]);
                    });
                    if (self.data.target() === undefined && self.data.storageRouters().length > 0) {
                        self.data.target(self.data.storageRouters()[0]);
                    }
                });

            if (generic.xhrCompleted(self.loadVPoolsHandle)) {
                var options = {
                    contents: ''
                };
                self.loadVPoolsHandle = api.get('vpools', { queryparams: options })
                    .done(function (data) {
                        var guids = [], vpData = {};
                        $.each(data.data, function (index, item) {
                            guids.push(item.guid);
                            vpData[item.guid] = item;
                        });
                        generic.crossFiller(
                            guids, self.data.vPools,
                            function (guid) {
                                return new VPool(guid);
                            }, 'guid'
                        );
                        $.each(self.data.vPools(), function (index, vpool) {
                            if (guids.contains(vpool.guid())) {
                                vpool.fillData(vpData[vpool.guid()]);
                            }
                        });
                    });
            }
        };
    };
});
