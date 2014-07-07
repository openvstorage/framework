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
    'ovs/shared', 'ovs/api', 'ovs/generic',
    '../../containers/storagerouter', '../../containers/storagedriver', './data'
], function($, ko, shared, api, generic, StorageRouter, StorageDriver, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared                   = shared;
        self.data                     = data;
        self.loadStorageRoutersHandle = undefined;
        self.checkS3Handle            = undefined;
        self.checkMtptHandle          = undefined;
        self.loadStorageRouterHandle  = undefined;

        // Observables
        self.preValidateResult = ko.observable({ valid: true, reasons: [], fields: [] });

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, showErrors = false, reasons = [], fields = [], preValidation = self.preValidateResult();
            if (!self.data.name.valid()) {
                valid = false;
                fields.push('name');
                reasons.push($.t('ovs:wizards.addvpool.gathervpool.invalidname'));
            }
            if (self.data.backend().match(/^.+_S3$/)) {
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
                        if (self.data.backend().match(/^.+_S3$/)) {
                            generic.xhrAbort(self.checkS3Handle);
                            var postData = {
                                host: self.data.host(),
                                port: self.data.port(),
                                accesskey: self.data.accesskey(),
                                secretkey: self.data.secretkey()
                            };
                            self.checkS3Handle = api.post('storagerouters/' + self.data.target().guid() + '/check_s3', postData)
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
                        self.checkMtptHandle = api.post('storagerouters/' + self.data.target().guid() + '/check_mtpt', postData)
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
                var calls = [
                    $.Deferred(function(mtptDeferred) {
                        generic.xhrAbort(self.loadStorageRouterHandle);
                        self.loadStorageRouterHandle = api.post('storagerouters/' + self.data.target().guid() + '/get_physical_metadata', {})
                            .then(self.shared.tasks.wait)
                            .then(function(data) {
                                self.data.mountpoints(data.mountpoints);
                                self.data.ipAddresses(data.ipaddresses);
                                self.data.vRouterPort(data.xmlrpcport);
                                self.data.files(data.files);
                                self.data.allowVPool(data.allow_vpool);
                            })
                            .done(function() {
                                mtptDeferred.resolve();
                            })
                            .fail(mtptDeferred.reject);
                    }).promise()
                ];
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

        // Durandal
        self.activate = function() {
            generic.xhrAbort(self.loadStorageRoutersHandle);
            self.loadStorageRoutersHandle = api.get('storagerouters', undefined, {
                contents: 'storageDrivers',
                sort: 'name'
            })
                .done(function(data) {
                    var guids = [], srdata = {};
                    $.each(data, function(index, item) {
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
        };
    };
});
