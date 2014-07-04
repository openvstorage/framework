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
    '../../containers/volumestoragerouter', './data'
], function($, ko, api, generic, shared, Refresher, VolumeStorageRouter, data) {
    "use strict";
    return function(parent) {
        var self = this;

        // Variables
        self.shared     = shared;
        self.vSRLoading = $.Deferred();
        self.refresher  = new Refresher();

        // Observables
        self.vsr = ko.observable();

        // Objects
        self.RemoveValidation = function(vsrGuid) {
            var self = this;

            // Variables
            self.vsrGuid = vsrGuid;

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
                    reasons.push($.t('ovs:wizards.storageappliancetovpool.confirm.errorvalidating'));
                } else if (self.inUse() === false) {
                    valid = false;
                    reasons.push($.t('ovs:wizards.storageappliancetovpool.confirm.inuse'));
                }
                return { valid: valid, reasons: reasons, fields: [] };
            });

            // Functions
            self.validate = function() {
                api.post('volumestoragerouters/' + self.vsrGuid + '/can_be_deleted')
                    .done(function(data) {
                        self.loaded(true);
                        self.inUse(data);
                    })
                    .fail(function() {
                        self.loaded(undefined);
                    });
            };
        };
        self.AddValidation = function(vsr, storageAppliance, vsrDeferred, data) {
            var self = this;

            // Variables
            self.storageAppliance = storageAppliance;
            self.vsr              = vsr;
            self.shared           = shared;
            self.vsrDeferred      = vsrDeferred;
            self.data             = data;

            // Observables
            self.loaded      = ko.observable(false);
            self.allowVPool  = ko.observable(true);
            self.mtptOK      = ko.observable(true);
            self.vRouterPort = ko.observable();
            self.vsrs        = ko.observableArray([]);
            self.mountpoints = ko.observableArray([]);
            self.ipAddresses = ko.observableArray([]);

            // Computed
            self.validationState = ko.computed(function() {
                var valid = true, reasons = [], fields = [];
                if (self.loaded() === false) {
                    valid = undefined;
                } else if (self.loaded() === undefined) {
                    valid = false;
                    reasons.push($.t('ovs:wizards.storageappliancetovpool.confirm.errorvalidating'));
                } else {
                    $.each(self.vsrs(), function(index, vsr) {
                        if (self.vsr().mountpointCache() === vsr.mountpointCache() && $.inArray('cache', fields) === -1) {
                            valid = false;
                            fields.push('cache');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
                        }
                        if (self.vsr().mountpointBFS() === vsr.mountpointBFS() && $.inArray('bfs', fields) === -1) {
                            valid = false;
                            fields.push('bfs');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.bfs') }));
                        }
                        if (self.vsr().mountpointMD() === vsr.mountpointMD() && $.inArray('md', fields) === -1) {
                            valid = false;
                            fields.push('md');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.mdfs') }));
                        }
                        if (self.vsr().mountpointTemp() === vsr.mountpointTemp() && $.inArray('temp', fields) === -1) {
                            valid = false;
                            fields.push('temp');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.tempfs') }));
                        }
                        if (self.vsr().port() === vsr.port() && $.inArray('port', fields) === -1) {
                            valid = false;
                            fields.push('port');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.portinuse'));
                        }
                        return true;
                    });
                    if ($.inArray(self.vsr().storageIP(), self.ipAddresses()) === -1 && $.inArray('ip', fields) === -1) {
                        valid = false;
                        fields.push('ip');
                        reasons.push($.t('ovs:wizards.storageappliancetovpool.confirm.ipnotavailable', { what: self.vsr().storageIP() }));
                    }
                    if (!self.allowVPool() && $.inArray('vpool', fields) === -1) {
                        valid = false;
                        fields.push('vpool');
                        reasons.push($.t('ovs:wizards.storageappliancetovpool.confirm.vpoolnotallowed'));
                    }
                    if (!self.mtptOK()) {
                        valid = false;
                        fields.push('mtpt');
                        reasons.push($.t('ovs:wizards.storageappliancetovpool.confirm.mtptinuse', { what: self.data.vPool().name() }));
                    }
                }
                return { valid: valid, reasons: reasons, fields: fields };
            });

            // Functions
            self.validate = function() {
                var calls = [
                    $.Deferred(function(physicalDeferred) {
                        api.post('storageappliances/' + self.storageAppliance.guid() + '/get_physical_metadata')
                            .then(self.shared.tasks.wait)
                            .then(function(data) {
                                self.mountpoints(data.mountpoints);
                                self.ipAddresses(data.ipaddresses);
                                self.vRouterPort(data.xmlrpcport);
                                self.allowVPool(data.allow_vpool);
                            })
                            .done(physicalDeferred.resolve)
                            .fail(physicalDeferred.reject);
                    }).promise(),
                    $.Deferred(function(mtptDeferred) {
                        var postData = {
                            name: self.data.vPool().name()
                        };
                        api.post('storageappliances/' + self.storageAppliance.guid() + '/check_mtpt', postData)
                            .then(self.shared.tasks.wait)
                            .done(function(data) {
                                self.mtptOK(data);
                                mtptDeferred.resolve();
                            })
                            .fail(mtptDeferred.reject);
                    }).promise(),
                    self.vsrDeferred.promise()
                ];
                generic.crossFiller(
                    self.storageAppliance.vSRGuids, self.vsrs,
                    function(guid) {
                        var vsr = new VolumeStorageRouter(guid);
                        calls.push($.Deferred(function(deferred) {
                            api.get('volumestoragerouters/' + guid)
                                .done(function(vsrData) {
                                    vsr.fillData(vsrData);
                                    deferred.resolve();
                                })
                                .fail(deferred.reject);
                        }).promise());
                        return vsr;
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
            $.each(self.data.pendingStorageAppliances(), function(index, storageAppliance) {
                if (!self._addValidations.hasOwnProperty(storageAppliance.guid())) {
                    self._addValidations[storageAppliance.guid()] = new self.AddValidation(self.vsr, storageAppliance, self.vSRLoading, self.data);
                    self._addValidations[storageAppliance.guid()].validate();
                }
            });
            return self._addValidations;
        });
        self.removeValidations = ko.computed(function() {
            $.each(self.data.removingStorageAppliances(), function(index, storageAppliance) {
                var vsrGuid;
                $.each(storageAppliance.vSRGuids, function(vsrIndex, vSRGuid) {
                    $.each(self.data.vPool().vSRGuids(), function(pIndex, pVSRGuid) {
                        if (pVSRGuid === vSRGuid) {
                            vsrGuid = vSRGuid;
                            return false;
                        }
                        return true;
                    });
                });
                if (!self._removeValidations.hasOwnProperty(storageAppliance.guid())) {
                    self._removeValidations[storageAppliance.guid()] = new self.RemoveValidation(vsrGuid);
                    self._removeValidations[storageAppliance.guid()].validate();
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
                var storageApplianceGuids = [], vsrGuids = [];
                $.each(self.addValidations(), function(storageApplianceGuid, validation) {
                    if (validation.validationState().valid === true) {
                        storageApplianceGuids.push(storageApplianceGuid);
                    }
                });
                $.each(self.removeValidations(), function(storageApplianceGuid, validation) {
                    if (validation.validationState().valid === true) {
                        vsrGuids.push(validation.vsrGuid);
                    }
                });
                api.post('vpools/' + self.data.vPool().guid() + '/update_vsrs', {
                    vsr_guid: self.vsr().guid(),
                    storageappliance_guids: storageApplianceGuids.join(','),
                    vsr_guids: vsrGuids.join(',')
                })
                    .then(self.shared.tasks.wait)
                    .done(function(data) {
                        if (data === true) {
                            generic.alertSuccess(
                                $.t('ovs:wizards.storageappliancetovpool.confirm.complete'),
                                $.t('ovs:wizards.storageappliancetovpool.confirm.success')
                            );
                        } else {
                            generic.alert(
                                $.t('ovs:wizards.storageappliancetovpool.confirm.complete'),
                                $.t('ovs:wizards.storageappliancetovpool.confirm.somefailed')
                            );
                        }
                    })
                    .fail(function() {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.storageappliancetovpool.confirm.allfailed')
                        );
                    })
                    .always(function() {
                        if (self.data.completed !== undefined) {
                            self.data.completed.resolve(true);
                        }
                    });
                generic.alertInfo($.t('ovs:wizards.storageappliancetovpool.confirm.started'), $.t('ovs:wizards.storageappliancetovpool.confirm.inprogress'));
                deferred.resolve();
            }).promise();
        };

        // Durandal
        self.activate = function() {
            self.data.vPool().load('vsrs', { skipDisks: true })
                .then(function() {
                    self.vsr(new VolumeStorageRouter(self.data.vPool().vSRGuids()[0]));
                    api.get('volumestoragerouters/' + self.vsr().guid())
                        .done(function(vsrData) {
                            self.vsr().fillData(vsrData);
                            self.vSRLoading.resolve();
                        })
                        .fail(self.vSRLoading.reject);
                });
            self.refresher.init(function() {
                $.each(self.data.pendingStorageAppliances(), function(index, storageAppliance) {
                    self.addValidations()[storageAppliance.guid()].validate();
                });
                $.each(self.data.removingStorageAppliances(), function(index, storageAppliance) {
                    self.removeValidations()[storageAppliance.guid()].validate();
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
