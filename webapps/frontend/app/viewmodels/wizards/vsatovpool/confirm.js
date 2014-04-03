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
        self.Validation = function(vsr, vsa, vsrDeferred, data) {
            var self = this;

            // Variables
            self.vsa         = vsa;
            self.vsr         = vsr;
            self.shared      = shared;
            self.vsrDeferred = vsrDeferred;
            self.data        = data;

            // Observables
            self.loaded      = ko.observable(false);
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
                    reasons.push($.t('ovs:wizards.vsatovpool.confirm.errorvalidating'));
                } else {
                    $.each(self.vsrs(), function(index, vsr) {
                        if (self.vsr().mountpointCache() === vsr.mountpointCache() && $.inArray('cache', fields) === -1) {
                            valid = false;
                            fields.push('cache');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.cachefs') }));
                        }
                        if (self.vsr().mountpointDFS() === vsr.mountpointDFS() && $.inArray('dfs', fields) === -1) {
                            valid = false;
                            fields.push('dfs');
                            reasons.push($.t('ovs:wizards.addvpool.gathermountpoints.mtptinuse', { what: $.t('ovs:generic.dfs') }));
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
                        reasons.push($.t('ovs:wizards.vsatovpool.confirm.ipnotavailable', { what: self.vsr().storageIP() }));
                    }
                }
                return { valid: valid, reasons: reasons };
            });

            // Functions
            self.validate = function() {
                var calls = [
                    $.Deferred(function(physicalDeferred) {
                        api.get('vmachines/' + self.vsa.guid() + '/get_physical_metadata')
                            .then(self.shared.tasks.wait)
                            .then(function(data) {
                                self.mountpoints(data.mountpoints);
                                self.ipAddresses(data.ipaddresses);
                                self.vRouterPort(data.xmlrpcport);
                            })
                            .done(physicalDeferred.resolve)
                            .fail(physicalDeferred.reject);
                    }).promise(),
                    self.vsrDeferred.promise()
                ];
                generic.crossFiller(
                    self.vsa.servedVSRGuids, self.vsrs,
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
        self.data = data;

        // Computed
        self.validations = ko.computed(function() {
            var validations = {};
            $.each(self.data.vSAs(), function(index, vsa) {
                validations[vsa.guid()] = new self.Validation(self.vsr, vsa, self.vSRLoading, self.data);
                validations[vsa.guid()].validate();
            });
            return validations;
        });
        self.canContinue = ko.computed(function() {
            var valid = true, hasValid = false;
            $.each(self.validations(), function(index, validation) {
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
                var vsaGuids = [];
                $.each(self.validations(), function(vsaGuid, validation) {
                    if (validation.validationState().valid === true) {
                        vsaGuids.push(vsaGuid);
                    }
                });
                api.post('vpools/' + self.data.vPool().guid() + '/vsas_to_vpool', {
                    vsr_guid: self.vsr().guid(),
                    vsa_guids: vsaGuids.join(',')
                })
                    .then(self.shared.tasks.wait)
                    .done(function(data) {
                        if (data === true) {
                            generic.alertSuccess(
                                $.t('ovs:wizards.vsatovpool.confirm.complete'),
                                $.t('ovs:wizards.vsatovpool.confirm.success')
                            );
                        } else {
                            generic.alert(
                                $.t('ovs:wizards.vsatovpool.confirm.complete'),
                                $.t('ovs:wizards.vsatovpool.confirm.somefailed')
                            );
                        }
                    })
                    .fail(function() {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:wizards.vsatovpool.confirm.allfailed')
                        );
                    })
                    .always(function() {
                        if (self.data.completed !== undefined) {
                            self.data.completed.resolve(true);
                        }
                    });
                generic.alertInfo($.t('ovs:wizards.vsatovpool.confirm.started'), $.t('ovs:wizards.vsatovpool.confirm.inprogress'));
                deferred.resolve();
            }).promise();
        };

        // Durandal
        self.activate = function() {
            self.vsr(new VolumeStorageRouter(self.data.vPool().vSRGuids()[0]));
            api.get('volumestoragerouters/' + self.vsr().guid())
                .done(function(vsrData) {
                    self.vsr().fillData(vsrData);
                    self.vSRLoading.resolve();
                })
                .fail(self.vSRLoading.reject);
            self.refresher.init(function() {
                $.each(self.data.vSAs(), function(index, vsa) {
                    self.validations()[vsa.guid()].validate();
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
