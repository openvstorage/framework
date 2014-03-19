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
/*global define, window */
define([
    'jquery', 'knockout',
    'ovs/shared', 'ovs/api', 'ovs/generic',
    '../../containers/vmachine', '../../containers/volumestoragerouter', './data'
], function($, ko, shared, api, generic, VMachine, VolumeStorageRouter, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared           = shared;
        self.data             = data;
        self.loadVSASHandle   = undefined;
        self.loadVSAHandle    = undefined;
        self.loadVSRsHandle   = {};

        // Computed
        self.canContinue = ko.computed(function() {
            var valid = true, reasons = [], fields = [];
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
            return { value: valid, reasons: reasons, fields: fields };
        });

        // Functions
        self.next = function() {
            return $.Deferred(function(deferred) {
                var calls = [
                    $.Deferred(function(mtptDeferred) {
                        generic.xhrAbort(self.loadVSAHandle);
                        self.loadVSAHandle = api.get('vmachines/' + self.data.target().guid() + '/get_physical_metadata')
                            .then(self.shared.tasks.wait)
                            .then(function(data) {
                                self.data.mountpoints(data.mountpoints);
                                self.data.ipAddresses(data.ipaddresses);
                                self.data.vRouterPort(data.xmlrpcport);
                            })
                            .done(function() {
                                mtptDeferred.resolve();
                            })
                            .fail(mtptDeferred.reject);
                    }).promise()
                ];
                generic.crossFiller(
                    self.data.target().servedVSRGuids, self.data.vsrs,
                    function(guid) {
                        var vsr = new VolumeStorageRouter(guid);
                        calls.push($.Deferred(function(deferred) {
                            generic.xhrAbort(self.loadVSRsHandle[guid]);
                            self.loadVSAHandle[guid] = api.get('volumestoragerouters/' + guid)
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
                    .done(deferred.resolve)
                    .fail(deferred.reject);
            });
        };

        // Durandal
        self.activate = function() {
            generic.xhrAbort(self.loadVSASHandle);
            var query = {
                query: {
                    type: 'AND',
                    items: [['is_internal', 'EQUALS', true]]
                }
            };
            self.loadVSASHandle = api.post('vmachines/filter', query, {
                full: true,
                contents: 'served_vsrs',
                sort: 'name'
            })
                .done(function(data) {
                    var guids = [], vmdata = {};
                    $.each(data, function(index, item) {
                        guids.push(item.guid);
                        vmdata[item.guid] = item;
                    });
                    generic.crossFiller(
                        guids, self.data.vsas,
                        function(guid) {
                            return new VMachine(guid);
                        }, 'guid'
                    );
                    $.each(self.data.vsas(), function(index, vmachine) {
                        vmachine.fillData(vmdata[vmachine.guid()]);
                    });
                    if (self.data.target() === undefined && self.data.vsas().length > 0) {
                        self.data.target(self.data.vsas()[0]);
                    }
                });
        };
    };
});
