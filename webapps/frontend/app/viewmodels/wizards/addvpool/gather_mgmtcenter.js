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
    '../../containers/storagerouter', '../../containers/storagedriver', './data'
], function ($, ko, shared, api, generic, StorageRouter, StorageDriver, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.shared = shared;
        self.data   = data;

        // Computed
        self.canContinue = ko.computed(function () {
            return { value: self.data.mgmtcenterLoaded(), showErrors: false, reasons: [], fields: [] };
        });

        // Functions
        self.next = function() {
            return true;
        };

        // Durandal
        self.activate = function() {
            self.getMgmtCenterInfo = api.get('storagerouters/' + self.data.target().guid() + '/get_mgmtcenter_info')
                .done(function (data) {
                    if (data.username) {
                        self.data.hasMgmtCenter(true);
                        self.data.integratemgmt(true);
                        self.data.mgmtcenterUser(data.username);
                        self.data.mgmtcenterName(data.name);
                        self.data.mgmtcenterIp(data.ip);
                        self.data.mgmtcenterType(data.type);
                     } else {
                        self.data.hasMgmtCenter(false);
                        self.data.integratemgmt(false);
                     }
                })
                .fail(function() {
                    self.data.hasMgmtCenter(false);
                    self.data.integratemgmt(false);
                })
                .always(function() {
                    self.data.mgmtcenterLoaded(true);
                });
        };
    };
});
