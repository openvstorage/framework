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
    'jquery', 'knockout', 'ovs/api', './data'
], function ($, ko, api, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data = data;

        // Computed
        self.canContinue = ko.computed(function () {
            return { value: self.data.mgmtcenterLoaded(), showErrors: false, reasons: [], fields: [] };
        });

        // Durandal
        self.activate = function() {
            self.getMgmtCenterInfo = api.get('storagerouters/' + self.data.storageRouter().guid() + '/get_mgmtcenter_info')
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
