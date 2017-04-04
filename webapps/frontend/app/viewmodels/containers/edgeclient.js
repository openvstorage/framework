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
    'jquery', 'knockout'
], function($, ko) {
    "use strict";
    return function (key) {
        var self = this;

        // Handles
        self.loadHandle = undefined;

        // External dependencies
        self.vDisk = ko.observable();

        // Observables
        self.clientIp   = ko.observable();
        self.clientPort = ko.observable();
        self.loading    = ko.observable(false);
        self.loaded     = ko.observable(false);
        self.key        = ko.observable(key);
        self.objectId   = ko.observable();
        self.serverIp   = ko.observable();
        self.serverPort = ko.observable();

        // Functions
        self.fillData = function (data) {
            self.objectId(data.object_id);
            self.clientIp(data.ip);
            self.clientPort(data.port);
            self.serverIp(data.server_ip);
            self.serverPort(data.server_port);
            self.loaded(true);
            self.loading(false);
        };
    }
});
