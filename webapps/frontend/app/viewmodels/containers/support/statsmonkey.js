// Copyright (C) 2017 iNuron NV
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
    'jquery', 'knockout',
    'ovs/generic',
    'viewmodels/containers/shared/base_container'
], function($, ko,
            generic,
            BaseContainer) {
    "use strict";

     function StatsMonkey(data) {
        var self = this;

        self.host        = ko.observable().extend({regex: generic.ipRegex});
        self.port        = ko.observable(2003).extend({numeric: {min: 1035, max: 65535}});
        self.database    = ko.observable('openvstorage.fwk').extend({removeWhiteSpaces: null});
        self.username    = ko.observable().extend({removeWhiteSpaces: null});
        self.password    = ko.observable().extend({removeWhiteSpaces: null});
        self.interval    = ko.observable(1).extend({numeric: {min: 1, max: 86400}});
        self.transport   = ko.observable();
        self.transports  = ko.observableArray(['influxdb', 'redis','graphite']);
        self.environment = ko.observable().extend({removeWhiteSpaces: null});

        BaseContainer.call(this);

        // Default data: required to set the mappedProperties for ko.mapping
        var vmData = $.extend({
            host: undefined,
            port: 1,
            database: undefined,
            username: undefined,
            password: undefined,
            interval: 60,
            transport: undefined,
            environment: undefined
        }, data);

        // Bind the data into self
        ko.mapping.fromJS(vmData, {}, self);

        // Computed
        self.isInitialized = ko.computed(function() {
            return self.host() !== undefined && self.host.valid();
        });

        // Functions
        self.validate = function() {
            var fields = [];
            var reasons = [];
            if (self.host() === undefined || self.host() === '') {
                fields.push('host');
                reasons.push($.t('ovs:wizards.stats_monkey_configure.host_required'));
            }
            else if (!self.host.valid()) {
                fields.push('host');
                reasons.push($.t('ovs:wizards.stats_monkey_configure.host_invalid'));
            }
            if (self.database() === undefined || self.database() === '') {
                fields.push('database');
                reasons.push($.t('ovs:wizards.stats_monkey_configure.database_required'));
            }
            if (self.environment() === undefined || self.environment() === '') {
                fields.push('environment');
                reasons.push($.t('ovs:wizards.stats_monkey_configure.environment_required'));
            }
            if ((self.username() === undefined || self.username() === '') && self.transport() === 'influxdb') {
                fields.push('username');
                reasons.push($.t('ovs:wizards.stats_monkey_configure.username_required'));
            }
            if ((self.password() === undefined || self.password() === '') && (self.transport() === 'influxdb' ||  self.transport() === 'redis')) {
                fields.push('password');
                reasons.push($.t('ovs:wizards.stats_monkey_configure.password_required'));
            }
            return { value: reasons.length === 0, reasons: reasons, fields: fields };
        }
    }
    StatsMonkey.prototype = $.extend({}, BaseContainer.prototype);
    return StatsMonkey
});
