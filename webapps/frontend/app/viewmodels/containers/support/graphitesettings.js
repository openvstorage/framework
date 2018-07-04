// Copyright (C) 2018 iNuron NV
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

     function GraphiteSettings(data) {
        var self = this;

        self.host = ko.observable().extend({regex: generic.ipRegex});
        self.port = ko.observable(2003).extend({numeric: {min: 1035, max: 65535}});


        BaseContainer.call(this);

        // Default data: required to set the mappedProperties for ko.mapping
        var vmData = $.extend({
            host: undefined,
            port: 2003
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
                reasons.push($.t('ovs:wizards.graphite.host_required'));
            }
            else if (!self.host.valid()) {
                fields.push('host');
                reasons.push($.t('ovs:wizards.stats_monkey_configure.host_invalid'));
            }
            return { value: reasons.length === 0, reasons: reasons, fields: fields };
        }
    }
    GraphiteSettings.prototype = $.extend({}, BaseContainer.prototype);
    return GraphiteSettings
});
