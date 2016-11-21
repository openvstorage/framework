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
    'jquery', 'knockout', 'ovs/generic', './data'
], function ($, ko, generic, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data = data;

        // Computed
        self.dtlMode = ko.computed({
            write: function(mode) {
                if (mode.name === 'no_sync') {
                    self.data.dtlEnabled(false);
                } else {
                    self.data.dtlEnabled(true);
                }
                self.data.dtlMode(mode);
            },
            read: function() {
                if (self.data.vPool() !== undefined && self.data.dtlEnabled() === false) {
                    return {name: 'no_sync', disabled: false};
                }
                return self.data.dtlMode();
            }
        });
        self.canContinue = ko.computed(function () {
            var reasons = [], fields = [];
            if (self.data.writeBufferGlobal() * 1024 * 1024 * 1024 > self.data.writeBufferGlobalMax()) {
                fields.push('writeBufferGlobal');
                reasons.push($.t('ovs:wizards.add_vpool.gather_config.over_allocation'));
            }
            return { value: reasons.length === 0, reasons: reasons, fields: fields };
        });
    };
});
