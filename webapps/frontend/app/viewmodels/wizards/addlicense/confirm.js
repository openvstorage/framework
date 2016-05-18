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
    'jquery', 'knockout',
    './data',
    'ovs/api', 'ovs/generic', 'ovs/shared'
], function($, ko, data, api, generic, shared) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data    = data;
        self.shared  = shared;
        self.generic = generic;

        // Computed
        self.canContinue = ko.computed(function() {
            return { value: true, reasons: [], fields: [] };
        });

        // Functions
        self.finish = function() {
            return $.Deferred(function(deferred) {
                var postData = {
                    license_string: self.data.licenseString(),
                    validate_only: false
                };
                api.post('licenses', { data: postData })
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        generic.alertSuccess($.t('ovs:generic.saved'), $.t('ovs:wizards.addlicense.confirm.success'));
                    })
                    .fail(function() {
                        generic.alertError($.t('ovs:generic.error'), $.t('ovs:generic.messages.errorwhile', { what: $.t('ovs:wizards.addlicense.confirm.adding') }));
                    });
                generic.alertInfo($.t('ovs:wizards.addlicense.confirm.started'), $.t('ovs:wizards.addlicense.confirm.inprogress'));
                deferred.resolve();
            }).promise();
        };
    };
});
