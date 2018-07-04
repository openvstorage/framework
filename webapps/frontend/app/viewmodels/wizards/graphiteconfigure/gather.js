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
    'jquery', 'knockout', './data'
], function($, ko, data) {
    "use strict";
    return function() {
        var self = this;

        // Variables
        self.data = data;

        // Computed
        self.canContinue = ko.computed(function() {
            var configValid = self.data.origConfig.validate();
            return {value: configValid.reasons.length === 0, reasons: configValid.reasons, fields: configValid.fields};
        });

        // Functions
        self.finish = function() {
            return $.when().then(function () {
                var origConfig = self.data.origConfig.toJS();
                self.data.newConfig.update(origConfig);  // Passed by reference from main support page
            })
        };
    };
});
