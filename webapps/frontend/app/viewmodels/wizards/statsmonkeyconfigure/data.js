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
    'knockout'
], function(ko){
    "use strict";
        return function(newConfig, origConfig){
            var self = this;

            self.newConfig = newConfig;
            self.origConfig = origConfig;

            self.resetTransport = ko.computed(function() {
                var transport = self.origConfig.transport();
                if (transport === 'graphite') {
                    // Reset the Database name
                    self.origConfig.database('openvstorage.fwk')
                }
            })
    };
});
