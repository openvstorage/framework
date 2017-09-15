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
    'ovs/api', 'ovs/generic', 'ovs/shared',
    './data'
], function($, ko, api, generic, shared, data) {
    "use strict";
    return function(options) {
        var self = this;

        // Variables
        self.data = options !== undefined && options.data !== undefined ? options.data : data;
        self.shared = shared;
        self.options = options;

        self.useFragmentCacheBackend    = ko.obserable(false);
        self.fragmentCacheSettings      = ko.observableArray(['write', 'read', 'rw', 'none']);
        
        // Computed
        self.canContinue = ko.computed(function () {
            var reasons = [], fields = [];
            return { value: reasons.length === 0, reasons: reasons, fields: fields };
        });

        self.useFragmentCache = ko.computed(function(){
            if (self.data.fragmentCache() === undefined){
                return undefined
            }
            return !!(self.data.fragmentCache().read() || self.data.fragmentCache().write());

        });
        self.fragmentCacheSetting = ko.computed({
            read: function() {
                if (self.data.fragmentCache() === undefined){
                    return undefined
                }
                if (self.data.fragmentCache().read() && self.data.fragmentCache().write()) {
                    return 'rw';
                }
                if (self.data.fragmentCache().read()) {
                    return 'read';
                }
                if (self.data.fragmentCache().write()) {
                    return 'write';
                }
                return 'none';
            },
            write: function(cache) {
                self.data.fragmentCache().read(['rw', 'read'].contains(cache));
                self.data.fragmentCache().write(['rw', 'write'].contains(cache));
                if (cache === 'none') {
                    self.data.useFC(false);
                }
            }
        });
    }     
});
