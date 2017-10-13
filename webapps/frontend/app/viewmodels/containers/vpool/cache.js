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
    'ovs/generic',
    './shared/backend_info'
], function($, ko, generic, backendInfoViewModel) {
    "use strict";
    // Caching data viewModel which is parsed from JS
    // Return a constructor for a nested viewModel
    var cacheMapping = {
        'fragment_cache': {
            create: function (options) {
                if (options.data !== null) return new cacheTypeViewModel(options.data);
            }
        },
        'block_cache': {
            create: function (options) {
                if (options.data !== null) return new cacheTypeViewModel(options.data);
            }
        }
    };
    var cacheTypeMapping = {
        'backend_info': {
            create: function (options) {
                if (options.data !== null) return new backendInfoViewModel(options.data);
            }
        }
    };
    var cacheViewModel = function(data) {
        var self = this;

        // Default data
        var vmData = $.extend({
            fragment_cache: {},
            block_cache: {}
        }, data);

        ko.mapping.fromJS(vmData, cacheMapping, self)  // Bind the data into this
    };
    var cacheTypeViewModel = function(data) {
        var self = this;
        // Observables
        self.cacheSettings      = ko.observableArray(['write', 'read', 'rw', 'none']);

        // Default data
        var vmData = $.extend({
            read: false,
            write: false,
            is_backend: false,
            quota: undefined,
            backend_info: {}
        }, data);

        ko.mapping.fromJS(vmData, cacheTypeMapping, self);

        // Computed
        self.isUsed = ko.pureComputed(function() {
           return self.cacheSetting() !== 'none';

        });
        self.cacheSetting = ko.computed({
            read: function() {
                if (self.read() && self.write()) {
                    return 'rw';
                }
                if (self.read()) {
                    return 'read';
                }
                if (self.write()) {
                    return 'write';
                }
                return 'none';
            },
            write: function(cache) {
                self.read(['rw', 'read'].contains(cache));
                self.write(['rw', 'write'].contains(cache));
            }
        });

    };
    return cacheViewModel;
});
