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
define(['jquery', 'knockout', 'ovs/shared', 'ovs/generic', 'ovs/api', '../containers/domain'],
function($, ko, shared, generic, api, Domain) {
    "use strict";
    return function () {
        var self = this;

        // Variables
        self.widgets       = [];
        self.shared        = shared;
        self.guard         = { authenticated: true };
        self.domainHeaders = [{key: 'name', value: $.t('ovs:generic.name'), width: undefined}];

        // Handles
        self.domainsHandle = {};

        // Functions
        self.loadDomains = function(options) {
            return $.Deferred(function(deferred) {
                if (generic.xhrCompleted(self.domainsHandle[options.page])) {
                    options.sort = 'name';
                    options.contents = '';
                    self.domainsHandle[options.page] = api.get('domains', { queryparams: options })
                        .done(function(data) {
                            deferred.resolve({
                                data: data,
                                loader: function(guid) {
                                    return new Domain(guid);
                                }
                            });
                        })
                        .fail(function() { deferred.reject(); });
                } else {
                    deferred.resolve();
                }
            }).promise();
        };

        // Durandal
        self.deactivate = function() {
            $.each(self.widgets, function(i, item) {
                item.deactivate();
            });
        };
    };
});
