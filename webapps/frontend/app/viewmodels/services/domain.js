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
/**
 * Service to help with iscsinode related tasks
 */
define([
    'jquery', 'knockout',
    'ovs/api', 'ovs/shared'
], function ($, ko,
             api, shared) {
    function DomainService() {
        var self = this;

        self.shared = shared;

        // Functions
        /**
         * Loads in all domains for the currently supplied data
         * @param queryparams: Additional query params.
         * @returns {Deferred}
         */
        self.loadDomains = function (queryparams) {
            return api.get('domains', {queryparams: queryparams})
        };
    }
    return new DomainService();

});
