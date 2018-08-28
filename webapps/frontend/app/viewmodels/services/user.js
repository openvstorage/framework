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
/**
 * Service to help with backend related tasks
 */
define([
    'jquery', 'knockout',
    'ovs/api'
], function ($, ko, api) {

    function UserService() {
        var self = this;

        // Variables
        self.cacheTypes = ko.observableArray(['fragment_cache', 'block_cache']);

        // Functions
        /**
         * Loads in all vpools for the current supplied data
         * @param queryParams: Additional query params. Defaults to no params
         * @returns {Deferred}
         */
        self.fetchUser = function(guid) {
            return api.get('users/' + guid);
        };

    }
    return new UserService();
});