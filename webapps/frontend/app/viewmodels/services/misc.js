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
 * Service to help with StorageDriver related tasks
 */
define([
    'jquery', 'knockout',
    'ovs/api',
], function ($, ko, api) {
    /**
     * Returns a singleton instance of this service (same instance is served throughout the application)
     */
    function MiscService(){
        var self = this;

       /**
         * Fetch the branding info
         * @return {object}
         */
        self.branding = function() {
            return api.get('branding')
        };
        self.metadata = function() {
            return api.get('')
        };
    }
    return new MiscService()
});