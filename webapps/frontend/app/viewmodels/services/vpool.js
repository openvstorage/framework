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
    'ovs/api', 'ovs/generic'
], function ($, ko, api, generic) {
    /**
     * Loads in all vpools for the current supplied data
     * @param queryParams: Additional query params. Defaults to no params
     * @param relay: Relay to use (Optional, defaults to no relay)
     * @returns {Deferred}
    */
    function loadVPools(queryParams, relay) {
        relay = (typeof sortFunction !== 'undefined') ? relay: '';
        queryParams = (typeof queryParams !== 'undefined') ? queryParams : {};
        return api.get(relay + 'vpools', { queryparams: queryParams })
    }
    function loadVPool(vPoolGuid, queryParams, relay) {
        relay = (typeof sortFunction !== 'undefined') ? relay: '';
        queryParams = (typeof queryParams !== 'undefined') ? queryParams : {};
        return api.get(relay + 'vpools/{0}'.format([vPoolGuid]), { queryparams: queryParams })
    }
    return {
        loadVPool: loadVPool,
        loadVPools: loadVPools
    }

});