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
    'ovs/api', 'ovs/shared'
], function ($, ko,
             api, shared) {

    function VPoolService() {
        var self = this;

        // Variables
        self.cacheTypes = ko.observableArray(['fragment_cache', 'block_cache']);

        // Functions
        /**
         * Loads in all vpools for the current supplied data
         * @param queryParams: Additional query params. Defaults to no params
         * @param relayParams: Relay to use (Optional, defaults to no relay)
         * @returns {Deferred}
         */
        self.loadVPools = function(queryParams, relayParams) {
            return api.get('vpools', { queryparams: queryParams, relayParams: relayParams })
        };

            /**
         * Load a vPool
         * @param vPoolGuid: guid of the vpool to shrink
         * @param queryParams: Additional query params. Defaults to no params
         * @param relayParams: Relay to use (Optional, defaults to no relay)
         * @return {Promise<T>}
         */
        self.loadVPool = function(vPoolGuid, queryParams, relayParams) {
            return api.get('vpools/{0}'.format([vPoolGuid]), { queryparams: queryParams, relayParams: relayParams })
        };


        /**
         * Shrink given vPool
         * @param vPoolGuid: guid of the vpool to shrink
         * @param storagerouterGuid: guid of the storagerouter to shrink from
         * @return {Promise<T>}
         */
        self.shrinkVPool = function(vPoolGuid, storagerouterGuid) {
            return api.post('vpools/{0}/shrink_vpool'.format([vPoolGuid]), { data: { storagerouter_guid: storagerouterGuid } })
                                            .then(shared.tasks.wait)
        };

    }
    return new VPoolService();
});
