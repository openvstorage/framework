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
], function ($, ko, api, shared) {
    function StatisticsService() {
        var self = this;


        // Functions
        /**
         * Fetch the currently saved graphite credentials
         * @returns {Deferred}
         */
        self.graphiteStatistics = function() {
            return api.get('ovs/statistics/graphite');
        };
        /**
         * Generates an empty slot for an AlbaNode
         * @param metaData: host IP and port
         * @return {*|Promise}
         */
        self.postStatistics = function(metaData) {
            return api.post('ovs/statistics/graphite', { data: metaData })
                .then(shared.tasks.wait)
        };
    }
    return new StatisticsService();
});