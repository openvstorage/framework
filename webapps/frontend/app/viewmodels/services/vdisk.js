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
 * Service to help with vdisk related tasks
 */
define([
    'jquery', 'knockout',
    'ovs/api', 'ovs/shared'
], function ($, ko,
             api, shared) {
    function VDiskService() {
        var self = this;

        self.shared = shared;

        /**
         * Loads the vdisk
         * @param guid: guid of the vdisk
         * @returns {Deferred}
         */
        self.loadVDisk = function (guid) {
            return api.get('vdisks/' + guid)
        };
        /**
         * Loads the config params of the vdisk
         * @param guid: Additional query params.
         * @returns {Deferred}
         */
        self.loadConfigHandle = function (guid) {
            return api.get('vdisks/' + guid + '/get_config_params')
                .then(self.shared.tasks.wait)

        };
    }
    return new VDiskService();

});
