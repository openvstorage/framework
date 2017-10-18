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
 * Service to help with StorageDriver related tasks
 */
define([
], function() {

    /**
     * Returns a singleton instance of this service (same instance is served throughout the application)
     */
    var StorageDriverService = function(){
        var self = this;

        // Properties
        self.tlogMultiplierMap  = {  // Maps sco size to a tlog_multiplier
            4: 16,
            8: 8,
            16: 4,
            32: 2,
            64: 1,
            128: 1
        };

        // Functions
        /**
         * Calculate the number of scos in tlog and the non disposable scos factor
         * @return {object}
         */
        self.calculateAdvancedFactors = function(sco_size, write_buffer) {
            var numberOfScosInTlog = self.tlogMultiplierMap[sco_size];
            var nonDisposableScoFactor = write_buffer / numberOfScosInTlog / sco_size;
            return {
                number_of_scos_in_tlog: numberOfScosInTlog,
                non_disposable_scos_factor: nonDisposableScoFactor
            }
        };
        /**
         * Calculate the number of scos in tlog and the non disposable scos factor
         * @return {object}
         */
        self.calculateVolumeWriteBuffer = function(numberOfScosInTlog, nonDisposableScoFactor) {
            var volumeWriteBuffer = nonDisposableScoFactor * (numberOfScosInTlog * self.sco_size);
            return {
                volume_write_buffer: volumeWriteBuffer
            }
        };
    };
    return new StorageDriverService()
});