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
/*global define, window */
define([
    'jquery', 'ovs/shared', 'ovs/generic'
], function($, shared, generic) {
    'use strict';

    /**
     * Set of helpers related to promises
     * @constructor
     */
    function GenericPromise() {}

    var functions = {
        /**
         * Retries a promise at most maxRetries times
         * @param otherArgs: Arguem
         * @param maxRetries
         * @param promise
         */
        retryPromiseAtMost: function() {
            throw new Error('Needs to be implemented')
        }
    };
    GenericPromise.prototype = functions;
    return new GenericPromise()
});
