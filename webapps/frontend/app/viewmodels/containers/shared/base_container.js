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
define([
    'jquery', 'knockout'
], function($, ko) {
    "use strict";
    // Return a constructor for a basic viewModel
    var baseModel = function() {
        var self = this;

        // Functions
        /**
         * Return a JSON representation of this object
         * Will respect the mapping applied to the viewModel
         * @return {string|*}
         */
        self.toJSON = function(){
            return ko.toJSON(self.toJS())
        };
        /**
         * return a javascript Object from this object
         * Will respect the mapping applied to the viewModel
         * @return {object}
         */
        self.toJS = function() {
            return ko.mapping.toJS(self)
        }
    };
    return baseModel;
});
