// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'knockout',
    'ovs/shared'
], function(ko, shared) {
    "use strict";
    return function() {
        var self = this;

        // System
        self.shared = shared;
    };
});