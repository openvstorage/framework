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
define(['jquery'
], function ($) {
    "use strict";

    function ViewCache() {
        var self = this;
        self.cache = {};
    }

    var functions = {
        get_cached_page: function(plugin_name, guid){
            var self = this;
            if (self.cache.hasOwnProperty(plugin_name) && self.cache[plugin_name].hasOwnProperty(guid)){
                return self.cache[plugin_name][guid]
            }
            return null
        },
        put_cached_page: function(plugin_name, guid, page){
            var self = this;
            if (!self.cache.hasOwnProperty(plugin_name)){
                self.cache[plugin_name] = {}
            }
            self.cache[plugin_name][guid] = page
        }
    };

    ViewCache.prototype = $.extend({}, functions);
        return new ViewCache();
});