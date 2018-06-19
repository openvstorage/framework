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

    var genericID = 'generic_{0}'.format([Math.random()]);
    function ViewCache() {
        var self = this;
        self.cache = {};
    }

    var functions = {
        get: function(plugin_name, guid){
            guid = guid || genericID;
            var self = this;
            if (self.cache.hasOwnProperty(plugin_name) && self.cache[plugin_name].hasOwnProperty(guid)){
                return self.cache[plugin_name][guid]
            }
            return []
        },
        get_by_page: function(page, guid) {
            var self = this;
            return Object.keys(self.cache).reduce(function(acc, cur){
                // Acc is the list passed. cur is the key of the object
                acc = acc.concat(self.get(cur, guid).filter(function(item) {
                    return item.page === page
                }));
                return acc
            }, [])
        },
        put: function(plugin_name, page, guid){
            guid = guid || genericID;
            var self = this;
            if (!self.cache.hasOwnProperty(plugin_name)){
                self.cache[plugin_name] = {}
            }
            if (!self.cache[plugin_name].hasOwnProperty(guid)){
                self.cache[plugin_name][guid] = []
            }
            self.cache[plugin_name][guid].push(page);
        }
    };

    ViewCache.prototype = $.extend({}, functions);
    return new ViewCache();
});