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

    function FilterItem(prop, value) {
        this.prop = prop;
        this.value = value;
    }
    FilterItem.prototype = {
        evaluate: function(object) {
            return object[this.prop] === this.value
        }
    };

    var genericID = 'generic_{0}'.format(Math.random());
    function ViewCache() {
        var self = this;
        self.cache = {};
    }
    // Private functions
    /**
     * Filter views based on FilterItems
     * Any number of FilterItems can be passed
     * @param instanceID: Identifier of the page activator
     * @return {Array}
     */
    function get_by(instanceID) {
        var self = this;
        // Identical to the REST operator function(...args)
        var args = Array.prototype.slice.call(arguments, get_by.length);

        return Object.keys(self.cache).reduce(function(acc, cur){
            // Acc is the list passed. cur is the key of the object
            acc = acc.concat(self.get(cur, instanceID).filter(function(item) {
                // Only AND is currently supported
                return args.every(function(filterItem) {
                    return filterItem.evaluate(item)
                })
            }));
            return acc
        }, [])
    }
    var functions = {
        /**
         * Retrieve all cached pages
         * @param plugin_name: Name of the plugin to retrieve pages for
         * @param instanceID: Identifier for the page activator (generic one if not given)
         * @return {Array}
         */
        get: function(plugin_name, instanceID){
            instanceID = instanceID || genericID;
            var self = this;
            if (self.cache.hasOwnProperty(plugin_name) && self.cache[plugin_name].hasOwnProperty(instanceID)){
                return self.cache[plugin_name][instanceID]
            }
            return []
        },
        /**
         * Get all page-type views with the given pageID
         * @param pageID: Page identifier
         * @param instanceID: Identifier for the view activator
         * @return {Array}
         */
        get_by_page: function(pageID, instanceID) {
            var filterItems = [new FilterItem('page', pageID), new FilterItem('type', 'page')];
            var args = [instanceID].concat(filterItems);
            // Mimick spread operator get_by(instanceID, ...filterItems)
            return get_by.apply(this, args)
        },
        /**
         * Get all wizard-type views
         * @param instanceID: Identifier for the view activator
         * @return {Array}
         */
        get_wizards: function(instanceID) {
            var filterItems = [new FilterItem('type', 'wizard')];
            var args = [instanceID].concat(filterItems);
            return get_by.apply(this, args)
        },
        /**
         * Store a new item in the cache
         * @param plugin_name: Name of the plugin to store page for
         * @param view: Object containing view information
         * Example: {module: moduleInstance, module_constructor: module, activator: activator.create(), type: 'dashboard'};
         * @param guid: Identifier for the page activator
         */
        put: function(plugin_name, view, guid){
            guid = guid || genericID;
            if (!this.cache.hasOwnProperty(plugin_name)){
                this.cache[plugin_name] = {}
            }
            if (!this.cache[plugin_name].hasOwnProperty(guid)){
                this.cache[plugin_name][guid] = []
            }
            this.cache[plugin_name][guid].push(view);
        }
    };

    ViewCache.prototype = $.extend({}, functions);
    return new ViewCache();
});