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
    function BaseModel()  {
        var self = this;

        // Variables
        self.disposables = [];

        // Functions
        // @Todo cleanup these instanced methods as they do not support prototypical inheritance
        // These methods are currently pointing towards the prototyped function for backwards compatibility
        self.toJSON = function(){
            BaseModel.prototype.toJSON.call(self);
        };
        self.toJS = function() {
            BaseModel.prototype.toJS.call(self)
        };
        self.update = function(data) {
            BaseModel.prototype.update.call(self, data)
        }
    }
    BaseModel.prototype = {
        /**
         * Disposes all possible subscriptions (Events/subscriptions/...)
         * Ideally called in the deactivator
         */
        dispose: function() {
            $.each(this.disposables, this.disposeOne);  // Remove the registered disposables
            $.each(this, this.disposeOne);  // Loop over all properties and check if they are disposable, if so, dispose
        },
        // little helper that handles being given a value or prop + value
        /**
         *
         * @param propOrValue: Property key or value (key in case of an object, value in case of array)
         * @param value: Value (Provided in case this function was passed as a callback to an object loop)
         */
        disposeOne: function(propOrValue, value) {
            var disposable = value || propOrValue;
            if (disposable && typeof disposable.dispose === "function") {
                disposable.dispose();
            }
        },
        /**
         * Return a JSON representation of this object
         * Will respect the mapping applied to the viewModel
         * @return {string|*}
         */
        toJSON: function () {
            return ko.toJSON(this.toJS())
        },
        /**
         * Return a javascript Object from this object
         * Will respect the mapping applied to the viewModel
         * @return {object}
         */
        toJS: function() {
            return ko.mapping.toJS(this)
        },
        /**
         * Update the current view model with the supplied data
         * @param data: Data to update on this view model (keys map with the observables)
         * @type data: Object
         */
        update: function(data) {
            ko.mapping.fromJS(data, this)
        }
    };
    return BaseModel;
});
