// Copyright 2015 iNuron NV
//
// Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/OVS_NON_COMMERCIAL
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'knockout',
    'ovs/generic'
], function(ko, generic) {
    "use strict";
    return function(guid) {
        var self = this;

        // Observables
        self.guid       = ko.observable(guid);
        self.name       = ko.observable();
        self.component  = ko.observable();
        self.validUntil = ko.observable();
        self.data       = ko.observable();
        self.signature  = ko.observable();
        self.canRemove  = ko.observable();
        self.token      = ko.observable();
        self.loading    = ko.observable(false);

        // Functions
        self.fillData = function(data) {
            self.name(data.name);
            self.component(data.component);
            self.data(data.data);
            self.token(data.token);
            self.validUntil(data.valid_until);
            self.signature(data.signature);
            generic.trySet(self.canRemove, data, 'can_remove');
            self.loading(false);
        };
    };
});
