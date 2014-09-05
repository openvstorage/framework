// Copyright 2014 CloudFounders NV
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define([
    'jquery', 'knockout',
    'ovs/generic', 'ovs/api'
], function($, ko, generic, api) {
    "use strict";
    return function(guid) {
        var self = this;

        // Handles
        self.loadHandle = undefined;

        // Observables
        self.loading         = ko.observable(false);
        self.loaded          = ko.observable(false);
        self.guid            = ko.observable(guid);
        self.name            = ko.observable();
        self.description     = ko.observable();
        self.groupJunctions  = ko.observableArray([]);
        self.clientJunctions = ko.observableArray([]);

        // Functions
        self.fillData = function(data) {
            self.name(data.name);
            self.description(data.description);
            generic.trySet(self.groupJunctions, data, 'groups_guids');
            generic.trySet(self.clientJunctions, data, 'clients_guids');

            self.loaded(true);
            self.loading(false);
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                if (generic.xhrCompleted(self.loadHandle)) {
                    self.loadHandle = api.get('clients/' + self.guid())
                        .done(function(data) {
                            self.fillData(data);
                            deferred.resolve();
                        })
                        .fail(deferred.reject)
                        .always(function() {
                            self.loading(false);
                        });
                } else {
                    deferred.reject();
                }
            }).promise();
        };
    };
});
