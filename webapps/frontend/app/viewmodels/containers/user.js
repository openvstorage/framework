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

        // External dependencies
        self.group   = ko.observable();
        self.clients = ko.observableArray([]);

        // Observables
        self.edit        = ko.observable(false);
        self.loading     = ko.observable(false);
        self.loaded      = ko.observable(false);
        self.guid        = ko.observable(guid);
        self.active      = ko.observable();
        self.username    = ko.observable();
        self.groupGuid   = ko.observable();
        self.backupValue = ko.observable();

        // Functions
        self.fillData = function(data) {
            self.username(data.username);
            self.active(data.is_active);
            generic.trySet(self.groupGuid, data, 'group_guid');

            self.loaded(true);
            self.loading(false);
        };
        self.load = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                if (generic.xhrCompleted(self.loadHandle)) {
                    self.loadHandle = api.get('users/' + self.guid())
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
        self.save = function() {
            return $.Deferred(function(deferred) {
                self.loading(true);
                api.patch('users/' + self.guid(), {
                        group_guid: self.groupGuid()
                    }, {
                        contents: '_relations'
                    })
                    .done(function() {
                        generic.alertSuccess(
                            $.t('ovs:users.save.complete'),
                            $.t('ovs:users.save.success', { what: self.username() })
                        );
                        self.loading(false);
                        deferred.resolve();
                    })
                    .fail(function(error) {
                        error = $.parseJSON(error.responseText);
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:users.save.failed', {
                                what: self.username(),
                                why: error.detail
                            })
                        );
                        self.loading(false);
                        deferred.reject();
                    });
            }).promise();
        };
    };
});
