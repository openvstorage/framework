// Copyright 2014 iNuron NV
//
// Licensed under the Open vStorage Modified Apache License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.openvstorage.org/license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*global define */
define(['knockout', 'jquery'], function(ko, $){
    "use strict";
    var singleton = function() {
        var data = {
            partition:     ko.observable(),
            disk:          ko.observable(),
            storageRouter: ko.observable(),
            roles:         ko.observableArray([]),
            currentUsage:  ko.observable()
        };
        data.availableRoles = ko.computed(function() {
            if (data.currentUsage() === undefined) {
                return [];
            }
            var db = {name: 'db'},
                read = {name: 'read'},
                write = {name: 'write'},
                scrub = {name: 'scrub'},
                roles = [db, read, write, scrub],
                hide_db = false,
                hide_scrub = false,
                dictionary = {DB: db, READ: read, WRITE: write, SCRUB: scrub};
            $.each(data.currentUsage(), function(role, partitions) {
                if (role !== 'BACKEND') {
                    if (partitions.length > 0) {
                        if (role === 'DB') { hide_db = true; }
                        if (role === 'SCRUB') { hide_scrub = true; }
                    }
                    $.each(partitions, function (index, partition) {
                        if (partition.guid === data.partition().guid())  {
                            if (partition.in_use === true) {
                                dictionary[role].disabled = true;
                            }
                            if (partition.in_use === false) {
                                dictionary[role].disabled = false;
                                if (role === 'DB') { hide_db = false; }
                                if (role === 'SCRUB') { hide_scrub = false; }
                            }
                        }
                    });
                }
            });
            if (hide_db === true) { dictionary['DB'].disabled = true; }
            if (hide_scrub === true) { dictionary['SCRUB'].disabled = true; }
            return roles
        });
        return data;
    };
    return singleton();
});
