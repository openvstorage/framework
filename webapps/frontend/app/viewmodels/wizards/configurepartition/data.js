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
                dtl = {name: 'dtl'},
                write = {name: 'write'},
                scrub = {name: 'scrub'},
                roles = [db, dtl, scrub, write],
                hide_db = false,
                hide_dtl = false,
                dictionary = {DB: db, DTL: dtl, WRITE: write, SCRUB: scrub};
            $.each(data.currentUsage(), function(role, partitions) {
                if (role !== 'BACKEND') {
                    if (partitions.length > 0) {
                        if (role === 'DB') { hide_db = true; }
                        else if (role === 'DTL') { hide_dtl = true; }
                    }
                    $.each(partitions, function (index, partition) {
                        if (partition.guid === data.partition().guid())  {
                            if (partition.in_use === true) {
                                dictionary[role].disabled = true;
                            }
                            if (partition.in_use === false) {
                                dictionary[role].disabled = false;
                                if (role === 'DB') { hide_db = false; }
                                else if (role === 'DTL') { hide_dtl = false; }
                            }
                        }
                    });
                }
            });
            if (hide_db === true) { dictionary['DB'].disabled = true; }
            if (hide_dtl === true) { dictionary['DTL'].disabled = true; }
            return roles
        });
        return data;
    };
    return singleton();
});
