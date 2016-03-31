// Copyright 2016 iNuron NV
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
define(['jquery', 'knockout'], function($, ko){
    "use strict";
    var nameRegex, singleton;
    nameRegex = /^[0-9a-z][\-a-z0-9]{1,48}[a-z0-9]$/;

    singleton = function() {
        var wizardData = {
            name:                       ko.observable('').extend({ regex: nameRegex }),
            size_entry:                 ko.observable(0).extend({ numeric: { min: 1, max: 999 } }),
            size_unit:                  ko.observable('gib'),
            size_units:                 ko.observableArray(['gib', 'tib']),
            storageRouter:              ko.observable(),
            storageRouters:             ko.observableArray([]),
            vPool:                      ko.observable(),
            vPools:                     ko.observableArray([])
        };

        // Computed
        wizardData.size = ko.computed(function () {
            var size = wizardData.size_entry();
            if (wizardData.size_unit() === 'tib') {
                size *= 1024;
            }
            return size;
        });

        wizardData.storageRoutersByVpool = ko.computed(function() {
            var guids = [], result = [];
            $.each(wizardData.storageRouters(), function(index, storageRouter) {
                if (wizardData.vPool() !== undefined &&
                    storageRouter.vPoolGuids.contains(wizardData.vPool().guid())) {
                    result.push(storageRouter);
                    guids.push(storageRouter.guid());
                }
            });
            if (result.length > 0 && wizardData.storageRouter() && !guids.contains(wizardData.storageRouter().guid())) {
                wizardData.storageRouter(result[0]);
            }
            return result;
        });

        return wizardData;
    };

    return singleton();
});
