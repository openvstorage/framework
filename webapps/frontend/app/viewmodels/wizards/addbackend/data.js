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
define(['knockout'], function(ko){
    "use strict";
    var nameRegex, singleton;
    nameRegex = /^[0-9a-z][\-a-z0-9]{1,48}[a-z0-9]$/;
    singleton = function() {
        return {
            backends:                ko.observableArray([]),
            backendType:             ko.observable(),
            backendTypes:            ko.observableArray([]),
            name:                    ko.observable().extend({ regex: nameRegex }),
            storageRoutersChecked:   ko.observable(false),
            validStorageRouterFound: ko.observable()
        };
    };
    return singleton();
});
