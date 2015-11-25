// Copyright 2015 iNuron NV
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
define(['knockout'], function(ko){
    "use strict";
    var singleton = function() {
        var data = {
            licenseString: ko.observable(),
            licenseInfo:   ko.observable()
        };
        data.licenseEntries = ko.computed(function() {
            var entries = [];
            if (data.licenseInfo() !== undefined) {
                $.each(data.licenseInfo(), function(cname, component) {
                    component.component = cname;
                    entries.push(component);
                });
            }
            return entries;
        });
        return data;
    };
    return singleton();
});
