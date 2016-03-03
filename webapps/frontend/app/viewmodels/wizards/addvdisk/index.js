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
define([
    'jquery', 'ovs/generic',
    '../build', './data', './gather'
], function($, generic, build, data, Gather) {
    "use strict";
    return function(options) {
        var self = this;
        build(self);

        // Variables
        self.data = data;

        // Setup
        self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.add_vdisk.title')));
        self.modal(generic.tryGet(options, 'modal', false));

        self.steps([new Gather()]);
        self.activateStep();

        // Cleaning data
        data.name('');
        data.size_entry(1);
        data.size_unit('gib');
        data.storageRouters([]);
    };
});
