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
    '../build', './gather', './data'
], function($, generic, build, Gather, data) {
    "use strict";
    return function(options) {
        var self = this;
        build(self);

        var pmachinemap = {};
        $.each(options.pmachines, function(index, pmachine) {
            pmachinemap[pmachine.guid()] = pmachine;
        });

        var mgmtcenters = [undefined];
        $.each(options.mgmtcenters, function(index, mgmtcenter) {
            mgmtcenters.push(mgmtcenter);
        });

        // Variables
        self.data = data;
        self.data.pmachinemap(pmachinemap);
        self.data.pmachines(options.pmachines);
        self.data.mgmtcenters(mgmtcenters);

        // Setup
        self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.linkhosts.title')));
        self.modal(generic.tryGet(options, 'modal', false));
        self.steps([new Gather()]);
        self.activateStep();
        self.data.configure(true);
    };
});
