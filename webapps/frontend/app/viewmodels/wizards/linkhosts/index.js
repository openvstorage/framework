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
