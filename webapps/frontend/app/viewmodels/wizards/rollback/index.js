// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'ovs/generic',
    '../build', './gather', './data'
], function($, generic, build, Gather, data) {
    "use strict";
    return function(options) {
        var self = this;

        self.data = data;
        build(self);

        self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.rollback.title')));
        self.modal(generic.tryGet(options, 'modal', false));

        self.data.machineGuid(options.machineguid);
        self.steps([new Gather()]);

        self.activateStep();
    };
});
