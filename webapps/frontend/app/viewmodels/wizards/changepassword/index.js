// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define([
    'jquery', 'ovs/generic',
    '../build', './confirm'
], function($, generic, build, Confirm) {
    "use strict";
    return function(options) {
        var self = this;

        build(self);

        self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.changepassword.title')));
        self.modal(generic.tryGet(options, 'modal', false));

        self.steps([new Confirm()]);

        self.activateStep();
    };
});