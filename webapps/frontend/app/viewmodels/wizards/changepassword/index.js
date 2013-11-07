/*global define */
define([
    'jquery', 'ovs/generic',
    '../build', './confirm'
], function($, generic, build, Confirm) {
    "use strict";
    return function(options) {
        var self = this;

        build(self);

        self.title(generic.tryGet(options, 'title', 'Change password'));
        self.modal(generic.tryGet(options, 'modal', false));

        self.steps([new Confirm()]);

        self.activateStep();
    };
});