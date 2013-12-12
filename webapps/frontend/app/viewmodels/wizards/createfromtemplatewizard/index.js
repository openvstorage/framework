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

        self.title(generic.tryGet(options, 'title', $.t('ovs:wizards.createft.title')));
        self.modal(generic.tryGet(options, 'modal', false));

        self.data.guid(options.pmachineguid);
        self.steps([new Gather()]);

        self.activateStep();

        self.compositionComplete = function() {
            var i, fields = ['amount', 'startnr'], element;
            for (i = 0; i < fields.length; i += 1) {
                element = $("#" + fields[i]);
                element.on('keypress', function(e) {
                    // Guard keypresses, only accepting numeric values
                    return !(e.which !== 8 && e.which !== 0 && (e.which < 48 || e.which > 57));
                });
                element.on('change', function() {
                    // Guard ctrl+v
                    element.val(Math.max(1, parseInt('0' + element.val(), 10)));
                });
            }
        };
    };
});
