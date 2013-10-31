define([
    'jquery', 'knockout',
    './data', 'ovs/shared', 'ovs/generic', 'ovs/api'
], function($, ko, data, shared, generic, api) {
    "use strict";
    return function() {
        var self = this;

        self.shared = shared;
        self.data   = data;

        self.canContinue = ko.observable({value: true, reason: undefined});

        self.finish = function() {
            return $.Deferred(function(deferred) {
                var i, clones = [],
                    data = {},
                    vm = self.data.vm(),
                    disks = vm.vDisks();
                data.disks = {};
                for (i = 0; i < disks.length; i += 1) {
                    data.disks[disks[i].guid()] = disks[i].selectedSnapshot();
                }
                for (i = 1; i <= self.data.amount(); i += 1) {
                    (function() {
                        var current_data = $.extend({}, data);
                        current_data.name = self.data.amount() === 1 ? self.data.name() : self.data.name() + '-' + i;
                        clones.push($.Deferred(function(entry_deferred) {
                            api.post('vmachines/' + vm.guid() + '/clone', current_data)
                                .then(self.shared.tasks.wait)
                                .done(function() {
                                    entry_deferred.resolve(true);
                                })
                                .fail(function(error) {
                                    generic.alertError('Error', 'Error while cloning ' + vm.name() + ': ' + error);
                                    entry_deferred.resolve(false);
                                });
                        }).promise());
                    }());
                }
                generic.alertInfo('Clone started', 'Machine ' + vm.name() + ' cloning in progress...');
                deferred.resolve();
                $.when.apply($, clones)
                    .done(function() {
                        var i, args = Array.prototype.slice.call(arguments),
                            success = 0;
                        for (i = 0; i < args.length; i += 1) {
                            success += (args[i] ? 1 : 0);
                        }
                        if (success === args.length) {
                            generic.alertSuccess('Clones completed', 'Machine ' + vm.name() + ' cloned successfully.');
                        } else if (success > 0) {
                            generic.alert('Clones complete', 'Machine ' + vm.name() + ' cloned. However, some of the clones could not be created.');
                        } else if (clones.length > 2) {
                            generic.alertError('Error', 'All clones for machine ' + vm.name() + ' failed.');
                        }
                    });
            }).promise();
        };
    };
});