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

        self._clone = function(i, vm, call_data) {
            var current_data = $.extend({}, call_data);
            current_data.name = self.data.amount() === 1 ? self.data.name() : self.data.name() + '-' + i;
            return $.Deferred(function(deferred) {
                api.post('vmachines/' + vm.guid() + '/clone', current_data)
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        deferred.resolve(true);
                    })
                    .fail(function(error) {
                        generic.alertError('Error', 'Error while cloning ' + vm.name() + ': ' + error);
                        deferred.resolve(false);
                    });
            }).promise();
        };

        self.finish = function() {
            return $.Deferred(function(deferred) {
                var i, clones = [],
                    call_data = {},
                    vm = self.data.vm(),
                    disks = vm.vDisks();
                call_data.disks = {};
                for (i = 0; i < disks.length; i += 1) {
                    call_data.disks[disks[i].guid()] = disks[i].selectedSnapshot();
                }
                for (i = 1; i <= self.data.amount(); i += 1) {
                    clones.push(self._clone(i, vm, call_data));
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