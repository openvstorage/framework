/*global define */
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

        self._clone = function(i, vm, callData) {
            var currentData = $.extend({}, callData);
            currentData.name = self.data.amount() === 1 ? self.data.name() : self.data.name() + '-' + i;
            return $.Deferred(function(deferred) {
                api.post('vmachines/' + vm.guid() + '/clone', currentData)
                    .then(self.shared.tasks.wait)
                    .done(function() {
                        deferred.resolve(true);
                    })
                    .fail(function(error) {
                        generic.alertError(
                            $.t('ovs:generic.error'),
                            $.t('ovs:generic.errorwhile', {
                                context: 'error',
                                what: $.t('ovs:wizards.clone.confirm.cloning', { what: vm.name() }),
                                error: error
                            })
                        );
                        deferred.resolve(false);
                    });
            }).promise();
        };

        self.finish = function() {
            return $.Deferred(function(deferred) {
                var i, clones = [],
                    callData = {},
                    vm = self.data.vm(),
                    disks = vm.vDisks();
                callData.disks = {};
                for (i = 0; i < disks.length; i += 1) {
                    callData.disks[disks[i].guid()] = disks[i].selectedSnapshot();
                }
                for (i = 1; i <= self.data.amount(); i += 1) {
                    clones.push(self._clone(i, vm, callData));
                }
                generic.alertInfo($.t('ovs:wizards.clone.confirm.clonestarted'), $.t('ovs:wizards.clone.confirm.inprogress', { what: vm.name() }));
                deferred.resolve();
                $.when.apply($, clones)
                    .done(function() {
                        var i, args = Array.prototype.slice.call(arguments),
                            success = 0;
                        for (i = 0; i < args.length; i += 1) {
                            success += (args[i] ? 1 : 0);
                        }
                        if (success === args.length) {
                            generic.alertSuccess($.t('ovs:wizards.clone.confirm.complete'), $.t('ovs:wizards.clone.confirm.success', { what: vm.name() }));
                        } else if (success > 0) {
                            generic.alert($.t('ovs:wizards.clone.confirm.complete'), $.t('ovs:wizards.clone.confirm.somefailed', { what: vm.name() }));
                        } else if (clones.length > 2) {
                            generic.alertError($.t('ovs:generic.error'), $.t('ovs:wizards.clone.confirm.allfailed', { what: vm.name() }));
                        }
                    });
            }).promise();
        };
    };
});