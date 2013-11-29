// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define(['knockout'], function(ko){
    "use strict";
    var singleton = function() {
        var data = {
            name:        ko.observable(),
            machineGuid: ko.observable(),
            vm:          ko.observable(),
            snapshot:    ko.observable(),
            amount:      ko.observable(1)
        };
        data.snapshotDisplay = ko.computed(function() {
            var date;
            if (data.snapshot()) {
                if (!!data.snapshot().label) {
                    return data.snapshot().label + (data.snapshot().is_consistent ? ' (consistent)' : '');
                }
                date = new Date(data.snapshot().timestamp * 1000);
                return date.toLocaleDateString() + ' ' + date.toLocaleTimeString() + (data.snapshot().is_consistent ? ' (consistent)' : '');
            }
            return '';
        });
        return data;
    };
    return singleton();
});
