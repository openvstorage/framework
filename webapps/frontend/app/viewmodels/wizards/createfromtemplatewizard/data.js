// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define(['knockout'], function(ko){
    "use strict";
    var singleton = function() {
        return {
            guid:              ko.observable(),
            vm:                ko.observable(),
            amount:            ko.observable(1),
            startnr:           ko.observable(1),
            name:              ko.observable(),
            description:       ko.observable(''),
            selectedPMachines: ko.observableArray([]),
            pMachines:         ko.observableArray([]),
            pMachineGuids:     []
        };
    };
    return singleton();
});
