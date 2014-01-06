// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define(['knockout'], function(ko){
    "use strict";
    var singleton = function() {
        return {
            name:         ko.observable(),
            machineGuid:  ko.observable(),
            vm:           ko.observable(),
            isConsistent: ko.observable(false)
        };
    };
    return singleton();
});
