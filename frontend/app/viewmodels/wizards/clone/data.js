define(['knockout'], function(ko){
    "use strict";
    var singleton = function() {
        return {
            name:        ko.observable(),
            machineGuid: ko.observable(),
            vm:          ko.observable()
        };
    };
    return singleton();
});