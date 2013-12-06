// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define(['knockout'], function(ko){
    "use strict";
    var singleton = function() {
        return {
            name:        ko.observable(),
            machineGuid: ko.observable(),
            vm:          ko.observable(),
            snapshot:    ko.observable(),
            list: ko.observableArray([{test: 'test1'}, {test: 'test2'}]),
            element: ko.observable()
        };
    };
    return singleton();
});
