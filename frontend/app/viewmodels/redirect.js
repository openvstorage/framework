define(function () {
    "use strict";
    return {
        canActivate: function () {
            return { redirect: '#full/' };
        }
    };
});