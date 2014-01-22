// license see http://www.openvstorage.com/licenses/opensource/
/*global define */
define(['jquery', 'ovs/generic'], function($, generic){
    "use strict";
    var mainRoutes, siteRoutes, buildSiteRoutes, loadHash;

    mainRoutes = [
       { route: '',              moduleId: 'viewmodels/redirect', nav: false },
       { route: ':mode*details', moduleId: 'viewmodels/index',    nav: false }
    ];
    siteRoutes = [
        { route: '',            moduleId: 'dashboard',    title: $.t('ovs:dashboard.title'),     titlecode: 'ovs:dashboard.title',     nav: false },
        { route: 'vsas',        moduleId: 'vsas',         title: $.t('ovs:vsas.title'),          titlecode: 'ovs:vsas.title',          nav: true  },
        { route: 'vsa/:guid',   moduleId: 'vsa-detail',   title: $.t('ovs:vsas.detail.title'),   titlecode: 'ovs:vsas.detail.title',   nav: false },
        { route: 'vpools',      moduleId: 'vpools',       title: $.t('ovs:vpools.title'),        titlecode: 'ovs:vpools.title',        nav: true  },
        { route: 'vpool/:guid', moduleId: 'vpool-detail', title: $.t('ovs:vpools.detail.title'), titlecode: 'ovs:vpools.detail.title', nav: false },
        { route: 'vmachines',   moduleId: 'vmachines',    title: $.t('ovs:vmachines.title'),     titlecode: 'ovs:vmachines.title',     nav: true  },
        { route: 'vdisks',      moduleId: 'vdisks',       title: $.t('ovs:vdisks.title'),        titlecode: 'ovs:vdisks.title',        nav: true  },
        { route: 'vtemplates',  moduleId: 'vtemplates',   title: $.t('ovs:vtemplates.title'),    titlecode: 'ovs:vtemplates.title',    nav: true  },
        { route: 'statistics',  moduleId: 'statistics',   title: $.t('ovs:statistics.title'),    titlecode: 'ovs:statistics.title',    nav: false },
        { route: 'login',       moduleId: 'login',        title: $.t('ovs:login.title'),         titlecode: 'ovs:login.title',         nav: false }
    ];

    buildSiteRoutes = function(mode) {
        var i;
        for (i = 0; i < siteRoutes.length; i += 1) {
            siteRoutes[i].hash = '#' + mode + '/' + siteRoutes[i].route;
        }
    };
    loadHash = function(module, params) {
        var i, item, hash;
        params = params || {};
        module = '/' + module;
        for (i = 0; i < siteRoutes.length; i += 1) {
            if (siteRoutes[i].moduleId.indexOf(module, siteRoutes[i].moduleId.length - module.length) !== -1) {
                hash = siteRoutes[i].hash;
                for (item in params) {
                    if (params.hasOwnProperty(item)) {
                        hash = hash.replace(':' + item, params[item].call ? params[item]() : params[item]);
                    }
                }
                return hash;
            }
        }
        return '/';
    };

    return {
        mainRoutes     : mainRoutes,
        siteRoutes     : siteRoutes,
        buildSiteRoutes: buildSiteRoutes,
        loadHash       : loadHash
    };
});
